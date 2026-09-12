use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, HOST, USER_AGENT};
use http::{Method, Request, StatusCode, Uri};
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};
use url::Url;

use crate::body::ReqBody;
use crate::connector::Connector;
use crate::cookies::SharedJar;
use crate::decompress;
use crate::error::{Error, Result};
use crate::into_url::IntoUrl;
use crate::proxy::{NoProxy, Proxy};
use crate::redirect::Policy;
use crate::request::RequestBuilder;
use crate::resolver::{GlobalResolver, SharedResolver};
use crate::response::Response;

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    hyper: HyperClient<Connector, BoxBody<Bytes, Error>>,
    proxy: Option<Arc<Proxy>>,
    no_proxy: Option<Arc<NoProxy>>,
    user_agent: Option<HeaderValue>,
    default_headers: HeaderMap,
    redirect: Policy,
    timeout: Option<Duration>,
    auto_gzip: bool,
    auto_brotli: bool,
    auto_deflate: bool,
    cookie_jar: Option<SharedJar>,
    accepts: HeaderValue,
}

impl Client {
    pub fn new() -> Self {
        ClientBuilder::new().build().expect("default client build")
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub fn get<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::GET, url)
    }

    pub fn post<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::POST, url)
    }

    pub fn put<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::PUT, url)
    }

    pub fn delete<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::DELETE, url)
    }

    pub fn head<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::HEAD, url)
    }

    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        RequestBuilder::new(self.clone(), method, url.into_url())
    }

    pub fn jar_cookies(&self, url: &Url) -> Option<HeaderValue> {
        self.inner.cookie_jar.as_ref()?.cookies(url)
    }

    pub(crate) async fn execute(&self, mut rb: RequestBuilder) -> Result<Response> {
        let url = rb.url?;
        let timeout = rb.timeout.or(self.inner.timeout);

        let mut headers = HeaderMap::new();
        for (k, v) in self.inner.default_headers.iter() {
            headers.append(k.clone(), v.clone());
        }
        if let Some(ua) = &self.inner.user_agent {
            if !rb.headers.contains_key(USER_AGENT) {
                headers.insert(USER_AGENT, ua.clone());
            }
        }
        // `drain` yields the name only on a group's first entry (`None` after belongs to it): track last name so multi-valued request headers survive and drop defaults the request overrides
        let mut last_name: Option<http::HeaderName> = None;
        let mut overridden: std::collections::HashSet<http::HeaderName> =
            std::collections::HashSet::new();
        for (k, v) in rb.headers.drain() {
            let name = match k {
                Some(n) => {
                    last_name = Some(n.clone());
                    n
                }
                None => match last_name.clone() {
                    Some(n) => n,
                    None => continue,
                },
            };
            if overridden.insert(name.clone()) {
                headers.remove(&name);
            }
            headers.append(name, v);
        }

        let body = std::mem::replace(&mut rb.body, ReqBody::Empty);
        let fut = self.send_with_redirects(rb.method, url, headers, body);
        match timeout {
            Some(d) => tokio::time::timeout(d, fut)
                .await
                .map_err(|_| Error::Timeout)?,
            None => fut.await,
        }
    }

    async fn send_with_redirects(
        &self,
        method: Method,
        mut url: Url,
        mut headers: HeaderMap,
        mut body: ReqBody,
    ) -> Result<Response> {
        let mut method = method;
        let mut redirects = 0usize;

        loop {
            let resp = self
                .send_once(&method, &url, &headers, body.clone())
                .await?;
            let status = resp.status();
            if !is_redirect_status(status) {
                return Ok(resp);
            }
            redirects += 1;
            if redirects > self.inner.redirect.max {
                return Err(Error::Redirect(format!(
                    "exceeded {} redirects",
                    self.inner.redirect.max
                )));
            }

            let location = resp
                .headers()
                .get(http::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| Error::Redirect("missing Location header".into()))?
                .to_string();
            let next = url
                .join(&location)
                .map_err(|e| Error::Redirect(e.to_string()))?;

            // RFC 7231 §6.4: 301/302/303 may downgrade method to GET, 307/308 preserve method and body
            if matches!(
                status,
                StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND | StatusCode::SEE_OTHER
            ) && method != Method::GET
                && method != Method::HEAD
            {
                method = Method::GET;
                body = ReqBody::Empty;
                headers.remove(CONTENT_LENGTH);
                headers.remove(http::header::CONTENT_TYPE);
                // Strip caller-supplied body-framing headers too. Forwarding `Transfer-Encoding: chunked` together with `ReqBody::Empty` would produce a malformed request the upstream may reject or interpret ambiguously
                headers.remove(http::header::TRANSFER_ENCODING);
            }

            // Drop sensitive headers on cross-origin redirect
            if !same_origin(&url, &next) {
                headers.remove(http::header::AUTHORIZATION);
                headers.remove(http::header::COOKIE);
                // Always strip an explicit Host so `send_once` can regenerate it from the new origin. Otherwise an explicit Host (set by the caller or via `default_headers`) would leak the old origin to the redirect target
                headers.remove(HOST);
            }

            resp.drain().await?;
            url = next;
        }
    }

    async fn send_once(
        &self,
        method: &Method,
        url: &Url,
        headers: &HeaderMap,
        body: ReqBody,
    ) -> Result<Response> {
        let uri: Uri = url
            .as_str()
            .parse()
            .map_err(|e: http::uri::InvalidUri| Error::Url(e.to_string()))?;

        let mut builder = Request::builder().method(method.clone()).uri(uri);
        let req_headers = builder
            .headers_mut()
            .ok_or_else(|| Error::Builder("no headers".into()))?;
        // `HeaderMap::iter` yields the header name on every entry (unlike `drain`, which only yields it on the first entry of a group), so appending each (k, v) is enough to preserve multi-valued headers
        for (k, v) in headers.iter() {
            req_headers.append(k.clone(), v.clone());
        }
        // Hyper requires a Host header; fill from URL if absent
        if !req_headers.contains_key(HOST) {
            if let Some(host) = url.host_str() {
                let value = if let Some(port) = url.port() {
                    format!("{host}:{port}")
                } else {
                    host.to_string()
                };
                if let Ok(v) = HeaderValue::from_str(&value) {
                    req_headers.insert(HOST, v);
                }
            }
        }

        if let Some(proxy) = &self.inner.proxy {
            let bypassed = self
                .inner
                .no_proxy
                .as_deref()
                .is_some_and(|matcher| matcher.matches_url(url));
            if url.scheme() != "https"
                && !bypassed
                && !req_headers.contains_key(http::header::PROXY_AUTHORIZATION)
            {
                if let Some(auth) = proxy.http_basic_authorization() {
                    if let Ok(value) = HeaderValue::from_str(&auth) {
                        req_headers.insert(http::header::PROXY_AUTHORIZATION, value);
                    }
                }
            }
        }
        // Auto Accept-Encoding when the user enabled compression and didn't override it themselves. Skip when no codec is enabled so we don't emit an empty header (which servers may treat as ambiguous)
        if !self.inner.accepts.is_empty()
            && !req_headers.contains_key(http::header::ACCEPT_ENCODING)
        {
            req_headers.insert(http::header::ACCEPT_ENCODING, self.inner.accepts.clone());
        }
        // Cookie jar injection. Preserve any caller-provided Cookie header (matching reqwest's behaviour) so manual overrides win over the jar and we never silently clobber the user's value
        if let Some(jar) = &self.inner.cookie_jar {
            if !req_headers.contains_key(http::header::COOKIE) {
                if let Some(cookie) = jar.cookies(url) {
                    req_headers.insert(http::header::COOKIE, cookie);
                }
            }
        }

        // For streaming bodies with a known length, advertise Content-Length when the caller didn't already set framing headers. Without either Content-Length or Transfer-Encoding hyper would buffer the entire body to compute one — defeating streaming. Bytes/Empty bodies are sized automatically by hyper from the body's `size_hint`
        if matches!(body, ReqBody::Stream { .. }) {
            let has_length = req_headers.contains_key(CONTENT_LENGTH);
            // RFC 9112: a request that already carries Transfer-Encoding *must not* also advertise Content-Length, regardless of which codings appear (the final coding is implicitly chunked). Detect both an explicit "chunked" token and the broader "any TE header present at all" case so we never inject a conflicting Content-Length on top of the caller's framing
            let has_chunked = req_headers
                .get_all(http::header::TRANSFER_ENCODING)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .flat_map(|s| s.split(','))
                .any(|tok| tok.trim().eq_ignore_ascii_case("chunked"));
            let has_te = req_headers.contains_key(http::header::TRANSFER_ENCODING);
            if !has_length && !has_chunked && !has_te {
                if let Some(len) = body.content_length() {
                    if let Ok(v) = HeaderValue::from_str(&len.to_string()) {
                        req_headers.insert(CONTENT_LENGTH, v);
                    }
                } else {
                    // Unknown length — let hyper send chunked
                    req_headers.insert(
                        http::header::TRANSFER_ENCODING,
                        HeaderValue::from_static("chunked"),
                    );
                }
            }
        }

        let req = builder
            .body(body.into_hyper_body())
            .map_err(|e| Error::Builder(e.to_string()))?;

        let resp = self.inner.hyper.request(req).await?;

        let (parts, body) = resp.into_parts();

        // Capture Set-Cookie before we move headers into Response
        if let Some(jar) = &self.inner.cookie_jar {
            let mut iter = parts.headers.get_all(http::header::SET_COOKIE).into_iter();
            jar.set_cookies(&mut iter, url);
        }

        let encoding = parts
            .headers
            .get(http::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let should_decode = match encoding.as_deref() {
            Some("gzip" | "x-gzip") => self.inner.auto_gzip,
            Some("br") => self.inner.auto_brotli,
            Some("deflate") => self.inner.auto_deflate,
            _ => false,
        };

        let resp_body = if should_decode {
            let boxed: crate::body::RespBody = body.map_err(Error::from).boxed();
            decompress::maybe_decompress(boxed, encoding.as_deref())
        } else {
            body.map_err(Error::from).boxed()
        };

        // Strip Content-Length / Content-Encoding when we decoded so callers don't trust stale lengths
        let mut response_headers = parts.headers;
        if should_decode {
            response_headers.remove(CONTENT_LENGTH);
            response_headers.remove(http::header::CONTENT_ENCODING);
        }

        Ok(Response::new(
            parts.status,
            parts.version,
            response_headers,
            url.clone(),
            resp_body,
        ))
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

fn is_redirect_status(s: StatusCode) -> bool {
    matches!(
        s,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
}

// --- Builder --

pub struct ClientBuilder {
    user_agent: Option<HeaderValue>,
    default_headers: HeaderMap,
    redirect: Policy,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    pool_idle_timeout: Option<Duration>,
    pool_max_idle_per_host: Option<usize>,
    tcp_nodelay: bool,
    tcp_keepalive: Option<Duration>,
    auto_gzip: bool,
    auto_brotli: bool,
    auto_deflate: bool,
    danger_accept_invalid_certs: bool,
    proxy: Option<Proxy>,
    no_proxy: Option<NoProxy>,
    cookie_jar: Option<SharedJar>,
    resolver: Option<SharedResolver>,
    extra_root_certs: Vec<Vec<u8>>,
    local_addr: Option<SocketAddr>,
    direct_address_filter: Option<Arc<dyn Fn(IpAddr) -> bool + Send + Sync>>,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            user_agent: None,
            default_headers: HeaderMap::new(),
            redirect: Policy::default(),
            timeout: None,
            connect_timeout: None,
            pool_idle_timeout: Some(Duration::from_secs(90)),
            pool_max_idle_per_host: None,
            tcp_nodelay: true,
            tcp_keepalive: None,
            auto_gzip: false,
            auto_brotli: false,
            auto_deflate: false,
            danger_accept_invalid_certs: false,
            proxy: None,
            no_proxy: None,
            cookie_jar: None,
            resolver: None,
            extra_root_certs: Vec::new(),
            local_addr: None,
            direct_address_filter: None,
        }
    }

    pub fn user_agent<V: TryInto<HeaderValue>>(mut self, ua: V) -> Self {
        self.user_agent = ua.try_into().ok();
        self
    }

    pub fn default_headers(mut self, headers: HeaderMap) -> Self {
        self.default_headers = headers;
        self
    }

    pub fn redirect(mut self, policy: Policy) -> Self {
        self.redirect = policy;
        self
    }

    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    pub fn connect_timeout(mut self, t: Duration) -> Self {
        self.connect_timeout = Some(t);
        self
    }

    pub fn pool_idle_timeout(mut self, t: Duration) -> Self {
        self.pool_idle_timeout = Some(t);
        self
    }

    pub fn pool_max_idle_per_host(mut self, n: usize) -> Self {
        self.pool_max_idle_per_host = Some(n);
        self
    }

    pub fn tcp_nodelay(mut self, b: bool) -> Self {
        self.tcp_nodelay = b;
        self
    }

    pub fn tcp_keepalive<D: Into<Option<Duration>>>(mut self, d: D) -> Self {
        self.tcp_keepalive = d.into();
        self
    }

    pub fn gzip(mut self, b: bool) -> Self {
        self.auto_gzip = b;
        self
    }
    pub fn brotli(mut self, b: bool) -> Self {
        self.auto_brotli = b;
        self
    }
    pub fn deflate(mut self, b: bool) -> Self {
        self.auto_deflate = b;
        self
    }

    pub fn danger_accept_invalid_certs(mut self, b: bool) -> Self {
        self.danger_accept_invalid_certs = b;
        self
    }

    pub fn proxy(mut self, p: Proxy) -> Self {
        if self.no_proxy.is_none() {
            self.no_proxy = p.no_proxy.as_deref().cloned();
        }
        self.proxy = Some(p);
        self
    }

    pub fn no_proxy(mut self, no_proxy: NoProxy) -> Self {
        self.no_proxy = Some(no_proxy);
        self
    }

    pub fn no_proxy_str(mut self, value: impl AsRef<str>) -> Self {
        self.no_proxy = Some(NoProxy::parse(value));
        self
    }

    pub fn cookie_provider(mut self, jar: Arc<dyn crate::cookies::CookieStore>) -> Self {
        self.cookie_jar = Some(jar);
        self
    }

    pub fn resolver_arc(mut self, r: SharedResolver) -> Self {
        self.resolver = Some(r);
        self
    }

    pub fn local_address(mut self, address: IpAddr) -> Self {
        self.local_addr = Some(SocketAddr::new(address, 0));
        self
    }

    pub fn local_socket_address(mut self, address: SocketAddr) -> Self {
        self.local_addr = Some(address);
        self
    }

    /// Filter resolved addresses used for direct destination connections
    pub fn direct_address_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(IpAddr) -> bool + Send + Sync + 'static,
    {
        self.direct_address_filter = Some(Arc::new(filter));
        self
    }

    pub fn add_root_certificate(mut self, der: impl Into<Vec<u8>>) -> Self {
        self.extra_root_certs.push(der.into());
        self
    }

    pub fn build(self) -> Result<Client> {
        let tls = build_tls(self.danger_accept_invalid_certs, &self.extra_root_certs)?;

        let no_proxy_matcher = self
            .no_proxy
            .or_else(|| {
                self.proxy
                    .as_ref()
                    .and_then(|p| p.no_proxy.as_deref().cloned())
            })
            .unwrap_or_default();
        let has_proxy = self.proxy.is_some();
        let proxy = self.proxy.map(Arc::new);
        let no_proxy = has_proxy.then(|| Arc::new(no_proxy_matcher));
        let connector = Connector {
            tls: Arc::new(tls),
            resolver: self.resolver.unwrap_or_else(|| Arc::new(GlobalResolver)),
            proxy: proxy.clone(),
            no_proxy: no_proxy.clone(),
            connect_timeout: self.connect_timeout,
            tcp_nodelay: self.tcp_nodelay,
            tcp_keepalive: self.tcp_keepalive,
            local_addr: self.local_addr,
            direct_address_filter: self.direct_address_filter,
        };

        let mut hyper_builder = HyperClient::builder(TokioExecutor::new());
        if let Some(d) = self.pool_idle_timeout {
            hyper_builder.pool_idle_timeout(d);
        }
        if let Some(n) = self.pool_max_idle_per_host {
            hyper_builder.pool_max_idle_per_host(n);
        }
        let hyper = hyper_builder.build::<Connector, BoxBody<Bytes, Error>>(connector);

        let mut accepts: Vec<&str> = Vec::new();
        if self.auto_gzip {
            accepts.push("gzip");
        }
        if self.auto_brotli {
            accepts.push("br");
        }
        if self.auto_deflate {
            accepts.push("deflate");
        }
        let accepts = HeaderValue::from_str(&accepts.join(", "))
            .unwrap_or_else(|_| HeaderValue::from_static(""));

        let inner = ClientInner {
            hyper,
            proxy,
            no_proxy,
            user_agent: self.user_agent,
            default_headers: self.default_headers,
            redirect: self.redirect,
            timeout: self.timeout,
            auto_gzip: self.auto_gzip,
            auto_brotli: self.auto_brotli,
            auto_deflate: self.auto_deflate,
            cookie_jar: self.cookie_jar,
            accepts,
        };

        Ok(Client {
            inner: Arc::new(inner),
        })
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn build_tls(danger_accept_invalid: bool, extra: &[Vec<u8>]) -> Result<ClientConfig> {
    static INSTALL_PROVIDER: std::sync::Once = std::sync::Once::new();
    INSTALL_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });

    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for der in extra {
        let cert = CertificateDer::from(der.clone());
        roots
            .add(cert)
            .map_err(|e| Error::Tls(format!("invalid extra root: {e}")))?;
    }

    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    if danger_accept_invalid {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertVerifier));
    }

    Ok(config)
}

#[derive(Debug)]
struct NoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA256,
            RSA_PKCS1_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP256_SHA256,
            ECDSA_NISTP384_SHA384,
            ED25519,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    struct RoutingResolver(HashMap<String, SocketAddr>);

    impl crate::resolver::Resolve for RoutingResolver {
        fn resolve(&self, host: &str) -> crate::resolver::Resolving {
            let address = self
                .0
                .get(host)
                .copied()
                .unwrap_or_else(|| "127.0.0.1:1".parse().unwrap());
            Box::pin(
                async move { Ok(Box::new(std::iter::once(address)) as crate::resolver::Addrs) },
            )
        }
    }

    #[test]
    fn build_tls_advertises_h2_then_http11_alpn() {
        let config = build_tls(false, &[]).unwrap();
        assert_eq!(
            config.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }

    #[tokio::test]
    async fn direct_address_filter_rejects_resolved_destination() {
        let client = Client::builder()
            .resolver_arc(Arc::new(RoutingResolver(HashMap::from([(
                "blocked.example".to_string(),
                "127.0.0.1:0".parse().unwrap(),
            )]))))
            .direct_address_filter(|ip| !ip.is_loopback())
            .build()
            .unwrap();

        let error = client.get("http://blocked.example/").send().await.unwrap_err();
        assert!(error.to_string().contains("Connect"), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn plain_http_proxy_uses_absolute_form_and_authentication() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0, "proxy client closed before finishing headers");
                request.extend_from_slice(&chunk[..read]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET http://example.test/file?part=1 HTTP/1.1\r\n"));
            assert!(
                request.contains("proxy-authorization: Basic dXNlcjpwYXNz\r\n"),
                "unexpected proxy request: {request:?}"
            );
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });

        let client = Client::builder()
            .proxy(Proxy::all(format!("http://user:pass@{address}")).unwrap())
            .build()
            .unwrap();
        assert_eq!(
            client
                .get("http://example.test/file?part=1")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "ok"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn direct_client_binds_configured_local_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, peer) = listener.accept().await.unwrap();
            assert_eq!(
                peer.ip(),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            );
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });

        let client = Client::builder()
            .local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
            .build()
            .unwrap();
        assert_eq!(
            client
                .get(format!("http://{address}/announce"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "ok"
        );
        server.await.unwrap();
    }

    #[test]
    fn local_socket_address_preserves_the_configured_port() {
        let address: SocketAddr = "127.0.0.1:43123".parse().unwrap();
        let builder = Client::builder().local_socket_address(address);

        assert_eq!(builder.local_addr, Some(address));
    }

    #[tokio::test]
    async fn proxy_connection_does_not_inherit_direct_source_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, peer) = listener.accept().await.unwrap();
            assert!(peer.is_ipv4());
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });

        let client = Client::builder()
            .proxy(Proxy::all(format!("http://{address}")).unwrap())
            // A v6 source cannot connect to this v4 proxy. The request must
            // still succeed because only direct destination routes are bound.
            .local_address(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST))
            .build()
            .unwrap();
        assert_eq!(
            client
                .get("http://tracker.example/announce")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "ok"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn redirect_recomputes_no_proxy_for_the_new_host() {
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_address = proxy_listener.local_addr().unwrap();
        let direct_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let direct_address = direct_listener.local_addr().unwrap();
        let direct_port = direct_address.port();

        let proxy = tokio::spawn(async move {
            let (mut socket, _) = proxy_listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET http://start.example/ HTTP/1.1\r\n"));
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://bypass.example:{direct_port}/end\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let direct = tokio::spawn(async move {
            let (mut socket, _) = direct_listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /end HTTP/1.1\r\n"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\ndirect",
                )
                .await
                .unwrap();
        });

        let resolver = RoutingResolver(HashMap::from([
            ("proxy.example".to_string(), proxy_address),
            ("bypass.example".to_string(), direct_address),
        ]));
        let client = Client::builder()
            .proxy(Proxy::all(format!("http://proxy.example:{}", proxy_address.port())).unwrap())
            .no_proxy(NoProxy::parse("bypass.example"))
            .resolver_arc(Arc::new(resolver))
            .build()
            .unwrap();
        let response = client.get("http://start.example/").send().await.unwrap();
        assert_eq!(response.text().await.unwrap(), "direct");
        proxy.await.unwrap();
        direct.await.unwrap();
    }
}
