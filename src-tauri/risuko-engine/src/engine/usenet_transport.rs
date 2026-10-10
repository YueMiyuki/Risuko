use crate::engine::usenet::{UsenetCredentials, UsenetProviderProfile};
use parking_lot::Mutex;
use risuko_bt::limiter::Throttle;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_LINE: usize = 64 * 1024;
const MAX_ARTICLE_BYTES: usize = 16 * 1024 * 1024;
const MAX_MULTILINE_BYTES: usize = 256 * 1024 * 1024;
const MAX_MULTILINE_LINES: usize = MAX_MULTILINE_BYTES / MAX_LINE;
const READ_BUFFER_BYTES: usize = 64 * 1024;
const HEALTH_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverClass {
    Retryable,
    Authentication,
    Permanent,
}

#[derive(Debug, thiserror::Error)]
pub enum NntpError {
    #[error("NNTP profile is invalid: {0}")]
    InvalidProfile(String),
    #[error("plain NNTP is not explicitly enabled for this profile")]
    PlainNotAllowed,
    #[error("NNTP connection timed out")]
    Timeout,
    #[error("Download cancelled")]
    Cancelled,
    #[error("NNTP connection capacity is currently unavailable")]
    CapacityUnavailable,
    #[error("NNTP I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("NNTP TLS error: {0}")]
    Tls(String),
    #[error("NNTP protocol error ({code}): {message}")]
    Protocol { code: u16, message: String },
    #[error("NNTP service unavailable ({code}): {message}")]
    ServiceUnavailable { code: u16, message: String },
    #[error("NNTP authentication failed ({code}): {message}")]
    AuthenticationFailed { code: u16, message: String },
    #[error("NNTP article is unavailable ({code}): {message}")]
    ArticleUnavailable { code: u16, message: String },
    #[error("NNTP article has invalid yEnc data: {message}")]
    ArticleCorrupt { message: String },
    #[error("NNTP response line exceeds the {MAX_LINE} byte limit")]
    ResponseTooLong,
    #[error("NNTP article exceeds the {MAX_ARTICLE_BYTES} byte limit")]
    ArticleTooLarge,
}

impl NntpError {
    pub fn failover_class(&self) -> FailoverClass {
        match self {
            Self::Timeout
            | Self::Io(_)
            | Self::Tls(_)
            | Self::ArticleUnavailable { .. }
            | Self::ArticleCorrupt { .. } => FailoverClass::Retryable,
            Self::ServiceUnavailable { .. } | Self::CapacityUnavailable => FailoverClass::Retryable,
            Self::AuthenticationFailed { .. } => FailoverClass::Authentication,
            Self::InvalidProfile(_)
            | Self::PlainNotAllowed
            | Self::Cancelled
            | Self::Protocol { .. }
            | Self::ResponseTooLong
            | Self::ArticleTooLarge => FailoverClass::Permanent,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NntpResponse {
    pub code: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NntpCapabilities {
    pub capabilities: Vec<String>,
}

impl NntpCapabilities {
    pub fn supports(&self, value: &str) -> bool {
        self.capabilities
            .iter()
            .any(|cap| cap.eq_ignore_ascii_case(value))
    }
}

trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncReadWrite for T {}

struct BoxedStream(Box<dyn AsyncReadWrite>);

impl AsyncRead for BoxedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for BoxedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_shutdown(cx)
    }
}

pub struct NntpConnection {
    reader: BufReader<BoxedStream>,
    profile_id: String,
    greeted: bool,
    throttle: Option<Throttle>,
}

const THROTTLE_BATCH_BYTES: usize = 16 * 1024;

impl fmt::Debug for NntpConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NntpConnection")
            .field("profile_id", &self.profile_id)
            .field("greeted", &self.greeted)
            .finish()
    }
}

impl NntpConnection {
    pub fn profile_id(&self) -> &str {
        &self.profile_id
    }

    pub fn set_throttle(&mut self, throttle: Throttle) {
        self.throttle = Some(throttle);
    }

    pub async fn connect(
        profile: &UsenetProviderProfile,
        credentials: Option<UsenetCredentials>,
    ) -> Result<Self, NntpError> {
        Self::connect_with_proxy(profile, credentials, None).await
    }

    pub async fn connect_with_proxy(
        profile: &UsenetProviderProfile,
        credentials: Option<UsenetCredentials>,
        proxy: Option<&risuko_http::ProxyConnector>,
    ) -> Result<Self, NntpError> {
        validate_profile(profile)?;
        if profile.security_mode == "implicit-tls" {
            let mut connection = Self::connect_implicit(profile, proxy).await?;
            if let Some(credentials) = credentials {
                connection.authenticate(credentials).await?;
            }
            return Ok(connection);
        }
        let stream: BoxedStream = match proxy {
            Some(proxy) => BoxedStream(Box::new(
                timeout(
                    CONNECT_TIMEOUT,
                    proxy.connect_tcp(&profile.host, profile.port),
                )
                .await
                .map_err(|_| NntpError::Timeout)?
                .map_err(proxy_connection_error)?,
            )),
            None => BoxedStream(Box::new(
                timeout(
                    CONNECT_TIMEOUT,
                    TcpStream::connect((profile.host.as_str(), profile.port)),
                )
                .await
                .map_err(|_| NntpError::Timeout)?
                .map_err(NntpError::Io)
                .and_then(nodelay)?,
            )),
        };
        let mut connection = Self {
            reader: BufReader::with_capacity(READ_BUFFER_BYTES, stream),
            profile_id: profile.id.clone(),
            greeted: false,
            throttle: None,
        };
        let greeting = connection.read_response().await?;
        if greeting.code != 200 && greeting.code != 201 {
            return Err(response_error(greeting, "server rejected greeting"));
        }
        connection.greeted = true;

        if profile.security_mode == "starttls" {
            let starttls = connection.command("STARTTLS").await?;
            if starttls.code != 382 {
                return Err(response_error(starttls, "STARTTLS was not accepted"));
            }
            connection = connection.upgrade_tls(&profile.host).await?;
        }

        if let Some(credentials) = credentials {
            connection.authenticate(credentials).await?;
        }
        Ok(connection)
    }

    async fn connect_implicit(
        profile: &UsenetProviderProfile,
        proxy: Option<&risuko_http::ProxyConnector>,
    ) -> Result<Self, NntpError> {
        let stream: BoxedStream = match proxy {
            Some(proxy) => BoxedStream(Box::new(
                timeout(
                    CONNECT_TIMEOUT,
                    proxy.connect_tcp(&profile.host, profile.port),
                )
                .await
                .map_err(|_| NntpError::Timeout)?
                .map_err(proxy_connection_error)?,
            )),
            None => BoxedStream(Box::new(
                timeout(
                    CONNECT_TIMEOUT,
                    TcpStream::connect((profile.host.as_str(), profile.port)),
                )
                .await
                .map_err(|_| NntpError::Timeout)?
                .map_err(NntpError::Io)
                .and_then(nodelay)?,
            )),
        };
        let tls = timeout(
            IO_TIMEOUT,
            tls_connector().connect(server_name(&profile.host)?, stream),
        )
        .await
        .map_err(|_| NntpError::Timeout)?
        .map_err(|e| NntpError::Tls(e.to_string()))?;
        let mut connection = Self {
            reader: BufReader::with_capacity(READ_BUFFER_BYTES, BoxedStream(Box::new(tls))),
            profile_id: profile.id.clone(),
            greeted: false,
            throttle: None,
        };
        let greeting = connection.read_response().await?;
        if greeting.code != 200 && greeting.code != 201 {
            return Err(response_error(greeting, "server rejected greeting"));
        }
        connection.greeted = true;
        Ok(connection)
    }

    async fn upgrade_tls(mut self, host: &str) -> Result<Self, NntpError> {
        if !self.reader.buffer().is_empty() {
            return Err(NntpError::Protocol {
                code: 0,
                message: "STARTTLS response left buffered plaintext".into(),
            });
        }
        let stream = self.reader.into_inner();
        let tls = timeout(
            IO_TIMEOUT,
            tls_connector().connect(server_name(host)?, stream.0),
        )
        .await
        .map_err(|_| NntpError::Timeout)?
        .map_err(|e| NntpError::Tls(e.to_string()))?;
        self.reader = BufReader::with_capacity(READ_BUFFER_BYTES, BoxedStream(Box::new(tls)));
        Ok(self)
    }

    async fn authenticate(&mut self, credentials: UsenetCredentials) -> Result<(), NntpError> {
        let username = credentials
            .username
            .filter(|value| !value.is_empty())
            .ok_or_else(|| NntpError::AuthenticationFailed {
                code: 481,
                message: "username is required when credentials are configured".into(),
            })?;
        let user = self.command(&format!("AUTHINFO USER {username}")).await?;
        if user.code == 281 {
            return Ok(());
        }
        if user.code != 381 {
            return Err(authentication_error(user));
        }
        let password = credentials
            .password
            .ok_or_else(|| NntpError::AuthenticationFailed {
                code: 481,
                message: "password is required by the server".into(),
            })?;
        let pass = self.command(&format!("AUTHINFO PASS {password}")).await?;
        if pass.code != 281 {
            return Err(authentication_error(pass));
        }
        Ok(())
    }

    pub async fn capabilities(&mut self) -> Result<NntpCapabilities, NntpError> {
        let response = self.command("CAPABILITIES").await?;
        if response.code != 101 {
            return Err(response_error(response, "CAPABILITIES was not accepted"));
        }
        let lines = self.read_multiline().await?;
        Ok(NntpCapabilities {
            capabilities: lines
                .into_iter()
                .filter_map(|line| line.split_whitespace().next().map(str::to_ascii_uppercase))
                .collect(),
        })
    }

    pub async fn group(&mut self, group: &str) -> Result<NntpResponse, NntpError> {
        if group.trim().is_empty()
            || group
                .chars()
                .any(|character| character.is_ascii_control() || character.is_whitespace())
        {
            return Err(NntpError::Protocol {
                code: 0,
                message: "invalid newsgroup".into(),
            });
        }
        let response = self.command(&format!("GROUP {group}")).await?;
        if response.code != 211 {
            return Err(response_error(response, "GROUP failed"));
        }
        Ok(response)
    }

    pub async fn article(&mut self, message_id: &str) -> Result<Vec<u8>, NntpError> {
        self.send_article_requests(&[message_id.to_string()])
            .await?;
        self.read_article().await
    }

    pub async fn send_article_requests(&mut self, message_ids: &[String]) -> Result<(), NntpError> {
        let mut wire = Vec::new();
        for message_id in message_ids {
            let message_id = canonical_message_id(message_id)?;
            wire.extend_from_slice(format!("ARTICLE {message_id}\r\n").as_bytes());
        }
        let stream = self.reader.get_mut();
        timeout(IO_TIMEOUT, async {
            stream.write_all(&wire).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| NntpError::Timeout)?
        .map_err(NntpError::Io)
    }

    pub async fn read_article(&mut self) -> Result<Vec<u8>, NntpError> {
        let response = self.read_response().await?;
        if response.code != 220 && response.code != 221 && response.code != 222 {
            return Err(article_error(response));
        }
        let mut bytes = Vec::new();
        let throttle = self.throttle.clone();
        let mut unpaid = 0usize;
        let mut partial: Vec<u8> = Vec::new();
        loop {
            if self.reader.buffer().is_empty() {
                timeout(IO_TIMEOUT, self.reader.fill_buf())
                    .await
                    .map_err(|_| NntpError::Timeout)?
                    .map_err(NntpError::Io)?;
            }
            let available = self.reader.buffer();
            if available.is_empty() {
                return Err(NntpError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "NNTP server closed the connection",
                )));
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let take = newline.map_or(available.len(), |index| index + 1);
            unpaid += take;
            let Some(index) = newline else {
                if partial.len().saturating_add(take) > MAX_LINE {
                    return Err(NntpError::ResponseTooLong);
                }
                partial.extend_from_slice(available);
                self.reader.consume(take);
                continue;
            };
            let finished = if partial.is_empty() {
                let done = push_article_line(&mut bytes, &available[..index])?;
                self.reader.consume(take);
                done
            } else {
                if partial.len().saturating_add(index) > MAX_LINE {
                    return Err(NntpError::ResponseTooLong);
                }
                partial.extend_from_slice(&available[..index]);
                self.reader.consume(take);
                let done = push_article_line(&mut bytes, &partial)?;
                partial.clear();
                done
            };
            if finished {
                pay(throttle.as_ref(), unpaid).await;
                break;
            }
            if unpaid >= THROTTLE_BATCH_BYTES {
                pay(throttle.as_ref(), std::mem::take(&mut unpaid)).await;
            }
        }
        Ok(bytes)
    }

    async fn command(&mut self, command: &str) -> Result<NntpResponse, NntpError> {
        if command.contains(['\r', '\n']) {
            return Err(NntpError::Protocol {
                code: 0,
                message: "invalid NNTP command".into(),
            });
        }
        let mut wire = Vec::with_capacity(command.len() + 2);
        wire.extend_from_slice(command.as_bytes());
        wire.extend_from_slice(b"\r\n");
        let stream = self.reader.get_mut();
        timeout(IO_TIMEOUT, async {
            stream.write_all(&wire).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| NntpError::Timeout)?
        .map_err(NntpError::Io)?;
        self.read_response().await
    }

    async fn read_response(&mut self) -> Result<NntpResponse, NntpError> {
        let line = self.read_line().await?;
        let code = line
            .get(..3)
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| NntpError::Protocol {
                code: 0,
                message: "malformed response code".into(),
            })?;
        Ok(NntpResponse { code })
    }

    async fn read_line(&mut self) -> Result<String, NntpError> {
        let mut line = Vec::new();
        self.read_line_into(&mut line).await?;
        while matches!(line.last(), Some(b'\r' | b'\n')) {
            line.pop();
        }
        String::from_utf8(line).map_err(|_| NntpError::Protocol {
            code: 0,
            message: "NNTP response is not UTF-8".into(),
        })
    }

    async fn read_multiline(&mut self) -> Result<Vec<String>, NntpError> {
        let mut lines = Vec::new();
        let mut total_bytes = 0usize;
        loop {
            let line = self.read_line().await?;
            if line == "." {
                break;
            }
            if lines.len() >= MAX_MULTILINE_LINES {
                return Err(NntpError::ArticleTooLarge);
            }
            total_bytes = total_bytes.saturating_add(line.len().saturating_add(1));
            if total_bytes > MAX_MULTILINE_BYTES {
                return Err(NntpError::ArticleTooLarge);
            }
            lines.push(line);
        }
        Ok(lines)
    }

    async fn read_line_into(&mut self, line: &mut Vec<u8>) -> Result<(), NntpError> {
        loop {
            let available = timeout(IO_TIMEOUT, self.reader.fill_buf())
                .await
                .map_err(|_| NntpError::Timeout)?
                .map_err(NntpError::Io)?;
            if available.is_empty() {
                if line.is_empty() {
                    return Err(NntpError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "NNTP server closed the connection",
                    )));
                }
                return Ok(());
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let take = newline.map_or(available.len(), |index| index + 1);
            if line.len().saturating_add(take) > MAX_LINE {
                return Err(NntpError::ResponseTooLong);
            }
            line.extend_from_slice(&available[..take]);
            self.reader.consume(take);
            if newline.is_some() {
                return Ok(());
            }
        }
    }
}

fn push_article_line(bytes: &mut Vec<u8>, line: &[u8]) -> Result<bool, NntpError> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line == b"." {
        return Ok(true);
    }
    let line = line.strip_prefix(b".").unwrap_or(line);
    if bytes.len().saturating_add(line.len()).saturating_add(1) > MAX_ARTICLE_BYTES {
        return Err(NntpError::ArticleTooLarge);
    }
    bytes.extend_from_slice(line);
    bytes.push(b'\n');
    Ok(false)
}

fn nodelay(stream: TcpStream) -> Result<TcpStream, NntpError> {
    stream.set_nodelay(true).map_err(NntpError::Io)?;
    Ok(stream)
}

async fn pay(throttle: Option<&Throttle>, bytes: usize) {
    if let Some(throttle) = throttle {
        throttle.acquire(bytes).await;
    }
}

fn proxy_connection_error(error: risuko_http::Error) -> NntpError {
    match error {
        risuko_http::Error::ProxyAuthentication(message) => {
            NntpError::AuthenticationFailed { code: 0, message }
        }
        error => NntpError::Io(io::Error::other(error.to_string())),
    }
}

fn tls_connector() -> TlsConnector {
    static CONNECTOR: OnceLock<TlsConnector> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| {
            static INSTALL_PROVIDER: std::sync::Once = std::sync::Once::new();
            INSTALL_PROVIDER.call_once(|| {
                let _ = rustls::crypto::ring::default_provider().install_default();
            });
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            TlsConnector::from(Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ))
        })
        .clone()
}

fn server_name(host: &str) -> Result<ServerName<'static>, NntpError> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ServerName::from(ip));
    }
    ServerName::try_from(host.to_owned())
        .map_err(|e| NntpError::Tls(format!("invalid server name: {e}")))
}

pub(crate) fn canonical_message_id(message_id: &str) -> Result<String, NntpError> {
    if message_id.trim().is_empty()
        || message_id
            .chars()
            .any(|character| character.is_ascii_control() || character.is_whitespace())
    {
        return Err(invalid_article_id());
    }

    let has_open = message_id.starts_with('<');
    let has_close = message_id.ends_with('>');
    let inner = match (has_open, has_close) {
        (false, false) => message_id,
        (true, true) => &message_id[1..message_id.len() - 1],
        _ => return Err(invalid_article_id()),
    };
    if inner.is_empty() || inner.contains(['<', '>']) {
        return Err(invalid_article_id());
    }
    Ok(format!("<{inner}>"))
}

fn invalid_article_id() -> NntpError {
    NntpError::Protocol {
        code: 0,
        message: "invalid article id".into(),
    }
}

fn validate_profile(profile: &UsenetProviderProfile) -> Result<(), NntpError> {
    if profile.id.trim().is_empty()
        || profile.host.trim().is_empty()
        || profile.port == 0
        || profile.max_connections == 0
    {
        return Err(NntpError::InvalidProfile(
            "id, host, a non-zero port, and a positive connection limit are required".into(),
        ));
    }
    match profile.security_mode.as_str() {
        "implicit-tls" | "starttls" => {}
        "plain" if profile.allow_plain => {}
        "plain" => return Err(NntpError::PlainNotAllowed),
        _ => {
            return Err(NntpError::InvalidProfile(
                "unsupported security mode".into(),
            ));
        }
    }
    Ok(())
}

fn response_error(response: NntpResponse, context: &str) -> NntpError {
    if matches!(response.code, 480..=483) {
        NntpError::AuthenticationFailed {
            code: response.code,
            message: "server requires or rejected authentication".into(),
        }
    } else if matches!(response.code, 400 | 401 | 502) {
        NntpError::ServiceUnavailable {
            code: response.code,
            message: context.into(),
        }
    } else {
        NntpError::Protocol {
            code: response.code,
            message: context.into(),
        }
    }
}

fn authentication_error(response: NntpResponse) -> NntpError {
    NntpError::AuthenticationFailed {
        code: response.code,
        message: "server rejected credentials".into(),
    }
}

fn article_error(response: NntpResponse) -> NntpError {
    if matches!(response.code, 430 | 423 | 412) {
        NntpError::ArticleUnavailable {
            code: response.code,
            message: "article is not available from this provider".into(),
        }
    } else {
        response_error(response, "ARTICLE failed")
    }
}

#[derive(Debug, Clone, Default)]
struct Health {
    failures: u32,
    unhealthy_until: Option<Instant>,
}

#[derive(Default)]
pub struct ProviderConnectionCapacityRegistry {
    providers: Mutex<HashMap<String, Arc<ProviderConnectionGate>>>,
}

impl ProviderConnectionCapacityRegistry {
    pub fn try_acquire(
        &self,
        profile: &UsenetProviderProfile,
    ) -> Result<Option<ProviderConnectionLease>, NntpError> {
        validate_profile(profile)?;
        let gate = self.gate_for(&profile.id);
        Ok(gate.try_acquire(profile.max_connections as usize))
    }

    pub async fn acquire(
        &self,
        profile: &UsenetProviderProfile,
    ) -> Result<ProviderConnectionLease, NntpError> {
        validate_profile(profile)?;
        let gate = self.gate_for(&profile.id);
        Ok(gate.acquire(profile.max_connections as usize).await)
    }

    fn gate_for(&self, profile_id: &str) -> Arc<ProviderConnectionGate> {
        self.providers
            .lock()
            .entry(profile_id.to_string())
            .or_insert_with(|| Arc::new(ProviderConnectionGate::new()))
            .clone()
    }
}

struct ProviderConnectionGate {
    state: Mutex<ProviderConnectionGateState>,
    changed: watch::Sender<u64>,
}

#[derive(Default)]
struct ProviderConnectionGateState {
    next_ticket: u64,
    waiting: BTreeMap<u64, usize>,
    leased: BTreeMap<u64, usize>,
}

impl ProviderConnectionGate {
    fn new() -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            state: Mutex::new(ProviderConnectionGateState::default()),
            changed,
        }
    }

    async fn acquire(self: &Arc<Self>, limit: usize) -> ProviderConnectionLease {
        let mut waiting = ProviderConnectionWaiter::new(self.clone(), limit);
        let mut changed = self.changed.subscribe();
        loop {
            if let Some(lease) = waiting.try_promote() {
                return lease;
            }
            let _ = changed.changed().await;
        }
    }

    fn try_acquire(self: &Arc<Self>, limit: usize) -> Option<ProviderConnectionLease> {
        let mut waiting = ProviderConnectionWaiter::new(self.clone(), limit);
        waiting.try_promote()
    }

    fn register_waiter(&self, limit: usize) -> u64 {
        let ticket = {
            let mut state = self.state.lock();
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            state.waiting.insert(ticket, limit);
            ticket
        };
        self.signal();
        ticket
    }

    fn try_promote(&self, ticket: u64) -> bool {
        let promoted = {
            let mut state = self.state.lock();
            if state.waiting.first_key_value().map(|(first, _)| *first) != Some(ticket) {
                return false;
            }
            let limit = state
                .waiting
                .values()
                .chain(state.leased.values())
                .copied()
                .min()
                .expect("waiting NNTP capacity ticket has no snapshot limit");
            if state.leased.len() >= limit {
                return false;
            }
            let snapshot_limit = state
                .waiting
                .remove(&ticket)
                .expect("waiting NNTP capacity ticket disappeared");
            state.leased.insert(ticket, snapshot_limit);
            true
        };
        if promoted {
            self.signal();
        }
        promoted
    }

    fn cancel_waiter(&self, ticket: u64) {
        let removed = self.state.lock().waiting.remove(&ticket).is_some();
        if removed {
            self.signal();
        }
    }

    fn release(&self, ticket: u64) {
        let released = self.state.lock().leased.remove(&ticket).is_some();
        if released {
            self.signal();
        }
    }

    fn signal(&self) {
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}

struct ProviderConnectionWaiter {
    gate: Arc<ProviderConnectionGate>,
    ticket: Option<u64>,
}

impl ProviderConnectionWaiter {
    fn new(gate: Arc<ProviderConnectionGate>, limit: usize) -> Self {
        let ticket = gate.register_waiter(limit);
        Self {
            gate,
            ticket: Some(ticket),
        }
    }

    fn try_promote(&mut self) -> Option<ProviderConnectionLease> {
        let ticket = self.ticket?;
        if !self.gate.try_promote(ticket) {
            return None;
        }
        self.ticket = None;
        Some(ProviderConnectionLease {
            gate: self.gate.clone(),
            ticket,
        })
    }
}

impl Drop for ProviderConnectionWaiter {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket {
            self.gate.cancel_waiter(ticket);
        }
    }
}

pub struct ProviderConnectionLease {
    gate: Arc<ProviderConnectionGate>,
    ticket: u64,
}

impl Drop for ProviderConnectionLease {
    fn drop(&mut self) {
        self.gate.release(self.ticket);
    }
}

#[derive(Debug)]
pub struct ProviderPool {
    profiles: Vec<UsenetProviderProfile>,
    health: Mutex<HashMap<String, Health>>,
    cursor: Mutex<usize>,
}

impl ProviderPool {
    pub fn new(profiles: Vec<UsenetProviderProfile>) -> Result<Self, String> {
        let profiles: Vec<_> = profiles
            .into_iter()
            .filter(|profile| profile.enabled && profile.deleted_at.is_none())
            .collect();
        for profile in &profiles {
            crate::engine::usenet::validate_provider_profile(profile)?;
        }
        Ok(Self {
            profiles,
            health: Mutex::new(HashMap::new()),
            cursor: Mutex::new(0),
        })
    }

    #[cfg(test)]
    pub fn profiles(&self) -> &[UsenetProviderProfile] {
        &self.profiles
    }

    pub fn ordered_profiles(&self) -> Vec<UsenetProviderProfile> {
        let now = Instant::now();
        let health = self.health.lock();
        let mut groups: Vec<(i32, Vec<UsenetProviderProfile>)> = Vec::new();
        for profile in self
            .profiles
            .iter()
            .filter(|profile| profile.enabled && profile.deleted_at.is_none())
        {
            if let Some(state) = health.get(&profile.id) {
                if state.unhealthy_until.is_some_and(|until| until > now) {
                    continue;
                }
            }
            if let Some((_, group)) = groups
                .iter_mut()
                .find(|(priority, _)| *priority == profile.priority)
            {
                group.push(profile.clone());
            } else {
                groups.push((profile.priority, vec![profile.clone()]));
            }
        }
        groups.sort_by_key(|(priority, _)| *priority);
        let mut ordered = Vec::new();
        let offset = *self.cursor.lock();
        for (_, mut group) in groups {
            if !group.is_empty() {
                let rotate = offset % group.len();
                group.rotate_left(rotate);
            }
            ordered.extend(group);
        }
        if ordered.is_empty() {
            ordered.extend(
                self.profiles
                    .iter()
                    .filter(|profile| profile.enabled && profile.deleted_at.is_none())
                    .cloned(),
            );
            ordered.sort_by_key(|profile| profile.priority);
        }
        ordered
    }

    pub fn ordered_profiles_with_preference(
        &self,
        preferred_profile_id: Option<&str>,
    ) -> Vec<UsenetProviderProfile> {
        let mut ordered = self.ordered_profiles();
        let Some(preferred_profile_id) = preferred_profile_id else {
            return ordered;
        };
        let Some(preferred_index) = ordered
            .iter()
            .position(|profile| profile.id == preferred_profile_id)
        else {
            return ordered;
        };
        if preferred_index > 0 && ordered[preferred_index].priority == ordered[0].priority {
            ordered.swap(0, preferred_index);
        }
        ordered
    }

    pub fn mark_success(&self, profile_id: &str) {
        self.health.lock().remove(profile_id);
        let mut cursor = self.cursor.lock();
        *cursor = cursor.wrapping_add(1);
    }

    pub fn mark_failure(&self, profile_id: &str, error: &NntpError) {
        if matches!(
            error,
            NntpError::ArticleUnavailable { .. } | NntpError::ArticleCorrupt { .. }
        ) {
            return;
        }
        let mut health = self.health.lock();
        let state = health.entry(profile_id.to_string()).or_default();
        state.failures = state.failures.saturating_add(1);
        if error.failover_class() != FailoverClass::Permanent {
            state.unhealthy_until = Some(Instant::now() + HEALTH_COOLDOWN);
        }
    }

    #[cfg(test)]
    pub async fn run_with_failover<T, F, Fut>(&self, operation: F) -> Result<T, NntpError>
    where
        F: Fn(UsenetProviderProfile) -> Fut,
        Fut: std::future::Future<Output = Result<T, NntpError>>,
    {
        self.run_with_failover_preferring(None, operation)
            .await
            .map(|(_, value)| value)
    }

    pub async fn run_with_failover_preferring<T, F, Fut>(
        &self,
        preferred_profile_id: Option<&str>,
        operation: F,
    ) -> Result<(String, T), NntpError>
    where
        F: Fn(UsenetProviderProfile) -> Fut,
        Fut: std::future::Future<Output = Result<T, NntpError>>,
    {
        let candidates = self.ordered_profiles_with_preference(preferred_profile_id);
        if candidates.is_empty() {
            return Err(NntpError::InvalidProfile(
                "no enabled Usenet providers".into(),
            ));
        }
        let mut last_error = None;
        for profile in candidates {
            let result = operation(profile.clone()).await;
            match result {
                Ok(value) => {
                    self.mark_success(&profile.id);
                    return Ok((profile.id, value));
                }
                Err(error) => {
                    let class = error.failover_class();
                    self.mark_failure(&profile.id, &error);
                    last_error = Some(error);
                    if class == FailoverClass::Permanent {
                        break;
                    }
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| NntpError::InvalidProfile("no provider candidates".into())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn profile(id: &str, priority: i32) -> UsenetProviderProfile {
        UsenetProviderProfile {
            id: id.into(),
            name: id.into(),
            host: "localhost".into(),
            port: 119,
            security_mode: "plain".into(),
            enabled: true,
            priority,
            max_connections: 2,
            allow_plain: true,
            deleted_at: None,
            updated_at: None,
        }
    }

    #[test]
    fn equal_priority_profiles_rotate_and_lower_priority_wins() {
        let pool =
            ProviderPool::new(vec![profile("a", 0), profile("b", 0), profile("c", 1)]).unwrap();
        assert_eq!(pool.ordered_profiles()[0].id, "a");
        pool.mark_success("a");
        assert_eq!(pool.ordered_profiles()[0].id, "b");
    }

    #[test]
    fn inactive_profiles_are_ignored_before_validation() {
        let mut disabled = profile("disabled", 0);
        disabled.enabled = false;
        disabled.host.clear();
        disabled.max_connections = 0;

        let mut tombstone = profile("tombstone", 0);
        tombstone.deleted_at = Some(1);
        tombstone.host.clear();
        tombstone.max_connections = 0;

        let pool = ProviderPool::new(vec![disabled, tombstone, profile("active", 0)]).unwrap();
        assert_eq!(
            pool.profiles()
                .iter()
                .map(|profile| profile.id.as_str())
                .collect::<Vec<_>>(),
            ["active"]
        );
    }

    #[test]
    fn enabled_invalid_profiles_are_still_rejected() {
        let mut invalid = profile("invalid", 0);
        invalid.host.clear();
        assert!(ProviderPool::new(vec![invalid]).is_err());
    }

    #[test]
    fn unhealthy_profiles_are_skipped() {
        let pool = ProviderPool::new(vec![profile("a", 0), profile("b", 0)]).unwrap();
        pool.mark_failure(
            "a",
            &NntpError::Io(io::Error::new(io::ErrorKind::ConnectionRefused, "down")),
        );
        assert_eq!(pool.ordered_profiles()[0].id, "b");
    }

    #[test]
    fn unavailable_article_does_not_cool_down_a_healthy_provider() {
        let pool = ProviderPool::new(vec![profile("a", 0), profile("b", 1)]).unwrap();

        pool.mark_failure(
            "a",
            &NntpError::ArticleUnavailable {
                code: 430,
                message: "expired".into(),
            },
        );

        assert_eq!(pool.ordered_profiles()[0].id, "a");
    }

    #[tokio::test]
    async fn capacity_registry_honors_overlapping_profile_snapshots() {
        let registry = Arc::new(ProviderConnectionCapacityRegistry::default());
        let high_limit = profile("shared", 0);
        let mut low_limit = high_limit.clone();
        low_limit.max_connections = 1;

        let first = registry.acquire(&high_limit).await.unwrap();
        let registry_for_waiter = registry.clone();
        let mut lower_waiter =
            tokio::spawn(async move { registry_for_waiter.acquire(&low_limit).await.unwrap() });
        assert!(
            timeout(Duration::from_millis(100), &mut lower_waiter)
                .await
                .is_err(),
            "a lower live profile snapshot did not constrain new streams"
        );

        drop(first);
        let lower_lease = timeout(Duration::from_secs(3), lower_waiter)
            .await
            .expect("lower snapshot did not wake after the old stream closed")
            .unwrap();
        drop(lower_lease);

        let high_one = registry.acquire(&high_limit).await.unwrap();
        let high_two = timeout(Duration::from_secs(3), registry.acquire(&high_limit))
            .await
            .expect("capacity did not return to the remaining profile snapshot")
            .unwrap();
        drop(high_one);
        drop(high_two);
    }

    #[test]
    fn plain_requires_opt_in_and_classes_are_stable() {
        let mut p = profile("a", 0);
        p.allow_plain = false;
        assert!(matches!(
            validate_profile(&p),
            Err(NntpError::PlainNotAllowed)
        ));
        assert_eq!(
            NntpError::ArticleUnavailable {
                code: 430,
                message: "x".into()
            }
            .failover_class(),
            FailoverClass::Retryable
        );
        assert_eq!(
            NntpError::AuthenticationFailed {
                code: 481,
                message: "x".into()
            }
            .failover_class(),
            FailoverClass::Authentication
        );
    }

    #[test]
    fn canonical_message_id_wraps_bare_values_once() {
        assert_eq!(canonical_message_id("id@nyuu").unwrap(), "<id@nyuu>");
        assert_eq!(canonical_message_id("<id@nyuu>").unwrap(), "<id@nyuu>");
        assert!(canonical_message_id("<id@nyuu").is_err());
        assert!(canonical_message_id("id@nyuu>").is_err());
        assert!(canonical_message_id("id @nyuu").is_err());
        assert!(canonical_message_id("id@nyuu\r\nGROUP alt.test").is_err());
    }

    #[tokio::test]
    async fn plain_server_supports_auth_capabilities_group_and_article() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            reader
                .get_mut()
                .write_all(b"200 test server ready\r\n")
                .await
                .unwrap();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let command = line.trim_end_matches(['\r', '\n']);
                let response = if command == "AUTHINFO USER alice" {
                    "381 password required\r\n"
                } else if command == "AUTHINFO PASS secret" {
                    "281 authenticated\r\n"
                } else if command == "CAPABILITIES" {
                    "101 capabilities\r\nSTARTTLS\r\nAUTHINFO USER\r\n.\r\n"
                } else if command == "GROUP alt.test" {
                    "211 1 1 1 alt.test\r\n"
                } else if command == "ARTICLE <id@example>" {
                    "220 article follows\r\nhello\r\n..dot\r\n.\r\n"
                } else {
                    "500 unknown command\r\n"
                };
                reader
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();
                if command == "ARTICLE <id@example>" {
                    break;
                }
            }
        });

        let mut p = profile("plain", 0);
        p.port = port;
        let mut connection = NntpConnection::connect(
            &p,
            Some(UsenetCredentials {
                username: Some("alice".into()),
                password: Some("secret".into()),
            }),
        )
        .await
        .unwrap();
        let capabilities = connection.capabilities().await.unwrap();
        assert!(capabilities.supports("starttls"));
        connection.group("alt.test").await.unwrap();
        assert_eq!(
            connection.article("id@example").await.unwrap(),
            b"hello\n.dot\n"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn article_bytes_are_charged_to_the_throttle() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let line = format!("{}\r\n", "x".repeat(1022));
        let body = line.repeat(96);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            reader.get_mut().write_all(b"200 ready\r\n").await.unwrap();
            let mut command = String::new();
            reader.read_line(&mut command).await.unwrap();
            let response = format!("220 follows\r\n{body}.\r\n");
            reader
                .get_mut()
                .write_all(response.as_bytes())
                .await
                .unwrap();
        });
        let mut p = profile("plain", 0);
        p.port = port;
        let mut connection = NntpConnection::connect(&p, None).await.unwrap();
        connection.set_throttle(Throttle::new(
            Arc::new(risuko_bt::limiter::RateLimiter::unlimited()),
            Arc::new(risuko_bt::limiter::RateLimiter::new(64 * 1024)),
        ));
        let start = std::time::Instant::now();
        let article = connection.article("id@example").await.unwrap();
        assert_eq!(article.len(), 96 * 1023);
        assert!(
            start.elapsed() >= Duration::from_millis(400),
            "{:?}",
            start.elapsed()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_an_unterminated_response_line_without_buffering_past_the_cap() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            reader
                .get_mut()
                .write_all(b"200 test server ready\r\n")
                .await
                .unwrap();
            let mut command = String::new();
            reader.read_line(&mut command).await.unwrap();
            reader
                .get_mut()
                .write_all(b"220 article follows\r\n")
                .await
                .unwrap();
            reader
                .get_mut()
                .write_all(&vec![b'x'; MAX_LINE + 1])
                .await
                .unwrap();
        });

        let mut p = profile("bounded", 0);
        p.port = port;
        let mut connection = NntpConnection::connect(&p, None).await.unwrap();
        assert!(matches!(
            connection.article("id@example").await,
            Err(NntpError::ResponseTooLong)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bounds_capabilities_empty_line_count() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            reader
                .get_mut()
                .write_all(b"200 test server ready\r\n")
                .await
                .unwrap();
            let mut command = String::new();
            reader.read_line(&mut command).await.unwrap();
            let mut response = b"101 capabilities\r\n".to_vec();
            response.extend(std::iter::repeat_n(b'\n', MAX_MULTILINE_LINES + 1));
            response.extend_from_slice(b".\r\n");
            reader.get_mut().write_all(&response).await.unwrap();
        });

        let mut p = profile("bounded-lines", 0);
        p.port = port;
        let mut connection = NntpConnection::connect(&p, None).await.unwrap();
        assert!(matches!(
            connection.capabilities().await,
            Err(NntpError::ArticleTooLarge)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_starttls_when_plaintext_is_buffered() {
        let (mut peer, stream) = tokio::io::duplex(64);
        peer.write_all(b"pending plaintext").await.unwrap();
        let mut connection = NntpConnection {
            reader: BufReader::new(BoxedStream(Box::new(stream))),
            profile_id: "buffered".into(),
            greeted: true,
            throttle: None,
        };
        connection.reader.fill_buf().await.unwrap();

        let error = connection.upgrade_tls("localhost").await.unwrap_err();

        assert!(matches!(error, NntpError::Protocol { code: 0, .. }));
    }

    #[test]
    fn corrupt_article_does_not_cool_down_a_provider() {
        let pool = ProviderPool::new(vec![profile("a", 0), profile("b", 1)]).unwrap();
        pool.mark_failure(
            "a",
            &NntpError::ArticleCorrupt {
                message: "bad crc".into(),
            },
        );
        assert_eq!(pool.ordered_profiles()[0].id, "a");
    }

    #[tokio::test]
    async fn article_unstuffs_dots_and_spans_buffer_boundaries() {
        let (mut peer, stream) = tokio::io::duplex(64);
        let mut body = b"220 0 <a@b>\r\n..dot\r\nplain\r\n".to_vec();
        let long = vec![b'x'; 200];
        body.extend_from_slice(&long);
        body.extend_from_slice(b"\r\n.\r\n");
        tokio::spawn(async move {
            peer.write_all(&body).await.unwrap();
            peer.flush().await.unwrap();
            let mut sink = [0u8; 64];
            let _ = tokio::io::AsyncReadExt::read(&mut peer, &mut sink).await;
        });
        let mut connection = NntpConnection {
            reader: BufReader::with_capacity(16, BoxedStream(Box::new(stream))),
            profile_id: "article".into(),
            greeted: true,
            throttle: None,
        };
        let bytes = connection.article("<a@b>").await.unwrap();
        let mut expected = b".dot\nplain\n".to_vec();
        expected.extend_from_slice(&long);
        expected.push(b'\n');
        assert_eq!(bytes, expected);
    }

    #[tokio::test]
    async fn rejects_zero_connection_capacity_at_the_registry_boundary() {
        let registry = ProviderConnectionCapacityRegistry::default();
        let mut p = profile("zero", 0);
        p.max_connections = 0;
        assert!(matches!(
            registry.acquire(&p).await,
            Err(NntpError::InvalidProfile(_))
        ));
    }

    #[tokio::test]
    async fn failover_retries_transient_provider_errors() {
        let pool = ProviderPool::new(vec![profile("a", 0), profile("b", 1)]).unwrap();
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let seen = attempts.clone();
        let value = pool
            .run_with_failover(|provider| {
                let seen = seen.clone();
                async move {
                    seen.lock().push(provider.id.clone());
                    if provider.id == "a" {
                        Err(NntpError::ServiceUnavailable {
                            code: 400,
                            message: "offline".into(),
                        })
                    } else {
                        Ok("article")
                    }
                }
            })
            .await
            .unwrap();
        assert_eq!(value, "article");
        assert_eq!(&*attempts.lock(), &["a", "b"]);
    }
}
