use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequest, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use base64::Engine as _;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use super::events::EventBroadcaster;
use super::manager::TaskManager;
use super::options::EngineOptions;
use super::rpc_paths;

const ENGINE_VERSION: &str = concat!("risuko-engine/", env!("CARGO_PKG_VERSION"));
const ARIA2NEXT_PRODUCT: &str = "aria2-next";
const ARIA2NEXT_VERSION: &str = "1.37.0";

const RPC_MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const RPC_BLOCKED_OPTION_STEM: &str = "risukocompletionscript";

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

const PBH_METHODS: &[&str] = &[
    "risuko.getVersion",
    "risuko.tellActive",
    "risuko.tellWaiting",
    "risuko.tellStopped",
    "risuko.getPeers",
    "risuko.setBtPeerBlocklist",
    "risuko.getGlobalOption",
    "risuko.changeGlobalOption",
    "risuko.getGlobalStat",
    "system.listMethods",
];

const PBH_READABLE_OPTIONS: &[&str] = &[
    "max-download-limit",
    "max-upload-limit",
    "listen-port",
    "max-overall-upload-limit",
    "max-overall-download-limit",
];

const PBH_WRITABLE_OPTIONS: &[&str] = &[
    "max-overall-upload-limit",
    "max-overall-download-limit",
    "listen-port",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcCompatMode {
    Standard,
    Aria2Next,
}

pub struct RpcServer {
    host: String,
    port: u16,
    secret: String,
    session_id: String,
    manager: Arc<TaskManager>,
    events: EventBroadcaster,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    rpc_shutdown_tx: tokio::sync::mpsc::Sender<()>,
    compat: RpcCompatMode,
}

#[derive(Clone)]
struct RpcState {
    manager: Arc<TaskManager>,
    events: EventBroadcaster,
    secret: String,
    session_id: String,
    bind_host: String,
    rpc_shutdown_tx: tokio::sync::mpsc::Sender<()>,
    compat: RpcCompatMode,
}

impl RpcServer {
    pub fn new(
        host: String,
        port: u16,
        secret: String,
        manager: Arc<TaskManager>,
        events: EventBroadcaster,
        rpc_shutdown_tx: tokio::sync::mpsc::Sender<()>,
    ) -> Self {
        Self::new_with_compat(
            host,
            port,
            secret,
            manager,
            events,
            rpc_shutdown_tx,
            RpcCompatMode::Standard,
        )
    }

    pub fn new_with_compat(
        host: String,
        port: u16,
        secret: String,
        manager: Arc<TaskManager>,
        events: EventBroadcaster,
        rpc_shutdown_tx: tokio::sync::mpsc::Sender<()>,
        compat: RpcCompatMode,
    ) -> Self {
        let session_id = uuid::Uuid::new_v4().simple().to_string();

        Self {
            host,
            port,
            secret,
            session_id,
            manager,
            events,
            shutdown_tx: None,
            rpc_shutdown_tx,
            compat,
        }
    }

    pub async fn start(&mut self) -> Result<(), String> {
        let state = RpcState {
            manager: self.manager.clone(),
            events: self.events.clone(),
            secret: self.secret.clone(),
            session_id: self.session_id.clone(),
            bind_host: self.host.clone(),
            rpc_shutdown_tx: self.rpc_shutdown_tx.clone(),
            compat: self.compat,
        };

        let allow_origin = if self.secret.is_empty() {
            AllowOrigin::predicate(|origin, _| {
                origin.to_str().is_ok_and(origin_allowed_without_secret)
            })
        } else {
            AllowOrigin::any()
        };
        let cors = CorsLayer::new()
            .allow_origin(allow_origin)
            .allow_methods(Any)
            .allow_headers(Any)
            .max_age(std::time::Duration::from_secs(1728000));

        let mut app = Router::new().route(
            "/jsonrpc",
            post(handle_http_post).get(handle_http_get_or_ws),
        );
        if matches!(self.compat, RpcCompatMode::Aria2Next) {
            app = app.route("/", post(handle_http_post));
        }
        let app = app
            .layer(axum::extract::DefaultBodyLimit::max(RPC_MAX_BODY_BYTES))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                dns_rebind_guard,
            ))
            .layer(cors)
            .with_state(state);

        let listener = tokio::net::TcpListener::bind((self.host.as_str(), self.port))
            .await
            .map_err(|e| {
                format!(
                    "Failed to resolve or bind RPC address {}:{}: {e}",
                    self.host, self.port
                )
            })?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("Failed to inspect bound RPC address: {e}"))?;

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.shutdown_tx = Some(tx);

        tracing::info!("RPC server listening on {}", addr);

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await
                .ok();
        });

        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn dns_rebind_guard(
    State(state): State<RpcState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if let Some(raw) = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
    {
        if !host_header_allowed(raw, &state.bind_host) {
            tracing::warn!("Rejected RPC request with disallowed Host header: {raw:?}");
            return (StatusCode::FORBIDDEN, "Forbidden: invalid Host header").into_response();
        }
    }
    if state.secret.is_empty() && browser_request_blocked(req.headers()) {
        tracing::warn!("Rejected cross-origin browser request to the RPC server without a secret");
        return (
            StatusCode::FORBIDDEN,
            "Forbidden: cross-origin requests need an rpc-secret",
        )
            .into_response();
    }
    next.run(req).await
}

fn browser_request_blocked(headers: &axum::http::HeaderMap) -> bool {
    if let Some(origin) = headers.get(header::ORIGIN) {
        return !origin.to_str().is_ok_and(origin_allowed_without_secret);
    }
    headers
        .get("sec-fetch-site")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("cross-site"))
}

fn origin_allowed_without_secret(origin: &str) -> bool {
    let origin = origin.trim();
    if origin.eq_ignore_ascii_case("null") {
        return false;
    }
    let Some((scheme, rest)) = origin.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return !scheme.eq_ignore_ascii_case("file");
    }
    let authority = rest.split('/').next().unwrap_or("");
    let host = host_hostname(authority);
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("tauri.localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn method_is_read_only(method: &str) -> bool {
    matches!(
        normalize_method(method).as_ref(),
        "risuko.tellStatus"
            | "risuko.tellActive"
            | "risuko.tellWaiting"
            | "risuko.tellStopped"
            | "risuko.getGlobalStat"
            | "risuko.getVersion"
            | "risuko.getSessionInfo"
            | "risuko.getUris"
            | "risuko.getFiles"
            | "risuko.getServers"
            | "risuko.getPeers"
            | "risuko.getOption"
            | "risuko.getGlobalOption"
            | "risuko.listRoutingRules"
            | "risuko.resolveRouting"
            | "system.listMethods"
            | "system.listNotifications"
    )
}

fn request_is_read_only(request: &Value) -> bool {
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return true;
    };
    if method == "system.multicall" {
        let params = request
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        return extract_multicall_methods(&params).iter().all(|call| {
            call.get("methodName")
                .and_then(Value::as_str)
                .is_some_and(method_is_read_only)
        });
    }
    method_is_read_only(method)
}

fn host_header_allowed(host_header: &str, bind_host: &str) -> bool {
    let hostname = host_hostname(host_header);
    if hostname.is_empty() || hostname.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if hostname.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    let bind_hostname = host_hostname(bind_host);
    !bind_hostname.is_empty() && hostname.eq_ignore_ascii_case(bind_hostname)
}

fn host_hostname(host_header: &str) -> &str {
    let h = host_header.trim();
    if let Some(rest) = h.strip_prefix('[') {
        return rest.split(']').next().unwrap_or("");
    }
    h.rsplit_once(':').map(|(name, _)| name).unwrap_or(h)
}

async fn handle_rpc_value(state: &RpcState, parsed: Value) -> Option<Value> {
    match parsed {
        Value::Array(batch) if batch.is_empty() => {
            Some(rpc_error(Value::Null, INVALID_REQUEST, "Invalid Request"))
        }
        Value::Array(batch) => {
            let mut results = Vec::with_capacity(batch.len());
            for item in batch {
                if let Some(r) = process_single_request(state, item).await {
                    results.push(r);
                }
            }
            (!results.is_empty()).then_some(Value::Array(results))
        }
        Value::Object(_) => process_single_request(state, parsed).await,
        _ => Some(rpc_error(Value::Null, INVALID_REQUEST, "Invalid Request")),
    }
}

async fn handle_http_post(State(state): State<RpcState>, body: String) -> Response {
    let parsed = match serde_json::from_str::<Value>(&body) {
        Ok(v) => v,
        Err(_) => {
            let err = rpc_error(Value::Null, PARSE_ERROR, "Parse error");
            return json_rpc_response(err);
        }
    };

    match handle_rpc_value(&state, parsed).await {
        Some(resp) => json_rpc_response(resp),
        None => (StatusCode::NO_CONTENT, "").into_response(),
    }
}

async fn handle_http_get_or_ws(
    State(state): State<RpcState>,
    req: axum::extract::Request,
) -> Response {
    let is_upgrade = req
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);

    if is_upgrade {
        let ws = match WebSocketUpgrade::from_request(req, &state).await {
            Ok(ws) => ws,
            Err(e) => return e.into_response(),
        };
        return ws
            .on_upgrade(move |socket| handle_ws_connection(state, socket))
            .into_response();
    }

    let query_str = req.uri().query().unwrap_or("");
    let params: HashMap<String, String> = url::form_urlencoded::parse(query_str.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();

    handle_get_query(state, params).await
}

async fn handle_get_query(state: RpcState, params: HashMap<String, String>) -> Response {
    let method = params.get("method").map(|s| s.as_str()).unwrap_or("");
    let id = params
        .get("id")
        .map(|s| Value::String(s.clone()))
        .unwrap_or(Value::Null);
    let callback = params
        .get("jsoncallback")
        .filter(|_| !state.secret.is_empty())
        .cloned();
    let guard_get = state.secret.is_empty();

    let rpc_params = if let Some(encoded) = params.get("params") {
        decode_get_params(encoded)
    } else {
        Value::Array(Vec::new())
    };

    if method.is_empty() && id == Value::Null {
        if let Value::Array(batch) = rpc_params {
            if batch.is_empty() {
                return json_rpc_response(rpc_error(
                    Value::Null,
                    INVALID_REQUEST,
                    "Invalid Request",
                ));
            }
            let mut results = Vec::with_capacity(batch.len());
            for item in batch {
                if guard_get && !request_is_read_only(&item) {
                    results.push(get_forbidden_error(item.get("id").cloned()));
                    continue;
                }
                if let Some(r) = process_single_request(&state, item).await {
                    results.push(r);
                }
            }
            return maybe_jsonp(Value::Array(results), callback);
        }
    }

    let request = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": rpc_params,
        "id": id,
    });

    if guard_get && !request_is_read_only(&request) {
        return json_rpc_response(get_forbidden_error(Some(id)));
    }

    let response = match process_single_request(&state, request).await {
        Some(r) => r,
        None => Value::Null,
    };

    maybe_jsonp(response, callback)
}

fn get_forbidden_error(id: Option<Value>) -> Value {
    rpc_error(
        id.unwrap_or(Value::Null),
        1,
        "State-changing methods over GET need an rpc-secret",
    )
}

fn decode_get_params(encoded: &str) -> Value {
    use base64::engine::general_purpose::{STANDARD, URL_SAFE};
    let bytes = STANDARD
        .decode(encoded)
        .or_else(|_| URL_SAFE.decode(encoded));
    match bytes {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(s) => serde_json::from_str::<Value>(&s).unwrap_or(Value::Array(Vec::new())),
            Err(_) => Value::Array(Vec::new()),
        },
        Err(_) => Value::Array(Vec::new()),
    }
}

fn json_rpc_response(body: Value) -> Response {
    let json_str = serde_json::to_string(&body).unwrap_or_default();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json-rpc")],
        json_str,
    )
        .into_response()
}

fn maybe_jsonp(body: Value, callback: Option<String>) -> Response {
    let safe_cb: String = callback
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
        .collect();
    if safe_cb.is_empty() {
        return json_rpc_response(body);
    }
    let json_str = serde_json::to_string(&body).unwrap_or_default();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/javascript")],
        format!("{safe_cb}({json_str});"),
    )
        .into_response()
}

fn uses_aria2_namespace(request: &Value) -> bool {
    match request {
        Value::Array(batch) => batch.iter().any(uses_aria2_namespace),
        _ => request
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| method.starts_with("aria2.")),
    }
}

async fn ws_send_json(socket: &mut WebSocket, value: &Value) -> bool {
    let text = serde_json::to_string(value).unwrap_or_default();
    socket.send(Message::Text(text.into())).await.is_ok()
}

async fn handle_ws_connection(state: RpcState, mut socket: WebSocket) {
    let mut event_rx = state.events.subscribe();
    let mut aria2_namespace = matches!(state.compat, RpcCompatMode::Aria2Next);

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let response = match serde_json::from_str::<Value>(&text) {
                            Ok(parsed) => {
                                aria2_namespace = aria2_namespace || uses_aria2_namespace(&parsed);
                                handle_rpc_value(&state, parsed).await
                            }
                            Err(_) => Some(rpc_error(Value::Null, PARSE_ERROR, "Parse error")),
                        };
                        if let Some(response) = response {
                            if !ws_send_json(&mut socket, &response).await {
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            event = event_rx.recv() => {
                match event {
                    Ok(event) => {
                        let notification = event.to_notification(aria2_namespace);
                        if !ws_send_json(&mut socket, &notification).await {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("WebSocket client lagged, dropped {n} event(s)");
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

async fn process_single_request(state: &RpcState, mut request: Value) -> Option<Value> {
    let id = request.get_mut("id").map(Value::take);
    let params = request.get_mut("params").map(Value::take);

    let method = match request.get("method").and_then(|v| v.as_str()) {
        Some(m) if !m.is_empty() => m,
        _ => {
            return Some(rpc_error(
                id.unwrap_or(Value::Null),
                INVALID_REQUEST,
                "Invalid Request",
            ));
        }
    };

    let params = params.unwrap_or(Value::Array(Vec::new()));

    let params_vec = match params {
        Value::Array(v) => v,
        Value::Object(_) => {
            return Some(rpc_error(
                id.unwrap_or(Value::Null),
                INVALID_PARAMS,
                "Named params not supported",
            ));
        }
        _ => vec![params],
    };

    // system.multicall skips outer auth, nested calls are authed individually
    let (authed_params, auth_ok) = if method == "system.multicall" {
        (params_vec, true)
    } else {
        check_auth(&state.secret, params_vec)
    };
    if !auth_ok {
        return Some(rpc_error(id.unwrap_or(Value::Null), 1, "Unauthorized"));
    }

    let normalized = normalize_method(method);

    let result = dispatch_method(state, &normalized, authed_params).await;

    let id = match id {
        Some(v) => v,
        None => return None,
    };

    Some(match result {
        Ok(value) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": value,
        }),
        Err(RpcError { code, message }) => rpc_error(id, code, &message),
    })
}

fn check_auth(secret: &str, mut params: Vec<Value>) -> (Vec<Value>, bool) {
    if secret.is_empty() {
        if let Some(first) = params.first() {
            if let Some(s) = first.as_str() {
                if s.starts_with("token:") {
                    params.remove(0);
                }
            }
        }
        return (params, true);
    }

    if let Some(first) = params.first() {
        if let Some(token_str) = first.as_str() {
            if let Some(provided) = token_str.strip_prefix("token:") {
                if secret_eq(provided, secret) {
                    params.remove(0);
                    return (params, true);
                }
            }
        }
    }
    (params, false)
}

fn secret_eq(provided: &str, secret: &str) -> bool {
    use sha2::{Digest, Sha256};
    let a = Sha256::digest(provided.as_bytes());
    let b = Sha256::digest(secret.as_bytes());
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn normalize_method(method: &str) -> std::borrow::Cow<'_, str> {
    if let Some(suffix) = method.strip_prefix("aria2.") {
        std::borrow::Cow::Owned(format!("risuko.{suffix}"))
    } else {
        std::borrow::Cow::Borrowed(method)
    }
}

fn extract_multicall_methods(params: &[Value]) -> Vec<Value> {
    params
        .first()
        .and_then(|v| v.as_array())
        .or_else(|| {
            params.get(1).and_then(|v| v.as_array()).filter(|_| {
                params
                    .first()
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s.starts_with("token:"))
            })
        })
        .cloned()
        .unwrap_or_default()
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl From<String> for RpcError {
    fn from(message: String) -> Self {
        Self { code: 1, message }
    }
}

fn task_options(params: &[Value], index: usize) -> Map<String, Value> {
    let mut options = params
        .get(index)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    strip_blocked_options(&mut options);
    options
}

fn is_blocked_option(key: &str) -> bool {
    if !key.is_ascii() {
        return true;
    }
    let folded: String = key
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    folded.starts_with(RPC_BLOCKED_OPTION_STEM)
}

fn strip_blocked_options(options: &mut Map<String, Value>) {
    options.retain(|key, _| !is_blocked_option(key));
}

fn invalid_params(message: String) -> RpcError {
    RpcError {
        code: INVALID_PARAMS,
        message,
    }
}

async fn current_options(state: &RpcState) -> EngineOptions {
    let global = match state.manager.get_global_option().await {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    EngineOptions { global }
}

async fn check_save_options(
    state: &RpcState,
    options: &Map<String, Value>,
) -> Result<(), RpcError> {
    let names_a_path = ["dir", "out"]
        .into_iter()
        .chain(rpc_paths::FILE_OPTIONS)
        .any(|key| options.contains_key(key));
    if !names_a_path {
        return Ok(());
    }
    let current = current_options(state).await;
    if rpc_paths::allows_any_dir(&current) {
        return Ok(());
    }
    rpc_paths::check_file_options(options).map_err(invalid_params)?;
    rpc_paths::check_task_options(options, &rpc_paths::allowed_roots(&current))
        .map_err(invalid_params)
}

async fn check_task_patch(
    state: &RpcState,
    patch: &crate::engine::task::TaskPatch,
) -> Result<(), RpcError> {
    let mut fields = Map::new();
    if let Some(dir) = &patch.dir {
        fields.insert("dir".into(), Value::String(dir.clone()));
    }
    if let Some(out) = &patch.out {
        fields.insert("out".into(), Value::String(out.clone()));
    }
    check_save_options(state, &fields).await?;
    match &patch.options {
        Some(options) => check_save_options(state, options).await,
        None => Ok(()),
    }
}

async fn check_routing_rule(
    state: &RpcState,
    rule: &super::routing::TaskRoutingRule,
) -> Result<(), RpcError> {
    let current = current_options(state).await;
    if rpc_paths::allows_any_dir(&current) {
        return Ok(());
    }
    rpc_paths::check_save_dir(&rule.dir, &rpc_paths::download_roots(&current))
        .map_err(invalid_params)
}

async fn keeps_stored_dir(state: &RpcState, rule: &super::routing::TaskRoutingRule) -> bool {
    state
        .manager
        .list_routing_rules()
        .await
        .into_iter()
        .find(|stored| stored.id == rule.id)
        .is_some_and(|stored| stored.dir == rule.dir)
}

fn decode_base64(data: &str) -> Result<Vec<u8>, RpcError> {
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| RpcError::from(format!("Invalid base64: {e}")))
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        }
    })
}

fn dispatch_method<'a>(
    state: &'a RpcState,
    method: &'a str,
    params: Vec<Value>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, RpcError>> + Send + 'a>> {
    Box::pin(async move {
        if state.compat == RpcCompatMode::Aria2Next && !PBH_METHODS.contains(&method) {
            return Err(RpcError {
                code: METHOD_NOT_FOUND,
                message: format!("Method not available on this listener: {method}"),
            });
        }

        match method {
            "risuko.addUri" => {
                let uris = params
                    .first()
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                if uris.is_empty() {
                    return Err("URI required".to_string().into());
                }

                let options = task_options(&params, 1);
                check_save_options(state, &options).await?;

                let gid = state.manager.add_http_task(uris, options).await?;
                Ok(Value::String(gid))
            }

            "risuko.addMedia" => {
                let uri = params
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RpcError::from("Media URL required".to_string()))?
                    .trim();

                if uri.is_empty() {
                    return Err(RpcError::from("Media URL required".to_string()));
                }

                let options = task_options(&params, 1);
                check_save_options(state, &options).await?;

                if !super::media::is_media_uri(uri) && !super::media::is_force_ytdlp(&options) {
                    return Err(RpcError::from("Not a supported media URL".to_string()));
                }

                let gid = state.manager.add_media_task(uri, options).await?;
                Ok(Value::String(gid))
            }

            "risuko.addTorrent" => {
                let torrent_b64 = params
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RpcError::from("Torrent data required".to_string()))?;

                let torrent_data = decode_base64(torrent_b64)?;

                let options = task_options(&params, 2);
                check_save_options(state, &options).await?;

                let gid = state
                    .manager
                    .add_torrent_task(torrent_data, options)
                    .await?;
                Ok(Value::String(gid))
            }

            "risuko.addMetalink" => {
                let metalink_b64 = params
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RpcError::from("Metalink data required".to_string()))?;

                let metalink_data = decode_base64(metalink_b64)?;

                let options = task_options(&params, 1);
                check_save_options(state, &options).await?;

                let gid = state
                    .manager
                    .add_metalink_task(metalink_data, options)
                    .await?;
                Ok(Value::String(gid))
            }

            "risuko.addNzb" => {
                let nzb_b64 = params
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RpcError::from("NZB data required".to_string()))?;
                let nzb_data = decode_base64(nzb_b64)?;
                let options = task_options(&params, 1);
                check_save_options(state, &options).await?;
                let gid = state.manager.add_nzb_task(nzb_data, options).await?;
                Ok(Value::String(gid))
            }

            "risuko.addEd2k" => {
                let uri = params
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RpcError::from("ed2k URI required".to_string()))?;

                let options = task_options(&params, 1);
                check_save_options(state, &options).await?;

                let gid = state.manager.add_ed2k_task(uri, options).await?;
                Ok(Value::String(gid))
            }

            "risuko.remove" | "risuko.forceRemove" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                state.manager.remove(&gid).await?;
                Ok(Value::String(gid))
            }

            "risuko.pause" | "risuko.forcePause" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                state.manager.pause(&gid).await?;
                Ok(Value::String(gid))
            }

            "risuko.pauseAll" | "risuko.forcePauseAll" => {
                state.manager.pause_all().await;
                Ok(Value::String("OK".into()))
            }

            "risuko.unpause" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                state.manager.unpause(&gid).await?;
                Ok(Value::String(gid))
            }

            "risuko.unpauseAll" => {
                state.manager.unpause_all().await;
                Ok(Value::String("OK".into()))
            }

            "risuko.tellStatus" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                let keys = get_keys(&params, 1);
                state
                    .manager
                    .tell_status(&gid, &keys)
                    .await
                    .map_err(RpcError::from)
            }

            "risuko.tellActive" => {
                let keys = get_keys(&params, 0);
                Ok(state.manager.tell_active(&keys).await)
            }

            "risuko.tellWaiting" => {
                let offset = params.first().and_then(|v| v.as_i64()).unwrap_or(0);
                let num = params
                    .get(1)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(5000)
                    .min(usize::MAX as u64) as usize;
                let keys = get_keys(&params, 2);
                Ok(state.manager.tell_waiting(offset, num, &keys).await)
            }

            "risuko.tellStopped" => {
                let offset = params.first().and_then(|v| v.as_i64()).unwrap_or(0);
                let num = params
                    .get(1)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(5000)
                    .min(usize::MAX as u64) as usize;
                let keys = get_keys(&params, 2);
                Ok(state.manager.tell_stopped(offset, num, &keys).await)
            }

            "risuko.getGlobalStat" => Ok(state.manager.get_global_stat().await),

            "risuko.changeOption" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                let opts = task_options(&params, 1);
                check_save_options(state, &opts).await?;
                state.manager.change_option(&gid, opts).await?;
                Ok(Value::String("OK".into()))
            }

            "risuko.updateTask" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                let patch = params.get(1).cloned().unwrap_or(Value::Object(Map::new()));
                let mut patch: crate::engine::task::TaskPatch = serde_json::from_value(patch)
                    .map_err(|e| RpcError::from(format!("Invalid task patch: {e}")))?;
                if let Some(options) = patch.options.as_mut() {
                    strip_blocked_options(options);
                }
                check_task_patch(state, &patch).await?;
                let outcome = state.manager.update_task(&gid, patch).await?;
                serde_json::to_value(outcome).map_err(|e| RpcError::from(e.to_string()))
            }

            "risuko.changeGlobalOption" => {
                let opts = task_options(&params, 0);
                if state.compat == RpcCompatMode::Aria2Next {
                    if let Some(key) = opts
                        .keys()
                        .find(|key| !PBH_WRITABLE_OPTIONS.contains(&key.as_str()))
                    {
                        return Err(RpcError {
                            code: INVALID_PARAMS,
                            message: format!("Option not allowed on this listener: {key}"),
                        });
                    }
                }
                let allow_any_dir = rpc_paths::allows_any_dir(&current_options(state).await);
                rpc_paths::check_global_options(&opts, allow_any_dir).map_err(invalid_params)?;
                state.manager.change_global_option(opts).await;
                Ok(Value::String("OK".into()))
            }

            "risuko.getOption" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                state.manager.get_option(&gid).await.map_err(RpcError::from)
            }

            "risuko.getGlobalOption" => {
                let options = state.manager.get_global_option().await;
                Ok(match (state.compat, options) {
                    (RpcCompatMode::Standard, options) => options,
                    (RpcCompatMode::Aria2Next, Value::Object(mut map)) => {
                        map.retain(|key, _| PBH_READABLE_OPTIONS.contains(&key.as_str()));
                        Value::Object(map)
                    }
                    (RpcCompatMode::Aria2Next, _) => Value::Object(Map::new()),
                })
            }

            "risuko.changePosition" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                let pos = params.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
                let how = params.get(2).and_then(|v| v.as_str()).unwrap_or("POS_SET");
                state
                    .manager
                    .change_position(&gid, pos, how)
                    .await
                    .map_err(RpcError::from)
            }

            "risuko.getPeers" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                let peers = state.manager.get_peers(&gid).await;
                Ok(match state.compat {
                    RpcCompatMode::Aria2Next => stringify_aria2_peer_speeds(peers),
                    RpcCompatMode::Standard => peers,
                })
            }

            "risuko.setBtPeerBlocklist" => {
                let entries = match params.first() {
                    Some(Value::Array(arr)) => arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>(),
                    _ => {
                        return Err(RpcError {
                            code: INVALID_PARAMS,
                            message: "setBtPeerBlocklist requires an array of IP/CIDR strings"
                                .into(),
                        });
                    }
                };
                let result = state.manager.set_bt_peer_blocklist(entries).await?;
                Ok(json!({
                    "revision": result.revision,
                    "ruleCount": result.rule_count,
                    "disconnectedPeers": result.disconnected_peers,
                    "removedPeers": result.removed_peers,
                }))
            }

            "risuko.getUris" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                state.manager.get_uris(&gid).await.map_err(RpcError::from)
            }

            "risuko.getFiles" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                state.manager.get_files(&gid).await.map_err(RpcError::from)
            }

            "risuko.getServers" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                state
                    .manager
                    .get_servers(&gid)
                    .await
                    .map_err(RpcError::from)
            }

            "risuko.getSessionInfo" => Ok(json!({ "sessionId": state.session_id })),

            "risuko.shutdown" => {
                if let Err(e) = state.manager.save_session().await {
                    tracing::error!("save_session failed during shutdown: {e}");
                }
                state
                    .rpc_shutdown_tx
                    .send(())
                    .await
                    .map_err(|_| RpcError::from("Failed to signal shutdown".to_string()))?;
                Ok(Value::String("OK".into()))
            }

            "risuko.forceShutdown" => {
                state
                    .rpc_shutdown_tx
                    .send(())
                    .await
                    .map_err(|_| RpcError::from("Failed to signal shutdown".to_string()))?;
                Ok(Value::String("OK".into()))
            }

            "risuko.saveSession" => {
                state.manager.save_session().await?;
                Ok(Value::String("OK".into()))
            }

            "risuko.purgeDownloadResult" => {
                state.manager.purge_download_result().await;
                Ok(Value::String("OK".into()))
            }

            "risuko.removeDownloadResult" => {
                let gid = resolve_gid(&params, &state.manager).await?;
                state.manager.remove_download_result(&gid).await?;
                Ok(Value::String("OK".into()))
            }

            "risuko.getVersion" => Ok(match state.compat {
                RpcCompatMode::Aria2Next => json!({
                    "version": ARIA2NEXT_VERSION,
                    "product": ARIA2NEXT_PRODUCT,
                    "rpcVersion": "1",
                    "enabledFeatures": [
                        "HTTP",
                        "HTTPS",
                        "FTP",
                        "FTPS",
                        "SFTP",
                        "BitTorrent",
                        "JSON-RPC",
                    ]
                }),
                RpcCompatMode::Standard => json!({
                    "version": ENGINE_VERSION,
                    "enabledFeatures": [
                        "HTTP",
                        "HTTPS",
                        "FTP",
                        "FTPS",
                        "SFTP",
                        "BitTorrent",
                        "JSON-RPC",
                    ]
                }),
            }),

            "risuko.listRoutingRules" => {
                let rules = state.manager.list_routing_rules().await;
                Ok(serde_json::to_value(rules).unwrap_or_default())
            }

            "risuko.addRoutingRule" => {
                let rule_value = params
                    .into_iter()
                    .next()
                    .ok_or_else(|| RpcError::from("Invalid routing rule".to_string()))?;
                let rule = serde_json::from_value::<super::routing::TaskRoutingRule>(rule_value)
                    .map_err(|e| RpcError::from(format!("Invalid routing rule: {e}")))?;
                check_routing_rule(state, &rule).await?;
                let added = state.manager.add_routing_rule(rule).await?;
                Ok(serde_json::to_value(added).unwrap_or_default())
            }

            "risuko.updateRoutingRule" => {
                let rule_value = params
                    .into_iter()
                    .next()
                    .ok_or_else(|| RpcError::from("Invalid routing rule".to_string()))?;
                let rule = serde_json::from_value::<super::routing::TaskRoutingRule>(rule_value)
                    .map_err(|e| RpcError::from(format!("Invalid routing rule: {e}")))?;
                if !keeps_stored_dir(state, &rule).await {
                    check_routing_rule(state, &rule).await?;
                }
                state.manager.update_routing_rule(rule).await?;
                Ok(Value::String("OK".into()))
            }

            "risuko.removeRoutingRule" => {
                let id = params
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RpcError::from("Rule ID required".to_string()))?;
                state.manager.remove_routing_rule(id).await?;
                Ok(Value::String("OK".into()))
            }

            "risuko.resolveRouting" => {
                let filename = params
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RpcError::from("Filename required".to_string()))?;
                let decision = state.manager.preview_routing(filename).await;
                Ok(serde_json::to_value(decision).unwrap_or_default())
            }

            "system.multicall" => {
                let methods = extract_multicall_methods(&params);

                let mut results = Vec::with_capacity(methods.len());
                for call in methods {
                    let method_name = call
                        .get("methodName")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let call_params = call
                        .get("params")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();

                    let (clean_params, auth_ok) = check_auth(&state.secret, call_params);
                    if !auth_ok {
                        results.push(json!({
                            "code": 1,
                            "message": "Unauthorized",
                        }));
                        continue;
                    }

                    let normalized = normalize_method(method_name);

                    match dispatch_method(state, &normalized, clean_params).await {
                        Ok(value) => results.push(Value::Array(vec![value])),
                        Err(e) => results.push(json!({
                            "code": e.code,
                            "message": e.message,
                        })),
                    }
                }
                Ok(Value::Array(results))
            }

            "system.listMethods" => Ok(match state.compat {
                RpcCompatMode::Aria2Next => list_pbh_methods(),
                RpcCompatMode::Standard => list_methods(),
            }),

            "system.listNotifications" => Ok(list_notifications()),

            _ => Err(RpcError {
                code: METHOD_NOT_FOUND,
                message: format!("Method not found: {method}"),
            }),
        }
    })
}

fn list_methods() -> Value {
    let risuko_methods = [
        "addUri",
        "addMedia",
        "addTorrent",
        "addMetalink",
        "addNzb",
        "addEd2k",
        "remove",
        "forceRemove",
        "pause",
        "forcePause",
        "pauseAll",
        "forcePauseAll",
        "unpause",
        "unpauseAll",
        "tellStatus",
        "tellActive",
        "tellWaiting",
        "tellStopped",
        "getGlobalStat",
        "changeOption",
        "updateTask",
        "changeGlobalOption",
        "getOption",
        "getGlobalOption",
        "changePosition",
        "getPeers",
        "setBtPeerBlocklist",
        "getUris",
        "getFiles",
        "getServers",
        "getSessionInfo",
        "shutdown",
        "forceShutdown",
        "saveSession",
        "purgeDownloadResult",
        "removeDownloadResult",
        "getVersion",
        "listRoutingRules",
        "addRoutingRule",
        "updateRoutingRule",
        "removeRoutingRule",
        "resolveRouting",
    ];

    let mut all: Vec<Value> = Vec::new();
    for m in &risuko_methods {
        all.push(Value::String(format!("aria2.{m}")));
        all.push(Value::String(format!("risuko.{m}")));
    }
    all.push(Value::String("system.multicall".into()));
    all.push(Value::String("system.listMethods".into()));
    all.push(Value::String("system.listNotifications".into()));
    Value::Array(all)
}

fn list_pbh_methods() -> Value {
    let mut all: Vec<Value> = Vec::new();
    for method in PBH_METHODS {
        match method.strip_prefix("risuko.") {
            Some(suffix) => {
                all.push(Value::String(format!("aria2.{suffix}")));
                all.push(Value::String((*method).into()));
            }
            None => all.push(Value::String((*method).into())),
        }
    }
    Value::Array(all)
}

fn list_notifications() -> Value {
    let names = [
        "onDownloadStart",
        "onDownloadPause",
        "onDownloadStop",
        "onDownloadComplete",
        "onDownloadError",
        "onBtDownloadComplete",
    ];

    let mut all: Vec<Value> = Vec::new();
    for n in &names {
        all.push(Value::String(format!("aria2.{n}")));
        all.push(Value::String(format!("risuko.{n}")));
    }
    Value::Array(all)
}

fn get_gid(params: &[Value]) -> Result<String, RpcError> {
    params
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| RpcError::from("GID required".to_string()))
}

async fn resolve_gid(params: &[Value], manager: &TaskManager) -> Result<String, RpcError> {
    let prefix = get_gid(params)?;
    manager.resolve_gid(&prefix).await.map_err(RpcError::from)
}

fn get_keys(params: &[Value], index: usize) -> Vec<String> {
    params
        .get(index)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn stringify_aria2_peer_speeds(peers: Value) -> Value {
    let Value::Array(mut items) = peers else {
        return peers;
    };
    for item in &mut items {
        let Some(obj) = item.as_object_mut() else {
            continue;
        };
        for key in ["downloadSpeed", "uploadSpeed"] {
            if let Some(n) = obj.get(key).and_then(Value::as_u64) {
                obj.insert(key.to_string(), Value::String(n.to_string()));
            }
        }
    }
    Value::Array(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::rpc_paths::ALLOW_ANY_DIR_KEY;
    use serde_json::json;
    use tempfile::TempDir;

    async fn make_rpc_state(secret: &str) -> (RpcState, TempDir) {
        let events = EventBroadcaster::default();
        let config_dir = TempDir::new().unwrap();
        let mut system = serde_json::Map::new();
        system.insert(
            "dir".into(),
            json!(config_dir
                .path()
                .join("downloads")
                .to_string_lossy()
                .to_string()),
        );
        system.insert("bt-enable-upnp".into(), json!(false));
        system.insert("bt-enable-lsd".into(), json!(false));
        let options =
            super::super::options::EngineOptions::from_config(&system, &serde_json::Map::new());
        let manager = Arc::new(
            TaskManager::new(config_dir.path(), options, events.clone())
                .await
                .unwrap(),
        );
        let (rpc_shutdown_tx, _rpc_shutdown_rx) = tokio::sync::mpsc::channel(1);

        (
            RpcState {
                manager,
                events,
                secret: secret.to_string(),
                session_id: "test-session".to_string(),
                bind_host: "127.0.0.1".to_string(),
                rpc_shutdown_tx,
                compat: RpcCompatMode::Standard,
            },
            config_dir,
        )
    }

    #[tokio::test]
    async fn empty_batch_is_an_invalid_request() {
        let (state, _dir) = make_rpc_state("").await;
        let response = handle_rpc_value(&state, json!([])).await.unwrap();
        assert_eq!(response["error"]["code"], INVALID_REQUEST);
        let response = handle_rpc_value(&state, json!(7)).await.unwrap();
        assert_eq!(response["error"]["code"], INVALID_REQUEST);
    }

    #[test]
    fn aria2_namespace_is_detected_in_single_and_batch_requests() {
        assert!(uses_aria2_namespace(&json!({"method": "aria2.tellActive"})));
        assert!(uses_aria2_namespace(&json!([
            {"method": "risuko.tellActive"},
            {"method": "aria2.getGlobalStat"}
        ])));
        assert!(!uses_aria2_namespace(
            &json!({"method": "risuko.tellActive"})
        ));
        assert!(!uses_aria2_namespace(
            &json!({"method": "system.listMethods"})
        ));
    }

    #[test]
    fn rpc_options_drop_completion_script_overrides() {
        let params = vec![
            json!(["http://example.com/a"]),
            json!({
                "dir": "/tmp",
                "risuko-completion-script-command": "sh",
                "risuko-completion-script-args": "{path}",
                "risuko-completion-script-enabled": true,
            }),
        ];
        let options = task_options(&params, 1);
        assert_eq!(options.len(), 1);
        assert_eq!(options.get("dir"), Some(&json!("/tmp")));
        assert!(task_options(&params, 5).is_empty());
    }

    #[test]
    fn rpc_options_drop_respelled_completion_script_keys() {
        let params = vec![
            json!(["http://example.com/a"]),
            json!({
                "dir": "/tmp",
                "out": "a.bin",
                "risuko-cookie-host": "example.com",
                "risuko_completion_script_command": "sh",
                "Risuko-Completion-Script-Args": "{path}",
                "RISUKO-COMPLETION-SCRIPT-ENABLED": "true",
                "risuko completion script timeout ms": "1000",
                "risuko.completion.script.command": "sh",
                "risukoCompletionScriptCommand": "sh",
                "risuko-completion'script-command": "sh",
                "r\u{00ed}suko-completion-script-command": "sh",
                "risuko-completion-script-command\u{200b}": "sh",
            }),
        ];
        let options = task_options(&params, 1);
        let mut keys: Vec<&str> = options.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["dir", "out", "risuko-cookie-host"]);
    }

    #[test]
    fn update_task_patches_drop_respelled_completion_script_keys() {
        let mut options = Map::new();
        options.insert("dir".into(), json!("/tmp"));
        options.insert("risuko_completion_script_command".into(), json!("sh"));
        strip_blocked_options(&mut options);
        assert_eq!(options.len(), 1);
        assert_eq!(options.get("dir"), Some(&json!("/tmp")));
    }

    #[test]
    fn host_guard_allows_loopback_and_ips() {
        assert!(host_header_allowed("localhost:6800", "127.0.0.1"));
        assert!(host_header_allowed("localhost", "127.0.0.1"));
        assert!(host_header_allowed("127.0.0.1:6800", "127.0.0.1"));
        assert!(host_header_allowed("127.0.0.1", "127.0.0.1"));
        assert!(host_header_allowed("[::1]:6800", "127.0.0.1"));
        assert!(host_header_allowed("192.168.1.5:6800", "0.0.0.0"));
        assert!(host_header_allowed("", "127.0.0.1"));
    }

    #[test]
    fn host_guard_rejects_dns_rebind_names() {
        assert!(!host_header_allowed("evil.com", "127.0.0.1"));
        assert!(!host_header_allowed("evil.com:6800", "127.0.0.1"));
        assert!(!host_header_allowed("attacker.example.org", "0.0.0.0"));
    }

    #[test]
    fn host_guard_allows_configured_hostname() {
        assert!(host_header_allowed("my-nas.local:6800", "my-nas.local"));
        assert!(host_header_allowed("MY-NAS.LOCAL", "my-nas.local"));
        assert!(!host_header_allowed("other.local", "my-nas.local"));
    }

    #[test]
    fn decode_get_params_standard_and_url_safe() {
        use base64::engine::general_purpose::{STANDARD, URL_SAFE};
        let json = r#"["token:a+b/c==",{"k":">>>"}]"#;
        let expected: Value = serde_json::from_str(json).unwrap();

        let std_enc = STANDARD.encode(json.as_bytes());
        assert_eq!(decode_get_params(&std_enc), expected);

        let url_enc = URL_SAFE.encode(json.as_bytes());
        assert_eq!(decode_get_params(&url_enc), expected);
    }

    #[test]
    fn decode_get_params_invalid_falls_back_to_empty_array() {
        assert_eq!(decode_get_params("!!!not-base64!!!"), Value::Array(vec![]));
    }

    #[test]
    fn auth_empty_secret_always_ok() {
        let params = vec![json!("hello")];
        let (out, ok) = check_auth("", params);
        assert!(ok);
        assert_eq!(out, vec![json!("hello")]);
    }

    #[test]
    fn auth_empty_secret_strips_token() {
        let params = vec![json!("token:abc"), json!("arg1")];
        let (out, ok) = check_auth("", params);
        assert!(ok);
        assert_eq!(out, vec![json!("arg1")]);
    }

    #[test]
    fn auth_valid_token() {
        let params = vec![json!("token:mysecret"), json!("gid1")];
        let (out, ok) = check_auth("mysecret", params);
        assert!(ok);
        assert_eq!(out, vec![json!("gid1")]);
    }

    #[test]
    fn auth_wrong_token() {
        let params = vec![json!("token:wrong"), json!("gid1")];
        let (out, ok) = check_auth("mysecret", params);
        assert!(!ok);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn auth_missing_token_param() {
        let params = vec![json!("gid1")];
        let (out, ok) = check_auth("mysecret", params);
        assert!(!ok);
        assert_eq!(out, vec![json!("gid1")]);
    }

    #[test]
    fn auth_non_string_first_param() {
        let params = vec![json!(42), json!("gid1")];
        let (_, ok) = check_auth("mysecret", params);
        assert!(!ok);
    }

    #[test]
    fn auth_empty_params_with_secret() {
        let params = vec![];
        let (out, ok) = check_auth("mysecret", params);
        assert!(!ok);
        assert!(out.is_empty());
    }

    #[test]
    fn normalize_aria2_to_risuko() {
        assert_eq!(normalize_method("aria2.addUri"), "risuko.addUri");
        assert_eq!(normalize_method("aria2.tellStatus"), "risuko.tellStatus");
    }

    #[test]
    fn normalize_passthrough() {
        assert_eq!(normalize_method("risuko.addUri"), "risuko.addUri");
        assert_eq!(normalize_method("system.listMethods"), "system.listMethods");
    }

    #[test]
    fn extract_multicall_methods_standard_shape() {
        let call = json!({
            "methodName": "aria2.tellActive",
            "params": ["token:secret", ["gid"]]
        });
        let params = vec![json!([call.clone()])];
        let methods = extract_multicall_methods(&params);
        assert_eq!(methods, vec![call]);
    }

    #[test]
    fn extract_multicall_methods_legacy_shape() {
        let call = json!({
            "methodName": "aria2.tellActive",
            "params": ["token:secret", ["gid"]]
        });
        let params = vec![json!("token:secret"), json!([call.clone()])];
        let methods = extract_multicall_methods(&params);
        assert_eq!(methods, vec![call]);
    }

    #[test]
    fn extract_multicall_methods_rejects_second_arg_without_token_prefix() {
        let call = json!({
            "methodName": "aria2.tellActive",
            "params": ["token:secret", ["gid"]]
        });
        let params = vec![json!("secret"), json!([call])];
        let methods = extract_multicall_methods(&params);
        assert!(methods.is_empty());
    }

    #[tokio::test]
    async fn multicall_mixed_nested_auth_standard_shape_keeps_processing() {
        let (state, _config_dir) = make_rpc_state("secret").await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "system.multicall",
            "params": [[
                {
                    "methodName": "aria2.getVersion",
                    "params": []
                },
                {
                    "methodName": "aria2.getVersion",
                    "params": ["token:secret"]
                }
            ]]
        });

        let response = process_single_request(&state, request).await.unwrap();
        let results = response.get("result").and_then(|v| v.as_array()).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("code").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(
            results[0].get("message").and_then(|v| v.as_str()),
            Some("Unauthorized")
        );

        let success = results[1].as_array().unwrap();
        let version_obj = success.first().and_then(|v| v.as_object()).unwrap();
        assert_eq!(
            version_obj.get("version").and_then(|v| v.as_str()),
            Some(ENGINE_VERSION)
        );
    }

    #[tokio::test]
    async fn multicall_mixed_nested_auth_legacy_shape_keeps_processing() {
        let (state, _config_dir) = make_rpc_state("secret").await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "system.multicall",
            "params": [
                "token:secret",
                [
                    {
                        "methodName": "aria2.getVersion",
                        "params": ["token:wrong"]
                    },
                    {
                        "methodName": "aria2.getVersion",
                        "params": ["token:secret"]
                    }
                ]
            ]
        });

        let response = process_single_request(&state, request).await.unwrap();
        let results = response.get("result").and_then(|v| v.as_array()).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("code").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(
            results[0].get("message").and_then(|v| v.as_str()),
            Some("Unauthorized")
        );

        let success = results[1].as_array().unwrap();
        let version_obj = success.first().and_then(|v| v.as_object()).unwrap();
        assert_eq!(
            version_obj.get("version").and_then(|v| v.as_str()),
            Some(ENGINE_VERSION)
        );
    }

    #[test]
    fn get_gid_ok() {
        let params = vec![json!("abc123")];
        assert_eq!(get_gid(&params).unwrap(), "abc123");
    }

    #[test]
    fn get_gid_empty_params() {
        let params: Vec<Value> = vec![];
        assert!(get_gid(&params).is_err());
    }

    #[test]
    fn get_gid_non_string() {
        let params = vec![json!(42)];
        assert!(get_gid(&params).is_err());
    }

    #[test]
    fn get_keys_extracts_strings() {
        let params = vec![json!("gid"), json!(["status", "totalLength"])];
        let keys = get_keys(&params, 1);
        assert_eq!(keys, vec!["status", "totalLength"]);
    }

    #[test]
    fn get_keys_missing_index_returns_empty() {
        let params = vec![json!("gid")];
        let keys = get_keys(&params, 1);
        assert!(keys.is_empty());
    }

    #[test]
    fn get_keys_non_array_returns_empty() {
        let params = vec![json!("gid"), json!("not_array")];
        let keys = get_keys(&params, 1);
        assert!(keys.is_empty());
    }

    #[test]
    fn list_methods_advertises_nzb_endpoint() {
        let methods = list_methods();
        assert!(methods
            .as_array()
            .unwrap()
            .iter()
            .any(|method| method.as_str() == Some("risuko.addNzb")));
    }

    #[test]
    fn list_methods_advertises_metalink() {
        let methods = list_methods();
        for name in ["aria2.addMetalink", "risuko.addMetalink"] {
            assert!(methods
                .as_array()
                .unwrap()
                .iter()
                .any(|method| method.as_str() == Some(name)));
        }
    }

    #[test]
    fn list_methods_advertises_set_bt_peer_blocklist() {
        let methods = list_methods();
        assert!(methods
            .as_array()
            .unwrap()
            .iter()
            .any(|method| method.as_str() == Some("risuko.setBtPeerBlocklist")));
        assert!(methods
            .as_array()
            .unwrap()
            .iter()
            .any(|method| method.as_str() == Some("aria2.setBtPeerBlocklist")));
    }

    #[tokio::test]
    async fn get_version_aria2next_reports_product() {
        let (mut state, _config_dir) = make_rpc_state("secret").await;
        state.compat = RpcCompatMode::Aria2Next;
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "aria2.getVersion",
            "params": ["token:secret"]
        });
        let response = process_single_request(&state, request).await.unwrap();
        let result = response.get("result").and_then(|v| v.as_object()).unwrap();
        assert_eq!(
            result.get("product").and_then(|v| v.as_str()),
            Some("aria2-next")
        );
        assert_eq!(
            result.get("version").and_then(|v| v.as_str()),
            Some("1.37.0")
        );
    }

    #[tokio::test]
    async fn get_version_standard_omits_aria2next_product() {
        let (state, _config_dir) = make_rpc_state("secret").await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "aria2.getVersion",
            "params": ["token:secret"]
        });
        let response = process_single_request(&state, request).await.unwrap();
        let result = response.get("result").and_then(|v| v.as_object()).unwrap();
        assert!(result.get("product").is_none());
        assert_eq!(
            result.get("version").and_then(|v| v.as_str()),
            Some(ENGINE_VERSION)
        );
    }

    #[tokio::test]
    async fn set_bt_peer_blocklist_returns_counters() {
        let (state, _config_dir) = make_rpc_state("secret").await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "aria2.setBtPeerBlocklist",
            "params": ["token:secret", ["1.2.3.4", "10.0.0.0/8"]]
        });
        let response = process_single_request(&state, request).await.unwrap();
        let result = response
            .get("result")
            .unwrap_or_else(|| panic!("expected result, got {response}"));
        assert_eq!(result.get("ruleCount").and_then(|v| v.as_u64()), Some(2));
        assert!(result.get("revision").and_then(|v| v.as_u64()).is_some());
        assert!(result
            .get("disconnectedPeers")
            .and_then(|v| v.as_u64())
            .is_some());
        assert!(result
            .get("removedPeers")
            .and_then(|v| v.as_u64())
            .is_some());
    }

    #[test]
    fn stringify_aria2_peer_speeds_converts_numeric_counters() {
        let peers = json!([{
            "downloadSpeed": 1024,
            "uploadSpeed": 512,
            "ip": "1.2.3.4"
        }]);
        let out = stringify_aria2_peer_speeds(peers);
        assert_eq!(out[0]["downloadSpeed"], "1024");
        assert_eq!(out[0]["uploadSpeed"], "512");
        assert_eq!(out[0]["ip"], "1.2.3.4");
    }

    #[tokio::test]
    async fn set_bt_peer_blocklist_rejects_missing_array() {
        let (state, _config_dir) = make_rpc_state("secret").await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "aria2.setBtPeerBlocklist",
            "params": ["token:secret"]
        });
        let response = process_single_request(&state, request).await.unwrap();
        let error = response.get("error").expect("expected parameter error");
        assert_eq!(
            error.get("code").and_then(|v| v.as_i64()),
            Some(INVALID_PARAMS)
        );
    }

    #[test]
    fn origin_policy_without_secret() {
        for ok in [
            "http://localhost:1420",
            "http://127.0.0.1:16800",
            "https://127.0.0.1",
            "http://[::1]:3000",
            "tauri://localhost",
            "http://tauri.localhost",
            "https://tauri.localhost",
            "chrome-extension://abcdef",
        ] {
            assert!(origin_allowed_without_secret(ok), "{ok} should pass");
        }
        for bad in [
            "https://evil.example",
            "http://evil.example:16800",
            "http://localhost.evil.example",
            "http://192.168.1.5",
            "null",
            "file:///tmp/x.html",
            "garbage",
        ] {
            assert!(!origin_allowed_without_secret(bad), "{bad} should fail");
        }
    }

    #[test]
    fn browser_request_blocked_checks_origin_then_fetch_site() {
        use axum::http::{HeaderMap, HeaderValue};
        let mut h = HeaderMap::new();
        assert!(!browser_request_blocked(&h));
        h.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
        assert!(browser_request_blocked(&h));
        h.insert(
            header::ORIGIN,
            HeaderValue::from_static("tauri://localhost"),
        );
        assert!(!browser_request_blocked(&h));
        h.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://evil.example"),
        );
        assert!(browser_request_blocked(&h));
    }

    #[test]
    fn read_only_gate_covers_multicall() {
        assert!(request_is_read_only(&json!({"method": "aria2.getVersion"})));
        assert!(!request_is_read_only(&json!({"method": "aria2.addUri"})));
        assert!(!request_is_read_only(&json!({"method": "aria2.shutdown"})));
        let mixed = json!({
            "method": "system.multicall",
            "params": [[
                {"methodName": "aria2.getVersion", "params": []},
                {"methodName": "aria2.addUri", "params": [["http://x"]]}
            ]]
        });
        assert!(!request_is_read_only(&mixed));
        let reads = json!({
            "method": "system.multicall",
            "params": [[{"methodName": "aria2.tellActive", "params": []}]]
        });
        assert!(request_is_read_only(&reads));
    }

    async fn serve_state(state: &RpcState, secret: &str) -> (RpcServer, u16) {
        let port = {
            let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            l.local_addr().unwrap().port()
        };
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut server = RpcServer::new(
            "127.0.0.1".into(),
            port,
            secret.into(),
            state.manager.clone(),
            state.events.clone(),
            tx,
        );
        server.start().await.unwrap();
        (server, port)
    }

    async fn http(
        port: u16,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\nContent-Length: {}\r\n",
            body.len()
        );
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        req.push_str(body);
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.unwrap();
        let text = String::from_utf8_lossy(&raw).to_string();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    fn get_path(method: &str, params: Value) -> String {
        use base64::engine::general_purpose::STANDARD;
        format!(
            "/jsonrpc?method={method}&id=1&params={}",
            url::form_urlencoded::byte_serialize(STANDARD.encode(params.to_string()).as_bytes())
                .collect::<String>()
        )
    }

    const VERSION_POST: &str =
        r#"{"jsonrpc":"2.0","id":"1","method":"aria2.getVersion","params":[]}"#;

    #[tokio::test]
    async fn empty_secret_rejects_web_origins_and_mutating_get() {
        let (state, _dir) = make_rpc_state("").await;
        let (mut server, port) = serve_state(&state, "").await;

        let (status, _) = http(
            port,
            "POST",
            "/jsonrpc",
            &[
                ("Origin", "https://evil.example"),
                ("Content-Type", "text/plain"),
            ],
            VERSION_POST,
        )
        .await;
        assert_eq!(status, 403);
        let (status, _) = http(
            port,
            "GET",
            &get_path("aria2.getVersion", json!([])),
            &[("Sec-Fetch-Site", "cross-site")],
            "",
        )
        .await;
        assert_eq!(status, 403);
        let (status, _) = http(
            port,
            "GET",
            "/jsonrpc",
            &[
                ("Origin", "https://evil.example"),
                ("Upgrade", "websocket"),
                ("Connection", "Upgrade"),
                ("Sec-WebSocket-Version", "13"),
                ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
            ],
            "",
        )
        .await;
        assert_eq!(status, 403);

        let (status, body) = http(
            port,
            "POST",
            "/jsonrpc",
            &[("Origin", "tauri://localhost")],
            VERSION_POST,
        )
        .await;
        assert_eq!(status, 200);
        assert!(body.contains("version"), "{body}");
        let (status, body) = http(port, "POST", "/jsonrpc", &[], VERSION_POST).await;
        assert_eq!(status, 200);
        assert!(body.contains("version"), "{body}");

        let (status, body) = http(
            port,
            "GET",
            &get_path("aria2.getVersion", json!([])),
            &[],
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert!(body.contains("version"), "{body}");
        let (status, body) = http(
            port,
            "GET",
            &get_path("aria2.addUri", json!([["http://example.com/a"]])),
            &[],
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert!(body.contains("need an rpc-secret"), "{body}");
        let active = state.manager.tell_active(&[]).await;
        assert_eq!(active.as_array().map(Vec::len), Some(0));
        let waiting = state.manager.tell_waiting(0, 10, &[]).await;
        assert_eq!(waiting.as_array().map(Vec::len), Some(0));

        let path = format!(
            "{}&jsoncallback=cb",
            get_path("aria2.getVersion", json!([]))
        );
        let (_, body) = http(port, "GET", &path, &[], "").await;
        assert!(!body.starts_with("cb("), "{body}");
        server.stop();
    }

    #[tokio::test]
    async fn secret_keeps_web_origins_but_requires_token() {
        let (state, _dir) = make_rpc_state("s3cret").await;
        let (mut server, port) = serve_state(&state, "s3cret").await;
        let origin = [("Origin", "https://ariang.example")];

        let (status, body) = http(port, "POST", "/jsonrpc", &origin, VERSION_POST).await;
        assert_eq!(status, 200);
        assert!(body.contains("Unauthorized"), "{body}");

        let authed =
            r#"{"jsonrpc":"2.0","id":"1","method":"aria2.getVersion","params":["token:s3cret"]}"#;
        let (status, body) = http(port, "POST", "/jsonrpc", &origin, authed).await;
        assert_eq!(status, 200);
        assert!(body.contains("version"), "{body}");

        let path = format!(
            "{}&jsoncallback=cb",
            get_path("aria2.getVersion", json!(["token:s3cret"]))
        );
        let (status, body) = http(port, "GET", &path, &origin, "").await;
        assert_eq!(status, 200);
        assert!(body.starts_with("cb("), "{body}");
        server.stop();
    }

    async fn pbh_state() -> (RpcState, TempDir) {
        let (mut state, config_dir) = make_rpc_state("secret").await;
        state.compat = RpcCompatMode::Aria2Next;
        (state, config_dir)
    }

    fn authed_call(method: &str, params: Vec<Value>) -> Value {
        let mut all = vec![json!("token:secret")];
        all.extend(params);
        json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": method,
            "params": all
        })
    }

    async fn call(state: &RpcState, method: &str, params: Vec<Value>) -> Value {
        process_single_request(state, authed_call(method, params))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn pbh_listener_refuses_methods_outside_its_list() {
        let (state, _config_dir) = pbh_state().await;
        let gid = json!("0000000000000000");
        let calls = [
            ("aria2.addUri", vec![json!(["http://127.0.0.1:1/a"])]),
            ("aria2.shutdown", vec![]),
            (
                "aria2.changeOption",
                vec![gid.clone(), json!({"dir": "/tmp/x"})],
            ),
            ("risuko.remove", vec![gid.clone()]),
            ("aria2.tellStatus", vec![gid.clone()]),
            ("aria2.getOption", vec![gid]),
            ("aria2.saveSession", vec![]),
        ];
        for (method, params) in calls {
            let response = call(&state, method, params).await;
            assert_eq!(
                response["error"]["code"], METHOD_NOT_FOUND,
                "{method}: {response}"
            );
        }
        for method in ["aria2.tellActive", "aria2.tellWaiting"] {
            let response = call(&state, method, vec![]).await;
            assert_eq!(response["result"], json!([]), "{method}: {response}");
        }
    }

    #[tokio::test]
    async fn pbh_listener_refuses_multicall() {
        let (state, _config_dir) = pbh_state().await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "system.multicall",
            "params": [[{
                "methodName": "aria2.addUri",
                "params": ["token:secret", ["http://127.0.0.1:1/a"]]
            }]]
        });
        let response = process_single_request(&state, request).await.unwrap();
        assert_eq!(response["error"]["code"], METHOD_NOT_FOUND, "{response}");
    }

    #[tokio::test]
    async fn pbh_listener_checks_every_batch_item() {
        let (state, _config_dir) = pbh_state().await;
        let batch = json!([
            authed_call("aria2.getVersion", vec![]),
            authed_call("aria2.addUri", vec![json!(["http://127.0.0.1:1/a"])]),
        ]);
        let response = handle_rpc_value(&state, batch).await.unwrap();
        let items = response.as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert!(items[0].get("result").is_some(), "{response}");
        assert_eq!(items[1]["error"]["code"], METHOD_NOT_FOUND, "{response}");
    }

    #[tokio::test]
    async fn pbh_listener_answers_what_peerbanhelper_calls() {
        let (state, _config_dir) = pbh_state().await;
        let fields = json!(["gid", "status"]);
        let calls = [
            ("aria2.getVersion", vec![]),
            ("aria2.tellActive", vec![fields.clone()]),
            ("aria2.tellWaiting", vec![fields.clone()]),
            ("aria2.tellStopped", vec![fields]),
            ("aria2.getGlobalOption", vec![]),
            ("aria2.getGlobalStat", vec![]),
            ("aria2.setBtPeerBlocklist", vec![json!(["192.0.2.1"])]),
        ];
        for (method, params) in calls {
            let response = call(&state, method, params).await;
            assert!(response.get("result").is_some(), "{method}: {response}");
            assert!(response.get("error").is_none(), "{method}: {response}");
        }
    }

    #[tokio::test]
    async fn pbh_listener_global_options_hide_everything_else() {
        let (mut state, _config_dir) = make_rpc_state("secret").await;
        let changed = call(
            &state,
            "aria2.changeGlobalOption",
            vec![json!({
                "rpc-secret": "topsecret",
                "ftp-passwd": "pw",
                "max-overall-download-limit": "1024"
            })],
        )
        .await;
        assert_eq!(changed["result"], "OK", "{changed}");

        state.compat = RpcCompatMode::Aria2Next;
        let response = call(&state, "aria2.getGlobalOption", vec![]).await;
        let result = response["result"].as_object().unwrap();
        for key in result.keys() {
            assert!(PBH_READABLE_OPTIONS.contains(&key.as_str()), "{key}");
        }
        assert!(result.contains_key("max-overall-download-limit"));
        let text = response["result"].to_string();
        assert!(!text.contains("topsecret"), "{text}");
        assert!(!text.contains("ftp-passwd"), "{text}");
    }

    #[tokio::test]
    async fn pbh_listener_limits_writable_options() {
        let (state, _config_dir) = pbh_state().await;

        let response = call(
            &state,
            "aria2.changeGlobalOption",
            vec![json!({
                "max-overall-upload-limit": "2048",
                "max-overall-download-limit": "0"
            })],
        )
        .await;
        assert_eq!(response["result"], "OK", "{response}");

        let response = call(
            &state,
            "aria2.changeGlobalOption",
            vec![json!({"listen-port": 23456})],
        )
        .await;
        assert_eq!(response["result"], "OK", "{response}");

        let response = call(
            &state,
            "aria2.changeGlobalOption",
            vec![json!({"dir": "/tmp/x"})],
        )
        .await;
        assert_eq!(response["error"]["code"], INVALID_PARAMS, "{response}");

        let response = call(
            &state,
            "aria2.changeGlobalOption",
            vec![json!({"max-overall-upload-limit": "1", "rpc-secret": "x"})],
        )
        .await;
        assert!(response.get("error").is_some(), "{response}");

        let global = state.manager.get_global_option().await;
        assert_eq!(global["max-overall-upload-limit"], "2048");
        assert_ne!(global.get("dir"), Some(&json!("/tmp/x")));
        assert!(global.get("rpc-secret").is_none());
    }

    #[tokio::test]
    async fn pbh_listener_lists_only_its_methods() {
        let (state, _config_dir) = pbh_state().await;
        let response = call(&state, "system.listMethods", vec![]).await;
        let methods: Vec<&str> = response["result"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert!(methods.contains(&"aria2.getPeers"));
        assert!(methods.contains(&"risuko.setBtPeerBlocklist"));
        assert!(!methods.contains(&"aria2.addUri"));
        assert!(!methods.contains(&"system.multicall"));
    }

    fn text(path: &std::path::Path) -> String {
        path.to_string_lossy().into_owned()
    }

    fn refused(response: &Value) -> bool {
        response["error"]["code"] == INVALID_PARAMS
    }

    fn error_message(response: &Value) -> String {
        response["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    async fn task_count(state: &RpcState) -> usize {
        [
            state.manager.tell_active(&[]).await,
            state.manager.tell_waiting(0, 100, &[]).await,
            state.manager.tell_stopped(0, 100, &[]).await,
        ]
        .iter()
        .map(|list| list.as_array().map_or(0, Vec::len))
        .sum()
    }

    async fn allow_any_dir(state: &RpcState) {
        let mut opts = Map::new();
        opts.insert(ALLOW_ANY_DIR_KEY.into(), json!(true));
        state.manager.change_global_option(opts).await;
    }

    async fn add_paused(state: &RpcState, mut options: Value) -> Value {
        options["pause"] = json!("true");
        call(
            state,
            "aria2.addUri",
            vec![json!(["http://127.0.0.1:1/a.bin"]), options],
        )
        .await
    }

    #[tokio::test]
    async fn adds_with_a_folder_outside_the_download_folders_are_refused() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let outside = text(&config_dir.path().join("elsewhere"));

        let response = add_paused(&state, json!({"dir": outside})).await;
        assert!(refused(&response), "{response}");
        let message = error_message(&response);
        assert!(message.contains(&outside), "{message}");
        assert!(
            message.contains("Let RPC clients use any local path"),
            "{message}"
        );
        assert_eq!(task_count(&state).await, 0);

        for out in ["../escape.bin", "/tmp/escape.bin"] {
            let response = add_paused(&state, json!({"out": out})).await;
            assert!(refused(&response), "{out}: {response}");
        }

        let options = json!({"dir": outside, "pause": "true"});
        let calls = [
            (
                "aria2.addTorrent",
                vec![json!("AAAA"), json!([]), options.clone()],
            ),
            ("aria2.addMetalink", vec![json!("AAAA"), options.clone()]),
            ("risuko.addNzb", vec![json!("AAAA"), options.clone()]),
            (
                "risuko.addMedia",
                vec![json!("https://www.youtube.com/watch?v=x"), options.clone()],
            ),
            (
                "risuko.addEd2k",
                vec![
                    json!("ed2k://|file|a.bin|1|0123456789ABCDEF0123456789ABCDEF|/"),
                    options,
                ],
            ),
        ];
        for (method, params) in calls {
            let response = call(&state, method, params).await;
            assert!(refused(&response), "{method}: {response}");
        }
        assert_eq!(task_count(&state).await, 0);
    }

    #[tokio::test]
    async fn adds_inside_the_download_folders_or_without_a_folder_are_accepted() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let inside = text(&config_dir.path().join("downloads").join("sub"));

        let response = add_paused(&state, json!({"dir": inside, "out": "sub/name.bin"})).await;
        assert!(response["result"].is_string(), "{response}");
        let response = add_paused(&state, json!({})).await;
        assert!(response["result"].is_string(), "{response}");

        let videos = config_dir.path().join("videos");
        let mut opts = Map::new();
        opts.insert("file-category-dirs".into(), json!({"video": text(&videos)}));
        state.manager.change_global_option(opts).await;
        let response = add_paused(&state, json!({"dir": text(&videos.join("x"))})).await;
        assert!(response["result"].is_string(), "{response}");
        assert_eq!(task_count(&state).await, 3);
    }

    #[tokio::test]
    async fn with_the_switch_on_any_folder_is_accepted() {
        let (state, config_dir) = make_rpc_state("secret").await;
        allow_any_dir(&state).await;
        let outside = text(&config_dir.path().join("elsewhere"));
        let response = add_paused(&state, json!({"dir": outside})).await;
        assert!(response["result"].is_string(), "{response}");
        assert_eq!(task_count(&state).await, 1);
    }

    #[tokio::test]
    async fn task_edits_refuse_folders_outside_the_download_folders() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let outside = text(&config_dir.path().join("elsewhere"));
        let inside = text(&config_dir.path().join("downloads").join("sub"));
        let added = add_paused(&state, json!({})).await;
        let gid = added["result"].as_str().unwrap().to_string();

        let response = call(
            &state,
            "aria2.changeOption",
            vec![json!(gid), json!({"dir": outside})],
        )
        .await;
        assert!(refused(&response), "{response}");
        let options = state.manager.get_option(&gid).await.unwrap();
        assert_ne!(options.get("dir"), Some(&json!(outside)));

        for patch in [
            json!({"dir": outside}),
            json!({"options": {"dir": outside}}),
            json!({"out": "../x.bin"}),
            json!({"options": {"out": "/tmp/x.bin"}}),
        ] {
            let response = call(&state, "risuko.updateTask", vec![json!(gid), patch.clone()]).await;
            assert!(refused(&response), "{patch}: {response}");
        }

        let response = call(
            &state,
            "aria2.changeOption",
            vec![json!(gid), json!({"dir": inside})],
        )
        .await;
        assert_eq!(response["result"], "OK", "{response}");
    }

    #[tokio::test]
    async fn global_folder_settings_cannot_be_changed_while_restricted() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let outside = text(&config_dir.path().join("elsewhere"));
        let changes = [
            ("dir", json!(outside)),
            ("file-category-dirs", json!({"video": outside})),
            (
                "task-routing-rules",
                json!([{"id": "r", "label": "All", "pattern": "*", "dir": outside}]),
            ),
        ];
        for (key, value) in &changes {
            let response = call(
                &state,
                "aria2.changeGlobalOption",
                vec![json!({ *key: value })],
            )
            .await;
            assert!(refused(&response), "{key}: {response}");
            assert!(error_message(&response).contains(key), "{response}");
        }
        let global = state.manager.get_global_option().await;
        assert_ne!(global["dir"], json!(outside));
        assert!(global.get("file-category-dirs").is_none());
        assert!(global.get("task-routing-rules").is_none());

        let response = call(
            &state,
            "aria2.changeGlobalOption",
            vec![json!({"max-overall-download-limit": "1024"})],
        )
        .await;
        assert_eq!(response["result"], "OK", "{response}");

        allow_any_dir(&state).await;
        let response = call(
            &state,
            "aria2.changeGlobalOption",
            vec![json!({"dir": outside})],
        )
        .await;
        assert_eq!(response["result"], "OK", "{response}");
        assert_eq!(
            state.manager.get_global_option().await["dir"],
            json!(outside)
        );
    }

    #[tokio::test]
    async fn the_switch_itself_cannot_be_changed_over_rpc() {
        for allowed in [false, true] {
            let (state, _config_dir) = make_rpc_state("secret").await;
            if allowed {
                allow_any_dir(&state).await;
            }
            for value in [json!(true), json!("true"), json!(false)] {
                let response = call(
                    &state,
                    "aria2.changeGlobalOption",
                    vec![json!({ ALLOW_ANY_DIR_KEY: value })],
                )
                .await;
                assert!(refused(&response), "{allowed} {value}: {response}");
                assert!(
                    error_message(&response).contains(ALLOW_ANY_DIR_KEY),
                    "{response}"
                );
            }
            let response = call(
                &state,
                "aria2.changeGlobalOption",
                vec![json!({"max-overall-download-limit": "2048", ALLOW_ANY_DIR_KEY: true})],
            )
            .await;
            assert!(refused(&response), "{response}");

            let global = state.manager.get_global_option().await;
            assert_ne!(global["max-overall-download-limit"], "2048");
            let options = crate::engine::options::EngineOptions {
                global: global.as_object().cloned().unwrap(),
            };
            assert_eq!(
                options.get_bool(ALLOW_ANY_DIR_KEY).unwrap_or(false),
                allowed
            );
        }
    }

    #[tokio::test]
    async fn routing_rules_must_stay_inside_the_download_folders() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let outside = text(&config_dir.path().join("elsewhere"));
        let inside = text(&config_dir.path().join("downloads").join("iso"));
        let rule =
            |id: &str, dir: &str| json!({"id": id, "label": "ISO", "pattern": "*.iso", "dir": dir});

        let response = call(&state, "aria2.addRoutingRule", vec![rule("a", &outside)]).await;
        assert!(refused(&response), "{response}");
        assert!(state.manager.list_routing_rules().await.is_empty());

        let response = call(&state, "aria2.addRoutingRule", vec![rule("b", &inside)]).await;
        assert_eq!(response["result"]["dir"], json!(inside), "{response}");
        let response = call(&state, "aria2.updateRoutingRule", vec![rule("b", &outside)]).await;
        assert!(refused(&response), "{response}");
        assert_eq!(state.manager.list_routing_rules().await[0].dir, inside);

        let trusted = config_dir.path().join("trusted");
        state
            .manager
            .add_routing_rule(super::super::routing::TaskRoutingRule {
                id: "t".into(),
                label: "Trusted".into(),
                pattern: "*.bin".into(),
                dir: text(&trusted),
                enabled: true,
            })
            .await
            .unwrap();
        let response = call(
            &state,
            "aria2.addRoutingRule",
            vec![rule("c", &text(&trusted.join("deeper")))],
        )
        .await;
        assert!(refused(&response), "{response}");
        let response = add_paused(&state, json!({"dir": text(&trusted.join("x"))})).await;
        assert!(response["result"].is_string(), "{response}");
    }

    #[tokio::test]
    async fn a_rule_with_an_outside_folder_can_be_edited_while_its_folder_stays() {
        fn rule(id: &str, label: &str, dir: &str) -> Value {
            json!({"id": id, "label": label, "pattern": "*.img", "dir": dir})
        }

        let (state, config_dir) = make_rpc_state("secret").await;
        let mine = text(&config_dir.path().join("mine"));
        let other = text(&config_dir.path().join("other"));
        let inside = text(&config_dir.path().join("downloads").join("iso"));
        let stored_rule = |id: &str, dir: &str| super::super::routing::TaskRoutingRule {
            id: id.into(),
            label: "Mine".into(),
            pattern: "*.iso".into(),
            dir: dir.into(),
            enabled: true,
        };
        for (id, dir) in [("m", &mine), ("d", &inside), ("d", &mine)] {
            state
                .manager
                .add_routing_rule(stored_rule(id, dir))
                .await
                .unwrap();
        }

        let response = call(
            &state,
            "aria2.updateRoutingRule",
            vec![rule("m", "Images", &mine)],
        )
        .await;
        assert_eq!(response["result"], "OK", "{response}");
        let stored = state.manager.list_routing_rules().await;
        assert_eq!(stored[0].label, "Images");
        assert_eq!(stored[0].pattern, "*.img");
        assert_eq!(stored[0].dir, mine);

        for (id, dir) in [("m", &other), ("x", &mine), ("x", &other), ("d", &mine)] {
            let response = call(
                &state,
                "aria2.updateRoutingRule",
                vec![rule(id, "Moved", dir)],
            )
            .await;
            assert!(refused(&response), "{id} {dir}: {response}");
            assert_eq!(state.manager.list_routing_rules().await, stored);
        }

        let response = call(
            &state,
            "aria2.addRoutingRule",
            vec![rule("m", "Images", &mine)],
        )
        .await;
        assert!(refused(&response), "{response}");
        assert_eq!(state.manager.list_routing_rules().await, stored);
    }

    #[tokio::test]
    async fn multicall_checks_every_entry() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let outside = text(&config_dir.path().join("elsewhere"));
        let inside = text(&config_dir.path().join("downloads"));
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "system.multicall",
            "params": [[
                {
                    "methodName": "aria2.addUri",
                    "params": ["token:secret", ["http://127.0.0.1:1/a.bin"], {"dir": outside, "pause": "true"}]
                },
                {
                    "methodName": "aria2.addUri",
                    "params": ["token:secret", ["http://127.0.0.1:1/b.bin"], {"dir": inside, "pause": "true"}]
                },
                {
                    "methodName": "aria2.changeGlobalOption",
                    "params": ["token:secret", { ALLOW_ANY_DIR_KEY: true }]
                }
            ]]
        });
        let response = process_single_request(&state, request).await.unwrap();
        let results = response["result"].as_array().unwrap();
        assert_eq!(results.len(), 3, "{response}");
        assert_eq!(results[0]["code"], INVALID_PARAMS, "{response}");
        assert!(results[1][0].is_string(), "{response}");
        assert_eq!(results[2]["code"], INVALID_PARAMS, "{response}");
        assert_eq!(task_count(&state).await, 1);
    }

    #[tokio::test]
    async fn adds_that_name_a_cookie_or_netrc_file_are_refused() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let file = text(&config_dir.path().join("secrets.txt"));
        for key in ["load-cookies", "netrc-path"] {
            let response = add_paused(&state, json!({ key: file })).await;
            assert!(refused(&response), "{key}: {response}");
            let message = error_message(&response);
            assert!(message.contains(&file), "{message}");
            assert!(message.contains(key), "{message}");
            assert!(
                message.contains("Let RPC clients use any local path"),
                "{message}"
            );
        }
        let response = add_paused(&state, json!({"load-cookies": 7})).await;
        assert!(refused(&response), "{response}");
        let response = call(
            &state,
            "risuko.addMedia",
            vec![
                json!("https://www.youtube.com/watch?v=x"),
                json!({"cookies": file, "pause": "true"}),
            ],
        )
        .await;
        assert!(refused(&response), "{response}");
        assert!(
            error_message(&response).contains("cookies names a file"),
            "{response}"
        );
        assert_eq!(task_count(&state).await, 0);
    }

    #[tokio::test]
    async fn adds_with_an_empty_cookie_file_are_accepted() {
        let (state, _config_dir) = make_rpc_state("secret").await;
        let response = add_paused(&state, json!({"load-cookies": ""})).await;
        assert!(response["result"].is_string(), "{response}");
        assert_eq!(task_count(&state).await, 1);
    }

    const PASTED_KEY: &str =
        "-----BEGIN OPENSSH PRIVATE KEY-----\nAAAA\n-----END OPENSSH PRIVATE KEY-----";

    #[tokio::test]
    async fn sftp_key_files_need_the_switch() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let key_file = text(&config_dir.path().join("id_ed25519"));
        let options = json!({"sftp-private-key": key_file});

        let response = add_paused(&state, options.clone()).await;
        assert!(refused(&response), "{response}");
        let message = error_message(&response);
        assert!(message.contains(&key_file), "{message}");
        assert!(
            message.contains("sftp-private-key names a file"),
            "{message}"
        );
        assert_eq!(task_count(&state).await, 0);

        let added = add_paused(&state, json!({})).await;
        let gid = added["result"].as_str().unwrap().to_string();
        let edits = [
            ("aria2.changeOption", vec![json!(gid), options.clone()]),
            (
                "risuko.updateTask",
                vec![json!(gid), json!({ "options": options })],
            ),
            ("aria2.changeGlobalOption", vec![options.clone()]),
        ];
        for (method, params) in &edits {
            let response = call(&state, method, params.clone()).await;
            assert!(refused(&response), "{method}: {response}");
        }
        let task = state.manager.get_option(&gid).await.unwrap();
        assert!(task.get("sftp-private-key").is_none(), "{task}");
        let global = state.manager.get_global_option().await;
        assert!(global.get("sftp-private-key").is_none(), "{global}");

        allow_any_dir(&state).await;
        let response = add_paused(&state, options.clone()).await;
        assert!(response["result"].is_string(), "{response}");
        for (method, params) in &edits {
            let response = call(&state, method, params.clone()).await;
            assert!(response.get("error").is_none(), "{method}: {response}");
        }
        assert_eq!(
            state.manager.get_global_option().await["sftp-private-key"],
            json!(key_file)
        );
    }

    #[tokio::test]
    async fn a_pasted_sftp_key_is_accepted_and_never_echoed() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let pasted = json!({"sftp-private-key": PASTED_KEY});

        let added = add_paused(&state, pasted.clone()).await;
        let gid = added["result"].as_str().expect("accepted").to_string();
        let calls = [
            ("aria2.changeOption", vec![json!(gid), pasted.clone()]),
            (
                "risuko.updateTask",
                vec![json!(gid), json!({ "options": pasted })],
            ),
            ("aria2.changeGlobalOption", vec![pasted.clone()]),
        ];
        for (method, params) in calls {
            let response = call(&state, method, params).await;
            assert!(response.get("error").is_none(), "{method}: {response}");
        }

        let outside = text(&config_dir.path().join("elsewhere"));
        let cookies = text(&config_dir.path().join("cookies.txt"));
        for options in [
            json!({"sftp-private-key": PASTED_KEY, "dir": outside}),
            json!({"sftp-private-key": PASTED_KEY, "load-cookies": cookies}),
            json!({"sftp-private-key": [PASTED_KEY]}),
        ] {
            let response = add_paused(&state, options.clone()).await;
            assert!(refused(&response), "{options}: {response}");
            let body = response.to_string();
            assert!(!body.contains("PRIVATE KEY"), "{body}");
            assert!(!body.contains("AAAA"), "{body}");
        }
        assert_eq!(task_count(&state).await, 1);
    }

    #[tokio::test]
    async fn with_the_switch_on_cookie_and_netrc_files_are_accepted() {
        let (state, config_dir) = make_rpc_state("secret").await;
        allow_any_dir(&state).await;
        let cookies = text(&config_dir.path().join("cookies.txt"));
        let netrc = text(&config_dir.path().join("netrc"));
        let response = add_paused(
            &state,
            json!({"load-cookies": cookies, "netrc-path": netrc}),
        )
        .await;
        let gid = response["result"].as_str().expect("accepted").to_string();
        let options = state.manager.get_option(&gid).await.unwrap();
        assert_eq!(options["load-cookies"], json!(cookies));
        assert_eq!(options["netrc-path"], json!(netrc));
    }

    #[tokio::test]
    async fn task_edits_refuse_a_cookie_file() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let file = text(&config_dir.path().join("cookies.txt"));
        let added = add_paused(&state, json!({})).await;
        let gid = added["result"].as_str().unwrap().to_string();

        let response = call(
            &state,
            "aria2.changeOption",
            vec![json!(gid), json!({"load-cookies": file})],
        )
        .await;
        assert!(refused(&response), "{response}");
        let response = call(
            &state,
            "risuko.updateTask",
            vec![json!(gid), json!({"options": {"load-cookies": file}})],
        )
        .await;
        assert!(refused(&response), "{response}");
        let options = state.manager.get_option(&gid).await.unwrap();
        assert!(options.get("load-cookies").is_none(), "{options}");
    }

    #[tokio::test]
    async fn the_global_netrc_file_needs_the_switch() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let file = text(&config_dir.path().join("netrc"));
        let change = || {
            call(
                &state,
                "aria2.changeGlobalOption",
                vec![json!({"netrc-path": file})],
            )
        };

        let response = change().await;
        assert!(refused(&response), "{response}");
        assert!(
            error_message(&response).contains("netrc-path"),
            "{response}"
        );
        let global = state.manager.get_global_option().await;
        assert!(global.get("netrc-path").is_none(), "{global}");

        allow_any_dir(&state).await;
        let response = change().await;
        assert_eq!(response["result"], "OK", "{response}");
        assert_eq!(
            state.manager.get_global_option().await["netrc-path"],
            json!(file)
        );
    }

    #[tokio::test]
    async fn multicall_refuses_an_add_that_names_a_cookie_file() {
        let (state, config_dir) = make_rpc_state("secret").await;
        let file = text(&config_dir.path().join("cookies.txt"));
        let request = json!({
            "jsonrpc": "2.0",
            "id": "1",
            "method": "system.multicall",
            "params": [[
                {
                    "methodName": "aria2.addUri",
                    "params": ["token:secret", ["http://127.0.0.1:1/a.bin"], {"load-cookies": file, "pause": "true"}]
                },
                {
                    "methodName": "aria2.addUri",
                    "params": ["token:secret", ["http://127.0.0.1:1/b.bin"], {"pause": "true"}]
                }
            ]]
        });
        let response = process_single_request(&state, request).await.unwrap();
        let results = response["result"].as_array().unwrap();
        assert_eq!(results.len(), 2, "{response}");
        assert_eq!(results[0]["code"], INVALID_PARAMS, "{response}");
        assert!(results[1][0].is_string(), "{response}");
        assert_eq!(task_count(&state).await, 1);
    }

    #[tokio::test]
    async fn standard_listener_is_unchanged() {
        let (state, _config_dir) = make_rpc_state("secret").await;
        let response = call(&state, "aria2.getGlobalOption", vec![]).await;
        let result = response["result"].as_object().unwrap();
        assert!(result
            .keys()
            .any(|key| !PBH_READABLE_OPTIONS.contains(&key.as_str())));
        assert!(result.contains_key("dir"));

        let response = call(&state, "system.listMethods", vec![]).await;
        let methods: Vec<&str> = response["result"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert!(methods.contains(&"aria2.addUri"));
    }
}
