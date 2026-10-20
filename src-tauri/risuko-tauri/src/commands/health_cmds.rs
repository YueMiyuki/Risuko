use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_OPEN_REPARSE_POINT,
};

use serde::Serialize;
use serde_json::Value;
use tauri::{AppHandle, State};

#[cfg(not(target_os = "android"))]
use tauri_plugin_autostart::ManagerExt;

use risuko_engine::engine::{
    self,
    ed2k::kad::{KadHealthSnapshot, KadState},
    media,
    options::EngineOptions,
    torrent::BtHealthSnapshot,
};

use crate::commands::event_cmds::sleep_inhibit_active;
use crate::state::AppState;

const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const LOG_TAIL_BYTES: u64 = 1_048_576;
const MAX_LOG_FILES: usize = 60;
const MAX_LOG_READ_BYTES: u64 = 2 * 1024 * 1024;
const MAX_LOG_READ_LINES: usize = 5_000;
const PROXY_PROBE_URL: &str = "https://www.cloudflare.com/cdn-cgi/trace";
const PROXY_PROBE_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum HealthStatus {
    Ok,
    Warn,
    Fail,
    Skipped,
}

impl HealthStatus {
    fn rank(self) -> u8 {
        match self {
            Self::Ok => 0,
            Self::Skipped => 1,
            Self::Warn => 2,
            Self::Fail => 3,
        }
    }
    fn worst(self, other: Self) -> Self {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthFix {
    pub kind: &'static str,
    pub target: Option<String>,
}

impl HealthFix {
    fn open_pref(target: &'static str) -> Self {
        Self {
            kind: "open-preference",
            target: Some(target.to_string()),
        }
    }
    fn restart_engine() -> Self {
        Self {
            kind: "restart-engine",
            target: None,
        }
    }
    fn open_log_dir() -> Self {
        Self {
            kind: "open-log-dir",
            target: None,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheck {
    pub id: String,
    pub status: HealthStatus,
    pub message: String,
    pub fix: Option<HealthFix>,
    pub details: Option<Value>,
}

impl HealthCheck {
    fn ok(id: &str, msg: impl Into<String>) -> Self {
        Self::new(id, HealthStatus::Ok, msg, None)
    }
    fn warn(id: &str, msg: impl Into<String>, fix: Option<HealthFix>) -> Self {
        Self::new(id, HealthStatus::Warn, msg, fix)
    }
    fn fail(id: &str, msg: impl Into<String>, fix: Option<HealthFix>) -> Self {
        Self::new(id, HealthStatus::Fail, msg, fix)
    }
    fn skipped(id: &str, msg: impl Into<String>) -> Self {
        Self::new(id, HealthStatus::Skipped, msg, None)
    }
    fn new(id: &str, status: HealthStatus, msg: impl Into<String>, fix: Option<HealthFix>) -> Self {
        Self {
            id: id.to_string(),
            status,
            message: msg.into(),
            fix,
            details: None,
        }
    }
    fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCategory {
    pub id: &'static str,
    pub status: HealthStatus,
    pub checks: Vec<HealthCheck>,
}

impl HealthCategory {
    fn from_checks(id: &'static str, checks: Vec<HealthCheck>) -> Self {
        let status = checks
            .iter()
            .map(|c| c.status)
            .fold(HealthStatus::Ok, HealthStatus::worst);
        Self { id, status, checks }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthReport {
    pub generated_at_ms: u64,
    pub engine_running: bool,
    pub overall_status: HealthStatus,
    pub categories: Vec<HealthCategory>,
    pub log_path: String,
}

#[tauri::command]
pub async fn run_health_checks(
    handle: AppHandle,
    state: State<'_, AppState>,
    categories: Option<Vec<String>>,
    slow_probes: Option<bool>,
) -> Result<HealthReport, String> {
    let slow = slow_probes.unwrap_or(false);
    let (system_cfg, user_cfg, log_dir) = {
        let cfg = state.config.lock().map_err(|e| e.to_string())?;
        (
            cfg.get_system_config().clone(),
            cfg.get_user_config().clone(),
            state.log_dir.clone(),
        )
    };
    let options = EngineOptions::from_config(&system_cfg, &user_cfg);
    let autostart_enabled = autostart_enabled(&handle);
    let prevent_sleep_while_downloading =
        parse_boolish(user_cfg.get("prevent-sleep-while-downloading"), true);

    let engine_running = engine::engine_uptime().is_some();
    let snapshot = engine::startup_snapshot();

    let want = |id: &str| {
        categories
            .as_ref()
            .map(|list| list.iter().any(|c| c == id))
            .unwrap_or(true)
    };

    let (bt_snapshot, tracker_urls, kad_snapshot) = match engine::get_manager().await {
        Some(mgr) if want("bittorrent") || want("network") => {
            let (snap, urls) = if want("bittorrent") {
                (
                    mgr.bt_health_snapshot().await,
                    mgr.list_active_tracker_urls().await,
                )
            } else {
                (None, Vec::new())
            };
            let kad = if want("network") {
                Some(mgr.kad_health_snapshot().await)
            } else {
                None
            };
            (snap, urls, kad)
        }
        _ => (None, Vec::new(), None),
    };

    let mut cats: Vec<HealthCategory> = Vec::new();

    if want("general") {
        cats.push(HealthCategory::from_checks(
            "general",
            check_general(&options, engine_running),
        ));
    }
    if want("network") {
        cats.push(HealthCategory::from_checks(
            "network",
            check_network(&options, user_cfg.get("proxy"), slow, kad_snapshot.as_ref()).await,
        ));
    }
    if want("bittorrent") {
        cats.push(HealthCategory::from_checks(
            "bittorrent",
            check_bittorrent(&options, bt_snapshot.as_ref(), &tracker_urls, slow).await,
        ));
    }
    if want("disk") {
        let (disk_options, disk_cfg) = (options.clone(), user_cfg.clone());
        let checks = tokio::task::spawn_blocking(move || check_disk(&disk_options, &disk_cfg))
            .await
            .map_err(|e| e.to_string())?;
        cats.push(HealthCategory::from_checks("disk", checks));
    }
    if want("system") {
        cats.push(HealthCategory::from_checks(
            "system",
            check_system(autostart_enabled, prevent_sleep_while_downloading),
        ));
    }
    if want("config") {
        cats.push(HealthCategory::from_checks(
            "config",
            check_config(&system_cfg, &user_cfg, snapshot.as_ref()),
        ));
    }
    if want("logs") {
        let dir = log_dir.clone();
        let checks = tokio::task::spawn_blocking(move || check_logs(&dir))
            .await
            .map_err(|e| e.to_string())?;
        cats.push(HealthCategory::from_checks("logs", checks));
    }
    if want("tools") && !cfg!(target_os = "android") {
        cats.push(HealthCategory::from_checks("tools", check_tools().await));
    }

    let overall_status = cats
        .iter()
        .map(|c| c.status)
        .fold(HealthStatus::Ok, HealthStatus::worst);

    Ok(HealthReport {
        generated_at_ms: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        engine_running,
        overall_status,
        categories: cats,
        log_path: log_dir.to_string_lossy().to_string(),
    })
}

fn check_general(options: &EngineOptions, running: bool) -> Vec<HealthCheck> {
    let mut out = Vec::new();

    if running {
        let secs = engine::engine_uptime().map(|d| d.as_secs()).unwrap_or(0);
        out.push(
            HealthCheck::ok("engine-running", "Engine is running")
                .with_details(serde_json::json!({ "uptimeSeconds": secs })),
        );
    } else {
        out.push(HealthCheck::fail(
            "engine-running",
            "Engine is not running",
            Some(HealthFix::restart_engine()),
        ));
    }

    out.push(
        HealthCheck::ok("app-version", format!("Risuko {}", APP_VERSION))
            .with_details(serde_json::json!({ "version": APP_VERSION })),
    );

    let port = options.rpc_listen_port();
    let host = options.rpc_host();
    out.push(
        HealthCheck::ok("rpc-endpoint", format!("RPC bound to {}:{}", host, port))
            .with_details(serde_json::json!({ "host": host, "port": port })),
    );

    if options.rpc_secret().is_empty() && !is_loopback(&host) {
        out.push(HealthCheck::warn(
            "rpc-secret",
            "RPC secret is empty and host is not loopback",
            Some(HealthFix::open_pref("advanced")),
        ));
    } else {
        out.push(HealthCheck::ok(
            "rpc-secret",
            "RPC interface protected (loopback-only or secret set)",
        ));
    }

    out
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]")
}

fn parse_boolish(v: Option<&Value>, default: bool) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s.eq_ignore_ascii_case("true"),
        _ => default,
    }
}

async fn check_network(
    options: &EngineOptions,
    proxy_config: Option<&Value>,
    slow: bool,
    kad: Option<&KadHealthSnapshot>,
) -> Vec<HealthCheck> {
    let mut out = Vec::new();

    let listen_port = options.get_u64("listen-port").unwrap_or(0);
    if (1..=65535).contains(&listen_port) {
        out.push(HealthCheck::ok(
            "listen-port",
            format!("BT listen port: {}", listen_port),
        ));
    } else {
        out.push(HealthCheck::warn(
            "listen-port",
            "BT listen port is not configured",
            Some(HealthFix::open_pref("advanced")),
        ));
    }

    let dht_port = options.get_u64("dht-listen-port").unwrap_or(0);
    if (1..=65535).contains(&dht_port) {
        out.push(HealthCheck::ok(
            "dht-listen-port",
            format!("DHT listen port: {}", dht_port),
        ));
    } else {
        out.push(HealthCheck::warn(
            "dht-listen-port",
            "DHT listen port is not configured",
            Some(HealthFix::open_pref("advanced")),
        ));
    }

    if options.bt_listen_v6() {
        out.push(HealthCheck::ok("ipv6", "IPv6 listener enabled"));
    } else {
        out.push(HealthCheck::skipped("ipv6", "IPv6 listener disabled"));
    }

    out.push(check_ed2k_kad(options, kad));

    let normalized_proxy = proxy_config
        .map(risuko_engine::config::normalize_proxy_config)
        .unwrap_or_else(|| serde_json::json!({}));
    let http_profile = normalized_proxy.get("http").and_then(Value::as_object);
    let http_enabled = http_profile
        .and_then(|profile| profile.get("enable"))
        .map(|value| parse_boolish(Some(value), false))
        .unwrap_or(false);
    let http_server = http_profile
        .and_then(|profile| profile.get("server"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let http_profile_is_explicit =
        proxy_config.is_some_and(risuko_engine::config::proxy_http_profile_is_explicit);
    let http_proxy = if http_enabled && !http_server.is_empty() {
        http_server
    } else if !http_profile_is_explicit {
        options.get_str("all-proxy").unwrap_or("").trim()
    } else {
        ""
    };
    let http_bypass = if http_enabled && !http_server.is_empty() {
        http_profile
            .and_then(|profile| profile.get("bypass"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
    } else if !http_profile_is_explicit {
        options.get_str("no-proxy").unwrap_or("").trim()
    } else {
        ""
    };
    let p2p_proxy = options.get_str("p2p-proxy").unwrap_or("").trim();
    let p2p_bypass = options.get_str("p2p-no-proxy").unwrap_or("").trim();
    let p2p_udp_proxy = options.get_str("p2p-udp-proxy").unwrap_or("").trim();
    let p2p_udp_bypass = options.get_str("p2p-udp-no-proxy").unwrap_or("").trim();
    let udp_differs = p2p_udp_proxy != p2p_proxy || p2p_udp_bypass != p2p_bypass;

    let (http, p2p, p2p_udp) = tokio::join!(
        check_proxy_profile("proxy", "HTTP", http_proxy, http_bypass, slow),
        check_proxy_profile("p2p-proxy", "P2P", p2p_proxy, p2p_bypass, slow),
        async {
            if udp_differs {
                Some(
                    check_proxy_profile(
                        "p2p-udp-proxy",
                        "P2P UDP",
                        p2p_udp_proxy,
                        p2p_udp_bypass,
                        slow,
                    )
                    .await,
                )
            } else {
                None
            }
        }
    );
    out.push(http);
    out.push(p2p);
    out.extend(p2p_udp);

    out
}

fn redact_proxy_url(value: &str) -> String {
    let value = value.trim();
    if let Ok(mut url) = risuko_http::Url::parse(value) {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        return url.to_string();
    }

    let Some((scheme, remainder)) = value.split_once("://") else {
        return "<invalid proxy>".to_string();
    };
    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    let host = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);
    if host.is_empty() || scheme.is_empty() {
        "<invalid proxy>".to_string()
    } else {
        format!("{}://{}", scheme.to_ascii_lowercase(), host)
    }
}

async fn check_proxy_profile(
    id: &'static str,
    label: &'static str,
    proxy: &str,
    bypass: &str,
    slow: bool,
) -> HealthCheck {
    if proxy.is_empty() {
        return HealthCheck::skipped(id, format!("No {label} proxy configured"));
    }

    let display = redact_proxy_url(proxy);
    let details = serde_json::json!({
        "profile": label.to_ascii_lowercase(),
        "proxy": display,
        "bypass": bypass,
    });

    if risuko_http::Proxy::all(proxy).is_err() {
        return HealthCheck::fail(
            id,
            format!("{label} proxy URL is invalid ({display})"),
            Some(HealthFix::open_pref("advanced")),
        )
        .with_details(details);
    }

    if slow {
        return probe_proxy_reachability(id, label, proxy)
            .await
            .with_details(details);
    }

    let message = if label.starts_with("P2P") {
        format!(
            "{label} proxy configured ({display}); SOCKS5/SOCKS5H is required for UDP DHT, trackers, and uTP"
        )
    } else {
        format!("HTTP proxy configured ({display})")
    };
    HealthCheck::ok(id, message).with_details(details)
}

fn check_ed2k_kad(options: &EngineOptions, kad: Option<&KadHealthSnapshot>) -> HealthCheck {
    let configured_port = match options.ed2k_kad_port_checked() {
        Ok(port) => port,
        Err(error) => {
            return HealthCheck::fail(
                "ed2k-kad",
                format!("ED2K Kad configuration error: {error}"),
                Some(HealthFix::open_pref("advanced")),
            );
        }
    };
    let Some(snapshot) = kad else {
        if !options.ed2k_enable_kad() {
            return HealthCheck::skipped("ed2k-kad", "ED2K Kad source discovery is disabled");
        }
        return HealthCheck::warn(
            "ed2k-kad",
            format!("ED2K Kad is enabled on UDP {configured_port}, but the engine is not running"),
            Some(HealthFix::restart_engine()),
        );
    };
    let details = serde_json::json!({
        "enabled": snapshot.enabled,
        "bound": snapshot.bound,
        "state": snapshot.state,
        "port": snapshot.udp_port,
        "routingContacts": snapshot.routing_contacts,
        "cachedContacts": snapshot.cached_contacts,
        "lastBootstrapAtMs": snapshot.last_bootstrap_at_ms,
        "lastLookupAtMs": snapshot.last_lookup_at_ms,
        "lastLookupSuccess": snapshot.last_lookup_success,
        "lastError": snapshot.last_error,
    });

    if snapshot.bound {
        if !options.ed2k_enable_kad() {
            return HealthCheck::warn(
                "ed2k-kad",
                format!(
                    "ED2K Kad remains bound on UDP {} until the engine is restarted",
                    snapshot.udp_port
                ),
                Some(HealthFix::restart_engine()),
            )
            .with_details(details);
        }
        if configured_port != snapshot.udp_port {
            return HealthCheck::warn(
                "ed2k-kad",
                format!(
                    "ED2K Kad is running on UDP {}; configured UDP {} applies after restart",
                    snapshot.udp_port, configured_port
                ),
                Some(HealthFix::restart_engine()),
            )
            .with_details(details);
        }
    }

    match snapshot.state {
        KadState::Disabled => {
            if options.ed2k_enable_kad() {
                HealthCheck::warn(
                    "ed2k-kad",
                    format!(
                        "ED2K Kad is enabled on UDP {configured_port}, but the service is not running"
                    ),
                    Some(HealthFix::restart_engine()),
                )
                .with_details(details)
            } else {
                HealthCheck::skipped("ed2k-kad", "ED2K Kad source discovery is disabled")
                    .with_details(details)
            }
        }
        KadState::Ready if snapshot.bound && snapshot.routing_contacts > 0 => HealthCheck::ok(
            "ed2k-kad",
            format!(
                "ED2K Kad ready on UDP {} ({} routing contacts)",
                snapshot.udp_port, snapshot.routing_contacts
            ),
        )
        .with_details(details),
        KadState::Bootstrapping => HealthCheck::warn(
            "ed2k-kad",
            format!(
                "ED2K Kad is bootstrapping on UDP {} ({} routing contacts)",
                snapshot.udp_port, snapshot.routing_contacts
            ),
            None,
        )
        .with_details(details),
        KadState::Searching => HealthCheck::warn(
            "ed2k-kad",
            format!(
                "ED2K Kad is searching on UDP {} ({} routing contacts)",
                snapshot.udp_port, snapshot.routing_contacts
            ),
            None,
        )
        .with_details(details),
        KadState::Timeout => HealthCheck::warn(
            "ed2k-kad",
            "ED2K Kad lookup timed out; ED2K server discovery remains available",
            Some(HealthFix::open_pref("advanced")),
        )
        .with_details(details),
        KadState::Error if snapshot.bound => HealthCheck::warn(
            "ed2k-kad",
            format!(
                "ED2K Kad lookup failed on UDP {}: {}; ED2K server discovery remains available",
                snapshot.udp_port,
                snapshot.last_error.as_deref().unwrap_or("unknown error")
            ),
            Some(HealthFix::open_pref("advanced")),
        )
        .with_details(details),
        KadState::Error => HealthCheck::fail(
            "ed2k-kad",
            format!(
                "ED2K Kad could not bind UDP {}: {}",
                snapshot.udp_port,
                snapshot.last_error.as_deref().unwrap_or("unknown error")
            ),
            Some(HealthFix::open_pref("advanced")),
        )
        .with_details(details),
        KadState::Ready if snapshot.bound => HealthCheck::warn(
            "ed2k-kad",
            format!(
                "ED2K Kad is bound on UDP {} but has not learned any routing contacts yet",
                snapshot.udp_port
            ),
            None,
        )
        .with_details(details),
        KadState::Ready => HealthCheck::warn(
            "ed2k-kad",
            format!("ED2K Kad is not bound on UDP {}", snapshot.udp_port),
            Some(HealthFix::open_pref("advanced")),
        )
        .with_details(details),
        KadState::Stopped => HealthCheck::warn(
            "ed2k-kad",
            "ED2K Kad service is stopped",
            Some(HealthFix::restart_engine()),
        )
        .with_details(details),
    }
}

async fn probe_proxy_reachability(id: &str, label: &str, proxy_url: &str) -> HealthCheck {
    use risuko_http::{ClientBuilder, Method, Proxy};

    let proxy = match Proxy::all(proxy_url) {
        Ok(p) => p,
        Err(_e) => {
            return HealthCheck::fail(
                id,
                format!("{label} proxy URL could not be parsed"),
                Some(HealthFix::open_pref("advanced")),
            );
        }
    };
    let client = match ClientBuilder::new()
        .proxy(proxy)
        .timeout(PROXY_PROBE_TIMEOUT)
        .connect_timeout(PROXY_PROBE_TIMEOUT)
        .build()
    {
        Ok(c) => c,
        Err(_e) => {
            return HealthCheck::fail(
                id,
                format!("{label} proxy client could not be initialized"),
                Some(HealthFix::open_pref("advanced")),
            );
        }
    };
    match client.request(Method::HEAD, PROXY_PROBE_URL).send().await {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() || status.is_redirection() {
                HealthCheck::ok(
                    id,
                    format!(
                        "{label} proxy reachable ({}, HTTP {})",
                        redact_proxy_url(proxy_url),
                        status.as_u16()
                    ),
                )
            } else {
                HealthCheck::warn(
                    id,
                    format!(
                        "{label} proxy returned HTTP {} for probe ({})",
                        status.as_u16(),
                        redact_proxy_url(proxy_url)
                    ),
                    Some(HealthFix::open_pref("advanced")),
                )
            }
        }
        Err(_e) => HealthCheck::fail(
            id,
            format!(
                "{label} proxy probe failed via {}",
                redact_proxy_url(proxy_url)
            ),
            Some(HealthFix::open_pref("advanced")),
        ),
    }
}

async fn check_bittorrent(
    options: &EngineOptions,
    bt: Option<&BtHealthSnapshot>,
    tracker_urls: &[String],
    slow: bool,
) -> Vec<HealthCheck> {
    let mut out = Vec::new();

    const BOOTSTRAP_WINDOW_SECS: u64 = 60;
    let uptime_secs = engine::engine_uptime().map(|d| d.as_secs()).unwrap_or(0);
    let bootstrapping = uptime_secs < BOOTSTRAP_WINDOW_SECS;

    out.push(HealthCheck::ok(
        "encryption-policy",
        format!("Encryption: {}", options.bt_encryption_policy()),
    ));

    let upnp_cfg = options.bt_enable_upnp();
    match (upnp_cfg, bt) {
        (false, _) => out.push(HealthCheck::skipped(
            "upnp",
            "UPnP port forwarding disabled",
        )),
        (true, None) => out.push(HealthCheck::skipped(
            "upnp",
            "UPnP enabled (BT session not started yet)",
        )),
        (true, Some(snap)) if snap.upnp_mappings > 0 => out.push(
            HealthCheck::ok(
                "upnp",
                format!("UPnP active — {} mapping(s) confirmed", snap.upnp_mappings),
            )
            .with_details(serde_json::json!({
                "mappings": snap.upnp_mappings,
                "listenPort": snap.listen_port,
            })),
        ),
        (true, Some(snap)) if snap.upnp_attempts == 0 => out.push(HealthCheck::skipped(
            "upnp",
            "UPnP negotiating with router…",
        )),
        (true, Some(_)) => out.push(HealthCheck::warn(
            "upnp",
            "UPnP enabled but no IGD responded — router may not support UPnP or has it disabled",
            None,
        )),
    }

    let lsd_cfg = options.bt_enable_lsd();
    match (lsd_cfg, bt) {
        (false, _) => out.push(HealthCheck::skipped(
            "lsd",
            "Local Service Discovery disabled",
        )),
        (true, None) => out.push(HealthCheck::skipped(
            "lsd",
            "LSD enabled (BT session not started yet)",
        )),
        (true, Some(snap)) if snap.lsd_active => {
            out.push(HealthCheck::ok("lsd", "Local Service Discovery active"))
        }
        (true, Some(_)) => out.push(HealthCheck::warn(
            "lsd",
            "LSD enabled but listener failed to start (no multicast?)",
            None,
        )),
    }

    if let Some(snap) = bt {
        out.push(
            HealthCheck::ok(
                "bt-session",
                format!(
                    "BT listening on port {} — {} torrent(s) loaded",
                    snap.listen_port, snap.torrents
                ),
            )
            .with_details(serde_json::json!({
                "listenPort": snap.listen_port,
                "torrents": snap.torrents,
            })),
        );

        let dht_check = if !snap.dht_active {
            HealthCheck::skipped("dht", "DHT disabled")
        } else if snap.dht_nodes == 0 && bootstrapping {
            HealthCheck::skipped("dht", "DHT bootstrapping…")
        } else if snap.dht_nodes == 0 {
            HealthCheck::warn(
                "dht",
                "DHT enabled but no nodes in routing table (UDP blocked?)",
                None,
            )
        } else if snap.dht_nodes < 8 {
            HealthCheck::warn(
                "dht",
                format!("DHT routing table sparse: {} node(s)", snap.dht_nodes),
                None,
            )
        } else {
            HealthCheck::ok(
                "dht",
                format!("DHT healthy — {} nodes in routing table", snap.dht_nodes),
            )
        };
        out.push(dht_check.with_details(serde_json::json!({
            "active": snap.dht_active,
            "nodes": snap.dht_nodes,
        })));
    }

    let max_peers = options.bt_max_peers_per_torrent();
    let max_outstanding = options.bt_max_outstanding_per_peer();
    out.push(
        HealthCheck::ok(
            "bt-tuning",
            format!(
                "Max peers/torrent: {}, max outstanding/peer: {}",
                fmt_opt(max_peers, "default"),
                fmt_opt(max_outstanding, "default"),
            ),
        )
        .with_details(serde_json::json!({
            "maxPeersPerTorrent": max_peers,
            "maxOutstandingPerPeer": max_outstanding,
        })),
    );

    if tracker_urls.is_empty() {
        out.push(HealthCheck::skipped(
            "trackers",
            "No active BT torrents — tracker probe skipped",
        ));
    } else if !slow {
        out.push(
            HealthCheck::skipped(
                "trackers",
                format!(
                    "{} unique tracker(s) configured — enable deep probes to test reachability",
                    tracker_urls.len()
                ),
            )
            .with_details(serde_json::json!({ "trackers": tracker_urls.len() })),
        );
    } else {
        out.push(probe_trackers(tracker_urls).await);
    }

    out
}

async fn probe_trackers(urls: &[String]) -> HealthCheck {
    use risuko_http::{ClientBuilder, Method};
    use tokio::task::JoinSet;

    const MAX_CONCURRENCY: usize = 8;
    const PER_TRACKER_TIMEOUT: Duration = Duration::from_secs(3);

    let client = match ClientBuilder::new()
        .timeout(PER_TRACKER_TIMEOUT)
        .connect_timeout(PER_TRACKER_TIMEOUT)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return HealthCheck::fail("trackers", format!("Probe client init failed: {e}"), None);
        }
    };

    #[derive(Default)]
    struct Counts {
        ok: usize,
        warn: usize,
        fail: usize,
        skipped: usize,
    }

    async fn probe_one(client: risuko_http::Client, url: String) -> &'static str {
        let lower = url.to_ascii_lowercase();
        if lower.starts_with("udp://") || lower.starts_with("ws://") || lower.starts_with("wss://")
        {
            return "skipped";
        }
        if !(lower.starts_with("http://") || lower.starts_with("https://")) {
            return "skipped";
        }
        match client.request(Method::HEAD, &url).send().await {
            Ok(resp) => {
                let s = resp.status();
                if s.is_success() || s.is_redirection() || s.as_u16() == 400 {
                    "ok"
                } else {
                    "warn"
                }
            }
            Err(_) => "fail",
        }
    }

    let mut counts = Counts::default();
    let mut iter = urls.iter().cloned();
    let mut set: JoinSet<&'static str> = JoinSet::new();
    for _ in 0..MAX_CONCURRENCY {
        if let Some(url) = iter.next() {
            let c = client.clone();
            set.spawn(async move { probe_one(c, url).await });
        } else {
            break;
        }
    }
    while let Some(joined) = set.join_next().await {
        let kind = joined.unwrap_or("fail");
        match kind {
            "ok" => counts.ok += 1,
            "warn" => counts.warn += 1,
            "fail" => counts.fail += 1,
            _ => counts.skipped += 1,
        }
        if let Some(url) = iter.next() {
            let c = client.clone();
            set.spawn(async move { probe_one(c, url).await });
        }
    }

    let total = counts.ok + counts.warn + counts.fail + counts.skipped;
    let details = serde_json::json!({
        "total": total,
        "ok": counts.ok,
        "warn": counts.warn,
        "fail": counts.fail,
        "skipped": counts.skipped,
    });
    let msg = format!(
        "Trackers: {} ok, {} warn, {} fail, {} skipped (of {})",
        counts.ok, counts.warn, counts.fail, counts.skipped, total
    );
    let check = if counts.fail > 0 && counts.ok == 0 {
        HealthCheck::fail("trackers", msg, None)
    } else if counts.fail > 0 || counts.warn > 0 {
        HealthCheck::warn("trackers", msg, None)
    } else if counts.ok == 0 {
        HealthCheck::skipped("trackers", msg)
    } else {
        HealthCheck::ok("trackers", msg)
    };
    check.with_details(details)
}

fn fmt_opt<T: std::fmt::Display>(v: Option<T>, fallback: &str) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| fallback.into())
}

fn check_disk(
    options: &EngineOptions,
    user_cfg: &serde_json::Map<String, Value>,
) -> Vec<HealthCheck> {
    let mut out = Vec::new();

    let dir = options.dir();
    let dir_check = probe_dir("download-dir", &dir, true);
    out.push(dir_check);

    let alloc = options.get_str("file-allocation").unwrap_or("falloc");
    out.push(HealthCheck::ok(
        "file-allocation",
        format!("File allocation: {}", alloc),
    ));

    if let Some(arr) = user_cfg
        .get("recent-saved-paths")
        .and_then(|v| v.as_array())
    {
        for (i, v) in arr.iter().enumerate() {
            let Some(path) = v.as_str() else { continue };
            if path == dir.as_str() {
                continue;
            }
            let id = format!("recent-dir-{}", i);
            let mut probed = probe_dir(&id, path, false);
            if probed.status == HealthStatus::Fail {
                probed.status = HealthStatus::Warn;
            }
            out.push(probed);
        }
    }

    out
}

fn probe_dir(id: &str, path: &str, write_test: bool) -> HealthCheck {
    if path.is_empty() {
        return HealthCheck::fail(
            id,
            "Download directory is not configured",
            Some(HealthFix::open_pref("basic")),
        );
    }
    let p = Path::new(path);
    if !p.exists() {
        return HealthCheck::fail(
            id,
            format!("Path does not exist: {}", path),
            Some(HealthFix::open_pref("basic")),
        );
    }
    if !p.is_dir() {
        return HealthCheck::fail(
            id,
            format!("Path is not a directory: {}", path),
            Some(HealthFix::open_pref("basic")),
        );
    }

    let writable = if write_test {
        tempfile::NamedTempFile::new_in(p)
            .and_then(|mut probe| {
                probe.write_all(b"probe")?;
                probe.flush()
            })
            .is_ok()
    } else {
        p.metadata()
            .map(|meta| !meta.permissions().readonly())
            .unwrap_or(false)
    };
    if !writable {
        return HealthCheck::fail(
            id,
            format!("Directory is not writable: {}", path),
            Some(HealthFix::open_pref("basic")),
        );
    }

    let (free, total) = (fs4::available_space(p).ok(), fs4::total_space(p).ok());
    let mut details = serde_json::json!({ "path": path });
    if let (Some(free_b), Some(total_b)) = (free, total) {
        details["freeBytes"] = serde_json::json!(free_b);
        details["totalBytes"] = serde_json::json!(total_b);

        let pct = if total_b > 0 {
            (free_b as f64 / total_b as f64) * 100.0
        } else {
            100.0
        };
        let msg = format!(
            "{} — {} free of {} ({:.1}%)",
            path,
            crate::cli::progress::format_size(free_b),
            crate::cli::progress::format_size(total_b),
            pct
        );
        if free_b < 1024 * 1024 * 1024 || pct < 5.0 {
            return HealthCheck::fail(id, msg, Some(HealthFix::open_pref("basic")))
                .with_details(details);
        }
        if free_b < 5 * 1024 * 1024 * 1024 || pct < 10.0 {
            return HealthCheck::warn(id, msg, None).with_details(details);
        }
        return HealthCheck::ok(id, msg).with_details(details);
    }

    HealthCheck::ok(id, format!("{} writable (free space unknown)", path)).with_details(details)
}

fn check_system(autostart: bool, prevent_sleep_while_downloading: bool) -> Vec<HealthCheck> {
    let mut out = Vec::new();
    if autostart {
        out.push(HealthCheck::ok("autostart", "Launch at login enabled"));
    } else {
        out.push(HealthCheck::skipped(
            "autostart",
            "Launch at login disabled",
        ));
    }

    if !prevent_sleep_while_downloading {
        out.push(HealthCheck::skipped(
            "sleep-inhibit",
            "Sleep inhibit disabled in preferences",
        ));
    } else if sleep_inhibit_active() {
        out.push(HealthCheck::ok("sleep-inhibit", "Sleep inhibit active"));
    } else {
        out.push(HealthCheck::skipped(
            "sleep-inhibit",
            "Sleep inhibit enabled (will activate during downloads)",
        ));
    }
    out
}

fn autostart_enabled(handle: &AppHandle) -> bool {
    #[cfg(target_os = "android")]
    {
        let _ = handle;
        false
    }
    #[cfg(not(target_os = "android"))]
    {
        handle.autolaunch().is_enabled().unwrap_or(false)
    }
}

fn check_config(
    system: &serde_json::Map<String, Value>,
    user: &serde_json::Map<String, Value>,
    snapshot: Option<&HashMap<String, Value>>,
) -> Vec<HealthCheck> {
    let mut out = Vec::new();

    let Some(snap) = snapshot else {
        out.push(HealthCheck::skipped(
            "startup-key-drift",
            "Engine has not started yet",
        ));
        return out;
    };

    let mut drifted: Vec<String> = Vec::new();
    for key in engine::STARTUP_ONLY_KEYS {
        let current = system
            .get(*key)
            .or_else(|| user.get(*key))
            .cloned()
            .unwrap_or(Value::Null);
        let prev = snap.get(*key).cloned().unwrap_or(Value::Null);
        if current != prev {
            drifted.push((*key).to_string());
        }
    }

    if drifted.is_empty() {
        out.push(HealthCheck::ok(
            "startup-key-drift",
            "All startup-only keys match the running engine",
        ));
    } else {
        out.push(
            HealthCheck::warn(
                "startup-key-drift",
                format!(
                    "{} startup-only key(s) changed since the engine started — restart to apply",
                    drifted.len()
                ),
                Some(HealthFix::restart_engine()),
            )
            .with_details(serde_json::json!({ "keys": drifted })),
        );
    }

    out
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFileSummary {
    pub name: String,
    pub date: String,
    pub size_bytes: u64,
    pub modified_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    pub line_number: usize,
    pub timestamp: Option<String>,
    pub level: String,
    pub message: String,
    pub raw: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogReadResult {
    pub name: String,
    pub entries: Vec<LogEntry>,
    pub truncated: bool,
    pub bytes_read: u64,
    pub total_bytes: u64,
    pub total_lines: usize,
    pub returned_lines: usize,
}

fn log_file_date(name: &str) -> Option<&str> {
    let date = name
        .strip_prefix("risuko.")
        .and_then(|rest| rest.strip_suffix(".log"))
        .or_else(|| name.strip_prefix("risuko.log."))?;
    if date.len() != 10
        || date.as_bytes().get(4) != Some(&b'-')
        || date.as_bytes().get(7) != Some(&b'-')
        || !date
            .bytes()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
    {
        return None;
    }
    let year = date[0..4].parse::<u16>().ok()?;
    let month = date[5..7].parse::<u8>().ok()?;
    let day = date[8..10].parse::<u8>().ok()?;
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        _ => return None,
    };
    if day == 0 || day > days_in_month {
        return None;
    }
    Some(date)
}

fn is_supported_log_name(name: &str) -> bool {
    log_file_date(name).is_some()
}

fn modified_at_ms(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn authorized_log_path(log_dir: &Path, name: &str) -> Result<std::path::PathBuf, String> {
    let requested = Path::new(name);
    if requested.file_name().and_then(|value| value.to_str()) != Some(name)
        || !is_supported_log_name(name)
    {
        return Err("Invalid log file name".to_string());
    }

    let root =
        std::fs::canonicalize(log_dir).map_err(|e| format!("Log directory unavailable: {e}"))?;
    let path = log_dir.join(name);
    let metadata =
        std::fs::symlink_metadata(&path).map_err(|e| format!("Log file unavailable: {e}"))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err("Log file is not a regular file".to_string());
    }
    let canonical =
        std::fs::canonicalize(&path).map_err(|e| format!("Log file unavailable: {e}"))?;
    if canonical.parent() != Some(root.as_path()) {
        return Err("Log file is outside the log directory".to_string());
    }
    Ok(canonical)
}

fn open_authorized_log(path: &Path) -> Result<File, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    #[cfg(windows)]
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);

    let file = options
        .open(path)
        .map_err(|e| format!("Failed to open log file: {e}"))?;
    let metadata = file
        .metadata()
        .map_err(|e| format!("Failed to inspect log file: {e}"))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err("Log file is not a regular file".to_string());
    }
    #[cfg(windows)]
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err("Log file is a reparse point".to_string());
    }
    Ok(file)
}

#[tauri::command(async)]
pub fn list_log_files(state: State<'_, AppState>) -> Result<Vec<LogFileSummary>, String> {
    let root = std::fs::canonicalize(&state.log_dir)
        .map_err(|e| format!("Log directory unavailable: {e}"))?;
    let mut files = Vec::new();
    let entries = std::fs::read_dir(&root).map_err(|e| format!("Failed to list log files: {e}"))?;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(date) = log_file_date(&name).map(str::to_string) else {
            continue;
        };
        let metadata = match std::fs::symlink_metadata(entry.path()) {
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
            {
                metadata
            }
            _ => continue,
        };
        let canonical = match std::fs::canonicalize(entry.path()) {
            Ok(path) if path.parent() == Some(root.as_path()) => path,
            _ => continue,
        };
        let _ = canonical;
        files.push(LogFileSummary {
            name,
            date,
            size_bytes: metadata.len(),
            modified_at_ms: modified_at_ms(&metadata),
        });
    }
    files.sort_by(|left, right| {
        right
            .modified_at_ms
            .cmp(&left.modified_at_ms)
            .then_with(|| right.name.cmp(&left.name))
    });
    files.truncate(MAX_LOG_FILES);
    Ok(files)
}

#[tauri::command(async)]
pub fn read_log_file(
    state: State<'_, AppState>,
    name: String,
    levels: Option<Vec<String>>,
) -> Result<LogReadResult, String> {
    let path = authorized_log_path(&state.log_dir, &name)?;
    let mut file = open_authorized_log(&path)?;
    let total_bytes = file
        .metadata()
        .map_err(|e| format!("Failed to stat log file: {e}"))?
        .len();
    let start = total_bytes.saturating_sub(MAX_LOG_READ_BYTES);
    let starts_mid_line = if start == 0 {
        false
    } else {
        file.seek(SeekFrom::Start(start - 1))
            .map_err(|e| format!("Failed to inspect log file boundary: {e}"))?;
        let mut previous = [0u8; 1];
        file.read_exact(&mut previous)
            .map_err(|e| format!("Failed to inspect log file boundary: {e}"))?;
        previous[0] != b'\n'
    };
    file.seek(SeekFrom::Start(start))
        .map_err(|e| format!("Failed to seek log file: {e}"))?;
    let mut bytes = Vec::with_capacity((total_bytes - start) as usize);
    file.take(MAX_LOG_READ_BYTES)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("Failed to read log file: {e}"))?;

    Ok(parse_log_bytes(
        name,
        bytes,
        total_bytes,
        start > 0,
        starts_mid_line,
        levels,
    ))
}

fn parse_log_bytes(
    name: String,
    mut bytes: Vec<u8>,
    total_bytes: u64,
    mut truncated: bool,
    mut starts_mid_line: bool,
    levels: Option<Vec<String>>,
) -> LogReadResult {
    if bytes.len() as u64 > MAX_LOG_READ_BYTES {
        let start = bytes.len() - MAX_LOG_READ_BYTES as usize;
        bytes = bytes.split_off(start);
        truncated = true;
        starts_mid_line = true;
    }

    if starts_mid_line {
        if let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
            bytes.drain(..=newline);
        } else {
            bytes.clear();
        }
    }

    let ends_with_newline = bytes.last() == Some(&b'\n');
    let text = String::from_utf8_lossy(&bytes);
    let mut lines: Vec<String> = if bytes.is_empty() {
        Vec::new()
    } else {
        text.split('\n')
            .map(|line| line.strip_suffix('\r').unwrap_or(line).to_string())
            .collect()
    };
    if ends_with_newline {
        lines.pop();
    } else if !lines.is_empty() {
        lines.pop();
        truncated = true;
    }
    let total_lines = lines.len();
    if lines.len() > MAX_LOG_READ_LINES {
        truncated = true;
        lines.drain(..lines.len() - MAX_LOG_READ_LINES);
    }

    let level_filter = levels.map(|values| {
        values
            .into_iter()
            .filter_map(|value| normalize_level(&value).map(str::to_string))
            .collect::<HashSet<_>>()
    });
    let mut entries = Vec::with_capacity(lines.len());
    for (index, raw) in lines.into_iter().enumerate() {
        let (timestamp, level, message) = parse_log_line(&raw);
        if level_filter
            .as_ref()
            .is_some_and(|filter| !filter.is_empty() && !filter.contains(&level))
        {
            continue;
        }
        entries.push(LogEntry {
            line_number: index + 1,
            timestamp,
            level,
            message,
            raw,
        });
    }

    let returned_lines = entries.len();
    LogReadResult {
        name,
        entries,
        truncated,
        bytes_read: bytes.len() as u64,
        total_bytes,
        total_lines,
        returned_lines,
    }
}

fn normalize_level(value: &str) -> Option<&'static str> {
    let value = value
        .trim()
        .trim_matches(|c: char| !c.is_ascii_alphabetic());
    const LEVELS: [(&str, &str); 8] = [
        ("trace", "trace"),
        ("debug", "debug"),
        ("info", "info"),
        ("warn", "warn"),
        ("warning", "warn"),
        ("error", "error"),
        ("err", "error"),
        ("unknown", "unknown"),
    ];
    LEVELS
        .iter()
        .find(|(name, _)| value.eq_ignore_ascii_case(name))
        .map(|(_, level)| *level)
}

fn scan_log_line(raw: &str) -> (Option<&str>, &'static str, Option<usize>) {
    let mut timestamp = None;
    let mut level = "unknown";
    let mut level_end = None;
    let mut tokens = raw.split_whitespace();
    let first = tokens.next();
    if first.is_some_and(|value| {
        value.len() >= 10
            && value.as_bytes().get(4) == Some(&b'-')
            && value.as_bytes().get(7) == Some(&b'-')
    }) {
        timestamp = first;
    }

    let level_token = if timestamp.is_some() {
        tokens.next()
    } else {
        first
    };
    if let Some(token) = level_token {
        if let Some(parsed) = normalize_level(token) {
            level = parsed;
            let start = token.as_ptr() as usize - raw.as_ptr() as usize;
            level_end = Some(start + token.len());
        }
    }
    (timestamp, level, level_end)
}

fn parse_log_line(raw: &str) -> (Option<String>, String, String) {
    let (timestamp, level, level_end) = scan_log_line(raw);
    let message = level_end
        .and_then(|end| raw.get(end..))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(raw)
        .to_string();
    (timestamp.map(str::to_string), level.to_string(), message)
}

fn check_logs(log_dir: &Path) -> Vec<HealthCheck> {
    let mut out = Vec::new();

    if !log_dir.exists() {
        out.push(HealthCheck::warn(
            "log-dir",
            format!("Log directory missing: {}", log_dir.display()),
            None,
        ));
        return out;
    }
    out.push(HealthCheck::ok(
        "log-dir",
        format!("Log directory: {}", log_dir.display()),
    ));

    let latest = std::fs::read_dir(log_dir)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            is_supported_log_name(&name)
                && e.file_type()
                    .map(|kind| kind.is_file() && !kind.is_symlink())
                    .unwrap_or(false)
        })
        .filter_map(|e| {
            let path = e.path();
            let mtime = e.metadata().and_then(|m| m.modified()).ok()?;
            Some((path, mtime))
        })
        .max_by_key(|(_, m)| *m);

    let Some((path, _)) = latest else {
        out.push(HealthCheck::skipped("log-file", "No log files yet"));
        return out;
    };

    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let (errs, warns) = tail_count_levels(&path).unwrap_or((0, 0));

    let details = serde_json::json!({
        "path": path.to_string_lossy(),
        "sizeBytes": size,
        "errorCount": errs,
        "warnCount": warns,
    });

    let msg = format!(
        "{} — {} (errors: {}, warnings: {})",
        path.file_name().unwrap_or_default().to_string_lossy(),
        crate::cli::progress::format_size(size),
        errs,
        warns
    );

    let check = if errs > 0 || warns > 5 {
        HealthCheck::warn("log-file", msg, Some(HealthFix::open_log_dir()))
    } else {
        HealthCheck::ok("log-file", msg)
    };
    out.push(check.with_details(details));
    out
}

#[derive(Clone)]
struct ToolFacts {
    yt_dlp: Option<String>,
    ffmpeg: Option<String>,
}

const TOOLS_CACHE_TTL: Duration = Duration::from_secs(300);
const TOOLS_MISSING_RETRY: Duration = Duration::from_secs(10);

static TOOLS_CACHE: std::sync::Mutex<Option<(Instant, ToolFacts)>> = std::sync::Mutex::new(None);

fn cached_tool_facts() -> Option<ToolFacts> {
    let cache = TOOLS_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let (at, facts) = cache.as_ref()?;
    let ttl = if facts.yt_dlp.is_some() && facts.ffmpeg.is_some() {
        TOOLS_CACHE_TTL
    } else {
        TOOLS_MISSING_RETRY
    };
    (at.elapsed() < ttl).then(|| facts.clone())
}

async fn probe_tool_facts() -> ToolFacts {
    let yt_dlp = tokio::process::Command::new("yt-dlp")
        .arg("--version")
        .output()
        .await
        .ok()
        .filter(|o| o.status.success())
        .map(|o| {
            let version = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if version.is_empty() {
                "unknown".to_string()
            } else {
                version
            }
        });

    let ffmpeg = match media::find_ffmpeg().await {
        Some(path) => Some(
            tokio::process::Command::new(&path)
                .arg("-version")
                .output()
                .await
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .and_then(|s| s.lines().next().map(|l| l.trim().to_string()))
                .unwrap_or_else(|| "unknown".to_string()),
        ),
        None => None,
    };
    ToolFacts { yt_dlp, ffmpeg }
}

async fn check_tools() -> Vec<HealthCheck> {
    let facts = match cached_tool_facts() {
        Some(facts) => facts,
        None => {
            let facts = probe_tool_facts().await;
            *TOOLS_CACHE.lock().unwrap_or_else(|e| e.into_inner()) =
                Some((Instant::now(), facts.clone()));
            facts
        }
    };
    tool_checks(&facts)
}

fn tool_checks(facts: &ToolFacts) -> Vec<HealthCheck> {
    let mut out = Vec::new();

    match &facts.yt_dlp {
        Some(version) => out.push(
            HealthCheck::ok("yt-dlp", format!("yt-dlp available ({})", version))
                .with_details(serde_json::json!({ "version": version })),
        ),
        None => out.push(HealthCheck::fail(
            "yt-dlp",
            "yt-dlp not found in PATH — media-site downloads will fail",
            None,
        )),
    }

    match &facts.ffmpeg {
        Some(version) => out.push(
            HealthCheck::ok("ffmpeg", format!("ffmpeg available ({version})"))
                .with_details(serde_json::json!({ "version": version })),
        ),
        None => out.push(HealthCheck::warn(
            "ffmpeg",
            "ffmpeg not found in PATH — media downloads fall back to single-file \
             quality (no video+audio merge)",
            None,
        )),
    }

    out
}

fn tail_count_levels(path: &Path) -> std::io::Result<(usize, usize)> {
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    let start = len.saturating_sub(LOG_TAIL_BYTES);
    f.seek(SeekFrom::Start(start))?;
    let mut buf = Vec::with_capacity((len - start) as usize);
    f.take(LOG_TAIL_BYTES).read_to_end(&mut buf)?;
    let text = String::from_utf8_lossy(&buf);

    let mut errors = 0usize;
    let mut warnings = 0usize;
    for line in text.lines() {
        match scan_log_line(line).1 {
            "error" => errors += 1,
            "warn" => warnings += 1,
            _ => {}
        }
    }
    Ok((errors, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map};

    fn kad_options(enabled: bool, port: Value) -> EngineOptions {
        let mut system = Map::new();
        system.insert("ed2k-enable-kad".into(), Value::Bool(enabled));
        system.insert("ed2k-kad-port".into(), port);
        EngineOptions::from_config(&system, &Map::new())
    }

    fn kad_snapshot(state: KadState, routing_contacts: usize) -> KadHealthSnapshot {
        KadHealthSnapshot {
            enabled: !matches!(state, KadState::Disabled),
            bound: !matches!(
                state,
                KadState::Disabled | KadState::Error | KadState::Stopped
            ),
            state,
            udp_port: 4672,
            node_id: "00112233445566778899aabbccddeeff".into(),
            routing_contacts,
            cached_contacts: routing_contacts.saturating_add(2),
            last_bootstrap_at_ms: Some(1_000),
            last_lookup_at_ms: Some(2_000),
            last_lookup_success: Some(matches!(state, KadState::Ready)),
            last_error: if matches!(state, KadState::Error) {
                Some("bind failed".into())
            } else {
                None
            },
        }
    }

    #[test]
    fn worst_status_picks_highest_rank() {
        assert_eq!(
            HealthStatus::Ok.worst(HealthStatus::Warn),
            HealthStatus::Warn
        );
        assert_eq!(
            HealthStatus::Fail.worst(HealthStatus::Warn),
            HealthStatus::Fail
        );
        assert_eq!(
            HealthStatus::Skipped.worst(HealthStatus::Ok),
            HealthStatus::Skipped
        );
    }

    #[test]
    fn category_status_rolls_up_to_worst() {
        let cat = HealthCategory::from_checks(
            "x",
            vec![
                HealthCheck::ok("a", "ok"),
                HealthCheck::warn("b", "warn", None),
            ],
        );
        assert_eq!(cat.status, HealthStatus::Warn);
    }

    #[test]
    fn proxy_diagnostics_redact_userinfo() {
        let redacted = redact_proxy_url("socks5h://alice:s3cret@proxy.example:1080/path");
        assert_eq!(redacted, "socks5h://proxy.example:1080/path");
        assert!(!redacted.contains("alice"));
        assert!(!redacted.contains("s3cret"));

        let malformed = redact_proxy_url("http://alice:s3cret@proxy.example:bad");
        assert_eq!(malformed, "http://proxy.example:bad");
        assert!(!malformed.contains("s3cret"));
    }

    #[test]
    fn proxy_diagnostics_never_echoes_unparseable_credentials() {
        let redacted = redact_proxy_url("proxy-without-scheme alice:s3cret");
        assert_eq!(redacted, "<invalid proxy>");
        assert!(!redacted.contains("s3cret"));
    }

    #[test]
    fn kad_health_classifies_disabled_invalid_and_runtime_states() {
        let disabled = check_ed2k_kad(&kad_options(false, json!(4672)), None);
        assert_eq!(disabled.status, HealthStatus::Skipped);

        let disabled_invalid = check_ed2k_kad(&kad_options(false, json!(0)), None);
        assert_eq!(disabled_invalid.status, HealthStatus::Fail);

        let invalid = check_ed2k_kad(&kad_options(true, json!(0)), None);
        assert_eq!(invalid.status, HealthStatus::Fail);
        assert!(invalid.message.contains("configuration error"));

        let not_running = check_ed2k_kad(&kad_options(true, json!(4672)), None);
        assert_eq!(not_running.status, HealthStatus::Warn);
        assert!(not_running.fix.is_some());

        let cases = [
            (KadState::Disabled, 0, HealthStatus::Warn),
            (KadState::Bootstrapping, 0, HealthStatus::Warn),
            (KadState::Searching, 2, HealthStatus::Warn),
            (KadState::Ready, 0, HealthStatus::Warn),
            (KadState::Ready, 4, HealthStatus::Ok),
            (KadState::Timeout, 1, HealthStatus::Warn),
            (KadState::Error, 0, HealthStatus::Fail),
            (KadState::Stopped, 0, HealthStatus::Warn),
        ];
        for (state, contacts, expected) in cases {
            let check = check_ed2k_kad(
                &kad_options(true, json!(4672)),
                Some(&kad_snapshot(state, contacts)),
            );
            assert_eq!(
                check.status, expected,
                "state={state:?}, contacts={contacts}"
            );
            assert!(
                check.details.is_some(),
                "state={state:?} should expose details"
            );
        }

        let mut lookup_error = kad_snapshot(KadState::Error, 3);
        lookup_error.bound = true;
        let lookup_check = check_ed2k_kad(&kad_options(true, json!(4672)), Some(&lookup_error));
        assert_eq!(lookup_check.status, HealthStatus::Warn);
        assert!(lookup_check.message.contains("lookup failed"));

        let searching = kad_snapshot(KadState::Searching, 2);
        let searching_check = check_ed2k_kad(&kad_options(true, json!(4672)), Some(&searching));
        assert!(searching_check.message.contains("searching"));
        assert!(!searching_check.message.contains("bootstrapping"));

        let ready_without_contacts = kad_snapshot(KadState::Ready, 0);
        let ready_check = check_ed2k_kad(
            &kad_options(true, json!(4672)),
            Some(&ready_without_contacts),
        );
        assert!(ready_check.message.contains("bound on UDP"));
        assert!(ready_check.message.contains("routing contacts"));
        assert!(!ready_check.message.contains("not bound"));

        let running = kad_snapshot(KadState::Ready, 3);
        let drift_check = check_ed2k_kad(&kad_options(false, json!(4672)), Some(&running));
        assert_eq!(drift_check.status, HealthStatus::Warn);
        assert!(drift_check.message.contains("remains bound"));
    }

    #[test]
    fn kad_health_details_include_runtime_counters() {
        let check = check_ed2k_kad(
            &kad_options(true, json!(4672)),
            Some(&kad_snapshot(KadState::Ready, 5)),
        );
        let details = check.details.expect("Kad details");
        assert_eq!(details.get("enabled"), Some(&json!(true)));
        assert_eq!(details.get("bound"), Some(&json!(true)));
        assert_eq!(details.get("port"), Some(&json!(4672)));
        assert_eq!(details.get("routingContacts"), Some(&json!(5)));
        assert_eq!(details.get("cachedContacts"), Some(&json!(7)));
        assert_eq!(details.get("lastBootstrapAtMs"), Some(&json!(1000)));
        assert_eq!(details.get("lastLookupAtMs"), Some(&json!(2000)));
        assert_eq!(details.get("lastLookupSuccess"), Some(&json!(true)));
    }

    #[test]
    fn scan_log_line_finds_level_without_allocating_message() {
        let (ts, level, end) = scan_log_line("2026-01-02T03:04:05Z  WARN risuko: slow disk");
        assert_eq!(ts, Some("2026-01-02T03:04:05Z"));
        assert_eq!(level, "warn");
        assert!(end.is_some());
        assert_eq!(scan_log_line("no level here").1, "unknown");
    }

    #[test]
    fn tool_checks_reflect_missing_and_found_tools() {
        let checks = tool_checks(&ToolFacts {
            yt_dlp: Some("2026.01.01".into()),
            ffmpeg: None,
        });
        assert_eq!(checks[0].id, "yt-dlp");
        assert_eq!(checks[0].status, HealthStatus::Ok);
        assert_eq!(checks[1].id, "ffmpeg");
        assert_eq!(checks[1].status, HealthStatus::Warn);
    }

    #[test]
    fn probe_dir_without_write_test_leaves_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let check = probe_dir("recent", dir.path().to_str().unwrap(), false);
        assert_ne!(check.status, HealthStatus::Fail);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn tail_count_levels_counts_warn_and_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");
        let body = "2026-05-03 INFO  ok\n2026-05-03 WARN  bad\n2026-05-03 ERROR boom\n2026-05-03 ERROR boom2\n";
        std::fs::write(&path, body).unwrap();
        let (errs, warns) = tail_count_levels(&path).unwrap();
        assert_eq!(errs, 2);
        assert_eq!(warns, 1);
    }

    #[test]
    fn log_file_names_support_current_and_legacy_formats_only() {
        assert_eq!(log_file_date("risuko.2026-08-09.log"), Some("2026-08-09"));
        assert_eq!(log_file_date("risuko.log.2026-08-09"), Some("2026-08-09"));
        assert!(log_file_date("risuko.log").is_none());
        assert!(log_file_date("risuko.2026-99-99.log").is_none());
        assert!(log_file_date("risuko.2026-02-29.log").is_none());
        assert!(log_file_date("risuko.2024-02-29.log").is_some());
        assert!(log_file_date("risuko.2026-08-09.log.bak").is_none());
    }

    #[test]
    fn parses_and_normalizes_log_levels() {
        let (timestamp, level, message) =
            parse_log_line("2026-08-09T10:00:00Z WARN retrying request");
        assert_eq!(timestamp.as_deref(), Some("2026-08-09T10:00:00Z"));
        assert_eq!(level, "warn");
        assert_eq!(message, "retrying request");

        let (_, level, message) = parse_log_line("no level here");
        assert_eq!(level, "unknown");
        assert_eq!(message, "no level here");

        let (_, level, message) = parse_log_line("2026-08-09 INFO request failed with error");
        assert_eq!(level, "info");
        assert_eq!(message, "request failed with error");

        let (_, level, message) = parse_log_line("[WARN] retrying request");
        assert_eq!(level, "warn");
        assert_eq!(message, "retrying request");
    }

    #[test]
    fn authorized_log_path_rejects_traversal_and_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("risuko.2026-08-09.log");
        std::fs::write(&path, "INFO ok\n").unwrap();
        assert!(authorized_log_path(dir.path(), "../risuko.2026-08-09.log").is_err());
        assert!(authorized_log_path(dir.path(), "risuko.2026-08-09.log").is_ok());

        let outside = tempfile::NamedTempFile::new().unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), dir.path().join("risuko.2026-08-08.log"))
            .unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(
            outside.path(),
            dir.path().join("risuko.2026-08-08.log"),
        )
        .unwrap();
        assert!(authorized_log_path(dir.path(), "risuko.2026-08-08.log").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn no_follow_open_rejects_a_path_replaced_after_authorization() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("risuko.2026-08-09.log");
        std::fs::write(&path, "INFO safe\n").unwrap();
        let canonical = std::fs::canonicalize(&path).unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(outside.path(), &path).unwrap();

        assert!(open_authorized_log(&canonical).is_err());
    }

    #[test]
    fn log_reads_are_byte_and_line_bounded_and_drop_partial_tail_line() {
        let mut bytes = vec![b'x'; MAX_LOG_READ_BYTES as usize + 32];
        bytes.extend_from_slice(b"\n2026-08-09 INFO newest\n");
        let result = parse_log_bytes(
            "risuko.2026-08-09.log".to_string(),
            bytes,
            MAX_LOG_READ_BYTES + 64,
            false,
            false,
            None,
        );

        assert!(result.truncated);
        assert!(result.bytes_read <= MAX_LOG_READ_BYTES);
        assert!(result
            .entries
            .iter()
            .all(|entry| !entry.raw.starts_with('x')));

        let many_lines = (0..(MAX_LOG_READ_LINES + 37))
            .map(|index| format!("2026-08-09 INFO line-{index}\n"))
            .collect::<String>();
        let result = parse_log_bytes(
            "risuko.2026-08-09.log".to_string(),
            many_lines.into_bytes(),
            0,
            false,
            false,
            None,
        );
        assert!(result.truncated);
        assert_eq!(result.entries.len(), MAX_LOG_READ_LINES);

        let result = parse_log_bytes(
            "risuko.2026-08-09.log".to_string(),
            b"2026-08-09 INFO complete\n2026-08-09 WARN incomplete".to_vec(),
            0,
            false,
            false,
            None,
        );
        assert!(result.truncated);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].message, "complete");

        let result = parse_log_bytes(
            "risuko.2026-08-09.log".to_string(),
            Vec::new(),
            0,
            false,
            false,
            None,
        );
        assert!(result.entries.is_empty());
        assert!(!result.truncated);
    }

    #[test]
    fn bounded_reader_never_collects_more_than_the_log_byte_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("risuko.2026-08-09.log");
        let total = MAX_LOG_READ_BYTES + 1024;
        std::fs::write(&path, vec![b'x'; total as usize]).unwrap();

        let mut file = std::fs::File::open(path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.take(MAX_LOG_READ_BYTES)
            .read_to_end(&mut bytes)
            .unwrap();

        assert_eq!(bytes.len(), MAX_LOG_READ_BYTES as usize);
    }

    #[test]
    fn truncated_log_tail_keeps_a_line_that_starts_on_a_boundary() {
        let result = parse_log_bytes(
            "risuko.2026-08-09.log".to_string(),
            b"2026-08-09 INFO first\n2026-08-09 WARN second\n".to_vec(),
            MAX_LOG_READ_BYTES + 1,
            true,
            false,
            None,
        );

        assert!(result.truncated);
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].message, "first");
    }

    #[test]
    fn log_reads_filter_normalized_levels_without_matching_message_words() {
        let bytes = b"2026-08-09 INFO request failed with error\n2026-08-09 WARN retry\n".to_vec();
        let result = parse_log_bytes(
            "risuko.2026-08-09.log".to_string(),
            bytes,
            0,
            false,
            false,
            Some(vec!["warning".to_string()]),
        );
        assert_eq!(result.returned_lines, 1);
        assert_eq!(result.entries[0].level, "warn");
        assert_eq!(result.entries[0].message, "retry");
    }
}
