use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::speed_limiter::parse_speed_limit;

pub const DEFAULT_ED2K_PORT: u16 = 4662;
pub const DEFAULT_ED2K_KAD_PORT: u16 = 4672;
pub const DEFAULT_PBH_LISTEN_PORT: u16 = 16801;
pub(crate) const TASK_P2P_PROXY_OVERRIDE_KEY: &str = "risuko-task-p2p-proxy-override";

fn is_reserved_engine_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    normalized.starts_with("rpc-")
        || normalized.ends_with("-port")
        || normalized.ends_with("-secret")
}

fn apply_engine_overrides(global: &mut Map<String, Value>, user: &Map<String, Value>) {
    if let Some(Value::Object(overrides)) = user.get("engine-overrides") {
        for (k, v) in overrides {
            if is_reserved_engine_key(k) {
                continue;
            }
            global.insert(k.clone(), v.clone());
        }
    } else if let Some(value) = user.get("engine-overrides") {
        tracing::warn!(
            "Ignoring invalid engine-overrides value: expected object, got {}",
            value
        );
    }
}

#[derive(Debug, Clone)]
pub struct PbhRpcConfig {
    pub port: u16,
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineOptions {
    pub global: Map<String, Value>,
}

impl EngineOptions {
    pub fn from_config(system: &Map<String, Value>, user: &Map<String, Value>) -> Self {
        let mut global = system.clone();

        for key in [
            "rpc-host",
            "m3u8-output-format",
            "keep-seeding",
            "bt-create-subfolder",
            "bt-enable-upnp",
            "bt-upnp-lease",
            "bt-encryption-policy",
            "bt-listen-v6",
            "bt-enable-lsd",
            "purge-record-on-start",
            "task-routing-rules",
            "file-category-dirs",
            "max-worker-retries",
            "usenet-profiles",
            "usenet-archive-limits",
            "usenet-cleanup-mode",
            "pbh-enable",
            "pbh-listen-port",
            "pbh-rpc-secret",
        ] {
            if let Some(v) = user.get(key) {
                global.insert(key.into(), v.clone());
            }
        }

        apply_engine_overrides(&mut global, user);

        if let Some(proxy) = user.get("proxy") {
            let normalized = crate::config::normalize_proxy_config(proxy);
            let http_profile_is_explicit = crate::config::proxy_http_profile_is_explicit(proxy);
            let http = normalized.get("http").and_then(Value::as_object);
            let http_enabled = http
                .and_then(|p| p.get("enable"))
                .is_some_and(value_as_bool);
            let http_server = http
                .and_then(|p| p.get("server"))
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or("");
            let http_active = http_enabled && !http_server.is_empty();
            let download = http
                .and_then(|p| p.get("scope"))
                .and_then(Value::as_array)
                .is_some_and(|scopes| scopes.iter().any(|v| v.as_str() == Some("download")));

            if http_enabled || http_profile_is_explicit {
                global.insert(
                    "all-proxy".into(),
                    Value::String(if http_active && download {
                        http_server.to_string()
                    } else {
                        String::new()
                    }),
                );
                global.insert(
                    "no-proxy".into(),
                    Value::String(if http_active && download {
                        http.and_then(|p| p.get("bypass"))
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string()
                    } else {
                        String::new()
                    }),
                );
            }

            if crate::config::proxy_p2p_profile_is_explicit(proxy) {
                let p2p = normalized.get("p2p").and_then(Value::as_object);
                let p2p_enabled = p2p.and_then(|p| p.get("enable")).is_some_and(value_as_bool);
                let p2p_server = p2p
                    .and_then(|p| p.get("server"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                let p2p_tcp_active = p2p_enabled && !p2p_server.is_empty();
                global.insert(
                    "p2p-proxy".into(),
                    Value::String(if p2p_tcp_active {
                        p2p_server.to_string()
                    } else {
                        String::new()
                    }),
                );
                global.insert(
                    "p2p-no-proxy".into(),
                    Value::String(if p2p_tcp_active {
                        p2p.and_then(|p| p.get("bypass"))
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string()
                    } else {
                        String::new()
                    }),
                );

                let udp = p2p.and_then(|p| p.get("udp")).and_then(Value::as_object);
                let udp_server_override = udp
                    .and_then(|profile| profile.get("server"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                let udp_server = if udp_server_override.is_empty() {
                    p2p_server
                } else {
                    udp_server_override
                };
                let udp_bypass = if udp_server_override.is_empty() {
                    p2p.and_then(|p| p.get("bypass"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                } else {
                    udp.and_then(|profile| profile.get("bypass"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                };
                let p2p_route_active = p2p_enabled && !udp_server.is_empty();
                global.insert(
                    "p2p-udp-proxy".into(),
                    Value::String(if p2p_route_active {
                        udp_server.to_string()
                    } else {
                        String::new()
                    }),
                );
                global.insert(
                    "p2p-udp-no-proxy".into(),
                    Value::String(if p2p_route_active {
                        udp_bypass.to_string()
                    } else {
                        String::new()
                    }),
                );
            }
        }

        Self { global }
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.global.get(key).and_then(|v| v.as_str())
    }

    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.global.get(key).and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.global.get(key).and_then(json_bool)
    }

    pub fn p2p_proxy_connector(&self) -> Result<risuko_http::ProxyConnector, String> {
        build_p2p_proxy_connector(
            self.get_str("p2p-proxy").unwrap_or(""),
            self.get_str("p2p-no-proxy").unwrap_or(""),
            self.get_str("p2p-udp-proxy").unwrap_or(""),
            self.get_str("p2p-udp-no-proxy").unwrap_or(""),
        )
    }

    pub fn set(&mut self, key: String, value: Value) {
        self.global.insert(key, value);
    }

    pub fn dir(&self) -> String {
        self.get_str("dir").unwrap_or(".").to_string()
    }

    pub fn max_concurrent_downloads(&self) -> usize {
        self.get_u64("max-concurrent-downloads").unwrap_or(5) as usize
    }

    pub fn max_overall_download_limit(&self) -> u64 {
        self.global
            .get("max-overall-download-limit")
            .map(parse_speed_limit)
            .unwrap_or(0)
    }

    pub fn max_overall_upload_limit(&self) -> u64 {
        self.global
            .get("max-overall-upload-limit")
            .map(parse_speed_limit)
            .unwrap_or(0)
    }

    pub fn rpc_listen_port(&self) -> u16 {
        self.get_u64("rpc-listen-port").unwrap_or(16800) as u16
    }

    pub fn rpc_host(&self) -> String {
        self.get_str("rpc-host").unwrap_or("127.0.0.1").to_string()
    }

    pub fn rpc_secret(&self) -> String {
        self.get_str("rpc-secret").unwrap_or("").to_string()
    }

    pub fn pbh_enable(&self) -> bool {
        self.get_bool("pbh-enable").unwrap_or(false)
    }

    pub fn pbh_listen_port_checked(&self) -> Result<u16, String> {
        let Some(value) = self.global.get("pbh-listen-port") else {
            return Ok(DEFAULT_PBH_LISTEN_PORT);
        };

        parse_port(value, "pbh-listen-port")
    }

    pub fn pbh_listen_port(&self) -> u16 {
        self.pbh_listen_port_checked()
            .unwrap_or(DEFAULT_PBH_LISTEN_PORT)
    }

    pub fn pbh_rpc_secret(&self) -> String {
        self.get_str("pbh-rpc-secret").unwrap_or("").to_string()
    }

    pub fn pbh_rpc_config(&self, rpc_port: u16) -> Result<Option<PbhRpcConfig>, String> {
        if !self.pbh_enable() {
            return Ok(None);
        }
        let port = self.pbh_listen_port_checked()?;
        if port == rpc_port {
            return Err(format!(
                "pbh-listen-port ({port}) must differ from rpc-listen-port ({rpc_port})"
            ));
        }
        let secret = self.pbh_rpc_secret();
        if secret.is_empty() {
            return Err("pbh-rpc-secret must be non-empty when pbh-enable is true".to_string());
        }
        Ok(Some(PbhRpcConfig { port, secret }))
    }

    pub fn seed_ratio(&self) -> f64 {
        self.global
            .get("seed-ratio")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0.0)
    }

    pub fn seed_time(&self) -> u64 {
        self.get_u64("seed-time").unwrap_or(0)
    }

    pub fn keep_seeding(&self) -> bool {
        self.get_bool("keep-seeding").unwrap_or(false)
    }

    pub fn bt_max_outstanding_per_peer(&self) -> Option<usize> {
        self.get_u64("bt-max-outstanding-per-peer")
            .filter(|&v| v != 0)
            .map(|v| v as usize)
    }

    pub fn bt_max_peers_per_torrent(&self) -> Option<usize> {
        self.get_u64("bt-max-peers-per-torrent")
            .filter(|&v| v != 0)
            .map(|v| v as usize)
    }

    pub fn bt_max_connections(&self) -> usize {
        self.get_u64("bt-max-connections")
            .map_or(400, |v| v.clamp(20, 5000) as usize)
    }

    pub fn bt_hash_fail_ban_strikes(&self) -> Option<u8> {
        if !self.get_bool("bt-ban-corrupt-peers").unwrap_or(true) {
            return None;
        }
        let strikes = self
            .get_u64("bt-ban-corrupt-strikes")
            .filter(|&v| v != 0)
            .unwrap_or(3);
        Some(strikes.min(u64::from(u8::MAX)) as u8)
    }

    pub fn bt_upload_rate_limit(&self) -> Option<u64> {
        self.global
            .get("bt-upload-rate-limit")
            .map(parse_speed_limit)
            .filter(|&v| v != 0)
    }

    pub fn effective_bt_upload_limit(&self) -> u64 {
        risuko_bt::limiter::tightest(
            self.bt_upload_rate_limit().unwrap_or(0),
            self.max_overall_upload_limit(),
        )
    }

    pub fn bt_enable_upnp(&self) -> bool {
        self.get_bool("bt-enable-upnp").unwrap_or(true)
    }

    pub fn bt_upnp_lease(&self) -> Option<std::time::Duration> {
        self.get_u64("bt-upnp-lease")
            .filter(|&v| v != 0)
            .map(std::time::Duration::from_secs)
    }
    pub fn bt_encryption_policy(&self) -> &'static str {
        match self.get_str("bt-encryption-policy").unwrap_or("prefer") {
            "plaintext" => "plaintext",
            "require" => "require",
            _ if self.get_bool("bt-force-encryption").unwrap_or(false) => "require",
            _ => "prefer",
        }
    }

    pub fn bt_listen_port(&self) -> Option<u16> {
        let raw = match self.global.get("listen-port")? {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => return None,
        };
        raw.split([',', '-'])
            .next()
            .and_then(|first| first.trim().parse::<u16>().ok())
            .filter(|&port| port != 0)
    }

    pub fn bt_listen_v6(&self) -> bool {
        self.get_bool("bt-listen-v6").unwrap_or(false)
    }

    pub fn bt_enable_lsd(&self) -> bool {
        self.get_bool("bt-enable-lsd").unwrap_or(true)
    }

    pub fn purge_record_on_start(&self) -> bool {
        self.get_bool("purge-record-on-start").unwrap_or(false)
    }

    pub fn ed2k_servers(&self) -> Vec<String> {
        self.get_str("ed2k-server")
            .unwrap_or("")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    pub fn ed2k_port(&self) -> u16 {
        self.get_u64("ed2k-port")
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port != 0)
            .unwrap_or(DEFAULT_ED2K_PORT)
    }

    pub fn ed2k_enable_kad(&self) -> bool {
        self.get_bool("ed2k-enable-kad").unwrap_or(true)
    }

    pub fn ed2k_kad_port_checked(&self) -> Result<u16, String> {
        let Some(value) = self.global.get("ed2k-kad-port") else {
            return Ok(DEFAULT_ED2K_KAD_PORT);
        };

        parse_port(value, "ed2k-kad-port")
    }

    pub fn ed2k_kad_port(&self) -> u16 {
        self.ed2k_kad_port_checked()
            .unwrap_or(DEFAULT_ED2K_KAD_PORT)
    }

    pub fn task_routing_rules(&self) -> Vec<super::routing::TaskRoutingRule> {
        self.global
            .get("task-routing-rules")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }

    pub fn file_category_dirs(&self) -> std::collections::HashMap<String, String> {
        self.global
            .get("file-category-dirs")
            .and_then(|v| {
                if let Value::Object(map) = v {
                    let mut result = std::collections::HashMap::new();
                    for (k, val) in map {
                        if let Some(s) = val.as_str() {
                            result.insert(k.clone(), s.to_string());
                        }
                    }
                    Some(result)
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    pub fn merge_task_options(&self, task_opts: &Map<String, Value>) -> Map<String, Value> {
        let mut merged = self.global.clone();
        for (k, v) in task_opts {
            merged.insert(k.clone(), v.clone());
        }

        let task_has_proxy = task_opts.contains_key("proxy");
        let task_proxy_has_nested_p2p = task_opts
            .get("proxy")
            .and_then(Value::as_object)
            .is_some_and(|proxy| proxy.contains_key("p2p"));
        let task_proxy_has_nested_p2p_udp = task_opts
            .get("proxy")
            .and_then(Value::as_object)
            .and_then(|proxy| proxy.get("p2p"))
            .and_then(Value::as_object)
            .is_some_and(|p2p| p2p.contains_key("udp"));
        let task_has_http_route = task_opts.contains_key("all-proxy")
            || task_opts.contains_key("no-proxy")
            || task_has_proxy;
        let task_has_p2p_route = task_opts.contains_key("p2p-proxy")
            || task_opts.contains_key("p2p-no-proxy")
            || task_opts.contains_key("p2p-udp-proxy")
            || task_opts.contains_key("p2p-udp-no-proxy")
            || task_proxy_has_nested_p2p;
        if task_has_proxy {
            match task_opts.get("proxy") {
                Some(Value::String(server)) => {
                    merged.insert("all-proxy".into(), Value::String(server.clone()));
                    merged.insert("p2p-proxy".into(), Value::String(server.clone()));
                    if !task_opts.contains_key("no-proxy") {
                        merged.insert("no-proxy".into(), Value::String(String::new()));
                    }
                    if !task_opts.contains_key("p2p-no-proxy") {
                        merged.insert("p2p-no-proxy".into(), Value::String(String::new()));
                    }
                    if !task_opts.contains_key("p2p-udp-proxy") {
                        merged.insert("p2p-udp-proxy".into(), Value::String(server.clone()));
                    }
                    if !task_opts.contains_key("p2p-udp-no-proxy") {
                        merged.insert("p2p-udp-no-proxy".into(), Value::String(String::new()));
                    }
                }
                Some(value @ Value::Object(_)) => {
                    let normalized = crate::config::normalize_proxy_config(value);
                    if let Some(http) = normalized.get("http").and_then(Value::as_object) {
                        let http_server = http
                            .get("server")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .unwrap_or("");
                        let http_active = value_as_bool(http.get("enable").unwrap_or(&Value::Null))
                            && !http_server.is_empty();
                        merged.insert(
                            "all-proxy".into(),
                            Value::String(if http_active {
                                http_server.to_string()
                            } else {
                                String::new()
                            }),
                        );
                        merged.insert(
                            "no-proxy".into(),
                            Value::String(if http_active {
                                http.get("bypass")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string()
                            } else {
                                String::new()
                            }),
                        );
                    }
                    if let Some(p2p) = normalized.get("p2p").and_then(Value::as_object) {
                        let p2p_server = p2p
                            .get("server")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .unwrap_or("");
                        let p2p_enabled = value_as_bool(p2p.get("enable").unwrap_or(&Value::Null));
                        let p2p_tcp_active = p2p_enabled && !p2p_server.is_empty();
                        merged.insert(
                            "p2p-proxy".into(),
                            Value::String(if p2p_tcp_active {
                                p2p_server.to_string()
                            } else {
                                String::new()
                            }),
                        );
                        merged.insert(
                            "p2p-no-proxy".into(),
                            Value::String(if p2p_tcp_active {
                                p2p.get("bypass")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string()
                            } else {
                                String::new()
                            }),
                        );

                        let udp = p2p.get("udp").and_then(Value::as_object);
                        let udp_override = udp
                            .and_then(|profile| profile.get("server"))
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .unwrap_or("");
                        let udp_server = if udp_override.is_empty() {
                            p2p_server
                        } else {
                            udp_override
                        };
                        let udp_bypass = if udp_override.is_empty() {
                            p2p.get("bypass").and_then(Value::as_str).unwrap_or("")
                        } else {
                            udp.and_then(|profile| profile.get("bypass"))
                                .and_then(Value::as_str)
                                .unwrap_or("")
                        };
                        let p2p_route_active = p2p_enabled && !udp_server.is_empty();
                        merged.insert(
                            "p2p-udp-proxy".into(),
                            Value::String(if p2p_route_active {
                                udp_server.to_string()
                            } else {
                                String::new()
                            }),
                        );
                        merged.insert(
                            "p2p-udp-no-proxy".into(),
                            Value::String(if p2p_route_active {
                                udp_bypass.to_string()
                            } else {
                                String::new()
                            }),
                        );
                    }
                }
                _ => {}
            }
        }
        if task_has_http_route && !task_has_p2p_route {
            if !task_opts.contains_key("p2p-proxy") {
                if let Some(value) = merged.get("all-proxy").cloned() {
                    merged.insert("p2p-proxy".into(), value);
                }
            }
            if !task_opts.contains_key("p2p-no-proxy") {
                if let Some(value) = merged.get("no-proxy").cloned() {
                    merged.insert("p2p-no-proxy".into(), value);
                }
            }
            if !task_opts.contains_key("p2p-udp-proxy") {
                if let Some(value) = merged.get("all-proxy").cloned() {
                    merged.insert("p2p-udp-proxy".into(), value);
                }
            }
            if !task_opts.contains_key("p2p-udp-no-proxy") {
                if let Some(value) = merged.get("no-proxy").cloned() {
                    merged.insert("p2p-udp-no-proxy".into(), value);
                }
            }
        }
        if task_has_p2p_route
            && !task_proxy_has_nested_p2p_udp
            && !task_opts.contains_key("p2p-udp-proxy")
            && !task_opts.contains_key("p2p-udp-no-proxy")
        {
            if let Some(value) = merged.get("p2p-proxy").cloned() {
                merged.insert("p2p-udp-proxy".into(), value);
            }
            if let Some(value) = merged.get("p2p-no-proxy").cloned() {
                merged.insert("p2p-udp-no-proxy".into(), value);
            }
        }
        if task_has_proxy || task_has_http_route || task_has_p2p_route {
            merged.insert(TASK_P2P_PROXY_OVERRIDE_KEY.to_string(), Value::Bool(true));
        }
        merged
    }
}

pub fn json_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(b) => Some(*b),
        Value::Number(n) => n
            .as_i64()
            .map(|v| v != 0)
            .or_else(|| n.as_u64().map(|v| v != 0)),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn parse_port(value: &Value, key: &str) -> Result<u16, String> {
    let parsed = match value {
        Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| format!("invalid {key} value {number}: expected an integer")),
        Value::String(text) => text
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("invalid {key} value {text:?}: expected an integer in 1..=65535")),
        other => Err(format!(
            "invalid {key} value {other}: expected an integer in 1..=65535"
        )),
    }?;

    if parsed == 0 || parsed > u16::MAX as u64 {
        return Err(format!(
            "invalid {key} value {parsed}: expected an integer in 1..=65535"
        ));
    }

    Ok(parsed as u16)
}

fn value_as_bool(value: &Value) -> bool {
    json_bool(value).unwrap_or(false)
}

pub(crate) fn build_p2p_proxy_connector(
    tcp_server: &str,
    tcp_bypass: &str,
    udp_server: &str,
    udp_bypass: &str,
) -> Result<risuko_http::ProxyConnector, String> {
    let tcp_server = tcp_server.trim();
    let tcp_bypass = tcp_bypass.trim();
    let udp_override = udp_server.trim();
    let effective_udp_server = if udp_override.is_empty() {
        tcp_server
    } else {
        udp_override
    };
    let effective_udp_bypass = if udp_override.is_empty() {
        tcp_bypass
    } else {
        udp_bypass.trim()
    };

    let tcp = if tcp_server.is_empty() {
        risuko_http::ProxyConnector::direct()
    } else {
        let proxy = risuko_http::Proxy::all_with_bypass(tcp_server, tcp_bypass)
            .map_err(|error| format!("invalid P2P TCP proxy: {error}"))?;
        risuko_http::ProxyConnector::from_proxy(proxy)
    };

    if effective_udp_server.is_empty()
        || (effective_udp_server == tcp_server && effective_udp_bypass == tcp_bypass)
    {
        return Ok(tcp);
    }

    let udp_proxy = risuko_http::Proxy::all_with_bypass(effective_udp_server, effective_udp_bypass)
        .map_err(|error| format!("invalid P2P UDP proxy: {error}"))?;
    let udp = risuko_http::ProxyConnector::from_proxy(udp_proxy);
    Ok(tcp.with_udp_proxy(Some(udp)))
}

#[cfg(test)]
mod tests {
    #[test]
    fn effective_bt_upload_limit_takes_the_smaller_non_zero() {
        let mut opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert_eq!(opts.effective_bt_upload_limit(), 0);
        opts.set("max-overall-upload-limit".into(), json!("1M"));
        assert_eq!(opts.effective_bt_upload_limit(), 1024 * 1024);
        opts.set("bt-upload-rate-limit".into(), json!(512 * 1024));
        assert_eq!(opts.effective_bt_upload_limit(), 512 * 1024);
        opts.set("bt-upload-rate-limit".into(), json!("2M"));
        assert_eq!(opts.effective_bt_upload_limit(), 1024 * 1024);
        opts.set("max-overall-upload-limit".into(), json!(0));
        assert_eq!(opts.effective_bt_upload_limit(), 2 * 1024 * 1024);
    }

    use super::*;
    use serde_json::json;

    fn make_system() -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("dir".into(), json!("/downloads"));
        m.insert("max-concurrent-downloads".into(), json!(3));
        m.insert("rpc-listen-port".into(), json!(16800));
        m.insert("rpc-secret".into(), json!("secret123"));
        m.insert("seed-ratio".into(), json!("1.5"));
        m.insert("ed2k-server".into(), json!("srv1,srv2"));
        m.insert("ed2k-port".into(), json!(5662));
        m
    }

    fn make_user() -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("rpc-host".into(), json!("0.0.0.0"));
        m.insert("m3u8-output-format".into(), json!("mp4"));
        m.insert("dir".into(), json!("/user-override"));
        m
    }

    #[test]
    fn from_config_copies_system_keys() {
        let opts = EngineOptions::from_config(&make_system(), &Map::new());
        assert_eq!(opts.dir(), "/downloads");
        assert_eq!(opts.max_concurrent_downloads(), 3);
    }

    #[test]
    fn from_config_applies_user_overrides() {
        let opts = EngineOptions::from_config(&make_system(), &make_user());
        assert_eq!(opts.rpc_host(), "0.0.0.0");
        assert_eq!(opts.get_str("m3u8-output-format"), Some("mp4"));
        assert_eq!(opts.dir(), "/downloads");
    }

    #[test]
    fn from_config_applies_engine_overrides_map() {
        let mut user = Map::new();
        user.insert(
            "engine-overrides".into(),
            json!({
                "dir": "/override-dir",
                "max-concurrent-downloads": 12,
                "rpc-host": "10.0.0.2",
                "rpc-listen-port": 17000,
                "rpc-secret": "override-secret"
            }),
        );

        let opts = EngineOptions::from_config(&make_system(), &user);
        assert_eq!(opts.dir(), "/override-dir");
        assert_eq!(opts.max_concurrent_downloads(), 12);
        assert_eq!(opts.rpc_host(), "127.0.0.1");
        assert_eq!(opts.rpc_listen_port(), 16800);
        assert_eq!(opts.rpc_secret(), "secret123");
    }

    #[test]
    fn from_config_preserves_system_http_proxy_for_nested_default_profile() {
        let mut system = make_system();
        system.insert("all-proxy".into(), json!("http://system-proxy:8080"));
        system.insert("no-proxy".into(), json!("localhost,127.0.0.1"));
        let user = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "http": {
                    "enable": false,
                    "server": "",
                    "bypass": "",
                    "scope": ["download", "update-app", "update-trackers"]
                },
                "p2p": {
                    "enable": false,
                    "server": "",
                    "bypass": ""
                }
            }
        }))
        .unwrap();

        let opts = EngineOptions::from_config(&system, &user);
        assert_eq!(opts.get_str("all-proxy"), Some("http://system-proxy:8080"));
        assert_eq!(opts.get_str("no-proxy"), Some("localhost,127.0.0.1"));
    }

    #[test]
    fn from_config_preserves_system_p2p_proxy_for_nested_default_profile() {
        let mut system = make_system();
        system.insert("p2p-proxy".into(), json!("socks5://system-p2p:1080"));
        system.insert("p2p-no-proxy".into(), json!("localhost"));
        system.insert(
            "p2p-udp-proxy".into(),
            json!("socks5h://system-p2p-udp:1080"),
        );
        system.insert("p2p-udp-no-proxy".into(), json!("127.0.0.1"));
        let user = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "http": {
                    "enable": false,
                    "server": "",
                    "bypass": "",
                    "scope": ["download", "update-app", "update-trackers"]
                },
                "p2p": {
                    "enable": false,
                    "server": "",
                    "bypass": "",
                    "udp": {
                        "server": "",
                        "bypass": ""
                    }
                }
            }
        }))
        .unwrap();

        let opts = EngineOptions::from_config(&system, &user);
        assert_eq!(opts.get_str("p2p-proxy"), Some("socks5://system-p2p:1080"));
        assert_eq!(opts.get_str("p2p-no-proxy"), Some("localhost"));
        assert_eq!(
            opts.get_str("p2p-udp-proxy"),
            Some("socks5h://system-p2p-udp:1080")
        );
        assert_eq!(opts.get_str("p2p-udp-no-proxy"), Some("127.0.0.1"));
    }

    #[test]
    fn from_config_legacy_disabled_proxy_clears_system_http_route() {
        let mut system = make_system();
        system.insert("all-proxy".into(), json!("http://system-proxy:8080"));
        system.insert("no-proxy".into(), json!("localhost"));
        let user = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "enable": false,
                "server": "",
                "bypass": "",
                "scope": ["download"]
            }
        }))
        .unwrap();

        let opts = EngineOptions::from_config(&system, &user);
        assert_eq!(opts.get_str("all-proxy"), Some(""));
        assert_eq!(opts.get_str("no-proxy"), Some(""));
    }

    #[test]
    fn getter_defaults_when_empty() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert_eq!(opts.dir(), ".");
        assert_eq!(opts.max_concurrent_downloads(), 5);
        assert_eq!(opts.rpc_listen_port(), 16800);
        assert_eq!(opts.rpc_host(), "127.0.0.1");
        assert_eq!(opts.rpc_secret(), "");
        assert_eq!(opts.seed_ratio(), 0.0);
        assert_eq!(opts.seed_time(), 0);
        assert!(opts.ed2k_servers().is_empty());
        assert_eq!(opts.ed2k_port(), 4662);
        assert!(opts.ed2k_enable_kad());
        assert_eq!(opts.ed2k_kad_port_checked().unwrap(), 4672);
        assert_eq!(opts.ed2k_kad_port(), 4672);
        assert_eq!(opts.pbh_listen_port_checked().unwrap(), 16801);
        assert_eq!(opts.pbh_listen_port(), 16801);
        assert!(opts.pbh_rpc_config(16800).unwrap().is_none());
    }

    #[test]
    fn p2p_proxy_connector_rejects_unsupported_schemes() {
        let mut system = Map::new();
        system.insert("p2p-proxy".into(), json!("https://proxy.example:443"));
        let opts = EngineOptions::from_config(&system, &Map::new());
        let error = opts
            .p2p_proxy_connector()
            .expect_err("https proxies are unsupported");
        assert!(error.contains("unsupported") || error.contains("not yet supported"));
    }

    #[test]
    fn p2p_proxy_connector_is_direct_when_profile_is_empty() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert!(opts.p2p_proxy_connector().unwrap().proxy().is_none());
    }

    #[test]
    fn p2p_proxy_connector_uses_an_independent_udp_profile() {
        let user = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "p2p": {
                    "enable": true,
                    "server": "http://tcp.example:8080",
                    "bypass": "tcp.example",
                    "udp": {
                        "server": "socks5h://udp.example:1080",
                        "bypass": "udp.example"
                    }
                }
            }
        }))
        .unwrap();
        let opts = EngineOptions::from_config(&Map::new(), &user);
        assert_eq!(opts.get_str("p2p-proxy"), Some("http://tcp.example:8080"));
        assert_eq!(
            opts.get_str("p2p-udp-proxy"),
            Some("socks5h://udp.example:1080")
        );
        let connector = opts.p2p_proxy_connector().unwrap();
        assert!(connector.proxy().is_some());
        assert!(connector.udp_proxy().is_some());
        assert!(connector.supports_udp());
        assert!(connector
            .no_proxy()
            .unwrap()
            .matches_host_port("tcp.example", Some(80)));
        assert!(connector
            .udp_no_proxy()
            .unwrap()
            .matches_host_port("udp.example", Some(80)));
    }

    #[test]
    fn udp_only_p2p_profile_keeps_the_udp_route() {
        let user = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "p2p": {
                    "enable": true,
                    "server": "",
                    "udp": {
                        "server": "socks5h://udp.example:1080",
                        "bypass": "localhost"
                    }
                }
            }
        }))
        .unwrap();
        let opts = EngineOptions::from_config(&Map::new(), &user);
        assert_eq!(opts.get_str("p2p-proxy"), Some(""));
        assert_eq!(
            opts.get_str("p2p-udp-proxy"),
            Some("socks5h://udp.example:1080")
        );
        let connector = opts.p2p_proxy_connector().unwrap();
        assert!(connector.proxy().is_none());
        assert!(connector.udp_proxy().is_some());
        assert!(connector.supports_udp());
    }

    #[test]
    fn serverless_p2p_profile_has_no_effective_bypass() {
        let user = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "p2p": {
                    "enable": true,
                    "server": "",
                    "bypass": "localhost"
                }
            }
        }))
        .unwrap();

        let opts = EngineOptions::from_config(&Map::new(), &user);
        assert_eq!(opts.get_str("p2p-proxy"), Some(""));
        assert_eq!(opts.get_str("p2p-no-proxy"), Some(""));
    }

    #[test]
    fn getter_values_from_config() {
        let opts = EngineOptions::from_config(&make_system(), &Map::new());
        assert_eq!(opts.rpc_secret(), "secret123");
        assert_eq!(opts.seed_ratio(), 1.5);
        assert_eq!(opts.ed2k_servers(), vec!["srv1", "srv2"]);
        assert_eq!(opts.ed2k_port(), 5662);
    }

    #[test]
    fn legacy_aria2_keys_are_kept_and_settable() {
        let mut sys = Map::new();
        sys.insert("continue".into(), json!(false));
        sys.insert("follow-torrent".into(), json!("mem"));
        let mut opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(opts.get_bool("continue"), Some(false));
        assert_eq!(opts.get_str("follow-torrent"), Some("mem"));
        opts.set("allow-overwrite".into(), json!(true));
        opts.set("bt-enable-lpd".into(), json!(false));
        assert_eq!(opts.get_bool("allow-overwrite"), Some(true));
        assert_eq!(opts.get_bool("bt-enable-lpd"), Some(false));
    }

    #[test]
    fn kad_options_parse_and_validate_port() {
        let mut sys = Map::new();
        sys.insert("ed2k-enable-kad".into(), json!(false));
        sys.insert("ed2k-kad-port".into(), json!(5000));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert!(!opts.ed2k_enable_kad());
        assert_eq!(opts.ed2k_kad_port_checked().unwrap(), 5000);

        for invalid in [json!(0), json!(65536), json!("not-a-port"), json!(-1)] {
            let mut sys = Map::new();
            sys.insert("ed2k-kad-port".into(), invalid);
            let opts = EngineOptions::from_config(&sys, &Map::new());
            assert!(opts.ed2k_kad_port_checked().is_err());
            assert_eq!(opts.ed2k_kad_port(), 4672);
        }
    }

    #[test]
    fn pbh_options_validate_port_secret_and_collision() {
        let mut sys = Map::new();
        sys.insert("pbh-enable".into(), json!(true));
        sys.insert("pbh-listen-port".into(), json!(16802));
        sys.insert("pbh-rpc-secret".into(), json!("token"));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        let cfg = opts.pbh_rpc_config(16800).unwrap().unwrap();
        assert_eq!(cfg.port, 16802);
        assert_eq!(cfg.secret, "token");

        for invalid in [json!(0), json!(65536), json!("not-a-port"), json!(-1)] {
            let mut sys = Map::new();
            sys.insert("pbh-listen-port".into(), invalid);
            let opts = EngineOptions::from_config(&sys, &Map::new());
            assert!(opts.pbh_listen_port_checked().is_err());
            assert_eq!(opts.pbh_listen_port(), DEFAULT_PBH_LISTEN_PORT);
        }

        let mut sys = Map::new();
        sys.insert("pbh-enable".into(), json!(true));
        sys.insert("pbh-rpc-secret".into(), json!("token"));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        let err = opts.pbh_rpc_config(16801).unwrap_err();
        assert!(err.contains("must differ"));

        let mut sys = Map::new();
        sys.insert("pbh-enable".into(), json!(true));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        let err = opts.pbh_rpc_config(16800).unwrap_err();
        assert!(err.contains("pbh-rpc-secret"));
    }

    #[test]
    fn from_config_forwards_user_pbh_overrides() {
        let mut user = Map::new();
        user.insert("pbh-enable".into(), json!(true));
        user.insert("pbh-listen-port".into(), json!(16802));
        user.insert("pbh-rpc-secret".into(), json!("user-token"));
        let opts = EngineOptions::from_config(&Map::new(), &user);
        let cfg = opts.pbh_rpc_config(16800).unwrap().unwrap();
        assert_eq!(cfg.port, 16802);
        assert_eq!(cfg.secret, "user-token");
    }

    #[test]
    fn get_u64_parses_string() {
        let mut sys = Map::new();
        sys.insert("rpc-listen-port".into(), json!("9999"));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(opts.rpc_listen_port(), 9999);
    }

    #[test]
    fn set_overrides_value() {
        let mut opts = EngineOptions::from_config(&make_system(), &Map::new());
        opts.set("dir".into(), json!("/new"));
        assert_eq!(opts.dir(), "/new");
    }

    #[test]
    fn merge_task_options_overrides_globals() {
        let opts = EngineOptions::from_config(&make_system(), &Map::new());
        let mut task = Map::new();
        task.insert("dir".into(), json!("/task-dir"));
        task.insert("out".into(), json!("file.zip"));

        let merged = opts.merge_task_options(&task);
        assert_eq!(merged.get("dir").unwrap(), "/task-dir");
        assert_eq!(merged.get("out").unwrap(), "file.zip");
        assert_eq!(merged.get("rpc-secret").unwrap(), "secret123");
    }

    #[test]
    fn merge_task_options_empty_task_returns_globals() {
        let mut global = make_system();
        global.insert("all-proxy".into(), json!("http://http-profile:8080"));
        global.insert("p2p-proxy".into(), json!("socks5://p2p-profile:1080"));
        let opts = EngineOptions::from_config(&global, &Map::new());
        let merged = opts.merge_task_options(&Map::new());
        assert_eq!(merged.get("dir").unwrap(), "/downloads");
        assert_eq!(
            merged.get("all-proxy"),
            Some(&json!("http://http-profile:8080"))
        );
        assert_eq!(
            merged.get("p2p-proxy"),
            Some(&json!("socks5://p2p-profile:1080"))
        );
        assert!(merged.get(TASK_P2P_PROXY_OVERRIDE_KEY).is_none());
    }

    #[test]
    fn merge_task_options_preserves_explicit_nested_p2p_profile() {
        let mut global = make_system();
        global.insert("p2p-proxy".into(), json!("socks5://global:1080"));
        let opts = EngineOptions::from_config(&global, &Map::new());
        let task = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "http": {
                    "enable": true,
                    "server": "http://task:8080"
                },
                "p2p": {
                    "enable": false,
                    "server": "socks5://ignored:1080"
                }
            }
        }))
        .unwrap();

        let merged = opts.merge_task_options(&task);
        assert_eq!(merged.get("all-proxy"), Some(&json!("http://task:8080")));
        assert_eq!(merged.get("p2p-proxy"), Some(&json!("")));
        assert_eq!(merged.get(TASK_P2P_PROXY_OVERRIDE_KEY), Some(&json!(true)));
    }

    #[test]
    fn merge_task_options_preserves_nested_udp_override() {
        let mut global = make_system();
        global.insert("p2p-proxy".into(), json!("socks5://global:1080"));
        global.insert("p2p-udp-proxy".into(), json!("socks5://global-udp:1080"));
        let opts = EngineOptions::from_config(&global, &Map::new());
        let task = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "p2p": {
                    "enable": true,
                    "server": "http://task-tcp:8080",
                    "bypass": "task-tcp.example",
                    "udp": {
                        "server": "socks5h://task-udp:1080",
                        "bypass": "task-udp.example"
                    }
                }
            }
        }))
        .unwrap();

        let merged = opts.merge_task_options(&task);
        assert_eq!(
            merged.get("p2p-proxy"),
            Some(&json!("http://task-tcp:8080"))
        );
        assert_eq!(merged.get("p2p-no-proxy"), Some(&json!("task-tcp.example")));
        assert_eq!(
            merged.get("p2p-udp-proxy"),
            Some(&json!("socks5h://task-udp:1080"))
        );
        assert_eq!(
            merged.get("p2p-udp-no-proxy"),
            Some(&json!("task-udp.example"))
        );
    }

    #[test]
    fn merge_task_options_nested_empty_udp_inherits_tcp_route() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        let task = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "p2p": {
                    "enable": true,
                    "server": "socks5://task:1080",
                    "bypass": "task.example",
                    "udp": {
                        "server": "",
                        "bypass": "ignored.example"
                    }
                }
            }
        }))
        .unwrap();

        let merged = opts.merge_task_options(&task);
        assert_eq!(
            merged.get("p2p-udp-proxy"),
            Some(&json!("socks5://task:1080"))
        );
        assert_eq!(merged.get("p2p-udp-no-proxy"), Some(&json!("task.example")));
    }

    #[test]
    fn merge_task_options_treats_serverless_profiles_as_direct() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        let task = serde_json::from_value::<Map<String, Value>>(json!({
            "proxy": {
                "http": {
                    "enable": true,
                    "server": "",
                    "bypass": "http.example"
                },
                "p2p": {
                    "enable": true,
                    "server": "",
                    "bypass": "peer.example"
                }
            }
        }))
        .unwrap();

        let merged = opts.merge_task_options(&task);
        assert_eq!(merged.get("all-proxy"), Some(&json!("")));
        assert_eq!(merged.get("no-proxy"), Some(&json!("")));
        assert_eq!(merged.get("p2p-proxy"), Some(&json!("")));
        assert_eq!(merged.get("p2p-no-proxy"), Some(&json!("")));
    }

    #[test]
    fn get_bool_accepts_native_strings_and_numbers() {
        let mut sys = Map::new();
        sys.insert("a".into(), json!(true));
        sys.insert("b".into(), json!("true"));
        sys.insert("c".into(), json!("yes"));
        sys.insert("d".into(), json!("1"));
        sys.insert("e".into(), json!(1));
        sys.insert("f".into(), json!(false));
        sys.insert("g".into(), json!("false"));
        sys.insert("h".into(), json!("no"));
        sys.insert("i".into(), json!("0"));
        sys.insert("j".into(), json!(0));
        sys.insert("k".into(), json!("garbage"));
        sys.insert("l".into(), json!(u64::MAX));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        for k in ["a", "b", "c", "d", "e", "l"] {
            assert_eq!(opts.get_bool(k), Some(true), "{k}");
        }
        for k in ["f", "g", "h", "i", "j"] {
            assert_eq!(opts.get_bool(k), Some(false), "{k}");
        }
        assert_eq!(opts.get_bool("k"), None);
        assert_eq!(opts.get_bool("missing"), None);
    }

    #[test]
    fn parse_port_rejects_out_of_range_and_non_integers() {
        assert_eq!(parse_port(&json!("6800"), "k"), Ok(6800));
        assert_eq!(parse_port(&json!(65535), "k"), Ok(65535));
        assert!(parse_port(&json!(0), "k").is_err());
        assert!(parse_port(&json!(70000), "k").is_err());
        assert!(parse_port(&json!("x"), "k").is_err());
        assert!(parse_port(&json!(1.5), "k").is_err());
        assert!(parse_port(&json!(true), "k").is_err());
    }

    #[test]
    fn json_bool_matches_value_as_bool() {
        for v in [
            json!(" TRUE "),
            json!("On"),
            json!("yes"),
            json!(-1),
            json!(2),
        ] {
            assert_eq!(json_bool(&v), Some(true), "{v}");
            assert!(value_as_bool(&v), "{v}");
        }
        for v in [json!(" False"), json!("OFF"), json!("no"), json!(0)] {
            assert_eq!(json_bool(&v), Some(false), "{v}");
            assert!(!value_as_bool(&v), "{v}");
        }
        for v in [json!("maybe"), json!(1.5), json!(null), json!([true])] {
            assert_eq!(json_bool(&v), None, "{v}");
            assert!(!value_as_bool(&v), "{v}");
        }
    }

    #[test]
    fn bt_enable_upnp_defaults_true() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert!(opts.bt_enable_upnp());
        let mut sys = Map::new();
        sys.insert("bt-enable-upnp".into(), json!(false));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert!(!opts.bt_enable_upnp());
    }

    #[test]
    fn bt_hash_fail_ban_defaults_on_and_can_be_disabled() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert_eq!(opts.bt_hash_fail_ban_strikes(), Some(3));
        let mut sys = Map::new();
        sys.insert("bt-ban-corrupt-strikes".into(), json!(5));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(opts.bt_hash_fail_ban_strikes(), Some(5));
        sys.insert("bt-ban-corrupt-strikes".into(), json!(1000));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(opts.bt_hash_fail_ban_strikes(), Some(u8::MAX));
        sys.insert("bt-ban-corrupt-peers".into(), json!(false));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(opts.bt_hash_fail_ban_strikes(), None);
    }

    #[test]
    fn bt_max_connections_defaults_and_clamps() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert_eq!(opts.bt_max_connections(), 400);
        for (input, want) in [
            (json!(250), 250),
            (json!("250"), 250),
            (json!(0), 20),
            (json!(5), 20),
            (json!(20), 20),
            (json!(5000), 5000),
            (json!(1_000_000), 5000),
            (json!("junk"), 400),
        ] {
            let mut sys = Map::new();
            sys.insert("bt-max-connections".into(), input);
            let opts = EngineOptions::from_config(&sys, &Map::new());
            assert_eq!(opts.bt_max_connections(), want);
        }
    }

    #[test]
    fn bt_upnp_lease_zero_means_default() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert_eq!(opts.bt_upnp_lease(), None);
        let mut sys = Map::new();
        sys.insert("bt-upnp-lease".into(), json!(0));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(opts.bt_upnp_lease(), None);
        let mut sys = Map::new();
        sys.insert("bt-upnp-lease".into(), json!(120));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(
            opts.bt_upnp_lease(),
            Some(std::time::Duration::from_secs(120))
        );
    }

    #[test]
    fn bt_encryption_policy_normalises_unknown_to_prefer() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert_eq!(opts.bt_encryption_policy(), "prefer");
        for (set, want) in [
            ("plaintext", "plaintext"),
            ("prefer", "prefer"),
            ("require", "require"),
            ("nonsense", "prefer"),
            ("REQUIRE", "prefer"),
        ] {
            let mut sys = Map::new();
            sys.insert("bt-encryption-policy".into(), json!(set));
            let opts = EngineOptions::from_config(&sys, &Map::new());
            assert_eq!(opts.bt_encryption_policy(), want, "set={set}");
        }
    }

    #[test]
    fn bt_force_encryption_upgrades_prefer_to_require() {
        let mut sys = Map::new();
        sys.insert("bt-force-encryption".into(), json!(true));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(opts.bt_encryption_policy(), "require");
        sys.insert("bt-encryption-policy".into(), json!("plaintext"));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert_eq!(opts.bt_encryption_policy(), "plaintext");
    }

    #[test]
    fn bt_listen_port_parses_numbers_and_ranges() {
        let port = |v: Value| {
            let mut sys = Map::new();
            sys.insert("listen-port".into(), v);
            EngineOptions::from_config(&sys, &Map::new()).bt_listen_port()
        };
        assert_eq!(port(json!(21301)), Some(21301));
        assert_eq!(port(json!("6881-6999")), Some(6881));
        assert_eq!(port(json!("6881,6890")), Some(6881));
        assert_eq!(port(json!("0")), None);
        assert_eq!(port(json!("junk")), None);
    }

    #[test]
    fn bt_listen_v6_defaults_false() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert!(!opts.bt_listen_v6());
        let mut sys = Map::new();
        sys.insert("bt-listen-v6".into(), json!(true));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert!(opts.bt_listen_v6());
    }

    #[test]
    fn bt_enable_lsd_defaults_true() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert!(opts.bt_enable_lsd());
        let mut sys = Map::new();
        sys.insert("bt-enable-lsd".into(), json!(false));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert!(!opts.bt_enable_lsd());
    }

    #[test]
    fn purge_record_on_start_defaults_false() {
        let opts = EngineOptions::from_config(&Map::new(), &Map::new());
        assert!(!opts.purge_record_on_start());
    }

    #[test]
    fn purge_record_on_start_from_bool_value() {
        let mut sys = Map::new();
        sys.insert("purge-record-on-start".into(), json!(true));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert!(opts.purge_record_on_start());

        let mut sys = Map::new();
        sys.insert("purge-record-on-start".into(), json!(false));
        let opts = EngineOptions::from_config(&sys, &Map::new());
        assert!(!opts.purge_record_on_start());
    }

    #[test]
    fn purge_record_on_start_from_string_coercion() {
        for (set, want) in [
            ("true", true),
            ("false", false),
            ("yes", true),
            ("no", false),
            ("1", true),
            ("0", false),
        ] {
            let mut sys = Map::new();
            sys.insert("purge-record-on-start".into(), json!(set));
            let opts = EngineOptions::from_config(&sys, &Map::new());
            assert_eq!(
                opts.purge_record_on_start(),
                want,
                "string value '{set}' should coerce to {want}"
            );
        }
    }
}
