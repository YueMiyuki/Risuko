use serde_json::{json, Value};

#[derive(Debug, PartialEq, Eq)]
pub enum EngineStatus {
    Running,
    NotRunning,
    AuthFailed(String),
    Unreachable(String),
}

impl EngineStatus {
    pub fn is_present(&self) -> bool {
        !matches!(self, EngineStatus::NotRunning)
    }

    pub fn problem(&self) -> Option<String> {
        match self {
            EngineStatus::Running | EngineStatus::NotRunning => None,
            EngineStatus::AuthFailed(e) => Some(format!(
                "A Risuko instance is running but rejected the RPC secret ({e}); pass --rpc-secret or check rpc-secret in system.json"
            )),
            EngineStatus::Unreachable(e) => Some(format!(
                "A Risuko instance is listening but did not answer the RPC request: {e}"
            )),
        }
    }
}

pub struct RpcClient {
    host: String,
    port: u16,
    url: String,
    secret: Option<String>,
    client: risuko_http::Client,
    id_counter: std::sync::atomic::AtomicU64,
}

impl RpcClient {
    pub fn new_with_host(host: &str, port: u16, secret: Option<String>) -> Self {
        let client = risuko_http::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("Failed to build HTTP client with custom config");
        let connect_host = match host.trim() {
            "" | "0.0.0.0" | "::" | "[::]" => "127.0.0.1",
            h => h,
        };
        let url = if connect_host.contains(':') && !connect_host.starts_with('[') {
            format!("http://[{}]:{}/jsonrpc", connect_host, port)
        } else {
            format!("http://{}:{}/jsonrpc", connect_host, port)
        };
        Self {
            host: connect_host.to_string(),
            port,
            url,
            secret,
            client,
            id_counter: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub async fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, RpcError> {
        let id = self
            .id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let params = if let Some(ref secret) = self.secret {
            let mut p = vec![json!(format!("token:{}", secret))];
            p.extend(params);
            p
        } else {
            params
        };

        let body = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let resp = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| RpcError::Connection(e.to_string()))?;

        let result: Value = resp
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))?;

        if let Some(error) = result.get("error") {
            let message = error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown RPC error");
            return Err(RpcError::Rpc(message.to_string()));
        }

        result
            .get("result")
            .cloned()
            .ok_or_else(|| RpcError::Parse("Missing 'result' in RPC response".to_string()))
    }

    pub async fn engine_status(&self) -> EngineStatus {
        let connect = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::net::TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await;
        match connect {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                return EngineStatus::NotRunning
            }
            Ok(Err(e)) => return EngineStatus::Unreachable(e.to_string()),
            Err(_) => return EngineStatus::Unreachable("connect timed out".into()),
        }
        match self.call("risuko.getVersion", vec![]).await {
            Ok(_) => EngineStatus::Running,
            Err(RpcError::Rpc(e)) => EngineStatus::AuthFailed(e),
            Err(e) => EngineStatus::Unreachable(e.to_string()),
        }
    }

    pub async fn is_engine_running(&self) -> bool {
        self.engine_status().await.is_present()
    }
}

#[derive(Debug)]
pub enum RpcError {
    Connection(String),
    Parse(String),
    Rpc(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Connection(e) => write!(f, "Connection error: {}", e),
            RpcError::Parse(e) => write!(f, "Parse error: {}", e),
            RpcError::Rpc(e) => write!(f, "RPC error: {}", e),
        }
    }
}

impl std::error::Error for RpcError {}

#[cfg(test)]
mod tests {
    use super::*;
    use risuko_engine::config::defaults;
    use risuko_engine::engine::events::EventBroadcaster;
    use risuko_engine::engine::manager::TaskManager;
    use risuko_engine::engine::options::EngineOptions;
    use risuko_engine::engine::rpc::RpcServer;
    use std::sync::Arc;

    async fn start_server(secret: &str) -> (RpcServer, u16, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let mut system = defaults::system_defaults();
        system.insert(
            "dir".into(),
            serde_json::json!(dir.path().join("dl").to_string_lossy()),
        );
        system.insert("bt-enable-upnp".into(), serde_json::json!(false));
        system.insert("bt-enable-lsd".into(), serde_json::json!(false));
        let options = EngineOptions::from_config(&system, &serde_json::Map::new());
        let events = EventBroadcaster::default();
        let manager = Arc::new(
            TaskManager::new(dir.path(), options, events.clone())
                .await
                .unwrap(),
        );
        let port = {
            let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            l.local_addr().unwrap().port()
        };
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut server =
            RpcServer::new("127.0.0.1".into(), port, secret.into(), manager, events, tx);
        server.start().await.unwrap();
        (server, port, dir)
    }

    #[tokio::test]
    async fn free_port_is_not_running() {
        let port = {
            let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            l.local_addr().unwrap().port()
        };
        let client = RpcClient::new_with_host("127.0.0.1", port, None);
        assert_eq!(client.engine_status().await, EngineStatus::NotRunning);
        assert!(!client.is_engine_running().await);
    }

    #[tokio::test]
    async fn wrong_secret_is_not_mistaken_for_not_running() {
        let (mut server, port, _dir) = start_server("right-secret").await;
        let wrong = RpcClient::new_with_host("127.0.0.1", port, Some("wrong".into()));
        let missing = RpcClient::new_with_host("127.0.0.1", port, None);
        let right = RpcClient::new_with_host("127.0.0.1", port, Some("right-secret".into()));

        assert!(matches!(
            wrong.engine_status().await,
            EngineStatus::AuthFailed(_)
        ));
        assert!(wrong.is_engine_running().await);
        assert!(wrong.engine_status().await.problem().is_some());
        assert!(matches!(
            missing.engine_status().await,
            EngineStatus::AuthFailed(_)
        ));
        assert_eq!(right.engine_status().await, EngineStatus::Running);
        server.stop();
    }
}
