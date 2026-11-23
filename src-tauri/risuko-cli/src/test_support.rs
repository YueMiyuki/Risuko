use std::sync::Arc;

use risuko_engine::config::defaults;
use risuko_engine::engine::events::EventBroadcaster;
use risuko_engine::engine::manager::TaskManager;
use risuko_engine::engine::options::EngineOptions;
use risuko_engine::engine::rpc::RpcServer;

pub(crate) async fn start_server(secret: &str) -> (RpcServer, u16, tempfile::TempDir) {
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
    let mut server = RpcServer::new("127.0.0.1".into(), port, secret.into(), manager, events, tx);
    server.start().await.unwrap();
    (server, port, dir)
}
