#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use risuko_bt::bencode::Value;
use risuko_bt::session::{ListenerOptions, Session, SessionOptions};

pub fn tiny_info(name: &str, private: bool) -> Value {
    let mut info = vec![
        (b"length".to_vec(), Value::Int(1024)),
        (b"name".to_vec(), Value::Bytes(name.as_bytes().to_vec())),
        (b"piece length".to_vec(), Value::Int(1024)),
        (b"pieces".to_vec(), Value::Bytes(vec![0u8; 20])),
    ];
    if private {
        info.push((b"private".to_vec(), Value::Int(1)));
    }
    Value::Dict(info)
}

pub fn quiet_session_opts() -> SessionOptions {
    SessionOptions {
        disable_dht: true,
        disable_local_service_discovery: true,
        ..Default::default()
    }
}

pub async fn loopback_session(dir: &Path, lsd: bool) -> Arc<Session> {
    Session::new_with_opts(
        dir.to_path_buf(),
        SessionOptions {
            disable_dht: true,
            disable_local_service_discovery: !lsd,
            listen: Some(ListenerOptions {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

pub async fn wait_until_finished(handle: &risuko_bt::ManagedTorrent, limit: Duration) {
    let deadline = std::time::Instant::now() + limit;
    loop {
        let stats = handle.stats();
        if stats.finished {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for finished: progress={} / {}",
            stats.progress_bytes,
            stats.total_bytes
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
