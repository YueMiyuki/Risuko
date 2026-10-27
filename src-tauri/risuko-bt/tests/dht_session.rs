use std::path::PathBuf;

use risuko_bt::dht::{Dht, DhtOptions};
use risuko_bt::session::{ListenerOptions, Session, SessionOptions};
use risuko_bt::Id20;

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_applies_dht_port_state_path_and_v6_announce_policy() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_udp_port();
    let state = tmp.path().join("dht-routing.state");
    let dht_options = DhtOptions {
        port: Some(port),
        routing_state_path: Some(state.clone()),
    };

    let session = Session::new_with_opts(
        PathBuf::from(tmp.path()),
        SessionOptions {
            dht: dht_options.clone(),
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(session.dht_local_port(), Some(port));
    let dht = Dht::current_shared().await.expect("shared DHT");
    assert!(!dht.announce_v6());

    dht.add_bootstrap_nodes([(
        Id20::from_slice(&[2u8; 20]).unwrap(),
        "8.8.8.8:6881".parse().unwrap(),
    )]);
    assert!(!state.exists());
    session.shutdown().await;
    assert!(state.exists());
    drop(session);

    if std::net::TcpListener::bind("[::1]:0").is_ok() {
        let session = Session::new_with_opts(
            PathBuf::from(tmp.path()),
            SessionOptions {
                dht: dht_options,
                listen: Some(ListenerOptions {
                    listen_ipv6: true,
                    ..Default::default()
                }),
                disable_local_service_discovery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(dht.announce_v6());
        drop(session);
    }
}
