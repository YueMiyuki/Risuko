use std::time::Duration;

pub(crate) async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(crate) fn ipv6_loopback_available() -> bool {
    std::net::UdpSocket::bind("[::1]:0").is_ok()
}

pub(crate) async fn socks5_grant_udp_associate(
    control: &mut tokio::net::TcpStream,
    relay_port: u16,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut greeting = [0u8; 2];
    control.read_exact(&mut greeting).await.unwrap();
    let mut methods = vec![0u8; usize::from(greeting[1])];
    control.read_exact(&mut methods).await.unwrap();
    control.write_all(&[5, 0]).await.unwrap();
    let mut associate = [0u8; 10];
    control.read_exact(&mut associate).await.unwrap();
    assert_eq!(&associate[..4], &[5, 3, 0, 1]);
    let [hi, lo] = relay_port.to_be_bytes();
    control
        .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, hi, lo])
        .await
        .unwrap();
}
