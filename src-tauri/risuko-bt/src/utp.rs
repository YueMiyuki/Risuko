//! µTP (BEP-29) Micro Transport Protocol over UDP: an additive TCP-alternative peer-wire transport; since the peer layer is generic over `AsyncRead + AsyncWrite`, a [`stream::UtpStream`] can replace a `TcpStream` with the BT/MSE handshake unchanged. Layout: [`packet`] wire header + extension codec (pure, no I/O), [`socket`] shared UDP endpoint demuxing datagrams to per-peer state machines, [`stream`] per-connection `AsyncRead`/`AsyncWrite` handle

mod dontfrag;
pub mod packet;
pub mod socket;
pub mod stream;

pub use socket::UtpSocket;
pub use stream::UtpStream;

/// Microsecond timestamp from a monotonic clock, truncated to µTP's 32-bit field; only differences matter, so wraparound is harmless
pub fn now_micros() -> u32 {
    use std::sync::OnceLock;
    use std::time::Instant;
    // Anchor to a fixed start so the value is a small monotonic microsecond counter, not truncated nanoseconds-since-epoch
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_micros() as u32
}
