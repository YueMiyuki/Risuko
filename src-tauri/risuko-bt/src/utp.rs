mod dontfrag;
pub mod packet;
pub mod socket;
pub mod stream;

pub use socket::UtpSocket;
pub use stream::UtpStream;

pub fn now_micros() -> u32 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_micros() as u32
}
