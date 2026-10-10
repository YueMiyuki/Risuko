use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::super::core::Id20;
use super::super::limiter::Throttle;
use super::super::wire::mse::{self, DhKeys, MseRc4, DH_LEN};
use super::super::wire::{Handshake, Message, MessageDecoder, MessageEncoder, HANDSHAKE_LEN};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EncryptionPolicy {
    PlaintextOnly,
    #[default]
    Prefer,
    PreferEncrypted,
    RequireEncryption,
}

#[derive(Debug)]
pub enum PeerEvent {
    Handshook {
        peer_id: Id20,
        reserved: [u8; 8],
        info_hash: Id20,
        encrypted: bool,
        utp: bool,
    },
    Message(Message),
    Disconnected {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub struct PeerEventSink {
    pid: u32,
    tx: mpsc::Sender<(u32, PeerEvent)>,
}

impl PeerEventSink {
    pub fn new(pid: u32, tx: mpsc::Sender<(u32, PeerEvent)>) -> Self {
        Self { pid, tx }
    }
}

enum EventOut {
    Own(mpsc::Sender<PeerEvent>),
    Sink(PeerEventSink),
}

impl EventOut {
    async fn send(&self, ev: PeerEvent) -> bool {
        match self {
            EventOut::Own(tx) => tx.send(ev).await.is_ok(),
            EventOut::Sink(sink) => sink.tx.send((sink.pid, ev)).await.is_ok(),
        }
    }
}

#[derive(Debug)]
pub enum PeerCommand {
    Send(Message),
    Disconnect,
}

pub struct PeerHandle {
    pub addr: SocketAddr,
    pub tx: mpsc::Sender<PeerCommand>,
    pub piece_tx: mpsc::Sender<Message>,
    pub io_abort: tokio::task::AbortHandle,
    pub gate: Arc<RecvGate>,
    pub bind: Option<tokio::sync::oneshot::Sender<PeerEventSink>>,
}

#[derive(Default)]
pub struct RecvGate(std::sync::OnceLock<Throttle>);

impl RecvGate {
    pub fn set(&self, throttle: Throttle) {
        let _ = self.0.set(throttle);
    }

    fn get(&self) -> Option<&Throttle> {
        self.0.get()
    }
}

pub type ExtHandshakeBuilder = Arc<dyn Fn(IpAddr) -> Bytes + Send + Sync>;

#[derive(Clone)]
pub struct SpawnPeer {
    pub addr: SocketAddr,
    pub info_hash: Id20,
    pub our_peer_id: Id20,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub encryption: EncryptionPolicy,
    pub advertise_v2: bool,
    pub advertise_dht: bool,
    pub ext_handshake_builder: Option<ExtHandshakeBuilder>,
    pub proxy: Option<risuko_http::ProxyConnector>,
    pub deferred: bool,
}

#[derive(Clone)]
pub struct KnownInfoHash {
    pub info_hash: Id20,
    pub advertise_v2: bool,
    pub advertise_dht: bool,
    pub ext_handshake_builder: Option<ExtHandshakeBuilder>,
}

impl From<Id20> for KnownInfoHash {
    fn from(info_hash: Id20) -> Self {
        Self {
            info_hash,
            advertise_v2: true,
            advertise_dht: true,
            ext_handshake_builder: None,
        }
    }
}

pub async fn connect(spawn: SpawnPeer) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    let stream = match &spawn.proxy {
        Some(proxy) => timeout(
            spawn.connect_timeout,
            proxy.connect_tcp(&spawn.addr.ip().to_string(), spawn.addr.port()),
        )
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"))?
        .map_err(|e| std::io::Error::other(e.to_string()))?,
        None => {
            let stream = timeout(spawn.connect_timeout, TcpStream::connect(spawn.addr))
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout")
                })??;
            let _ = stream.set_nodelay(true);
            risuko_http::BoxedIo::new(stream)
        }
    };
    drive_handshake(stream, spawn).await
}

pub async fn connect_utp_plaintext(
    stream: crate::utp::UtpStream,
    spawn: SpawnPeer,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    let addr = spawn.addr;
    let (reader, writer) = tokio::io::split(stream);
    connect_plaintext(reader, writer, addr, &spawn, true).await
}

async fn connect_utp(
    utp: &crate::utp::UtpSocket,
    spawn: SpawnPeer,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    let addr = spawn.addr;
    let dial = || async {
        utp.connect_timeout(addr, spawn.connect_timeout)
            .await
            .inspect_err(|e| tracing::debug!("µTP dial to {addr} failed: {e}"))
    };
    let mse = |stream: crate::utp::UtpStream| async {
        timeout(
            spawn.connect_timeout,
            connect_mse(stream, addr, &spawn, true),
        )
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "µTP mse handshake timeout")
        })?
    };
    match spawn.encryption {
        EncryptionPolicy::PlaintextOnly => timeout(
            spawn.connect_timeout,
            connect_utp_plaintext(dial().await?, spawn.clone()),
        )
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "µTP plaintext handshake timeout",
            )
        })?,
        EncryptionPolicy::RequireEncryption => mse(dial().await?).await,
        EncryptionPolicy::PreferEncrypted => match mse(dial().await?).await {
            Ok(v) => Ok(v),
            Err(e) if alternate_dial_cannot_help(&e) => Err(e),
            Err(e) => {
                tracing::debug!("µTP mse handshake to {addr} failed: {e}; trying plaintext");
                timeout(
                    spawn.connect_timeout,
                    connect_utp_plaintext(dial().await?, spawn.clone()),
                )
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "µTP plaintext fallback timeout",
                    )
                })?
            }
        },
        EncryptionPolicy::Prefer => {
            let stream = dial().await?;
            let plaintext = timeout(
                spawn.connect_timeout,
                connect_utp_plaintext(stream, spawn.clone()),
            )
            .await
            .unwrap_or_else(|_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "µTP plaintext handshake timeout",
                ))
            });
            match plaintext {
                Ok(v) => Ok(v),
                Err(e) if alternate_dial_cannot_help(&e) => Err(e),
                Err(e) => {
                    tracing::debug!("µTP plaintext handshake to {addr} failed: {e}; trying mse");
                    mse(dial().await?).await
                }
            }
        }
    }
}

pub async fn connect_with_utp_fallback(
    spawn: SpawnPeer,
    utp: Option<std::sync::Arc<crate::utp::UtpSocket>>,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    dial_with_transport_order(spawn, utp, false).await
}

pub async fn connect_prefer_utp(
    spawn: SpawnPeer,
    utp: Option<std::sync::Arc<crate::utp::UtpSocket>>,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    dial_with_transport_order(spawn, utp, true).await
}

async fn dial_with_transport_order(
    spawn: SpawnPeer,
    utp: Option<std::sync::Arc<crate::utp::UtpSocket>>,
    prefer_utp: bool,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    let Some(utp) = utp else {
        return connect(spawn).await;
    };
    let addr = spawn.addr;

    let try_utp = |spawn: SpawnPeer, utp: std::sync::Arc<crate::utp::UtpSocket>| async move {
        connect_utp(&utp, spawn).await
    };

    if prefer_utp {
        match try_utp(spawn.clone(), utp.clone()).await {
            Ok(v) => {
                tracing::debug!("connected to {addr} via µTP (preferred)");
                return Ok(v);
            }
            Err(utp_err) => {
                if alternate_dial_cannot_help(&utp_err) {
                    return Err(utp_err);
                }
                tracing::debug!("µTP-first to {addr} failed ({utp_err}); trying TCP");
                return connect(spawn).await;
            }
        }
    }

    match connect(spawn.clone()).await {
        Ok(v) => Ok(v),
        Err(tcp_err) => {
            if alternate_dial_cannot_help(&tcp_err) {
                return Err(tcp_err);
            }
            match try_utp(spawn, utp).await {
                Ok(v) => {
                    tracing::debug!("tcp dial to {addr} failed ({tcp_err}); connected via µTP");
                    Ok(v)
                }
                Err(utp_err) => {
                    tracing::debug!(
                        "tcp+µTP dial to {addr} both failed (tcp={tcp_err}, utp={utp_err})"
                    );
                    Err(tcp_err)
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct TerminalDialError(&'static str);

fn terminal_dial_error(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, TerminalDialError(message))
}

fn alternate_dial_cannot_help(err: &std::io::Error) -> bool {
    err.get_ref()
        .is_some_and(|inner| inner.is::<TerminalDialError>())
}

pub async fn accept(
    stream: TcpStream,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
    policy: EncryptionPolicy,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    accept_with(
        stream,
        our_peer_id,
        known_hashes,
        read_timeout,
        policy,
        false,
    )
    .await
}

pub async fn accept_deferred(
    stream: TcpStream,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
    policy: EncryptionPolicy,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    accept_with(
        stream,
        our_peer_id,
        known_hashes,
        read_timeout,
        policy,
        true,
    )
    .await
}

async fn accept_with(
    stream: TcpStream,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
    policy: EncryptionPolicy,
    deferred: bool,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    let addr = stream.peer_addr()?;

    // Read 20 bytes: a one-byte check misclassifies ~1/256 MSE connections
    let (mut reader, writer) = stream.into_split();
    let mut probe = [0u8; 20];
    timeout(read_timeout, reader.read_exact(&mut probe))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "probe timeout"))??;
    let plaintext_first_byte = probe[0] == 0x13 && &probe[1..20] == b"BitTorrent protocol";
    let reader = std::io::Cursor::new(probe.to_vec()).chain(reader);

    if plaintext_first_byte {
        if matches!(policy, EncryptionPolicy::RequireEncryption) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "policy requires encryption; rejecting plaintext",
            ));
        }
        accept_plaintext_generic(
            reader,
            writer,
            addr,
            our_peer_id,
            known_hashes,
            read_timeout,
            false,
            deferred,
        )
        .await
    } else {
        if matches!(policy, EncryptionPolicy::PlaintextOnly) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "policy forbids encryption; rejecting MSE",
            ));
        }
        accept_mse(
            reader,
            writer,
            addr,
            our_peer_id,
            known_hashes,
            read_timeout,
            policy,
            false,
            deferred,
        )
        .await
    }
}

pub async fn accept_utp_plaintext(
    stream: crate::utp::UtpStream,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    accept_utp(
        stream,
        our_peer_id,
        known_hashes,
        read_timeout,
        EncryptionPolicy::PlaintextOnly,
    )
    .await
}

pub async fn accept_utp(
    stream: crate::utp::UtpStream,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
    policy: EncryptionPolicy,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    accept_utp_with(
        stream,
        our_peer_id,
        known_hashes,
        read_timeout,
        policy,
        false,
    )
    .await
}

pub async fn accept_utp_deferred(
    stream: crate::utp::UtpStream,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
    policy: EncryptionPolicy,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    accept_utp_with(
        stream,
        our_peer_id,
        known_hashes,
        read_timeout,
        policy,
        true,
    )
    .await
}

async fn accept_utp_with(
    stream: crate::utp::UtpStream,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
    policy: EncryptionPolicy,
    deferred: bool,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    let addr = stream.peer_addr();
    let (mut reader, writer) = tokio::io::split(stream);
    let mut probe = [0u8; 20];
    timeout(read_timeout, reader.read_exact(&mut probe))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "µTP probe timeout"))??;
    let plaintext = probe[0] == 0x13 && &probe[1..20] == b"BitTorrent protocol";
    let reader = std::io::Cursor::new(probe.to_vec()).chain(reader);
    if plaintext {
        if matches!(policy, EncryptionPolicy::RequireEncryption) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "policy requires encryption; rejecting plaintext",
            ));
        }
        accept_plaintext_generic(
            reader,
            writer,
            addr,
            our_peer_id,
            known_hashes,
            read_timeout,
            true,
            deferred,
        )
        .await
    } else {
        if matches!(policy, EncryptionPolicy::PlaintextOnly) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "policy forbids encryption; rejecting MSE",
            ));
        }
        accept_mse(
            reader,
            writer,
            addr,
            our_peer_id,
            known_hashes,
            read_timeout,
            policy,
            true,
            deferred,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_plaintext_generic<R, W>(
    mut reader: R,
    mut writer: W,
    addr: SocketAddr,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
    utp: bool,
    deferred: bool,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut buf = [0u8; HANDSHAKE_LEN];
    timeout(read_timeout, reader.read_exact(&mut buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "hs read timeout"))??;
    let remote_hs = Handshake::parse(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}")))?;
    let Some(known) = known_hashes
        .iter()
        .find(|known| known.info_hash == remote_hs.info_hash)
    else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unknown info hash",
        ));
    };
    let our_hs = Handshake::new_with_features(
        remote_hs.info_hash,
        our_peer_id,
        known.advertise_v2,
        known.advertise_dht,
        true,
    );
    writer.write_all(&our_hs.to_bytes()).await?;
    write_ext_handshake_if_supported(
        &mut writer,
        &remote_hs,
        addr.ip(),
        known.ext_handshake_builder.as_ref(),
    )
    .await?;
    finish_spawn(
        addr,
        our_peer_id,
        remote_hs,
        false,
        utp,
        Box::new(reader),
        Box::new(writer),
        deferred,
    )
}

async fn dial_tcp(spawn: &SpawnPeer) -> std::io::Result<risuko_http::BoxedIo> {
    match &spawn.proxy {
        Some(proxy) => proxy
            .connect_tcp(&spawn.addr.ip().to_string(), spawn.addr.port())
            .await
            .map_err(|e| std::io::Error::other(e.to_string())),
        None => {
            let stream = TcpStream::connect(spawn.addr).await?;
            let _ = stream.set_nodelay(true);
            Ok(risuko_http::BoxedIo::new(stream))
        }
    }
}

async fn drive_handshake(
    stream: risuko_http::BoxedIo,
    spawn: SpawnPeer,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    let addr = spawn.addr;
    match spawn.encryption {
        EncryptionPolicy::PlaintextOnly => {
            let (reader, writer) = tokio::io::split(stream);
            timeout(
                spawn.connect_timeout,
                connect_plaintext(reader, writer, addr, &spawn, false),
            )
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "plaintext handshake timeout")
            })?
        }
        EncryptionPolicy::RequireEncryption => timeout(
            spawn.connect_timeout,
            connect_mse(stream, addr, &spawn, false),
        )
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "mse handshake timeout"))?,
        EncryptionPolicy::PreferEncrypted => {
            let mse = timeout(
                spawn.connect_timeout,
                connect_mse(stream, addr, &spawn, false),
            )
            .await
            .unwrap_or_else(|_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "mse handshake timeout",
                ))
            });
            match mse {
                Ok(v) => Ok(v),
                Err(e) if alternate_dial_cannot_help(&e) => Err(e),
                Err(e) => {
                    tracing::debug!("mse handshake to {addr} failed: {e}; trying plaintext");
                    timeout(spawn.connect_timeout, async {
                        let (reader, writer) = tokio::io::split(dial_tcp(&spawn).await?);
                        connect_plaintext(reader, writer, addr, &spawn, false).await
                    })
                    .await
                    .map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "plaintext fallback timeout",
                        )
                    })?
                }
            }
        }
        EncryptionPolicy::Prefer => {
            let (reader, writer) = tokio::io::split(stream);
            let plaintext = timeout(
                spawn.connect_timeout,
                connect_plaintext(reader, writer, addr, &spawn, false),
            )
            .await;
            let fallback_err: std::io::Error = match plaintext {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(e)) => e,
                Err(_) => {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "plaintext handshake timeout")
                }
            };
            if alternate_dial_cannot_help(&fallback_err) {
                tracing::debug!(
                    "plaintext handshake to {addr} failed: {fallback_err}; skipping mse"
                );
                return Err(fallback_err);
            }
            tracing::debug!("plaintext handshake to {addr} failed: {fallback_err}; trying mse");
            let mse = timeout(spawn.connect_timeout, async {
                connect_mse(dial_tcp(&spawn).await?, addr, &spawn, false).await
            })
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "mse fallback timeout")
            })?;
            match mse {
                Ok(v) => {
                    tracing::debug!("mse handshake to {addr} succeeded");
                    Ok(v)
                }
                Err(e) => {
                    tracing::debug!("mse handshake to {addr} failed: {e}");
                    Err(e)
                }
            }
        }
    }
}

async fn connect_plaintext<R, W>(
    mut reader: R,
    mut writer: W,
    addr: SocketAddr,
    spawn: &SpawnPeer,
    utp: bool,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let our_hs = Handshake::new_with_features(
        spawn.info_hash,
        spawn.our_peer_id,
        spawn.advertise_v2,
        spawn.advertise_dht,
        true,
    );
    writer.write_all(&our_hs.to_bytes()).await?;

    let mut buf = [0u8; HANDSHAKE_LEN];
    timeout(spawn.read_timeout, reader.read_exact(&mut buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "hs read timeout"))??;
    let remote_hs = Handshake::parse(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}")))?;
    if remote_hs.info_hash != spawn.info_hash {
        return Err(terminal_dial_error("info hash mismatch"));
    }
    write_ext_handshake_if_supported(
        &mut writer,
        &remote_hs,
        addr.ip(),
        spawn.ext_handshake_builder.as_ref(),
    )
    .await?;
    finish_spawn(
        addr,
        spawn.our_peer_id,
        remote_hs,
        false,
        utp,
        Box::new(reader),
        Box::new(writer),
        spawn.deferred,
    )
}

async fn read_until(
    r: &mut (impl AsyncRead + Unpin),
    buf: &mut Vec<u8>,
    n: usize,
    deadline: tokio::time::Instant,
    what: &str,
) -> std::io::Result<()> {
    let mut chunk = [0u8; 256];
    while buf.len() < n {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("{what} timeout"),
            ));
        }
        let read = timeout(remaining, r.read(&mut chunk))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, what.to_string()))??;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("eof in {what}"),
            ));
        }
        buf.extend_from_slice(&chunk[..read]);
    }
    Ok(())
}

async fn connect_mse<S>(
    stream: S,
    addr: SocketAddr,
    spawn: &SpawnPeer,
    utp: bool,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut read_h, mut write_h) = tokio::io::split(stream);
    let hs_timeout = spawn.connect_timeout;

    let keys = DhKeys::generate();
    let pad_a = mse::gen_pad(512);
    let mut out = Vec::with_capacity(DH_LEN + pad_a.len());
    out.extend_from_slice(&keys.public_be);
    out.extend_from_slice(&pad_a);
    write_h.write_all(&out).await?;

    let mut yb = [0u8; DH_LEN];
    timeout(hs_timeout, read_h.read_exact(&mut yb))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "mse yb timeout"))??;
    let s = keys.shared_secret(&yb)?;

    let skey: [u8; 20] = spawn.info_hash.0;
    let key_a = mse::rc4_key(b"keyA", &s, &skey);
    let key_b = mse::rc4_key(b"keyB", &s, &skey);
    let mut enc_out = mse::init_rc4(&key_a);
    let mut dec_in = mse::init_rc4(&key_b);

    let req1 = mse::req1(&s);
    let req2 = mse::req2(&skey);
    let req3 = mse::req3(&s);
    let req23 = mse::xor20(&req2, &req3);

    let pad_c = mse::gen_pad(512);
    let mut crypto_provide = mse::crypto::RC4;
    if !matches!(spawn.encryption, EncryptionPolicy::RequireEncryption) {
        crypto_provide |= mse::crypto::PLAINTEXT;
    }
    let our_hs_bytes = Handshake::new_with_features(
        spawn.info_hash,
        spawn.our_peer_id,
        spawn.advertise_v2,
        spawn.advertise_dht,
        true,
    )
    .to_bytes();
    let mut payload = mse::build_initiator_payload(crypto_provide, &pad_c, &our_hs_bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}")))?;
    enc_out.apply_keystream(&mut payload);

    let mut to_send = Vec::with_capacity(20 + 20 + payload.len());
    to_send.extend_from_slice(&req1);
    to_send.extend_from_slice(&req23);
    to_send.extend_from_slice(&payload);
    write_h.write_all(&to_send).await?;

    let max_scan = 1100usize;
    let mut keystream = vec![0u8; max_scan];
    dec_in.apply_keystream(&mut keystream);
    let mut recv = Vec::with_capacity(max_scan);
    let mut chunk = [0u8; 256];
    let mut found_offset: Option<usize> = None;
    let deadline = tokio::time::Instant::now() + hs_timeout;
    while recv.len() < max_scan {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "mse vc scan timeout",
            ));
        }
        let rn = match timeout(remaining, read_h.read(&mut chunk)).await {
            Ok(Ok(0)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof during mse vc scan",
                ));
            }
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "mse vc read timeout",
                ));
            }
        };
        let scan_from = mse::scan_start_after_append(recv.len(), 8);
        recv.extend_from_slice(&chunk[..rn]);

        if recv.len() >= 8 && keystream.len() >= 8 {
            let needle = &keystream[..8];
            found_offset = mse::find_subsequence_from(&recv, needle, scan_from);
        }
        if found_offset.is_some() {
            break;
        }
    }
    let off = found_offset
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "mse vc not found"))?;

    let mut dec_in = mse::init_rc4(&key_b);
    let tail_start = off;
    let mut tail = recv[tail_start..].to_vec();
    read_until(&mut read_h, &mut tail, 14, deadline, "mse header").await?;
    dec_in.apply_keystream(&mut tail[..14]);
    let crypto_select = u32::from_be_bytes([tail[8], tail[9], tail[10], tail[11]]);
    let pad_d_len = u16::from_be_bytes([tail[12], tail[13]]) as usize;
    if pad_d_len > 512 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "pad_d too long",
        ));
    }

    read_until(&mut read_h, &mut tail, 14 + pad_d_len, deadline, "mse padd").await?;
    if pad_d_len > 0 {
        dec_in.apply_keystream(&mut tail[14..14 + pad_d_len]);
    }
    let leftover = tail[14 + pad_d_len..].to_vec();

    let use_rc4 = if crypto_select & mse::crypto::RC4 != 0 {
        true
    } else if crypto_select & mse::crypto::PLAINTEXT != 0 {
        if matches!(spawn.encryption, EncryptionPolicy::RequireEncryption) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "peer selected plaintext but policy requires encryption",
            ));
        }
        false
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("peer returned unsupported crypto_select {crypto_select:#x}"),
        ));
    };

    let (reader_boxed, mut writer_boxed, remote_hs): (
        Box<dyn AsyncRead + Unpin + Send>,
        Box<dyn AsyncWrite + Unpin + Send>,
        Handshake,
    ) = if use_rc4 {
        let mut pending = leftover;
        let mut hs_bytes = Vec::with_capacity(HANDSHAKE_LEN);
        read_until(&mut read_h, &mut pending, HANDSHAKE_LEN, deadline, "mse hs").await?;
        hs_bytes.extend_from_slice(&pending[..HANDSHAKE_LEN]);
        dec_in.apply_keystream(&mut hs_bytes);
        let rest = pending[HANDSHAKE_LEN..].to_vec();
        let mut hs_arr = [0u8; HANDSHAKE_LEN];
        hs_arr.copy_from_slice(&hs_bytes);
        let remote_hs = Handshake::parse(&hs_arr)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}")))?;
        if remote_hs.info_hash != spawn.info_hash {
            return Err(terminal_dial_error("info hash mismatch after mse"));
        }
        let r = Rc4ReadHalf::new(read_h, dec_in, rest);
        let w = Rc4WriteHalf::new(write_h, enc_out);
        (Box::new(r), Box::new(w), remote_hs)
    } else {
        let mut pending = leftover;
        read_until(
            &mut read_h,
            &mut pending,
            HANDSHAKE_LEN,
            deadline,
            "mse/plain hs",
        )
        .await?;
        let hs_bytes = &pending[..HANDSHAKE_LEN];
        let rest = pending[HANDSHAKE_LEN..].to_vec();
        let mut hs_arr = [0u8; HANDSHAKE_LEN];
        hs_arr.copy_from_slice(hs_bytes);
        let remote_hs = Handshake::parse(&hs_arr)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}")))?;
        if remote_hs.info_hash != spawn.info_hash {
            return Err(terminal_dial_error("info hash mismatch after mse"));
        }
        let r = std::io::Cursor::new(rest).chain(read_h);
        (Box::new(r), Box::new(write_h), remote_hs)
    };

    write_ext_handshake_if_supported(
        &mut *writer_boxed,
        &remote_hs,
        addr.ip(),
        spawn.ext_handshake_builder.as_ref(),
    )
    .await?;
    finish_spawn(
        addr,
        spawn.our_peer_id,
        remote_hs,
        use_rc4,
        utp,
        reader_boxed,
        writer_boxed,
        spawn.deferred,
    )
}

#[allow(clippy::too_many_arguments)]
async fn accept_mse<R, W>(
    mut read_h: R,
    mut write_h: W,
    addr: SocketAddr,
    our_peer_id: Id20,
    known_hashes: Vec<KnownInfoHash>,
    read_timeout: Duration,
    policy: EncryptionPolicy,
    utp: bool,
    deferred: bool,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let deadline = tokio::time::Instant::now() + read_timeout;
    if known_hashes.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mse: no torrents hosted",
        ));
    }

    let mut ya = [0u8; DH_LEN];
    timeout(read_timeout, read_h.read_exact(&mut ya))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "mse ya timeout"))??;

    let keys = tokio::task::spawn_blocking(DhKeys::generate)
        .await
        .map_err(std::io::Error::other)?;
    let pad_b = mse::gen_pad(512);
    let mut out = Vec::with_capacity(DH_LEN + pad_b.len());
    out.extend_from_slice(&keys.public_be);
    out.extend_from_slice(&pad_b);
    write_h.write_all(&out).await?;

    let s = tokio::task::spawn_blocking(move || keys.shared_secret(&ya))
        .await
        .map_err(std::io::Error::other)??;
    let req1 = mse::req1(&s);
    let mut recv: Vec<u8> = Vec::with_capacity(1200);
    let mut chunk = [0u8; 256];
    let req1_off = loop {
        if recv.len() > 512 + 20 + 20 + 8 + 4 + 2 + 512 + HANDSHAKE_LEN + 256 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mse req1 not found in window",
            ));
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "mse req1 timeout",
            ));
        }
        let n = timeout(remaining, read_h.read(&mut chunk))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "mse req1"))??;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof before req1",
            ));
        }
        let scan_from = mse::scan_start_after_append(recv.len(), req1.len());
        recv.extend_from_slice(&chunk[..n]);
        if let Some(off) = mse::find_subsequence_from(&recv, &req1, scan_from) {
            break off;
        }
    };

    read_until(&mut read_h, &mut recv, req1_off + 40, deadline, "mse req2").await?;
    let mut req23 = [0u8; 20];
    req23.copy_from_slice(&recv[req1_off + 20..req1_off + 40]);
    let req3_s = mse::req3(&s);
    let want_req2 = mse::xor20(&req23, &req3_s);
    let mut known_info: Option<KnownInfoHash> = None;
    for known in &known_hashes {
        if mse::req2(&known.info_hash.0) == want_req2 {
            known_info = Some(known.clone());
            break;
        }
    }
    let known_info = known_info.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "mse: unknown info hash")
    })?;
    let skey = known_info.info_hash;

    let key_a = mse::rc4_key(b"keyA", &s, &skey.0);
    let key_b = mse::rc4_key(b"keyB", &s, &skey.0);
    let mut dec_in = mse::init_rc4(&key_a);
    let mut enc_out = mse::init_rc4(&key_b);

    let enc_start = req1_off + 40;
    let mut enc_buf = recv[enc_start..].to_vec();
    read_until(&mut read_h, &mut enc_buf, 14, deadline, "mse ia-head").await?;
    dec_in.apply_keystream(&mut enc_buf[..14]);
    if enc_buf[0..8] != mse::VC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mse: vc mismatch after req2 resolve",
        ));
    }
    let crypto_provide = u32::from_be_bytes([enc_buf[8], enc_buf[9], enc_buf[10], enc_buf[11]]);
    let pad_c_len = u16::from_be_bytes([enc_buf[12], enc_buf[13]]) as usize;
    if pad_c_len > 512 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "pad_c too long",
        ));
    }
    read_until(
        &mut read_h,
        &mut enc_buf,
        14 + pad_c_len + 2,
        deadline,
        "mse padc",
    )
    .await?;
    dec_in.apply_keystream(&mut enc_buf[14..14 + pad_c_len + 2]);
    let ia_len_off = 14 + pad_c_len;
    let ia_len = u16::from_be_bytes([enc_buf[ia_len_off], enc_buf[ia_len_off + 1]]) as usize;
    if ia_len > 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ia too long",
        ));
    }
    let ia_off = ia_len_off + 2;
    read_until(
        &mut read_h,
        &mut enc_buf,
        ia_off + ia_len,
        deadline,
        "mse ia",
    )
    .await?;
    if ia_len > 0 {
        dec_in.apply_keystream(&mut enc_buf[ia_off..ia_off + ia_len]);
    }
    let ia_bytes = enc_buf[ia_off..ia_off + ia_len].to_vec();
    let leftover = enc_buf[ia_off + ia_len..].to_vec();

    let ia_extra: Vec<u8> = if ia_bytes.len() > HANDSHAKE_LEN {
        ia_bytes[HANDSHAKE_LEN..].to_vec()
    } else {
        Vec::new()
    };

    let allow_plain = !matches!(policy, EncryptionPolicy::RequireEncryption);
    let crypto_select = if crypto_provide & mse::crypto::RC4 != 0 {
        mse::crypto::RC4
    } else if allow_plain && crypto_provide & mse::crypto::PLAINTEXT != 0 {
        mse::crypto::PLAINTEXT
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("no acceptable crypto (provide={crypto_provide:#x})"),
        ));
    };

    let pad_d = mse::gen_pad(512);
    let mut reply = mse::build_responder_payload(crypto_select, &pad_d)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}")))?;
    enc_out.apply_keystream(&mut reply);
    write_h.write_all(&reply).await?;

    if !ia_bytes.is_empty() && ia_bytes.len() < HANDSHAKE_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ia shorter than bt handshake",
        ));
    }
    let remote_hs_from_ia = if !ia_bytes.is_empty() {
        let mut ia_arr = [0u8; HANDSHAKE_LEN];
        ia_arr.copy_from_slice(&ia_bytes[..HANDSHAKE_LEN]);
        let hs = Handshake::parse(&ia_arr)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}")))?;
        if hs.info_hash != skey {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "info hash mismatch between req2 and ia",
            ));
        }
        Some(hs)
    } else {
        None
    };

    let our_hs = Handshake::new_with_features(
        skey,
        our_peer_id,
        known_info.advertise_v2,
        known_info.advertise_dht,
        true,
    )
    .to_bytes();
    if crypto_select == mse::crypto::RC4 {
        let mut encoded = our_hs.to_vec();
        enc_out.apply_keystream(&mut encoded);
        write_h.write_all(&encoded).await?;
        let mut r = Rc4ReadHalf::new(read_h, dec_in, leftover);
        let remote_hs = match remote_hs_from_ia {
            Some(hs) => hs,
            None => {
                let mut buf = [0u8; HANDSHAKE_LEN];
                timeout(read_timeout, r.read_exact(&mut buf))
                    .await
                    .map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::TimedOut, "mse deferred hs timeout")
                    })??;
                let hs = Handshake::parse(&buf).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}"))
                })?;
                if hs.info_hash != skey {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "info hash mismatch in deferred handshake",
                    ));
                }
                hs
            }
        };
        let mut w = Rc4WriteHalf::new(write_h, enc_out);
        write_ext_handshake_if_supported(
            &mut w,
            &remote_hs,
            addr.ip(),
            known_info.ext_handshake_builder.as_ref(),
        )
        .await?;
        // `ia_extra` must precede anything the encrypted reader yields
        let reader: Box<dyn AsyncRead + Unpin + Send> = if ia_extra.is_empty() {
            Box::new(r)
        } else {
            Box::new(std::io::Cursor::new(ia_extra).chain(r))
        };
        finish_spawn(
            addr,
            our_peer_id,
            remote_hs,
            true,
            utp,
            reader,
            Box::new(w),
            deferred,
        )
    } else {
        write_h.write_all(&our_hs).await?;
        let mut prefix = ia_extra;
        prefix.extend_from_slice(&leftover);
        let r = std::io::Cursor::new(prefix).chain(read_h);
        let remote_hs = match remote_hs_from_ia {
            Some(hs) => hs,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "empty ia with plaintext crypto not supported",
                ));
            }
        };
        write_ext_handshake_if_supported(
            &mut write_h,
            &remote_hs,
            addr.ip(),
            known_info.ext_handshake_builder.as_ref(),
        )
        .await?;
        finish_spawn(
            addr,
            our_peer_id,
            remote_hs,
            false,
            utp,
            Box::new(r),
            Box::new(write_h),
            deferred,
        )
    }
}

async fn write_ext_handshake_if_supported<W: AsyncWrite + Unpin + ?Sized>(
    writer: &mut W,
    remote_hs: &Handshake,
    peer_ip: IpAddr,
    builder: Option<&ExtHandshakeBuilder>,
) -> std::io::Result<()> {
    if !remote_hs.has_ext_protocol() {
        return Ok(());
    }
    let Some(builder) = builder else {
        return Ok(());
    };
    let bytes = builder(peer_ip);
    writer.write_all(&bytes).await
}

#[allow(clippy::too_many_arguments)]
fn finish_spawn(
    addr: SocketAddr,
    our_peer_id: Id20,
    remote_hs: Handshake,
    encrypted: bool,
    utp: bool,
    reader: Box<dyn AsyncRead + Unpin + Send>,
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    deferred: bool,
) -> std::io::Result<(PeerHandle, mpsc::Receiver<PeerEvent>)> {
    if remote_hs.peer_id == our_peer_id {
        return Err(terminal_dial_error(
            "self-connection (peer_id matches ours)",
        ));
    }
    let (event_tx, event_rx) = mpsc::channel(1024);
    let (cmd_tx, cmd_rx) = mpsc::channel(1024);
    let (piece_tx, piece_rx) = mpsc::channel(PIECE_LANE_SLOTS);

    event_tx
        .try_send(PeerEvent::Handshook {
            peer_id: remote_hs.peer_id,
            reserved: remote_hs.reserved,
            info_hash: remote_hs.info_hash,
            encrypted,
            utp,
        })
        .map_err(|e| std::io::Error::other(format!("{e}")))?;
    let mut bind = None;
    let target = if deferred {
        let (bind_tx, bind_rx) = tokio::sync::oneshot::channel();
        bind = Some(bind_tx);
        IoTarget::Bind(bind_rx)
    } else {
        IoTarget::Ready(EventOut::Own(event_tx))
    };

    let gate = Arc::new(RecvGate::default());
    let reader_gate = gate.clone();
    let io_task = tokio::spawn(async move {
        let out = match target {
            IoTarget::Ready(out) => out,
            IoTarget::Bind(rx) => match rx.await {
                Ok(sink) => EventOut::Sink(sink),
                Err(_) => return,
            },
        };
        let reason = {
            let reader = reader_task(reader, &out, reader_gate);
            let writer = writer_task(writer, cmd_rx, piece_rx);
            tokio::pin!(reader);
            tokio::pin!(writer);
            tokio::select! {
                reason = &mut reader => reason,
                reason = &mut writer => reason,
            }
        };
        let _ = out.send(PeerEvent::Disconnected { reason }).await;
    });

    Ok((
        PeerHandle {
            addr,
            tx: cmd_tx,
            piece_tx,
            io_abort: io_task.abort_handle(),
            gate,
            bind,
        },
        event_rx,
    ))
}

enum IoTarget {
    Ready(EventOut),
    Bind(tokio::sync::oneshot::Receiver<PeerEventSink>),
}

struct Rc4ReadHalf {
    inner: tokio::io::Chain<std::io::Cursor<Vec<u8>>, Box<dyn AsyncRead + Unpin + Send>>,
    cipher: MseRc4,
}

impl Rc4ReadHalf {
    fn new<R>(inner: R, cipher: MseRc4, prefix: Vec<u8>) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        Self {
            inner: std::io::Cursor::new(prefix).chain(Box::new(inner)),
            cipher,
        }
    }
}

impl AsyncRead for Rc4ReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let filled_after = buf.filled().len();
                if filled_after > filled_before {
                    let me = &mut *self;
                    me.cipher
                        .apply_keystream(&mut buf.filled_mut()[filled_before..filled_after]);
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

struct Rc4WriteHalf {
    inner: Box<dyn AsyncWrite + Unpin + Send>,
    cipher: MseRc4,
    pending: Vec<u8>,
    pending_off: usize,
}

impl Rc4WriteHalf {
    fn new<W>(inner: W, cipher: MseRc4) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        Self {
            inner: Box::new(inner),
            cipher,
            pending: Vec::new(),
            pending_off: 0,
        }
    }

    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        while self.pending_off < self.pending.len() {
            match Pin::new(&mut self.inner).poll_write(cx, &self.pending[self.pending_off..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "rc4 write zero",
                    )));
                }
                Poll::Ready(Ok(n)) => self.pending_off += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.pending.clear();
        self.pending_off = 0;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Rc4WriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.poll_drain(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let me = &mut *self;
        me.pending.clear();
        me.pending.extend_from_slice(buf);
        me.cipher.apply_keystream(&mut me.pending);
        me.pending_off = 0;
        let consumed = buf.len();
        match self.poll_drain(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) | Poll::Pending => Poll::Ready(Ok(consumed)),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.poll_drain(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_flush(cx),
            other => other,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.poll_drain(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_shutdown(cx),
            other => other,
        }
    }
}

async fn reader_task(
    mut reader: Box<dyn AsyncRead + Unpin + Send>,
    out: &EventOut,
    gate: Arc<RecvGate>,
) -> String {
    let mut buf = BytesMut::with_capacity(256 * 1024);
    loop {
        let mut block_bytes = 0usize;
        loop {
            match MessageDecoder::try_decode(&mut buf) {
                Ok(Some(msg)) => {
                    if let Message::Piece { data, .. } = &msg {
                        block_bytes += data.len();
                    }
                    if !out.send(PeerEvent::Message(msg)).await {
                        return "event receiver closed".into();
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return format!("decode: {e}");
                }
            }
        }
        if block_bytes > 0 {
            if let Some(throttle) = gate.get() {
                throttle.acquire(block_bytes).await;
            }
        }
        buf.reserve(64 * 1024);
        match reader.read_buf(&mut buf).await {
            Ok(0) => {
                return "eof".into();
            }
            Ok(_) => {}
            Err(e) => {
                return format!("io: {e}");
            }
        }
    }
}

pub const PIECE_LANE_SLOTS: usize = 8;

const WRITE_TIMEOUT: Duration = Duration::from_secs(60);
const DISCONNECT_FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

async fn writer_task(
    mut writer: Box<dyn AsyncWrite + Unpin + Send>,
    mut rx: mpsc::Receiver<PeerCommand>,
    mut piece_rx: mpsc::Receiver<Message>,
) -> String {
    let mut batch = BytesMut::with_capacity(64 * 1024);
    let mut piece_open = true;
    loop {
        batch.clear();
        let mut disconnect = false;
        tokio::select! {
            biased;
            cmd = rx.recv() => match cmd {
                Some(PeerCommand::Send(msg)) => MessageEncoder::encode_into(&mut batch, &msg),
                Some(PeerCommand::Disconnect) => disconnect = true,
                None => return "command channel closed".into(),
            },
            piece = piece_rx.recv(), if piece_open => match piece {
                Some(msg) => MessageEncoder::encode_into(&mut batch, &msg),
                None => piece_open = false,
            },
        }
        while !disconnect && batch.len() < 256 * 1024 {
            match rx.try_recv() {
                Ok(PeerCommand::Send(msg)) => MessageEncoder::encode_into(&mut batch, &msg),
                Ok(PeerCommand::Disconnect) => disconnect = true,
                Err(_) => break,
            }
        }
        while !disconnect && piece_open && batch.len() < 256 * 1024 {
            match piece_rx.try_recv() {
                Ok(msg) => MessageEncoder::encode_into(&mut batch, &msg),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    piece_open = false;
                    break;
                }
            }
        }
        if !batch.is_empty() {
            match tokio::time::timeout(WRITE_TIMEOUT, writer.write_all(&batch)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return format!("write: {e}"),
                Err(_) => return "write timeout".into(),
            }
        }
        if disconnect {
            let _ = tokio::time::timeout(DISCONNECT_FLUSH_TIMEOUT, writer.flush()).await;
            return "local disconnect".into();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Message;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::net::TcpListener;

    async fn run_pair() -> (
        PeerHandle,
        mpsc::Receiver<PeerEvent>,
        PeerHandle,
        mpsc::Receiver<PeerEvent>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let info_hash = Id20([1u8; 20]);
        let peer_a = Id20([2u8; 20]);
        let peer_b = Id20([3u8; 20]);

        let accept_fut = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept(
                stream,
                peer_b,
                vec![info_hash.into()],
                Duration::from_secs(5),
                EncryptionPolicy::Prefer,
            )
            .await
            .unwrap()
        });

        let (handle_a, rx_a) = connect(SpawnPeer {
            addr,
            info_hash,
            our_peer_id: peer_a,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            encryption: EncryptionPolicy::PlaintextOnly,
            advertise_v2: true,
            advertise_dht: true,
            ext_handshake_builder: None,
            proxy: None,
            deferred: false,
        })
        .await
        .unwrap();

        let (handle_b, rx_b) = accept_fut.await.unwrap();
        (handle_a, rx_a, handle_b, rx_b)
    }

    #[tokio::test]
    async fn writer_sends_control_frames_ahead_of_queued_pieces() {
        use tokio::io::AsyncReadExt;
        let (client, mut server) = tokio::io::duplex(1 << 20);
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (piece_tx, piece_rx) = mpsc::channel(PIECE_LANE_SLOTS);
        for i in 0..3u32 {
            piece_tx
                .send(Message::Piece {
                    index: i,
                    begin: 0,
                    data: bytes::Bytes::from(vec![7u8; 16]),
                })
                .await
                .unwrap();
        }
        cmd_tx
            .send(PeerCommand::Send(Message::Interested))
            .await
            .unwrap();
        cmd_tx.send(PeerCommand::Disconnect).await.unwrap();
        let reason = writer_task(Box::new(client), cmd_rx, piece_rx).await;
        assert_eq!(reason, "local disconnect");
        let mut out = Vec::new();
        server.read_to_end(&mut out).await.unwrap();
        assert_eq!(&out[..5], &[0, 0, 0, 1, 2]);
        drop(piece_tx);
    }

    #[tokio::test(start_paused = true)]
    async fn writer_evicts_a_peer_that_never_reads() {
        let (client, _server) = tokio::io::duplex(16);
        let (_cmd_tx, cmd_rx) = mpsc::channel(8);
        let (piece_tx, piece_rx) = mpsc::channel(PIECE_LANE_SLOTS);
        piece_tx
            .send(Message::Piece {
                index: 0,
                begin: 0,
                data: bytes::Bytes::from(vec![7u8; 1024]),
            })
            .await
            .unwrap();
        let reason = writer_task(Box::new(client), cmd_rx, piece_rx).await;
        assert_eq!(reason, "write timeout");
    }

    #[test]
    fn alternate_dials_are_only_suppressed_for_identity_failures() {
        for (kind, message) in [
            (std::io::ErrorKind::ConnectionReset, "reset"),
            (std::io::ErrorKind::UnexpectedEof, "eof"),
            (std::io::ErrorKind::BrokenPipe, "broken pipe"),
            (std::io::ErrorKind::NotConnected, "not connected"),
            (std::io::ErrorKind::TimedOut, "timeout"),
            (std::io::ErrorKind::InvalidData, "invalid handshake length"),
        ] {
            let err = std::io::Error::new(kind, message);
            assert!(
                !alternate_dial_cannot_help(&err),
                "{kind:?} / {message} should allow an alternate dial"
            );
        }

        for message in [
            "info hash mismatch",
            "info hash mismatch after mse",
            "self-connection (peer_id matches ours)",
        ] {
            let err = terminal_dial_error(message);
            assert!(alternate_dial_cannot_help(&err), "{message} is terminal");
            assert_eq!(err.to_string(), message);
        }
    }

    #[tokio::test]
    async fn local_disconnect_promptly_closes_actor_and_emits_once() {
        let (local, mut local_rx, remote, mut remote_rx) = run_pair().await;
        assert!(matches!(
            local_rx.recv().await,
            Some(PeerEvent::Handshook { .. })
        ));
        assert!(matches!(
            remote_rx.recv().await,
            Some(PeerEvent::Handshook { .. })
        ));

        let _remote = remote;
        local.tx.send(PeerCommand::Disconnect).await.unwrap();

        let event = tokio::time::timeout(Duration::from_millis(500), local_rx.recv())
            .await
            .expect("local disconnect event timed out")
            .expect("event channel closed before Disconnected");
        assert!(matches!(
            event,
            PeerEvent::Disconnected { ref reason } if reason == "local disconnect"
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(500), local_rx.recv())
                .await
                .expect("event channel did not close after Disconnected")
                .is_none(),
            "peer actor emitted more than one terminal event"
        );
    }

    #[tokio::test]
    async fn recv_gate_delays_reads_after_received_blocks() {
        use crate::limiter::RateLimiter;

        let (a, mut rx_a, b, mut rx_b) = run_pair().await;
        assert!(matches!(
            rx_a.recv().await.unwrap(),
            PeerEvent::Handshook { .. }
        ));
        assert!(matches!(
            rx_b.recv().await.unwrap(),
            PeerEvent::Handshook { .. }
        ));
        b.gate.set(Throttle::new(
            Arc::new(RateLimiter::unlimited()),
            Arc::new(RateLimiter::new(64 * 1024)),
        ));
        let start = std::time::Instant::now();
        for i in 0..8u32 {
            a.tx.send(PeerCommand::Send(Message::Piece {
                index: 0,
                begin: i * 16 * 1024,
                data: Bytes::from(vec![7u8; 16 * 1024]),
            }))
            .await
            .unwrap();
        }
        for _ in 0..8 {
            assert!(matches!(
                rx_b.recv().await.unwrap(),
                PeerEvent::Message(Message::Piece { .. })
            ));
        }
        a.tx.send(PeerCommand::Send(Message::Have { piece_index: 9 }))
            .await
            .unwrap();
        assert!(matches!(
            rx_b.recv().await.unwrap(),
            PeerEvent::Message(Message::Have { .. })
        ));
        let waited = start.elapsed();
        assert!(waited >= Duration::from_millis(800), "waited {waited:?}");
        assert!(waited < Duration::from_secs(5), "waited {waited:?}");
    }

    #[tokio::test]
    async fn handshake_and_message() {
        let (a, mut rx_a, b, mut rx_b) = run_pair().await;

        assert!(matches!(
            rx_a.recv().await.unwrap(),
            PeerEvent::Handshook { .. }
        ));
        assert!(matches!(
            rx_b.recv().await.unwrap(),
            PeerEvent::Handshook { .. }
        ));

        a.tx.send(PeerCommand::Send(Message::Interested))
            .await
            .unwrap();
        let ev = rx_b.recv().await.unwrap();
        match ev {
            PeerEvent::Message(Message::Interested) => {}
            _ => panic!("expected Interested, got {ev:?}"),
        }
        b.tx.send(PeerCommand::Send(Message::Have { piece_index: 42 }))
            .await
            .unwrap();
        let ev = rx_a.recv().await.unwrap();
        match ev {
            PeerEvent::Message(Message::Have { piece_index }) => assert_eq!(piece_index, 42),
            _ => panic!("expected Have"),
        }
    }

    #[tokio::test]
    async fn deferred_reader_waits_for_its_sink_then_feeds_it_directly() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let info_hash = Id20([1u8; 20]);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept(
                stream,
                Id20([3u8; 20]),
                vec![info_hash.into()],
                Duration::from_secs(5),
                EncryptionPolicy::PlaintextOnly,
            )
            .await
            .unwrap()
        });
        let (mut client, mut hello) = connect(SpawnPeer {
            addr,
            info_hash,
            our_peer_id: Id20([2u8; 20]),
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            encryption: EncryptionPolicy::PlaintextOnly,
            advertise_v2: true,
            advertise_dht: true,
            ext_handshake_builder: None,
            proxy: None,
            deferred: true,
        })
        .await
        .unwrap();
        let (remote, _remote_rx) = server.await.unwrap();
        assert!(matches!(
            hello.recv().await,
            Some(PeerEvent::Handshook { .. })
        ));
        assert!(hello.recv().await.is_none());

        let (tx, mut rx) = mpsc::channel(8);
        remote
            .tx
            .send(PeerCommand::Send(Message::Interested))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx.recv())
                .await
                .is_err(),
            "nothing is delivered before the sink is bound"
        );
        let bind = client.bind.take().unwrap();
        assert!(bind.send(PeerEventSink::new(7, tx)).is_ok());
        assert!(matches!(
            rx.recv().await,
            Some((7, PeerEvent::Message(Message::Interested)))
        ));
        remote.tx.send(PeerCommand::Disconnect).await.unwrap();
        loop {
            match rx.recv().await {
                Some((7, PeerEvent::Disconnected { .. })) => break,
                Some(_) => {}
                None => panic!("sink closed without a disconnect"),
            }
        }
    }

    async fn prefer_encrypted_outcome(server_policy: EncryptionPolicy) -> bool {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let info_hash = Id20([0x31u8; 20]);
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                if let Ok(accepted) = accept(
                    stream,
                    Id20([0x32u8; 20]),
                    vec![info_hash.into()],
                    Duration::from_secs(5),
                    server_policy,
                )
                .await
                {
                    return accepted;
                }
            }
            panic!("no handshake accepted");
        });
        let (_client, mut rx) = connect(SpawnPeer {
            addr,
            info_hash,
            our_peer_id: Id20([0x33u8; 20]),
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            encryption: EncryptionPolicy::PreferEncrypted,
            advertise_v2: false,
            advertise_dht: true,
            ext_handshake_builder: None,
            proxy: None,
            deferred: false,
        })
        .await
        .unwrap();
        let _server = server.await.unwrap();
        match rx.recv().await.unwrap() {
            PeerEvent::Handshook { encrypted, .. } => encrypted,
            e => panic!("unexpected event: {e:?}"),
        }
    }

    #[tokio::test]
    async fn prefer_encrypted_tries_mse_first_then_falls_back_to_plaintext() {
        assert!(prefer_encrypted_outcome(EncryptionPolicy::Prefer).await);
        assert!(!prefer_encrypted_outcome(EncryptionPolicy::PlaintextOnly).await);
    }

    #[tokio::test]
    async fn rejects_wrong_info_hash() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hash_a = Id20([1u8; 20]);
        let hash_b = Id20([9u8; 20]);
        let peer_a = Id20([2u8; 20]);
        let peer_b = Id20([3u8; 20]);

        let accept_fut = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept(
                stream,
                peer_b,
                vec![hash_b.into()],
                Duration::from_secs(5),
                EncryptionPolicy::Prefer,
            )
            .await
        });

        let res = connect(SpawnPeer {
            addr,
            info_hash: hash_a,
            our_peer_id: peer_a,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            encryption: EncryptionPolicy::PlaintextOnly,
            advertise_v2: true,
            advertise_dht: true,
            ext_handshake_builder: None,
            proxy: None,
            deferred: false,
        })
        .await;
        assert!(res.is_err() || accept_fut.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn private_spawn_does_not_advertise_dht() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let info_hash = Id20([0x41; 20]);
        let local_peer_id = Id20([0x42; 20]);
        let remote_peer_id = Id20([0x43; 20]);

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0u8; HANDSHAKE_LEN];
            stream.read_exact(&mut bytes).await.unwrap();
            let request = Handshake::parse(&bytes).unwrap();
            let (byte, mask) = crate::wire::handshake::reserved::DHT;
            assert_eq!(request.reserved[byte] & mask, 0);
            let response = Handshake::new_with_v2(info_hash, remote_peer_id, false);
            stream.write_all(&response.to_bytes()).await.unwrap();
        });

        let (handle, mut events) = connect(SpawnPeer {
            addr,
            info_hash,
            our_peer_id: local_peer_id,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            encryption: EncryptionPolicy::PlaintextOnly,
            advertise_v2: false,
            advertise_dht: false,
            ext_handshake_builder: None,
            proxy: None,
            deferred: false,
        })
        .await
        .unwrap();
        assert!(matches!(
            events.recv().await,
            Some(PeerEvent::Handshook { .. })
        ));
        handle.tx.send(PeerCommand::Disconnect).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn mse_end_to_end_handshake_and_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let info_hash = Id20([7u8; 20]);
        let peer_a = Id20([8u8; 20]);
        let peer_b = Id20([9u8; 20]);

        let accept_fut = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept(
                stream,
                peer_b,
                vec![info_hash.into()],
                Duration::from_secs(10),
                EncryptionPolicy::RequireEncryption,
            )
            .await
            .unwrap()
        });

        let (a, mut rx_a) = connect(SpawnPeer {
            addr,
            info_hash,
            our_peer_id: peer_a,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
            encryption: EncryptionPolicy::RequireEncryption,
            advertise_v2: true,
            advertise_dht: true,
            ext_handshake_builder: None,
            proxy: None,
            deferred: false,
        })
        .await
        .unwrap();
        let (b, mut rx_b) = accept_fut.await.unwrap();

        match rx_a.recv().await.unwrap() {
            PeerEvent::Handshook { encrypted, .. } => assert!(encrypted),
            e => panic!("unexpected event: {e:?}"),
        }
        match rx_b.recv().await.unwrap() {
            PeerEvent::Handshook { encrypted, .. } => assert!(encrypted),
            e => panic!("unexpected event: {e:?}"),
        }

        a.tx.send(PeerCommand::Send(Message::Interested))
            .await
            .unwrap();
        match rx_b.recv().await.unwrap() {
            PeerEvent::Message(Message::Interested) => {}
            e => panic!("unexpected: {e:?}"),
        }
        b.tx.send(PeerCommand::Send(Message::Have { piece_index: 7 }))
            .await
            .unwrap();
        match rx_a.recv().await.unwrap() {
            PeerEvent::Message(Message::Have { piece_index }) => assert_eq!(piece_index, 7),
            e => panic!("unexpected: {e:?}"),
        }
    }

    #[tokio::test]
    async fn prefer_utp_runs_mse_over_utp_when_encryption_required() {
        use crate::utp::UtpSocket;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_utp = UtpSocket::bind(addr).await.unwrap();
        let client_utp = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let info_hash = Id20([7u8; 20]);
        let peer_a = Id20([8u8; 20]);
        let peer_b = Id20([9u8; 20]);
        let tcp_seen = Arc::new(AtomicBool::new(false));

        let tcp_seen_task = tcp_seen.clone();
        let tcp_watch = tokio::spawn(async move {
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(1), listener.accept()).await
            {
                tcp_seen_task.store(true, Ordering::SeqCst);
            }
        });
        let accept_utp_sock = server_utp.clone();
        let accept_fut = tokio::spawn(async move {
            let stream = accept_utp_sock.accept().await.unwrap();
            accept_utp(
                stream,
                peer_b,
                vec![info_hash.into()],
                Duration::from_secs(10),
                EncryptionPolicy::RequireEncryption,
            )
            .await
            .unwrap()
        });

        let (_client, mut rx_client) = connect_prefer_utp(
            SpawnPeer {
                addr,
                info_hash,
                our_peer_id: peer_a,
                connect_timeout: Duration::from_secs(5),
                read_timeout: Duration::from_secs(10),
                encryption: EncryptionPolicy::RequireEncryption,
                advertise_v2: true,
                advertise_dht: true,
                ext_handshake_builder: None,
                proxy: None,
                deferred: false,
            },
            Some(client_utp),
        )
        .await
        .unwrap();
        let (_server, mut rx_server) = accept_fut.await.unwrap();

        for rx in [&mut rx_client, &mut rx_server] {
            match rx.recv().await.unwrap() {
                PeerEvent::Handshook { encrypted, utp, .. } => assert!(encrypted && utp),
                e => panic!("unexpected event: {e:?}"),
            }
        }
        tcp_watch.await.unwrap();
        assert!(!tcp_seen.load(Ordering::SeqCst), "TCP should not be needed");
    }

    #[tokio::test]
    async fn utp_mse_handshake_both_directions() {
        use crate::utp::UtpSocket;

        let info_hash = Id20([0x5au8; 20]);
        let client_id = Id20([1u8; 20]);
        let server_id = Id20([2u8; 20]);
        let server_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = server_sock.local_addr();

        let server = tokio::spawn(async move {
            let stream = server_sock.accept().await.unwrap();
            let (handle, mut rx) = accept_utp(
                stream,
                server_id,
                vec![info_hash.into()],
                Duration::from_secs(5),
                EncryptionPolicy::RequireEncryption,
            )
            .await
            .unwrap();
            match rx.recv().await.unwrap() {
                PeerEvent::Handshook { encrypted, utp, .. } => assert!(encrypted && utp),
                e => panic!("expected Handshook, got {e:?}"),
            }
            handle
                .tx
                .send(PeerCommand::Send(Message::Have { piece_index: 9 }))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let result = tokio::time::timeout(Duration::from_secs(10), async move {
            let (_handle, mut rx) = connect_utp(
                &client_sock,
                SpawnPeer {
                    addr: server_addr,
                    info_hash,
                    our_peer_id: client_id,
                    connect_timeout: Duration::from_secs(5),
                    read_timeout: Duration::from_secs(5),
                    encryption: EncryptionPolicy::RequireEncryption,
                    advertise_v2: false,
                    advertise_dht: true,
                    ext_handshake_builder: None,
                    proxy: None,
                    deferred: false,
                },
            )
            .await
            .unwrap();
            match rx.recv().await.unwrap() {
                PeerEvent::Handshook {
                    peer_id,
                    encrypted,
                    utp,
                    ..
                } => {
                    assert_eq!(peer_id, server_id);
                    assert!(encrypted && utp);
                }
                e => panic!("expected Handshook, got {e:?}"),
            }
            match rx.recv().await.unwrap() {
                PeerEvent::Message(Message::Have { piece_index }) => assert_eq!(piece_index, 9),
                e => panic!("expected Have over encrypted µTP, got {e:?}"),
            }
        })
        .await;
        result.expect("encrypted µTP exchange timed out");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn utp_accept_rejects_plaintext_when_encryption_required() {
        use crate::utp::UtpSocket;
        use tokio::io::AsyncWriteExt;

        let info_hash = Id20([0x5bu8; 20]);
        let server_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = server_sock.local_addr();
        let client = tokio::spawn(async move {
            let mut s = client_sock.connect(server_addr).await.unwrap();
            let hs = Handshake::new_with_v2(info_hash, Id20([3u8; 20]), false);
            s.write_all(&hs.to_bytes()).await.unwrap();
            s.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });
        let stream = server_sock.accept().await.unwrap();
        let result = accept_utp(
            stream,
            Id20([4u8; 20]),
            vec![info_hash.into()],
            Duration::from_secs(5),
            EncryptionPolicy::RequireEncryption,
        )
        .await;
        assert!(matches!(result, Err(e) if e.kind() == std::io::ErrorKind::InvalidData));
        client.await.unwrap();
    }

    #[tokio::test]
    async fn utp_plaintext_handshake_and_message_over_loopback() {
        use crate::utp::UtpSocket;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let info_hash = Id20([7u8; 20]);
        let client_id = Id20([1u8; 20]);
        let server_id = Id20([2u8; 20]);

        let server_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = server_sock.local_addr();

        let server = tokio::spawn(async move {
            let mut s = server_sock.accept().await.unwrap();
            let mut buf = [0u8; HANDSHAKE_LEN];
            s.read_exact(&mut buf).await.unwrap();
            let remote = Handshake::parse(&buf).unwrap();
            assert_eq!(remote.info_hash, info_hash);
            assert_eq!(remote.peer_id, client_id);
            let reply = Handshake::new_with_v2(remote.info_hash, server_id, false);
            s.write_all(&reply.to_bytes()).await.unwrap();
            s.write_all(&MessageEncoder::encode(&Message::Have { piece_index: 7 }))
                .await
                .unwrap();
            s.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let result = tokio::time::timeout(Duration::from_secs(10), async move {
            let utp = client_sock.connect(server_addr).await.unwrap();
            let (_handle, mut rx) = connect_utp_plaintext(
                utp,
                SpawnPeer {
                    addr: server_addr,
                    info_hash,
                    our_peer_id: client_id,
                    connect_timeout: Duration::from_secs(5),
                    read_timeout: Duration::from_secs(5),
                    encryption: EncryptionPolicy::PlaintextOnly,
                    advertise_v2: false,
                    advertise_dht: true,
                    ext_handshake_builder: None,
                    proxy: None,
                    deferred: false,
                },
            )
            .await
            .unwrap();

            match rx.recv().await.unwrap() {
                PeerEvent::Handshook {
                    peer_id,
                    info_hash: ih,
                    encrypted,
                    ..
                } => {
                    assert_eq!(peer_id, server_id);
                    assert_eq!(ih, info_hash);
                    assert!(!encrypted);
                }
                e => panic!("expected Handshook, got {e:?}"),
            }
            match rx.recv().await.unwrap() {
                PeerEvent::Message(Message::Have { piece_index }) => assert_eq!(piece_index, 7),
                e => panic!("expected Have over µTP, got {e:?}"),
            }
        })
        .await;
        result.expect("µTP handshake/message exchange timed out");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn utp_fallback_engages_when_tcp_refused() {
        use crate::utp::UtpSocket;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let info_hash = Id20([7u8; 20]);
        let client_id = Id20([1u8; 20]);
        let server_id = Id20([2u8; 20]);

        let server_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client_utp = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = server_sock.local_addr();

        let server = tokio::spawn(async move {
            let mut s = server_sock.accept().await.unwrap();
            let mut buf = [0u8; HANDSHAKE_LEN];
            s.read_exact(&mut buf).await.unwrap();
            let remote = Handshake::parse(&buf).unwrap();
            let reply = Handshake::new_with_v2(remote.info_hash, server_id, false);
            s.write_all(&reply.to_bytes()).await.unwrap();
            s.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let spawn = SpawnPeer {
            addr: server_addr,
            info_hash,
            our_peer_id: client_id,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            encryption: EncryptionPolicy::PlaintextOnly,
            advertise_v2: false,
            advertise_dht: true,
            ext_handshake_builder: None,
            proxy: None,
            deferred: false,
        };
        let (_handle, mut rx) = tokio::time::timeout(
            Duration::from_secs(10),
            connect_with_utp_fallback(spawn, Some(client_utp)),
        )
        .await
        .expect("utp fallback timed out")
        .expect("utp fallback connect failed");
        match rx.recv().await.unwrap() {
            PeerEvent::Handshook { peer_id, .. } => assert_eq!(peer_id, server_id),
            e => panic!("expected Handshook via µTP fallback, got {e:?}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn utp_inbound_accept_completes_handshake() {
        use crate::utp::UtpSocket;

        let info_hash = Id20([9u8; 20]);
        let client_id = Id20([1u8; 20]);
        let server_id = Id20([2u8; 20]);

        let server_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = server_sock.local_addr();

        let server = tokio::spawn(async move {
            let s = server_sock.accept().await.unwrap();
            let known = vec![KnownInfoHash {
                info_hash,
                advertise_v2: false,
                advertise_dht: true,
                ext_handshake_builder: None,
            }];
            let (handle, mut rx) =
                accept_utp_plaintext(s, server_id, known, Duration::from_secs(5))
                    .await
                    .unwrap();
            match rx.recv().await.unwrap() {
                PeerEvent::Handshook {
                    peer_id,
                    info_hash: ih,
                    ..
                } => {
                    assert_eq!(peer_id, client_id);
                    assert_eq!(ih, info_hash);
                }
                e => panic!("server expected Handshook, got {e:?}"),
            }
            handle
                .tx
                .send(PeerCommand::Send(Message::Have { piece_index: 9 }))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let result = tokio::time::timeout(Duration::from_secs(10), async move {
            let utp = client_sock.connect(server_addr).await.unwrap();
            let (_handle, mut rx) = connect_utp_plaintext(
                utp,
                SpawnPeer {
                    addr: server_addr,
                    info_hash,
                    our_peer_id: client_id,
                    connect_timeout: Duration::from_secs(5),
                    read_timeout: Duration::from_secs(5),
                    encryption: EncryptionPolicy::PlaintextOnly,
                    advertise_v2: false,
                    advertise_dht: true,
                    ext_handshake_builder: None,
                    proxy: None,
                    deferred: false,
                },
            )
            .await
            .unwrap();
            match rx.recv().await.unwrap() {
                PeerEvent::Handshook { peer_id, .. } => assert_eq!(peer_id, server_id),
                e => panic!("client expected Handshook, got {e:?}"),
            }
            match rx.recv().await.unwrap() {
                PeerEvent::Message(Message::Have { piece_index }) => assert_eq!(piece_index, 9),
                e => panic!("client expected Have over µTP, got {e:?}"),
            }
        })
        .await;
        result.expect("inbound µTP accept handshake timed out");
        server.await.unwrap();
    }
}
