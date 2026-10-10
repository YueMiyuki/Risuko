pub mod connection;

pub use connection::{
    accept, accept_deferred, accept_utp, accept_utp_deferred, accept_utp_plaintext, connect,
    connect_prefer_utp, connect_utp_plaintext, connect_with_utp_fallback, EncryptionPolicy,
    ExtHandshakeBuilder, KnownInfoHash, PeerCommand, PeerEvent, PeerEventSink, PeerHandle,
    RecvGate, SpawnPeer,
};
