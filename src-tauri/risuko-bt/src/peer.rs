//! Per-peer connection actor: a reader task decodes `Message` frames to `PeerEvent`s and a writer task sends `PeerCommand`s, both over mpsc to the torrent state machine; the actor owns local BT state (choking/interested/bitfield) while request/unchoke policy lives in `torrent::`

pub mod connection;

pub use connection::{
    accept, accept_utp, accept_utp_plaintext, connect, connect_prefer_utp, connect_utp_plaintext,
    connect_with_utp_fallback, EncryptionPolicy, ExtHandshakeBuilder, KnownInfoHash, PeerCommand,
    PeerEvent, PeerHandle, SpawnPeer,
};
