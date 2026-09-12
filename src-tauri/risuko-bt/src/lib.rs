//! BitTorrent v1 engine

pub mod api;
pub mod bencode;
pub mod blocklist;
pub mod core;
pub mod dht;
pub mod limiter;
pub mod lsd;
pub mod magnet;
pub mod peer;
pub mod piece;
pub mod session;
pub mod storage;
pub mod torrent;
pub mod tracker;
pub mod upnp;
pub mod utp;
pub mod webseed;
pub mod wire;

pub use api::TorrentIdOrHash;
pub use blocklist::{BlockList, BlocklistApplyResult};
pub use core::metainfo::{
    parse_torrent, FileDetails, TorrentMeta, TorrentMetaInfo, ValidatedTorrentMetaV1Info,
};
pub use core::{generate_peer_id, Id20, Lengths, Magnet};
pub use peer::EncryptionPolicy;
pub use session::{
    split_initial_peer_sources, AddTorrent, AddTorrentOptions, AddTorrentResponse,
    ListOnlyResponse, ListenerOptions, Session, SessionOptions, UpnpStatus,
};
pub use torrent::{ManagedTorrent, PeerCandidate, PeerSnapshot, PeerSource, TorrentStats};
