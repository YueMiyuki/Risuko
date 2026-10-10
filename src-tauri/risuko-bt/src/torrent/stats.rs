use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static PEERS_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct PeerSnapshot {
    pub addr: SocketAddr,
    pub bitfield: Arc<[u8]>,
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub seeder: bool,
    pub peer_id: Option<[u8; 20]>,
    pub client: Option<String>,
    pub downloaded: u64,
    pub uploaded: u64,
    pub dl_speed: u64,
    pub up_speed: u64,
    pub incoming: bool,
    pub snubbed: bool,
    pub progress: f64,
    pub optimistic_unchoke: bool,
}

#[derive(Clone, Default)]
pub struct SpeedSample {
    pub mbps: f32,
}

impl SpeedSample {
    pub fn update(&mut self, bytes: u64, dt: f32) {
        if dt <= 0.0 {
            return;
        }
        let instant = (bytes as f32 / dt) / 1_048_576.0;
        let alpha = 0.3;
        self.mbps = self.mbps * (1.0 - alpha) + instant * alpha;
    }
}

#[derive(Clone, Default)]
pub struct AggregatedLiveStats {
    pub live: u32,
}

#[derive(Clone, Default)]
pub struct Snapshot {
    pub peer_stats: AggregatedLiveStats,
}

#[derive(Clone, Default)]
pub struct LiveStats {
    pub snapshot: Snapshot,
    pub download_speed: SpeedSample,
    pub upload_speed: SpeedSample,
}

impl LiveStats {
    pub fn update(&mut self, dl: u64, ul: u64, dt: f32) {
        self.download_speed.update(dl, dt);
        self.upload_speed.update(ul, dt);
    }
}

#[derive(Clone)]
pub struct TorrentStats {
    pub total_bytes: u64,
    pub progress_bytes: u64,
    pub left_bytes: u64,
    pub uploaded_bytes: u64,
    pub(crate) session_downloaded: u64,
    pub finished: bool,
    pub file_progress: Arc<Vec<u64>>,
    pub error: Option<String>,
    pub live: Option<LiveStats>,
    pub peers: Arc<[PeerSnapshot]>,
    pub peers_seq: u64,
}

impl TorrentStats {
    pub(crate) fn initial(total_bytes: u64, left_bytes: u64, file_lens: Vec<u64>) -> Self {
        let file_progress = Arc::new(vec![0u64; file_lens.len()]);
        Self {
            total_bytes,
            progress_bytes: 0,
            left_bytes,
            uploaded_bytes: 0,
            session_downloaded: 0,
            finished: false,
            file_progress,
            error: None,
            live: Some(LiveStats::default()),
            peers: Arc::from(Vec::new()),
            peers_seq: 0,
        }
    }

    pub(crate) fn set_peers(&mut self, peers: Vec<PeerSnapshot>) {
        self.peers = peers.into();
        self.peers_seq = PEERS_SEQ.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn clear_peers(&mut self) {
        if !self.peers.is_empty() {
            self.set_peers(Vec::new());
        }
    }

    pub(crate) fn live_mut(&mut self) -> &mut LiveStats {
        self.live.get_or_insert_with(LiveStats::default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap() -> PeerSnapshot {
        PeerSnapshot {
            addr: "127.0.0.1:6881".parse().unwrap(),
            bitfield: Arc::from(Vec::new()),
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            seeder: false,
            peer_id: None,
            client: None,
            downloaded: 0,
            uploaded: 0,
            dl_speed: 0,
            up_speed: 0,
            incoming: false,
            snubbed: false,
            progress: 0.0,
            optimistic_unchoke: false,
        }
    }

    #[test]
    fn peers_seq_changes_only_when_the_list_does() {
        let mut stats = TorrentStats::initial(10, 10, vec![10]);
        let first = stats.peers_seq;
        stats.clear_peers();
        assert_eq!(stats.peers_seq, first);
        stats.set_peers(vec![snap()]);
        assert_ne!(stats.peers_seq, first);
        let polled = stats.clone();
        assert_eq!(polled.peers_seq, stats.peers_seq);
        assert!(Arc::ptr_eq(&polled.peers, &stats.peers));
        stats.clear_peers();
        assert_ne!(stats.peers_seq, polled.peers_seq);
        assert!(stats.peers.is_empty());
    }

    #[test]
    fn cloned_stats_share_file_progress_until_written() {
        let stats = TorrentStats::initial(10, 10, vec![5, 5]);
        let polled = stats.clone();
        assert!(Arc::ptr_eq(&polled.file_progress, &stats.file_progress));
    }
}
