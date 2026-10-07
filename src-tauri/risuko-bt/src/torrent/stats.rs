//! Per-torrent live stats

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

/// Snapshot of a single connected peer for UI consumption
#[derive(Clone)]
pub struct PeerSnapshot {
    pub addr: SocketAddr,
    /// Raw bitfield bytes; consumers can hex-encode for display
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

/// Exponential-moving-average speed tracker
#[derive(Clone)]
pub struct SpeedSample {
    pub mbps: f32,
    // internal
    bytes_since: u64,
    last_update: Option<Instant>,
}

impl Default for SpeedSample {
    fn default() -> Self {
        Self {
            mbps: 0.0,
            bytes_since: 0,
            last_update: None,
        }
    }
}

impl SpeedSample {
    /// Record that `bytes` have moved during the last `dt` seconds and update the EMA with a fixed alpha
    pub fn update(&mut self, bytes: u64, dt: f32) {
        let now = Instant::now();
        self.last_update = Some(now);
        if dt <= 0.0 {
            return;
        }
        let instant = (bytes as f32 / dt) / 1_048_576.0;
        let alpha = 0.3;
        self.mbps = self.mbps * (1.0 - alpha) + instant * alpha;
        self.bytes_since = bytes;
    }
}

#[derive(Clone, Default)]
pub struct AggregatedLiveStats {
    /// Number of peers currently connected
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
    /// Bytes still needed for the selected files
    pub left_bytes: u64,
    pub uploaded_bytes: u64,
    pub finished: bool,
    pub file_progress: Vec<u64>,
    pub live: Option<LiveStats>,
    pub peers: Vec<PeerSnapshot>,
    pub(crate) live_stats: LiveStats,
}

impl TorrentStats {
    pub(crate) fn initial(total_bytes: u64, file_lens: Vec<u64>) -> Self {
        let file_progress = vec![0u64; file_lens.len()];
        Self {
            total_bytes,
            progress_bytes: 0,
            left_bytes: total_bytes,
            uploaded_bytes: 0,
            finished: false,
            file_progress,
            live: Some(LiveStats::default()),
            peers: Vec::new(),
            live_stats: LiveStats::default(),
        }
    }

    pub(crate) fn refresh_live(&mut self) {
        self.live = Some(self.live_stats.clone());
    }
}
