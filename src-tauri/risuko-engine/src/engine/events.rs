use serde_json::Value;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub enum EngineEvent {
    DownloadStart { gid: String },
    DownloadPause { gid: String },
    DownloadStop { gid: String },
    DownloadComplete { gid: String },
    DownloadError { gid: String },
    BtDownloadComplete { gid: String },
}

impl EngineEvent {
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::DownloadStart { .. } => "risuko.onDownloadStart",
            Self::DownloadPause { .. } => "risuko.onDownloadPause",
            Self::DownloadStop { .. } => "risuko.onDownloadStop",
            Self::DownloadComplete { .. } => "risuko.onDownloadComplete",
            Self::DownloadError { .. } => "risuko.onDownloadError",
            Self::BtDownloadComplete { .. } => "risuko.onBtDownloadComplete",
        }
    }

    pub fn gid(&self) -> &str {
        match self {
            Self::DownloadStart { gid }
            | Self::DownloadPause { gid }
            | Self::DownloadStop { gid }
            | Self::DownloadComplete { gid }
            | Self::DownloadError { gid }
            | Self::BtDownloadComplete { gid } => gid,
        }
    }

    pub fn to_notification(&self) -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": self.method_name(),
            "params": [{"gid": self.gid()}]
        })
    }
}

#[derive(Clone)]
pub struct EventBroadcaster {
    sender: broadcast::Sender<EngineEvent>,
}

impl EventBroadcaster {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    pub fn send(&self, event: EngineEvent) {
        let _ = self.sender.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.sender.subscribe()
    }
}

impl Default for EventBroadcaster {
    fn default() -> Self {
        Self::new(256)
    }
}
