use serde_json::Value;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub enum EngineEvent {
    DownloadStart { gid: String },
    DownloadPause { gid: String },
    DownloadStop { gid: String },
    DownloadComplete { gid: String, seeded: bool },
    DownloadError { gid: String },
    BtDownloadComplete { gid: String, seeding: bool },
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
            | Self::DownloadComplete { gid, .. }
            | Self::DownloadError { gid }
            | Self::BtDownloadComplete { gid, .. } => gid,
        }
    }

    pub fn params(&self) -> Value {
        match self {
            Self::DownloadComplete { gid, seeded } => {
                serde_json::json!({ "gid": gid, "seeded": seeded })
            }
            Self::BtDownloadComplete { gid, seeding } => {
                serde_json::json!({ "gid": gid, "seeding": seeding })
            }
            _ => serde_json::json!({ "gid": self.gid() }),
        }
    }

    pub fn to_notification(&self) -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": self.method_name(),
            "params": [self.params()]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifications_carry_the_completion_flags() {
        let params = |event: EngineEvent| event.to_notification()["params"][0].clone();
        assert_eq!(
            params(EngineEvent::DownloadComplete {
                gid: "a".into(),
                seeded: true
            }),
            serde_json::json!({ "gid": "a", "seeded": true })
        );
        assert_eq!(
            params(EngineEvent::BtDownloadComplete {
                gid: "b".into(),
                seeding: false
            }),
            serde_json::json!({ "gid": "b", "seeding": false })
        );
        assert_eq!(
            params(EngineEvent::DownloadStart { gid: "c".into() }),
            serde_json::json!({ "gid": "c" })
        );
    }
}
