use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Active,
    #[default]
    Waiting,
    Paused,
    Scheduled,
    Complete,
    Error,
    Removed,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Waiting => "waiting",
            Self::Paused => "paused",
            Self::Scheduled => "scheduled",
            Self::Complete => "complete",
            Self::Error => "error",
            Self::Removed => "removed",
        }
    }

    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Complete | Self::Error | Self::Removed)
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskKind {
    #[default]
    Http,
    #[serde(alias = "youtube")]
    Media,
    Torrent,
    Ed2k,
    M3u8,
    Ftp,
    Metalink,
    Usenet,
    Adc,
    Gnutella,
    G2,
    Gift,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsenetTaskOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_limits: Option<serde_json::Value>,
    #[serde(default)]
    pub archive_limit_override_confirmed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsenetTaskFile {
    pub name: String,
    pub subject: String,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub segments: Vec<UsenetTaskSegment>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsenetTaskSegment {
    pub number: u32,
    pub bytes: u64,
    pub message_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsenetTaskData {
    pub options: UsenetTaskOptions,
    #[serde(default)]
    pub files: Vec<UsenetTaskFile>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsenetRepairFailure {
    pub needed_blocks: u32,
    pub available_blocks: u32,
    pub partials_retained: bool,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ed2kKadTaskStatus {
    pub state: String,
    pub queried_nodes: u32,
    pub discovered_sources: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadFile {
    pub index: String,
    pub path: String,
    pub length: String,
    pub completed_length: String,
    pub selected: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub uris: Vec<FileUri>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileUri {
    pub uri: String,
    pub status: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfo {
    pub ip: String,
    pub port: String,
    #[serde(default)]
    pub percent: u8,
    pub am_choking: String,
    pub peer_choking: String,
    pub seeder: String,
    #[serde(default)]
    pub peer_id: String,
    #[serde(default)]
    pub peer_client_name: String,
    #[serde(default)]
    pub am_interested: String,
    #[serde(default)]
    pub peer_interested: String,
    #[serde(default)]
    pub download_speed: u64,
    #[serde(default)]
    pub upload_speed: u64,
    #[serde(default)]
    pub downloaded: u64,
    #[serde(default)]
    pub uploaded: u64,
    #[serde(default)]
    pub progress: f64,
    #[serde(default)]
    pub incoming: bool,
    #[serde(default)]
    pub snubbed: bool,
    #[serde(default)]
    pub handshaking: bool,
    #[serde(default)]
    pub optimistic_unchoke: bool,
    #[serde(default)]
    pub bitfield: String,
    #[serde(skip)]
    pub raw_bitfield: std::sync::Arc<[u8]>,
}

#[derive(Clone, Default)]
pub struct ChunkProgress {
    pub completed: u64,
    pub total: u64,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DownloadTask {
    pub gid: String,
    pub status: TaskStatus,
    pub kind: TaskKind,
    pub uris: Vec<String>,
    pub dir: String,
    pub out: String,
    pub total_length: u64,
    pub completed_length: u64,
    pub download_speed: u64,
    pub upload_speed: u64,
    #[serde(default)]
    pub upload_length: u64,
    pub connections: u32,
    pub files: Vec<DownloadFile>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub options: Map<String, Value>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usenet: Option<UsenetTaskData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usenet_stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usenet_warning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usenet_repair_failure: Option<UsenetRepairFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ed2k_kad: Option<Ed2kKadTaskStatus>,
    pub info_hash: Option<String>,
    #[serde(default)]
    pub info_hash_v2: Option<String>,
    #[serde(default)]
    pub meta_version: Option<String>,
    pub bt_name: Option<String>,
    pub seeder: bool,
    pub num_seeders: u32,
    #[serde(skip)]
    pub peers: Vec<PeerInfo>,
    #[serde(default)]
    pub piece_length: u32,
    #[serde(default)]
    pub num_pieces: u32,
    #[serde(default)]
    pub bt_comment: Option<String>,
    #[serde(default)]
    pub bt_creation_date: Option<i64>,
    #[serde(default)]
    pub bt_announce_list: Vec<Vec<String>>,
    pub created_at: u64,
    #[serde(default)]
    pub seeding_since: u64,
    #[serde(default)]
    pub start_at: Option<u64>,
    #[serde(default)]
    pub schedule_missed: bool,
    #[serde(skip, default)]
    pub chunk_progress: Vec<ChunkProgress>,
    #[serde(skip, default)]
    pub bt_files_cache: Option<(u64, bool)>,
    #[serde(skip, default)]
    pub bt_peers_key: Option<(u64, u32)>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uris: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub out: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trackers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTaskOutcome {
    pub restarted: bool,
    pub trackers_added: usize,
    pub progress_preserved: bool,
}

impl DownloadTask {
    pub fn new_http(
        gid: String,
        uris: Vec<String>,
        dir: String,
        tag: Option<String>,
        options: Map<String, Value>,
    ) -> Self {
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let initial_files = if !uris.is_empty() {
            let file_uris: Vec<FileUri> = uris
                .iter()
                .map(|u| FileUri {
                    uri: u.clone(),
                    status: "waiting".to_string(),
                })
                .collect();
            let display_out = out.strip_suffix(".part").unwrap_or(&out);
            let initial_path = if !display_out.is_empty() {
                format!("{}/{}", dir, display_out)
            } else {
                uris.first().cloned().unwrap_or_default()
            };
            vec![DownloadFile {
                index: "1".to_string(),
                path: initial_path,
                length: "0".to_string(),
                completed_length: "0".to_string(),
                selected: "true".to_string(),
                uris: file_uris,
            }]
        } else {
            Vec::new()
        };

        Self {
            gid,
            uris,
            dir,
            out,
            files: initial_files,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn new_media(
        gid: String,
        uri: String,
        dir: String,
        tag: Option<String>,
        options: Map<String, Value>,
    ) -> Self {
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let initial_path = if !out.is_empty() {
            format!("{}/{}", dir, out)
        } else {
            uri.clone()
        };

        let initial_files = vec![DownloadFile {
            index: "1".to_string(),
            path: initial_path,
            length: "0".to_string(),
            completed_length: "0".to_string(),
            selected: "true".to_string(),
            uris: vec![FileUri {
                uri: uri.clone(),
                status: "waiting".to_string(),
            }],
        }];

        Self {
            gid,
            kind: TaskKind::Media,
            uris: vec![uri],
            dir,
            out,
            files: initial_files,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn new_torrent(
        gid: String,
        dir: String,
        tag: Option<String>,
        options: Map<String, Value>,
    ) -> Self {
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        Self {
            gid,
            kind: TaskKind::Torrent,
            dir,
            out,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn new_metalink(
        gid: String,
        dir: String,
        tag: Option<String>,
        options: Map<String, Value>,
        files: Vec<DownloadFile>,
    ) -> Self {
        Self {
            gid,
            kind: TaskKind::Metalink,
            dir,
            files,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn new_usenet(
        gid: String,
        dir: String,
        tag: Option<String>,
        title: Option<String>,
        options: Map<String, Value>,
        files: Vec<DownloadFile>,
    ) -> Self {
        let out = title.unwrap_or_default();
        Self {
            gid,
            kind: TaskKind::Usenet,
            dir,
            out,
            files,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn with_usenet_data(mut self, usenet: UsenetTaskData) -> Self {
        self.usenet = Some(usenet);
        self
    }

    pub fn new_ed2k(
        gid: String,
        uri: String,
        file_name: String,
        file_size: u64,
        dir: String,
        tag: Option<String>,
        options: Map<String, Value>,
    ) -> Self {
        let out = if !file_name.is_empty() {
            file_name.clone()
        } else {
            options
                .get("out")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };

        let file_path = if !out.is_empty() {
            format!("{}/{}", dir, out)
        } else {
            String::new()
        };

        let initial_files = vec![DownloadFile {
            index: "1".to_string(),
            path: file_path,
            length: file_size.to_string(),
            completed_length: "0".to_string(),
            selected: "true".to_string(),
            uris: vec![FileUri {
                uri: uri.clone(),
                status: "waiting".to_string(),
            }],
        }];

        Self {
            gid,
            kind: TaskKind::Ed2k,
            uris: vec![uri],
            dir,
            out,
            total_length: file_size,
            files: initial_files,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn new_m3u8(
        gid: String,
        uri: String,
        out: String,
        dir: String,
        tag: Option<String>,
        options: Map<String, Value>,
    ) -> Self {
        let out = if !out.is_empty() {
            out
        } else {
            options
                .get("out")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };

        let file_path = if !out.is_empty() {
            format!("{}/{}", dir, out)
        } else {
            String::new()
        };

        let initial_files = vec![DownloadFile {
            index: "1".to_string(),
            path: file_path,
            length: "0".to_string(),
            completed_length: "0".to_string(),
            selected: "true".to_string(),
            uris: vec![FileUri {
                uri: uri.clone(),
                status: "waiting".to_string(),
            }],
        }];

        Self {
            gid,
            kind: TaskKind::M3u8,
            uris: vec![uri],
            dir,
            out,
            files: initial_files,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn new_ftp(
        gid: String,
        uri: String,
        dir: String,
        tag: Option<String>,
        options: Map<String, Value>,
    ) -> Self {
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let display_out = out.strip_suffix(".part").unwrap_or(&out);
        let initial_path = if !display_out.is_empty() {
            format!("{}/{}", dir, display_out)
        } else {
            uri.clone()
        };

        let initial_files = vec![DownloadFile {
            index: "1".to_string(),
            path: initial_path,
            length: "0".to_string(),
            completed_length: "0".to_string(),
            selected: "true".to_string(),
            uris: vec![FileUri {
                uri: uri.clone(),
                status: "waiting".to_string(),
            }],
        }];

        Self {
            gid,
            kind: TaskKind::Ftp,
            uris: vec![uri],
            dir,
            out,
            files: initial_files,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn new_simple_protocol(
        gid: String,
        kind: TaskKind,
        uri: String,
        out_hint: String,
        size_hint: u64,
        dir: String,
        tag: Option<String>,
        options: Map<String, Value>,
    ) -> Self {
        let out = if !out_hint.is_empty() {
            out_hint
        } else {
            options
                .get("out")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        let file_path = if !out.is_empty() {
            format!("{}/{}", dir, out)
        } else {
            String::new()
        };
        let initial_files = vec![DownloadFile {
            index: "1".to_string(),
            path: file_path,
            length: size_hint.to_string(),
            completed_length: "0".to_string(),
            selected: "true".to_string(),
            uris: vec![FileUri {
                uri: uri.clone(),
                status: "waiting".to_string(),
            }],
        }];

        Self {
            gid,
            kind,
            uris: vec![uri],
            dir,
            out,
            total_length: size_hint,
            files: initial_files,
            options,
            tag,
            created_at: now_ms(),
            ..Default::default()
        }
    }

    pub fn to_rpc_status(&self, keys: &[String]) -> Value {
        let full = self.to_full_rpc_status(keys);
        if keys.is_empty() {
            return full;
        }
        let Value::Object(mut map) = full else {
            return full;
        };
        let mut filtered = Map::new();
        for key in keys {
            if let Some(val) = map.remove(key) {
                filtered.insert(key.clone(), val);
            }
        }
        Value::Object(filtered)
    }

    fn to_full_rpc_status(&self, keys: &[String]) -> Value {
        let want_files = keys.is_empty() || keys.iter().any(|k| k == "files");
        let mut m = Map::new();
        m.insert("gid".into(), Value::String(self.gid.clone()));
        m.insert(
            "status".into(),
            Value::String(self.status.as_str().to_string()),
        );
        if let Ok(Value::String(kind)) = serde_json::to_value(self.kind) {
            m.insert("kind".into(), Value::String(kind));
        }
        m.insert(
            "totalLength".into(),
            Value::String(self.total_length.to_string()),
        );
        m.insert(
            "completedLength".into(),
            Value::String(self.completed_length.to_string()),
        );
        m.insert(
            "downloadSpeed".into(),
            Value::String(self.download_speed.to_string()),
        );
        m.insert(
            "uploadSpeed".into(),
            Value::String(self.upload_speed.to_string()),
        );
        m.insert(
            "uploadLength".into(),
            Value::String(self.upload_length.to_string()),
        );
        m.insert(
            "connections".into(),
            Value::String(self.connections.to_string()),
        );
        m.insert("dir".into(), Value::String(self.dir.clone()));
        if let Some(ref tag) = self.tag {
            m.insert("tag".into(), Value::String(tag.clone()));
        }

        if want_files {
            m.insert(
                "files".into(),
                serde_json::to_value(&self.files).unwrap_or_default(),
            );
        }

        if let Some(ref code) = self.error_code {
            m.insert("errorCode".into(), Value::String(code.clone()));
        }
        if let Some(ref msg) = self.error_message {
            m.insert("errorMessage".into(), Value::String(msg.clone()));
        }

        m.insert(
            "createdAt".into(),
            Value::String(self.created_at.to_string()),
        );

        if let Some(ts) = self.start_at {
            m.insert("startAt".into(), Value::String(ts.to_string()));
        }
        if self.schedule_missed {
            m.insert("scheduleMissed".into(), Value::Bool(true));
        }

        if self.kind == TaskKind::Torrent {
            let mut bt = Map::new();
            if let Some(ref hash) = self.info_hash {
                m.insert("infoHash".into(), Value::String(hash.clone()));
                bt.insert("infoHash".into(), Value::String(hash.clone()));
            }
            if let Some(ref hash) = self.info_hash_v2 {
                m.insert("infoHashV2".into(), Value::String(hash.clone()));
                bt.insert("infoHashV2".into(), Value::String(hash.clone()));
            }
            if let Some(ref v) = self.meta_version {
                m.insert("metaVersion".into(), Value::String(v.clone()));
                bt.insert("metaVersion".into(), Value::String(v.clone()));
            }
            if let Some(ref name) = self.bt_name {
                let mut info = Map::new();
                info.insert("name".into(), Value::String(name.clone()));
                let dir_name = super::torrent::disk_component(name);
                if dir_name != *name {
                    info.insert("dirName".into(), Value::String(dir_name));
                }
                bt.insert("info".into(), Value::Object(info));
            }
            if let Some(ref c) = self.bt_comment {
                bt.insert("comment".into(), Value::String(c.clone()));
            }
            if let Some(ts) = self.bt_creation_date {
                bt.insert("creationDate".into(), Value::from(ts));
            }
            if !self.bt_announce_list.is_empty() {
                let tiers: Vec<Value> = self
                    .bt_announce_list
                    .iter()
                    .map(|tier| {
                        Value::Array(tier.iter().map(|u| Value::String(u.clone())).collect())
                    })
                    .collect();
                bt.insert("announceList".into(), Value::Array(tiers));
            }
            m.insert("bittorrent".into(), Value::Object(bt));
            m.insert(
                "seeder".into(),
                Value::String(if self.seeder { "true" } else { "false" }.into()),
            );
            m.insert(
                "numSeeders".into(),
                Value::String(self.num_seeders.to_string()),
            );
            if self.piece_length > 0 {
                m.insert(
                    "pieceLength".into(),
                    Value::String(self.piece_length.to_string()),
                );
            }
            if self.num_pieces > 0 {
                m.insert(
                    "numPieces".into(),
                    Value::String(self.num_pieces.to_string()),
                );
            }
        }

        if self.kind == TaskKind::Ed2k {
            if let Some(uri) = self.uris.first() {
                m.insert("ed2kLink".into(), Value::String(uri.clone()));
            }
            m.insert(
                "numPeers".into(),
                Value::String(self.connections.to_string()),
            );
            if let Some(status) = &self.ed2k_kad {
                m.insert(
                    "ed2kKad".into(),
                    serde_json::to_value(status).unwrap_or_default(),
                );
            }
        }

        if self.kind == TaskKind::M3u8 {
            if let Some(uri) = self.uris.first() {
                m.insert("m3u8Link".into(), Value::String(uri.clone()));
            }
        }

        if self.kind == TaskKind::Usenet {
            let want_usenet_manifest = keys.is_empty() || keys.iter().any(|key| key == "usenet");
            if want_usenet_manifest {
                if let Some(ref usenet) = self.usenet {
                    m.insert(
                        "usenet".into(),
                        serde_json::to_value(usenet).unwrap_or_default(),
                    );
                }
            }
            if keys.is_empty() || keys.iter().any(|key| key == "usenetStage") {
                if let Some(ref stage) = self.usenet_stage {
                    m.insert("usenetStage".into(), Value::String(stage.clone()));
                }
            }
            if keys.is_empty() || keys.iter().any(|key| key == "usenetWarning") {
                if let Some(ref warning) = self.usenet_warning {
                    m.insert("usenetWarning".into(), Value::String(warning.clone()));
                }
            }
            if keys.is_empty() || keys.iter().any(|key| key == "usenetRepairFailure") {
                if let Some(ref repair_failure) = self.usenet_repair_failure {
                    m.insert(
                        "usenetRepairFailure".into(),
                        serde_json::to_value(repair_failure).unwrap_or_default(),
                    );
                }
            }
        }

        if !self.chunk_progress.is_empty() {
            let chunks: Vec<Value> = self
                .chunk_progress
                .iter()
                .map(|cp| {
                    let mut cm = Map::new();
                    cm.insert(
                        "completedLength".into(),
                        Value::String(cp.completed.to_string()),
                    );
                    cm.insert("totalLength".into(), Value::String(cp.total.to_string()));
                    Value::Object(cm)
                })
                .collect();
            m.insert("chunkProgress".into(), Value::Array(chunks));
        }

        Value::Object(m)
    }
}

pub fn generate_gid() -> String {
    use rand::RngExt;
    format!("{:016x}", rand::rng().random::<u64>())
}

use crate::engine::util::now_ms;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map};

    #[test]
    fn gid_is_16_hex_chars() {
        let gid = generate_gid();
        assert_eq!(gid.len(), 16);
        assert!(gid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn gid_is_unique() {
        let a = generate_gid();
        let b = generate_gid();
        assert_ne!(a, b);
    }

    #[test]
    fn peer_info_serializes_aria2next_fields() {
        let peer = PeerInfo {
            ip: "1.2.3.4".into(),
            port: "6881".into(),
            percent: 50,
            am_choking: "true".into(),
            peer_choking: "false".into(),
            seeder: "false".into(),
            peer_id: "%2DRS0001%2D0123456789ab".into(),
            peer_client_name: "risuko-bt".into(),
            am_interested: "true".into(),
            peer_interested: "false".into(),
            download_speed: 1024,
            upload_speed: 512,
            downloaded: 4096,
            uploaded: 2048,
            progress: 0.5,
            incoming: true,
            snubbed: false,
            handshaking: false,
            optimistic_unchoke: true,
            bitfield: "ff00".into(),
            raw_bitfield: std::sync::Arc::from([].as_slice()),
        };
        let value = serde_json::to_value(&peer).unwrap();
        assert_eq!(value["peerId"], "%2DRS0001%2D0123456789ab");
        assert_eq!(value["peerClientName"], "risuko-bt");
        assert_eq!(value["amInterested"], "true");
        assert_eq!(value["peerInterested"], "false");
        assert_eq!(value["downloadSpeed"], 1024);
        assert_eq!(value["uploadSpeed"], 512);
        assert_eq!(value["downloaded"], 4096);
        assert_eq!(value["uploaded"], 2048);
        assert_eq!(value["progress"], 0.5);
        assert_eq!(value["incoming"], true);
        assert_eq!(value["snubbed"], false);
        assert_eq!(value["handshaking"], false);
        assert_eq!(value["optimisticUnchoke"], true);
        assert_eq!(value["bitfield"], "ff00");
        assert_eq!(value["amChoking"], "true");
    }

    #[test]
    fn status_as_str() {
        assert_eq!(TaskStatus::Active.as_str(), "active");
        assert_eq!(TaskStatus::Waiting.as_str(), "waiting");
        assert_eq!(TaskStatus::Paused.as_str(), "paused");
        assert_eq!(TaskStatus::Scheduled.as_str(), "scheduled");
        assert_eq!(TaskStatus::Complete.as_str(), "complete");
        assert_eq!(TaskStatus::Error.as_str(), "error");
        assert_eq!(TaskStatus::Removed.as_str(), "removed");
    }

    #[test]
    fn status_is_stopped() {
        assert!(TaskStatus::Complete.is_stopped());
        assert!(TaskStatus::Error.is_stopped());
        assert!(TaskStatus::Removed.is_stopped());
        assert!(!TaskStatus::Active.is_stopped());
        assert!(!TaskStatus::Waiting.is_stopped());
        assert!(!TaskStatus::Paused.is_stopped());
    }

    #[test]
    fn status_display() {
        assert_eq!(format!("{}", TaskStatus::Active), "active");
        assert_eq!(format!("{}", TaskStatus::Complete), "complete");
    }

    #[test]
    fn new_http_basic() {
        let opts = Map::new();
        let uris = vec!["http://example.com/file.zip".to_string()];
        let task = DownloadTask::new_http("gid1".into(), uris.clone(), "/tmp".into(), None, opts);

        assert_eq!(task.gid, "gid1");
        assert_eq!(task.status, TaskStatus::Waiting);
        assert_eq!(task.kind, TaskKind::Http);
        assert_eq!(task.uris, uris);
        assert_eq!(task.dir, "/tmp");
        assert_eq!(task.total_length, 0);
        assert_eq!(task.files.len(), 1);
        assert_eq!(task.files[0].uris.len(), 1);
    }

    #[test]
    fn new_http_strips_part_from_display_path() {
        let mut opts = Map::new();
        opts.insert("out".into(), json!("file.zip.part"));
        let uris = vec!["http://example.com/file.zip".to_string()];
        let task = DownloadTask::new_http("gid1".into(), uris, "/dl".into(), None, opts);

        assert_eq!(task.out, "file.zip.part");
        assert_eq!(task.files[0].path, "/dl/file.zip");
    }

    #[test]
    fn new_torrent_basic() {
        let opts = Map::new();
        let task = DownloadTask::new_torrent("gid2".into(), "/dl".into(), None, opts);

        assert_eq!(task.kind, TaskKind::Torrent);
        assert_eq!(task.status, TaskStatus::Waiting);
        assert!(task.files.is_empty());
        assert!(task.info_hash.is_none());
    }

    #[test]
    fn new_ed2k_sets_file_size() {
        let opts = Map::new();
        let task = DownloadTask::new_ed2k(
            "gid3".into(),
            "ed2k://|file|test.bin|1024|hash|/".into(),
            "test.bin".into(),
            1024,
            "/dl".into(),
            None,
            opts,
        );

        assert_eq!(task.kind, TaskKind::Ed2k);
        assert_eq!(task.total_length, 1024);
        assert_eq!(task.out, "test.bin");
        assert_eq!(task.files[0].length, "1024");
        assert_eq!(task.files[0].path, "/dl/test.bin");
    }

    #[test]
    fn new_m3u8_basic() {
        let opts = Map::new();
        let task = DownloadTask::new_m3u8(
            "gid4".into(),
            "http://example.com/stream.m3u8".into(),
            "stream.ts".into(),
            "/dl".into(),
            None,
            opts,
        );

        assert_eq!(task.kind, TaskKind::M3u8);
        assert_eq!(task.out, "stream.ts");
        assert_eq!(task.files[0].path, "/dl/stream.ts");
    }

    #[test]
    fn new_ftp_basic() {
        let opts = Map::new();
        let task = DownloadTask::new_ftp(
            "gid5".into(),
            "ftp://files.example.com/data.csv".into(),
            "/dl".into(),
            None,
            opts,
        );

        assert_eq!(task.kind, TaskKind::Ftp);
        assert_eq!(task.status, TaskStatus::Waiting);
        assert_eq!(task.uris[0], "ftp://files.example.com/data.csv");
    }

    #[test]
    fn new_usenet_sets_kind_and_file_metadata() {
        let files = vec![DownloadFile {
            index: "1".into(),
            path: "/dl/archive.part01.rar".into(),
            length: "42".into(),
            completed_length: "0".into(),
            selected: "true".into(),
            uris: Vec::new(),
        }];
        let metadata = UsenetTaskData {
            options: UsenetTaskOptions {
                profile_id: Some("provider-main".into()),
                ..Default::default()
            },
            files: vec![UsenetTaskFile {
                name: "archive.part01.rar".into(),
                subject: "archive.part01.rar yEnc".into(),
                groups: vec!["alt.binaries.example".into()],
                segments: vec![UsenetTaskSegment {
                    number: 1,
                    bytes: 42,
                    message_id: "<part-1@example>".into(),
                }],
            }],
        };

        let task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            Some("Release".into()),
            Map::new(),
            files,
        )
        .with_usenet_data(metadata.clone());

        assert_eq!(task.kind, TaskKind::Usenet);
        assert_eq!(task.out, "Release");
        assert_eq!(task.files.len(), 1);
        assert_eq!(task.usenet, Some(metadata));
    }

    #[test]
    fn usenet_metadata_round_trips_without_credentials() {
        let task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            None,
            Map::new(),
            Vec::new(),
        )
        .with_usenet_data(UsenetTaskData {
            options: UsenetTaskOptions {
                profile_id: Some("provider-main".into()),
                ..Default::default()
            },
            files: vec![UsenetTaskFile {
                name: "file.bin".into(),
                subject: "file.bin yEnc".into(),
                groups: vec!["alt.binaries.example".into()],
                segments: vec![UsenetTaskSegment {
                    number: 1,
                    bytes: 7,
                    message_id: "<one@example>".into(),
                }],
            }],
        });

        let encoded = serde_json::to_string(&task).unwrap();
        assert!(encoded.contains("profileId"));
        assert!(encoded.contains("messageId"));
        assert!(!encoded.contains("password"));

        let restored: DownloadTask = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.kind, TaskKind::Usenet);
        assert_eq!(restored.usenet, task.usenet);
    }

    #[test]
    fn usenet_repair_failure_round_trips_as_non_secret_task_metadata() {
        let mut task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            None,
            Map::new(),
            Vec::new(),
        );
        task.usenet_repair_failure = Some(UsenetRepairFailure {
            needed_blocks: 184,
            available_blocks: 62,
            partials_retained: true,
        });

        let encoded = serde_json::to_string(&task).unwrap();
        let restored: DownloadTask = serde_json::from_str(&encoded).unwrap();

        assert_eq!(restored.usenet_repair_failure, task.usenet_repair_failure);
    }

    #[test]
    fn usenet_repair_failure_defaults_when_absent_from_legacy_task_data() {
        let mut task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            None,
            Map::new(),
            Vec::new(),
        );
        task.usenet_repair_failure = Some(UsenetRepairFailure {
            needed_blocks: 184,
            available_blocks: 62,
            partials_retained: true,
        });
        let mut encoded = serde_json::to_value(task).unwrap();
        let removed = encoded
            .as_object_mut()
            .unwrap()
            .remove("usenet_repair_failure");

        assert!(
            removed.is_some(),
            "repair failure must be serialized with task data"
        );

        let restored: DownloadTask = serde_json::from_value(encoded).unwrap();

        assert_eq!(restored.usenet_repair_failure, None);
    }

    #[test]
    fn rpc_status_all_keys() {
        let task = DownloadTask::new_http(
            "test_gid".into(),
            vec!["http://example.com/f.bin".into()],
            "/tmp".into(),
            None,
            Map::new(),
        );
        let status = task.to_rpc_status(&[]);
        let obj = status.as_object().unwrap();

        assert_eq!(obj.get("gid").unwrap(), "test_gid");
        assert_eq!(obj.get("status").unwrap(), "waiting");
        assert!(obj.contains_key("totalLength"));
        assert!(obj.contains_key("files"));
        assert!(obj.contains_key("dir"));
    }

    #[test]
    fn rpc_status_filtered_keys() {
        let task = DownloadTask::new_http(
            "test_gid".into(),
            vec!["http://example.com/f.bin".into()],
            "/tmp".into(),
            None,
            Map::new(),
        );
        let keys = vec!["gid".to_string(), "status".to_string()];
        let status = task.to_rpc_status(&keys);
        let obj = status.as_object().unwrap();

        assert_eq!(obj.len(), 2);
        assert!(obj.contains_key("gid"));
        assert!(obj.contains_key("status"));
    }

    #[test]
    fn rpc_status_stage_request_skips_usenet_manifest() {
        let mut task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            None,
            Map::new(),
            Vec::new(),
        )
        .with_usenet_data(UsenetTaskData {
            options: UsenetTaskOptions::default(),
            files: vec![UsenetTaskFile {
                name: "large.nzb.file".into(),
                subject: "large.nzb.file".into(),
                groups: vec!["alt.binaries.example".into()],
                segments: vec![UsenetTaskSegment {
                    number: 1,
                    bytes: 1,
                    message_id: "example".into(),
                }],
            }],
        });
        task.usenet_stage = Some("assembling".into());

        let status = task.to_rpc_status(&["usenetStage".to_string()]);
        let object = status.as_object().unwrap();
        assert_eq!(
            object.get("usenetStage"),
            Some(&Value::String("assembling".into()))
        );
        assert!(!object.contains_key("usenet"));
    }

    #[test]
    fn rpc_status_torrent_has_bittorrent_field() {
        let mut task = DownloadTask::new_torrent("tgid".into(), "/dl".into(), None, Map::new());
        task.info_hash = Some("abc123".into());
        task.bt_name = Some("My Torrent".into());

        let status = task.to_rpc_status(&[]);
        let obj = status.as_object().unwrap();

        assert!(obj.contains_key("bittorrent"));
        assert!(obj.contains_key("seeder"));
        let bt = obj.get("bittorrent").unwrap().as_object().unwrap();
        assert_eq!(bt.get("infoHash").unwrap(), "abc123");
    }

    #[test]
    fn rpc_status_emits_usenet_repair_failure_for_usenet_tasks() {
        let mut task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            None,
            Map::new(),
            Vec::new(),
        );
        task.usenet_repair_failure = Some(UsenetRepairFailure {
            needed_blocks: 184,
            available_blocks: 62,
            partials_retained: true,
        });

        let status = task.to_rpc_status(&[]);

        assert_eq!(
            status.get("usenetRepairFailure"),
            Some(&json!({
                "neededBlocks": 184,
                "availableBlocks": 62,
                "partialsRetained": true,
            }))
        );
    }

    #[test]
    fn test_info_hash_v2_and_meta_version_emitted_both_places() {
        let mut task = DownloadTask::new_torrent("tgid".into(), "/dl".into(), None, Map::new());
        task.info_hash = Some("aabbccdd".into());
        task.info_hash_v2 = Some("deadbeef".into());
        task.meta_version = Some("hybrid".into());

        let status = task.to_rpc_status(&[]);
        let obj = status.as_object().unwrap();

        assert_eq!(
            obj.get("infoHashV2").unwrap(),
            "deadbeef",
            "infoHashV2 must be present at RPC root"
        );
        assert_eq!(
            obj.get("metaVersion").unwrap(),
            "hybrid",
            "metaVersion must be present at RPC root"
        );

        let bt = obj.get("bittorrent").unwrap().as_object().unwrap();
        assert_eq!(
            bt.get("infoHashV2").unwrap(),
            "deadbeef",
            "infoHashV2 must be present inside bittorrent"
        );
        assert_eq!(
            bt.get("metaVersion").unwrap(),
            "hybrid",
            "metaVersion must be present inside bittorrent"
        );
    }

    #[test]
    fn rpc_status_ed2k_has_ed2k_link() {
        let task = DownloadTask::new_ed2k(
            "egid".into(),
            "ed2k://|file|test|100|hash|/".into(),
            "test".into(),
            100,
            "/dl".into(),
            None,
            Map::new(),
        );
        let status = task.to_rpc_status(&[]);
        let obj = status.as_object().unwrap();
        assert!(obj.contains_key("ed2kLink"));
    }

    #[test]
    fn rpc_status_ed2k_includes_kad_status_and_filtered_status() {
        let mut task = DownloadTask::new_ed2k(
            "egid".into(),
            "ed2k://|file|test|100|hash|/".into(),
            "test".into(),
            100,
            "/dl".into(),
            None,
            Map::new(),
        );
        task.ed2k_kad = Some(Ed2kKadTaskStatus {
            state: "searching".into(),
            queried_nodes: 3,
            discovered_sources: 7,
            error: None,
        });

        let full = task.to_rpc_status(&[]);
        assert_eq!(
            full.get("ed2kKad"),
            Some(&json!({
                "state": "searching",
                "queriedNodes": 3,
                "discoveredSources": 7,
            }))
        );

        let filtered = task.to_rpc_status(&["ed2kKad".into()]);
        assert_eq!(filtered.get("ed2kKad"), full.get("ed2kKad"));
    }

    #[test]
    fn new_media_with_out() {
        let mut opts = Map::new();
        opts.insert("out".into(), json!("video.mp4"));
        let uri = "https://www.youtube.com/watch?v=test123".to_string();
        let task = DownloadTask::new_media("ygid1".into(), uri.clone(), "/dl".into(), None, opts);

        assert_eq!(task.kind, TaskKind::Media);
        assert_eq!(task.status, TaskStatus::Waiting);
        assert_eq!(task.uris, vec![uri.clone()]);
        assert_eq!(task.files[0].path, "/dl/video.mp4");
        assert_eq!(task.files[0].uris[0].uri, uri);
        assert_eq!(task.files[0].uris[0].status, "waiting");
    }

    #[test]
    fn new_media_without_out() {
        let opts = Map::new();
        let uri = "https://www.youtube.com/watch?v=abc".to_string();
        let task = DownloadTask::new_media("ygid2".into(), uri.clone(), "/dl".into(), None, opts);

        assert_eq!(task.kind, TaskKind::Media);
        assert_eq!(task.status, TaskStatus::Waiting);
        assert_eq!(task.uris, vec![uri.clone()]);
        assert_eq!(task.files[0].path, uri);
        assert_eq!(task.files[0].uris[0].status, "waiting");
    }
}
