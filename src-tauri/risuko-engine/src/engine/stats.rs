use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::traits::StorageBackend;

const STATS_STORE_KEY: &str = "download_history_stats";
const STATS_VERSION: u32 = 1;
const SPEED_RETENTION_SECS: i64 = 365 * 24 * 60 * 60;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskBaseline {
    pub completed_length: u64,
    pub kind: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpeedAggregate {
    pub download_sum: u64,
    pub upload_sum: u64,
    pub samples: u64,
    pub received_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpeedMinuteBucket {
    pub month: String,
    pub protocols: BTreeMap<String, SpeedAggregate>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadStatsStore {
    pub version: u32,
    pub baselines: HashMap<String, TaskBaseline>,
    pub monthly: BTreeMap<String, BTreeMap<String, u64>>,
    pub speed: BTreeMap<i64, SpeedMinuteBucket>,
}

impl Default for DownloadStatsStore {
    fn default() -> Self {
        Self {
            version: STATS_VERSION,
            baselines: HashMap::new(),
            monthly: BTreeMap::new(),
            speed: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadStatsMinuteInput {
    pub minute: i64,
    pub month: String,
    pub tasks: Vec<DownloadStatsTaskInput>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadStatsTaskInput {
    pub gid: String,
    pub kind: String,
    #[serde(default)]
    pub first_completed_length: Option<u64>,
    pub completed_length: u64,
    pub download_speed_sum: u64,
    pub upload_speed_sum: u64,
    pub samples: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadStatsQuery {
    pub start: i64,
    pub end: i64,
    pub start_month: String,
    pub end_month: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadStatsView {
    pub monthly: Vec<MonthlyProtocolTotal>,
    pub speed: Vec<SpeedPoint>,
    pub protocol_totals: Vec<ProtocolTotal>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MonthlyProtocolTotal {
    pub month: String,
    pub total: u64,
    pub protocols: Vec<ProtocolTotal>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolTotal {
    pub protocol: String,
    pub received_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpeedPoint {
    pub minute: i64,
    pub protocols: Vec<ProtocolSpeedPoint>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolSpeedPoint {
    pub protocol: String,
    pub download_speed: u64,
    pub upload_speed: u64,
    pub received_bytes: u64,
    pub samples: u64,
}

pub struct DownloadStatsManager {
    store: Arc<Mutex<DownloadStatsStore>>,
    storage: Arc<dyn StorageBackend>,
}

impl DownloadStatsManager {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            store: Arc::new(Mutex::new(DownloadStatsStore::default())),
            storage,
        }
    }

    pub fn load(&self) -> Result<(), String> {
        if let Some(val) = self.storage.load(STATS_STORE_KEY)? {
            let data_val = val.get("data").cloned().unwrap_or(val);
            let mut data: DownloadStatsStore = serde_json::from_value(data_val)
                .map_err(|e| format!("Failed to parse stats data: {e}"))?;
            data.version = STATS_VERSION;
            // `try_lock` avoids the `blocking_lock()` panic on a Tokio worker
            let mut store = self
                .store
                .try_lock()
                .map_err(|_| "stats store busy during load".to_string())?;
            *store = data;
        }
        Ok(())
    }

    pub async fn record_minute(&self, input: DownloadStatsMinuteInput) -> Result<(), String> {
        if input.tasks.is_empty() {
            return Ok(());
        }

        let minute = input.minute - input.minute.rem_euclid(60);
        let month = normalize_month(&input.month);
        let mut store = self.store.lock().await;

        for task in input.tasks {
            let gid = task.gid.trim();
            if gid.is_empty() {
                continue;
            }
            let kind = normalize_kind(&task.kind);
            let samples = task.samples.max(1);
            let first_completed_length =
                task.first_completed_length.unwrap_or(task.completed_length);
            let baseline = store
                .baselines
                .entry(gid.to_string())
                .or_insert_with(|| TaskBaseline {
                    completed_length: first_completed_length,
                    kind: kind.clone(),
                });

            let received = task
                .completed_length
                .saturating_sub(baseline.completed_length);
            baseline.completed_length = task.completed_length;
            baseline.kind = kind.clone();

            if received > 0 {
                *store
                    .monthly
                    .entry(month.clone())
                    .or_default()
                    .entry(kind.clone())
                    .or_default() += received;
            }

            let bucket = store
                .speed
                .entry(minute)
                .or_insert_with(|| SpeedMinuteBucket {
                    month: month.clone(),
                    protocols: BTreeMap::new(),
                });
            let speed = bucket.protocols.entry(kind).or_default();
            speed.download_sum = speed.download_sum.saturating_add(task.download_speed_sum);
            speed.upload_sum = speed.upload_sum.saturating_add(task.upload_speed_sum);
            speed.samples = speed.samples.saturating_add(samples);
            speed.received_bytes = speed.received_bytes.saturating_add(received);
        }

        prune_speed(&mut store, minute);
        drop(store);
        self.save().await
    }

    pub async fn query(&self, query: DownloadStatsQuery) -> Result<DownloadStatsView, String> {
        let query = normalize_query(query)?;
        let store = self.store.lock().await;
        Ok(build_view(&store, query))
    }

    pub async fn export(&self) -> Value {
        let store = self.store.lock().await;
        serde_json::to_value(&*store).unwrap_or(Value::Null)
    }

    pub async fn merge(&self, value: Value) -> Result<(), String> {
        let data_val = value.get("data").cloned().unwrap_or(value);
        let incoming: DownloadStatsStore = serde_json::from_value(data_val)
            .map_err(|e| format!("Failed to parse stats data: {e}"))?;
        let mut store = self.store.lock().await;
        merge_store(&mut store, incoming);
        if let Some(last_minute) = store.speed.keys().next_back().copied() {
            prune_speed(&mut store, last_minute);
        }
        drop(store);
        self.save().await
    }

    pub async fn clear(&self) -> Result<(), String> {
        let mut store = self.store.lock().await;
        *store = DownloadStatsStore::default();
        drop(store);
        self.save().await
    }

    pub fn clear_sync(&self) -> Result<(), String> {
        let mut store = self
            .store
            .try_lock()
            .map_err(|_| "stats store busy during clear".to_string())?;
        *store = DownloadStatsStore::default();
        let data =
            serde_json::to_value(&*store).map_err(|e| format!("Serialize stats failed: {e}"))?;
        drop(store);
        self.storage
            .save(STATS_STORE_KEY, &serde_json::json!({ "data": data }))
    }

    async fn save(&self) -> Result<(), String> {
        let data = {
            let store = self.store.lock().await;
            serde_json::to_value(&*store).map_err(|e| format!("Serialize stats failed: {e}"))?
        };
        self.storage
            .save(STATS_STORE_KEY, &serde_json::json!({ "data": data }))
    }
}

fn build_view(store: &DownloadStatsStore, query: DownloadStatsQuery) -> DownloadStatsView {
    let start = query.start.min(query.end);
    let end = query.start.max(query.end);
    let mut monthly: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let mut speed_protocols: BTreeMap<String, ()> = BTreeMap::new();
    let mut speed = Vec::new();

    for (minute, bucket) in store.speed.range(start..=end) {
        let mut point_protocols = Vec::new();
        for (protocol, agg) in &bucket.protocols {
            speed_protocols.entry(protocol.clone()).or_default();
            point_protocols.push(ProtocolSpeedPoint {
                protocol: protocol.clone(),
                download_speed: avg(agg.download_sum, agg.samples),
                upload_speed: avg(agg.upload_sum, agg.samples),
                received_bytes: agg.received_bytes,
                samples: agg.samples,
            });
        }
        speed.push(SpeedPoint {
            minute: *minute,
            protocols: point_protocols,
        });
    }

    for (month, protocols) in &store.monthly {
        if month >= &query.start_month && month <= &query.end_month {
            monthly.insert(month.clone(), protocols.clone());
        }
    }

    let monthly = monthly
        .into_iter()
        .map(|(month, protocols)| {
            let protocols = protocol_totals(protocols);
            let total = protocols.iter().map(|p| p.received_bytes).sum();
            MonthlyProtocolTotal {
                month,
                total,
                protocols,
            }
        })
        .collect::<Vec<_>>();

    let mut protocol_map: BTreeMap<String, u64> = BTreeMap::new();
    for month in &monthly {
        for item in &month.protocols {
            *protocol_map.entry(item.protocol.clone()).or_default() += item.received_bytes;
        }
    }
    for protocol in speed_protocols.keys() {
        protocol_map.entry(protocol.clone()).or_default();
    }
    let protocol_totals = protocol_totals(protocol_map);

    DownloadStatsView {
        monthly,
        speed,
        protocol_totals,
    }
}

fn merge_store(store: &mut DownloadStatsStore, incoming: DownloadStatsStore) {
    for (gid, baseline) in incoming.baselines {
        store
            .baselines
            .entry(gid)
            .and_modify(|current| {
                if baseline.completed_length > current.completed_length {
                    *current = baseline.clone();
                }
            })
            .or_insert(baseline);
    }

    for (month, protocols) in incoming.monthly {
        let target = store.monthly.entry(month).or_default();
        for (protocol, bytes) in protocols {
            let target_bytes = target.entry(protocol).or_default();
            *target_bytes = (*target_bytes).max(bytes);
        }
    }

    for (minute, bucket) in incoming.speed {
        let target = store
            .speed
            .entry(minute)
            .or_insert_with(|| SpeedMinuteBucket {
                month: bucket.month.clone(),
                protocols: BTreeMap::new(),
            });
        if target.month.is_empty() {
            target.month = bucket.month;
        }
        for (protocol, agg) in bucket.protocols {
            let target_agg = target.protocols.entry(protocol).or_default();
            target_agg.download_sum = target_agg.download_sum.max(agg.download_sum);
            target_agg.upload_sum = target_agg.upload_sum.max(agg.upload_sum);
            target_agg.samples = target_agg.samples.max(agg.samples);
            target_agg.received_bytes = target_agg.received_bytes.max(agg.received_bytes);
        }
    }
}

fn protocol_totals(protocols: BTreeMap<String, u64>) -> Vec<ProtocolTotal> {
    let mut result = protocols
        .into_iter()
        .map(|(protocol, received_bytes)| ProtocolTotal {
            protocol,
            received_bytes,
        })
        .collect::<Vec<_>>();
    result.sort_by(|a, b| {
        b.received_bytes
            .cmp(&a.received_bytes)
            .then_with(|| a.protocol.cmp(&b.protocol))
    });
    result
}

fn prune_speed(store: &mut DownloadStatsStore, current_minute: i64) {
    let cutoff = current_minute.saturating_sub(SPEED_RETENTION_SECS);
    store.speed.retain(|minute, _| *minute >= cutoff);
}

fn avg(sum: u64, samples: u64) -> u64 {
    sum.checked_div(samples).unwrap_or(0)
}

fn normalize_kind(kind: &str) -> String {
    let trimmed = kind.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed
    }
}

fn normalize_month(month: &str) -> String {
    let trimmed = month.trim();
    if is_valid_month(trimmed) {
        trimmed.to_string()
    } else {
        "unknown".to_string()
    }
}

fn normalize_query(mut query: DownloadStatsQuery) -> Result<DownloadStatsQuery, String> {
    query.start_month = validate_query_month(&query.start_month, "startMonth")?;
    query.end_month = validate_query_month(&query.end_month, "endMonth")?;
    if query.start_month > query.end_month {
        std::mem::swap(&mut query.start_month, &mut query.end_month);
    }
    Ok(query)
}

fn validate_query_month(month: &str, field: &str) -> Result<String, String> {
    let trimmed = month.trim();
    if is_valid_month(trimmed) {
        Ok(trimmed.to_string())
    } else {
        Err(format!("Invalid {field}: expected YYYY-MM"))
    }
}

fn is_valid_month(month: &str) -> bool {
    if month.len() != 7 || month.as_bytes().get(4) != Some(&b'-') {
        return false;
    }
    if !month[..4].bytes().all(|b| b.is_ascii_digit())
        || !month[5..].bytes().all(|b| b.is_ascii_digit())
    {
        return false;
    }
    matches!(month[5..].parse::<u8>(), Ok(1..=12))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::FileStorage;
    use tempfile::TempDir;

    fn task(gid: &str, completed: u64, dl: u64) -> DownloadStatsTaskInput {
        DownloadStatsTaskInput {
            gid: gid.to_string(),
            kind: "http".to_string(),
            first_completed_length: None,
            completed_length: completed,
            download_speed_sum: dl,
            upload_speed_sum: 0,
            samples: 1,
        }
    }

    #[tokio::test]
    async fn baselines_deltas_and_merge_are_idempotent() {
        let dir = TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let mgr = DownloadStatsManager::new(storage);

        mgr.record_minute(DownloadStatsMinuteInput {
            minute: 1_700_000_000,
            month: "2023-11".to_string(),
            tasks: vec![task("a", 100, 10)],
        })
        .await
        .unwrap();
        let view = mgr
            .query(DownloadStatsQuery {
                start: 1_699_999_000,
                end: 1_700_000_500,
                start_month: "2023-11".to_string(),
                end_month: "2023-11".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(
            view.protocol_totals
                .first()
                .map(|p| p.received_bytes)
                .unwrap_or(0),
            0
        );

        mgr.record_minute(DownloadStatsMinuteInput {
            minute: 1_700_000_060,
            month: "2023-11".to_string(),
            tasks: vec![task("a", 160, 20)],
        })
        .await
        .unwrap();
        let exported = mgr.export().await;
        mgr.merge(exported.clone()).await.unwrap();
        mgr.merge(exported).await.unwrap();
        let view = mgr
            .query(DownloadStatsQuery {
                start: 1_699_999_000,
                end: 1_700_001_000,
                start_month: "2023-11".to_string(),
                end_month: "2023-11".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(view.protocol_totals[0].received_bytes, 60);
        assert_eq!(view.monthly[0].protocols[0].received_bytes, 60);
    }

    #[tokio::test]
    async fn first_minute_counts_observed_delta() {
        let dir = TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let mgr = DownloadStatsManager::new(storage);

        mgr.record_minute(DownloadStatsMinuteInput {
            minute: 1_700_000_000,
            month: "2023-11".to_string(),
            tasks: vec![DownloadStatsTaskInput {
                first_completed_length: Some(100),
                ..task("a", 160, 20)
            }],
        })
        .await
        .unwrap();

        let view = mgr
            .query(DownloadStatsQuery {
                start: 1_699_999_000,
                end: 1_700_000_500,
                start_month: "2023-11".to_string(),
                end_month: "2023-11".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(view.protocol_totals[0].received_bytes, 60);
    }

    #[tokio::test]
    async fn query_rejects_invalid_months() {
        let dir = TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let mgr = DownloadStatsManager::new(storage);

        let err = mgr
            .query(DownloadStatsQuery {
                start: 0,
                end: 0,
                start_month: "2023-1".to_string(),
                end_month: "abcd-ef".to_string(),
            })
            .await
            .unwrap_err();
        assert!(err.contains("startMonth"));
    }

    #[test]
    fn normalize_month_rejects_non_digit_or_bad_month() {
        assert_eq!(normalize_month("2023-11"), "2023-11");
        assert_eq!(normalize_month("abcd-ef"), "unknown");
        assert_eq!(normalize_month("0000-00"), "unknown");
        assert_eq!(normalize_month("2023-13"), "unknown");
    }

    #[test]
    fn protocol_totals_include_monthly_protocols_without_speed() {
        let mut store = DownloadStatsStore::default();
        store
            .monthly
            .entry("2023-11".to_string())
            .or_default()
            .insert("http".to_string(), 100);
        store
            .monthly
            .entry("2023-11".to_string())
            .or_default()
            .insert("torrent".to_string(), 50);
        store.speed.insert(
            1_700_000_000,
            SpeedMinuteBucket {
                month: "2023-11".to_string(),
                protocols: BTreeMap::from([(
                    "http".to_string(),
                    SpeedAggregate {
                        received_bytes: 10,
                        ..SpeedAggregate::default()
                    },
                )]),
            },
        );

        let view = build_view(
            &store,
            DownloadStatsQuery {
                start: 1_699_999_000,
                end: 1_700_001_000,
                start_month: "2023-11".to_string(),
                end_month: "2023-11".to_string(),
            },
        );

        assert_eq!(view.protocol_totals.len(), 2);
        assert_eq!(view.protocol_totals[0].protocol, "http");
        assert_eq!(view.protocol_totals[0].received_bytes, 100);
        assert_eq!(view.protocol_totals[1].protocol, "torrent");
        assert_eq!(view.protocol_totals[1].received_bytes, 50);
    }
}
