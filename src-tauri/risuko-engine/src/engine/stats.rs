use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard};

use crate::traits::StorageBackend;

const STATS_STORE_KEY: &str = "download_history_stats";
const SPEED_PART_KEY_PREFIX: &str = "download_history_speed_";
const STATS_VERSION: u32 = 2;
const SPEED_RETENTION_SECS: i64 = 365 * 24 * 60 * 60;
const SPEED_PARTITION_SECS: i64 = 30 * 24 * 60 * 60;
const BASELINE_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskBaseline {
    pub completed_length: u64,
    pub kind: String,
    #[serde(default)]
    pub seen_minute: i64,
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredMain<'a> {
    version: u32,
    baselines: &'a HashMap<String, TaskBaseline>,
    monthly: &'a BTreeMap<String, BTreeMap<String, u64>>,
    speed: BTreeMap<i64, SpeedMinuteBucket>,
    speed_parts: &'a BTreeSet<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredIndex {
    #[serde(default)]
    speed_parts: Vec<i64>,
}

#[derive(Default)]
struct StatsState {
    store: DownloadStatsStore,
    speed_parts: BTreeSet<i64>,
    dirty_parts: BTreeSet<i64>,
}

struct StatsWrites {
    parts: Vec<(i64, Value)>,
    main: Value,
}

impl StatsState {
    fn reset(&mut self, stored_parts: Vec<i64>) {
        self.store = DownloadStatsStore::default();
        self.dirty_parts.extend(self.speed_parts.iter().copied());
        self.dirty_parts.extend(stored_parts);
    }

    fn take_writes(&mut self) -> Result<StatsWrites, String> {
        let mut speed_parts = self.speed_parts.clone();
        let mut parts = Vec::with_capacity(self.dirty_parts.len());
        for &part in &self.dirty_parts {
            let buckets: BTreeMap<_, _> = self.store.speed.range(partition_range(part)).collect();
            if buckets.is_empty() {
                speed_parts.remove(&part);
            } else {
                speed_parts.insert(part);
            }
            parts.push((part, stored_value(&buckets)?));
        }
        let main = stored_value(&StoredMain {
            version: STATS_VERSION,
            baselines: &self.store.baselines,
            monthly: &self.store.monthly,
            speed: BTreeMap::new(),
            speed_parts: &speed_parts,
        })?;
        self.speed_parts = speed_parts;
        self.dirty_parts.clear();
        Ok(StatsWrites { parts, main })
    }
}

pub struct DownloadStatsManager {
    state: Arc<Mutex<StatsState>>,
    storage: Arc<dyn StorageBackend>,
}

impl DownloadStatsManager {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            state: Arc::new(Mutex::new(StatsState::default())),
            storage,
        }
    }

    pub fn load(&self) -> Result<(), String> {
        self.load_at(current_secs())
    }

    fn load_at(&self, now: i64) -> Result<(), String> {
        let Some(val) = self.storage.load(STATS_STORE_KEY)? else {
            return Ok(());
        };
        let data_val = val.get("data").unwrap_or(&val);
        let parsed = DownloadStatsStore::deserialize(data_val)
            .and_then(|store| StoredIndex::deserialize(data_val).map(|index| (store, index)));
        let (mut data, index) = parsed.map_err(|e| {
            crate::traits::keep_unreadable_copy(self.storage.as_ref(), STATS_STORE_KEY, &val);
            format!("Failed to parse stats data: {e}")
        })?;
        data.version = STATS_VERSION;

        let mut speed_parts = BTreeSet::new();
        let mut dirty_parts = BTreeSet::new();
        if data.speed.is_empty() {
            for part in index.speed_parts {
                if !self.load_partition(part, &mut data.speed, &mut speed_parts) {
                    tracing::warn!("Stats partition {} is missing", partition_key(part));
                }
            }
        } else {
            dirty_parts.extend(data.speed.keys().map(|minute| speed_partition(*minute)));
            for part in recent_partitions(now) {
                self.load_partition(part, &mut data.speed, &mut speed_parts);
            }
        }

        // `try_lock` avoids the `blocking_lock()` panic on a Tokio worker
        let mut state = self
            .state
            .try_lock()
            .map_err(|_| "stats store busy during load".to_string())?;
        *state = StatsState {
            store: data,
            speed_parts,
            dirty_parts,
        };
        if state.dirty_parts.is_empty() {
            return Ok(());
        }
        if let Err(e) = self.save_locked(state) {
            tracing::warn!("Failed to save download stats in partitions: {e}");
        }
        Ok(())
    }

    pub async fn record_minute(&self, input: DownloadStatsMinuteInput) -> Result<(), String> {
        if input.tasks.is_empty() {
            return Ok(());
        }

        let minute = input.minute - input.minute.rem_euclid(60);
        let month = normalize_month(&input.month);
        let mut guard = self.state.lock().await;
        let state = &mut *guard;
        let store = &mut state.store;

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
                    seen_minute: minute,
                });

            let received = task
                .completed_length
                .saturating_sub(baseline.completed_length);
            baseline.completed_length = task.completed_length;
            baseline.kind = kind.clone();
            baseline.seen_minute = minute;

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
            state.dirty_parts.insert(speed_partition(minute));
        }

        prune_baselines(&mut store.baselines, minute);
        prune_speed(state, minute);
        drop(guard);
        self.save().await
    }

    pub async fn query(&self, query: DownloadStatsQuery) -> Result<DownloadStatsView, String> {
        let query = normalize_query(query)?;
        let state = self.state.lock().await;
        Ok(build_view(&state.store, query))
    }

    pub async fn export(&self) -> Value {
        let state = self.state.lock().await;
        serde_json::to_value(&state.store).unwrap_or(Value::Null)
    }

    pub async fn merge(&self, value: Value) -> Result<(), String> {
        let data_val = value.get("data").cloned().unwrap_or(value);
        let incoming: DownloadStatsStore = serde_json::from_value(data_val)
            .map_err(|e| format!("Failed to parse stats data: {e}"))?;
        let mut guard = self.state.lock().await;
        let state = &mut *guard;
        state
            .dirty_parts
            .extend(incoming.speed.keys().map(|minute| speed_partition(*minute)));
        merge_store(&mut state.store, incoming);
        if let Some(last_minute) = state.store.speed.keys().next_back().copied() {
            prune_speed(state, last_minute);
        }
        drop(guard);
        self.save().await
    }

    pub async fn clear(&self) -> Result<(), String> {
        let stored_parts = self.partitions_holding_data(current_secs());
        let mut state = self.state.lock().await;
        state.reset(stored_parts);
        drop(state);
        self.save().await
    }

    pub fn clear_sync(&self) -> Result<(), String> {
        self.clear_sync_at(current_secs())
    }

    fn clear_sync_at(&self, now: i64) -> Result<(), String> {
        let stored_parts = self.partitions_holding_data(now);
        let mut state = self
            .state
            .try_lock()
            .map_err(|_| "stats store busy during clear".to_string())?;
        state.reset(stored_parts);
        self.save_locked(state)
    }

    fn load_partition(
        &self,
        part: i64,
        speed: &mut BTreeMap<i64, SpeedMinuteBucket>,
        speed_parts: &mut BTreeSet<i64>,
    ) -> bool {
        let key = partition_key(part);
        match self.storage.load(&key) {
            Ok(Some(val)) => {
                let part_val = val.get("data").unwrap_or(&val);
                match BTreeMap::<i64, SpeedMinuteBucket>::deserialize(part_val) {
                    Ok(buckets) => {
                        if !buckets.is_empty() {
                            speed_parts.insert(part);
                        }
                        for (minute, bucket) in buckets {
                            speed.entry(minute).or_insert(bucket);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Skipping unreadable stats partition {key}: {e}");
                        crate::traits::keep_unreadable_copy(self.storage.as_ref(), &key, &val);
                    }
                }
                true
            }
            Ok(None) => false,
            Err(e) => {
                tracing::warn!("Skipping stats partition {key}: {e}");
                speed_parts.insert(part);
                true
            }
        }
    }

    fn partitions_holding_data(&self, now: i64) -> Vec<i64> {
        recent_partitions(now)
            .filter(|&part| match self.storage.load(&partition_key(part)) {
                Ok(Some(val)) => !val
                    .get("data")
                    .unwrap_or(&val)
                    .as_object()
                    .is_some_and(serde_json::Map::is_empty),
                Ok(None) => false,
                Err(_) => true,
            })
            .collect()
    }

    async fn save(&self) -> Result<(), String> {
        let writes = self.state.lock().await.take_writes()?;
        if let Err((unsaved, e)) = self.write(writes) {
            self.state.lock().await.dirty_parts.extend(unsaved);
            return Err(e);
        }
        Ok(())
    }

    fn save_locked(&self, mut state: MutexGuard<'_, StatsState>) -> Result<(), String> {
        let writes = state.take_writes()?;
        drop(state);
        if let Err((unsaved, e)) = self.write(writes) {
            if let Ok(mut state) = self.state.try_lock() {
                state.dirty_parts.extend(unsaved);
            }
            return Err(e);
        }
        Ok(())
    }

    fn write(&self, writes: StatsWrites) -> Result<(), (Vec<i64>, String)> {
        let mut parts = writes.parts.into_iter();
        while let Some((part, value)) = parts.next() {
            if let Err(e) = self.storage.save(&partition_key(part), &value) {
                let unsaved = std::iter::once(part)
                    .chain(parts.map(|(part, _)| part))
                    .collect();
                return Err((unsaved, e));
            }
        }
        self.storage
            .save(STATS_STORE_KEY, &writes.main)
            .map_err(|e| (Vec::new(), e))
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
                let seen_minute = current.seen_minute.max(baseline.seen_minute);
                if baseline.completed_length > current.completed_length {
                    *current = baseline.clone();
                }
                current.seen_minute = seen_minute;
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

fn prune_speed(state: &mut StatsState, current_minute: i64) {
    let cutoff = current_minute.saturating_sub(SPEED_RETENTION_SECS);
    while let Some(entry) = state.store.speed.first_entry() {
        if *entry.key() >= cutoff {
            break;
        }
        state.dirty_parts.insert(speed_partition(*entry.key()));
        entry.remove();
    }
    let cutoff_part = speed_partition(cutoff);
    state
        .dirty_parts
        .extend(state.speed_parts.range(..cutoff_part).copied());
}

fn prune_baselines(baselines: &mut HashMap<String, TaskBaseline>, current_minute: i64) {
    let cutoff = current_minute.saturating_sub(BASELINE_RETENTION_SECS);
    baselines.retain(|_, baseline| {
        if baseline.seen_minute == 0 {
            baseline.seen_minute = current_minute;
        }
        baseline.seen_minute >= cutoff
    });
}

fn current_secs() -> i64 {
    i64::try_from(crate::engine::util::now_secs()).unwrap_or(i64::MAX)
}

fn speed_partition(minute: i64) -> i64 {
    minute.div_euclid(SPEED_PARTITION_SECS)
}

fn recent_partitions(now: i64) -> RangeInclusive<i64> {
    speed_partition(now.saturating_sub(SPEED_RETENTION_SECS))..=speed_partition(now)
}

fn partition_range(part: i64) -> Range<i64> {
    let start = part.saturating_mul(SPEED_PARTITION_SECS);
    start..start.saturating_add(SPEED_PARTITION_SECS)
}

fn partition_key(part: i64) -> String {
    format!("{SPEED_PART_KEY_PREFIX}{part}")
}

fn stored_value<T: Serialize>(data: &T) -> Result<Value, String> {
    let data = serde_json::to_value(data).map_err(|e| format!("Serialize stats failed: {e}"))?;
    Ok(serde_json::json!({ "data": data }))
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
    use serde_json::json;
    use tempfile::TempDir;

    const DAY: i64 = 24 * 60 * 60;
    const PART_SECS: i64 = 30 * DAY;
    const BASE: i64 = 656 * PART_SECS;

    fn part_key(part: i64) -> String {
        format!("download_history_speed_{part}")
    }

    fn file_storage(dir: &TempDir) -> Arc<dyn StorageBackend> {
        Arc::new(FileStorage::new(dir.path().to_path_buf()))
    }

    fn at(minute: i64, tasks: Vec<DownloadStatsTaskInput>) -> DownloadStatsMinuteInput {
        DownloadStatsMinuteInput {
            minute,
            month: "2023-11".to_string(),
            tasks,
        }
    }

    fn stored(storage: &dyn StorageBackend, key: &str) -> Value {
        storage.load(key).unwrap().unwrap()["data"].clone()
    }

    async fn whole_view(mgr: &DownloadStatsManager) -> Value {
        let view = mgr
            .query(DownloadStatsQuery {
                start: 0,
                end: i64::MAX,
                start_month: "1970-01".to_string(),
                end_month: "9999-12".to_string(),
            })
            .await
            .unwrap();
        serde_json::to_value(view).unwrap()
    }

    fn bucket_json(download_sum: u64, received: u64) -> Value {
        json!({
            "month": "2023-11",
            "protocols": {
                "http": {
                    "downloadSum": download_sum,
                    "uploadSum": 0,
                    "samples": 2,
                    "receivedBytes": received
                }
            }
        })
    }

    struct CountingStorage {
        inner: FileStorage,
        saves: std::sync::Mutex<BTreeMap<String, usize>>,
    }

    impl StorageBackend for CountingStorage {
        fn load(&self, key: &str) -> Result<Option<Value>, String> {
            self.inner.load(key)
        }

        fn save(&self, key: &str, value: &Value) -> Result<(), String> {
            *self
                .saves
                .lock()
                .unwrap()
                .entry(key.to_string())
                .or_default() += 1;
            self.inner.save(key, value)
        }
    }

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
    fn unreadable_data_is_kept_when_load_fails() {
        let dir = TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let stored = serde_json::json!({
            "data": {"version": 1, "baselines": 5, "monthly": {}, "speed": {}}
        });
        storage.save(STATS_STORE_KEY, &stored).unwrap();

        assert!(DownloadStatsManager::new(storage).load().is_err());

        let prefix = format!("{STATS_STORE_KEY}.unreadable-");
        let backups: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(&prefix))
            })
            .collect();
        assert_eq!(backups.len(), 1);
        let kept: Value = serde_json::from_slice(&std::fs::read(&backups[0]).unwrap()).unwrap();
        assert_eq!(kept, stored);
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

    #[tokio::test]
    async fn a_minute_writes_the_main_file_and_one_partition() {
        let dir = TempDir::new().unwrap();
        let storage = file_storage(&dir);
        let mgr = DownloadStatsManager::new(storage.clone());

        mgr.record_minute(at(BASE, vec![task("a", 100, 10)]))
            .await
            .unwrap();
        mgr.record_minute(at(BASE + 60, vec![task("a", 160, 20)]))
            .await
            .unwrap();

        let main = stored(&*storage, STATS_STORE_KEY);
        assert_eq!(main["speed"], json!({}));
        assert_eq!(main["speedParts"], json!([656]));
        let first = stored(&*storage, &part_key(656));
        let minutes: Vec<_> = first.as_object().unwrap().keys().cloned().collect();
        assert_eq!(minutes, vec![BASE.to_string(), (BASE + 60).to_string()]);

        mgr.record_minute(at(BASE + 40 * DAY, vec![task("a", 200, 30)]))
            .await
            .unwrap();

        let main = stored(&*storage, STATS_STORE_KEY);
        assert_eq!(main["speedParts"], json!([656, 657]));
        assert_eq!(stored(&*storage, &part_key(656)), first);
    }

    #[tokio::test]
    async fn only_the_changed_partition_is_rewritten() {
        let dir = TempDir::new().unwrap();
        let writer = DownloadStatsManager::new(file_storage(&dir));
        for minute in [BASE, BASE + 30 * DAY, BASE + 60 * DAY] {
            writer
                .record_minute(at(minute, vec![task("a", 100, 10)]))
                .await
                .unwrap();
        }

        let counting = Arc::new(CountingStorage {
            inner: FileStorage::new(dir.path().to_path_buf()),
            saves: Default::default(),
        });
        let mgr = DownloadStatsManager::new(counting.clone());
        mgr.load().unwrap();
        assert!(counting.saves.lock().unwrap().is_empty());

        mgr.record_minute(at(BASE + 60 * DAY + 60, vec![task("a", 200, 20)]))
            .await
            .unwrap();

        let saves = counting.saves.lock().unwrap().clone();
        assert_eq!(
            saves,
            BTreeMap::from([(STATS_STORE_KEY.to_string(), 1), (part_key(658), 1)])
        );
    }

    #[tokio::test]
    async fn a_version_1_file_is_migrated() {
        let dir = TempDir::new().unwrap();
        let storage = file_storage(&dir);
        let speed = serde_json::Map::from_iter([
            (BASE.to_string(), bucket_json(100, 10)),
            ((BASE + 60).to_string(), bucket_json(200, 20)),
            ((BASE + 35 * DAY).to_string(), bucket_json(300, 30)),
        ]);
        storage
            .save(
                STATS_STORE_KEY,
                &json!({
                    "data": {
                        "version": 1,
                        "baselines": {"a": {"completedLength": 500, "kind": "http"}},
                        "monthly": {"2023-11": {"http": 400}},
                        "speed": speed
                    }
                }),
            )
            .unwrap();

        let mgr = DownloadStatsManager::new(storage.clone());
        mgr.load().unwrap();
        let before = whole_view(&mgr).await;
        assert_eq!(before["speed"].as_array().unwrap().len(), 3);

        mgr.record_minute(at(BASE + 35 * DAY + 60, vec![task("a", 600, 40)]))
            .await
            .unwrap();

        let view = whole_view(&mgr).await;
        let points = view["speed"].as_array().unwrap();
        assert_eq!(points.len(), 4);
        assert_eq!(points[..3], before["speed"].as_array().unwrap()[..]);
        assert_eq!(
            view["monthly"],
            json!([{
                "month": "2023-11",
                "total": 500,
                "protocols": [{"protocol": "http", "receivedBytes": 500}]
            }])
        );

        let main = stored(&*storage, STATS_STORE_KEY);
        assert_eq!(main["speed"], json!({}));
        assert_eq!(main["speedParts"], json!([656, 657]));
        let first: Vec<_> = stored(&*storage, &part_key(656))
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        assert_eq!(first, vec![BASE.to_string(), (BASE + 60).to_string()]);
        let second: Vec<_> = stored(&*storage, &part_key(657))
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        assert_eq!(
            second,
            vec![
                (BASE + 35 * DAY).to_string(),
                (BASE + 35 * DAY + 60).to_string()
            ]
        );

        let reloaded = DownloadStatsManager::new(storage);
        reloaded.load().unwrap();
        assert_eq!(whole_view(&reloaded).await, view);
    }

    #[tokio::test]
    async fn export_and_merge_keep_their_shape() {
        let dir = TempDir::new().unwrap();
        let mgr = DownloadStatsManager::new(file_storage(&dir));
        for (minute, completed) in [(BASE, 100), (BASE + 60, 150), (BASE + 40 * DAY, 300)] {
            mgr.record_minute(at(minute, vec![task("a", completed, 10)]))
                .await
                .unwrap();
        }

        let exported = mgr.export().await;
        assert_eq!(exported["speed"].as_object().unwrap().len(), 3);
        assert!(exported.get("speedParts").is_none());
        serde_json::from_value::<DownloadStatsStore>(exported.clone()).unwrap();

        let other_dir = TempDir::new().unwrap();
        let other = DownloadStatsManager::new(file_storage(&other_dir));
        other.merge(exported).await.unwrap();
        assert_eq!(whole_view(&other).await, whole_view(&mgr).await);
    }

    #[tokio::test]
    async fn an_old_app_version_can_read_the_main_file() {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OldBaseline {
            completed_length: u64,
            kind: String,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OldStore {
            version: u32,
            baselines: HashMap<String, OldBaseline>,
            monthly: BTreeMap<String, BTreeMap<String, u64>>,
            speed: BTreeMap<i64, SpeedMinuteBucket>,
        }

        let dir = TempDir::new().unwrap();
        let storage = file_storage(&dir);
        let mgr = DownloadStatsManager::new(storage.clone());
        mgr.record_minute(at(BASE, vec![task("a", 100, 10)]))
            .await
            .unwrap();
        mgr.record_minute(at(BASE + 60, vec![task("a", 160, 20)]))
            .await
            .unwrap();

        let old: OldStore = serde_json::from_value(stored(&*storage, STATS_STORE_KEY)).unwrap();
        assert!(old.version >= 1);
        assert_eq!(old.baselines["a"].completed_length, 160);
        assert_eq!(old.baselines["a"].kind, "http");
        assert_eq!(
            old.monthly,
            BTreeMap::from([(
                "2023-11".to_string(),
                BTreeMap::from([("http".to_string(), 60)])
            )])
        );
        assert!(old.speed.is_empty());
    }

    #[tokio::test]
    async fn pruning_empties_and_forgets_old_partitions() {
        let dir = TempDir::new().unwrap();
        let storage = file_storage(&dir);
        let mgr = DownloadStatsManager::new(storage.clone());

        mgr.record_minute(at(BASE, vec![task("a", 100, 10)]))
            .await
            .unwrap();
        mgr.record_minute(at(BASE + 366 * DAY, vec![task("a", 200, 20)]))
            .await
            .unwrap();

        let main = stored(&*storage, STATS_STORE_KEY);
        assert_eq!(main["speedParts"], json!([668]));
        assert_eq!(stored(&*storage, &part_key(656)), json!({}));
    }

    #[tokio::test]
    async fn baselines_not_seen_for_thirty_days_are_dropped() {
        let dir = TempDir::new().unwrap();
        let mgr = DownloadStatsManager::new(file_storage(&dir));

        mgr.record_minute(at(BASE, vec![task("a", 100, 10)]))
            .await
            .unwrap();
        mgr.record_minute(at(BASE + 31 * DAY, vec![task("b", 50, 10)]))
            .await
            .unwrap();

        let exported = mgr.export().await;
        let gids: Vec<_> = exported["baselines"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        assert_eq!(gids, vec!["b".to_string()]);

        mgr.record_minute(at(BASE + 31 * DAY + 60, vec![task("a", 300, 10)]))
            .await
            .unwrap();
        assert_eq!(mgr.export().await["monthly"], json!({}));
    }

    #[tokio::test]
    async fn a_missing_partition_does_not_fail_the_load() {
        let dir = TempDir::new().unwrap();
        let storage = file_storage(&dir);
        storage
            .save(
                STATS_STORE_KEY,
                &json!({
                    "data": {
                        "version": 2,
                        "baselines": {},
                        "monthly": {},
                        "speed": {},
                        "speedParts": [655, 656]
                    }
                }),
            )
            .unwrap();
        storage
            .save(
                &part_key(656),
                &json!({"data": {BASE.to_string(): bucket_json(100, 10)}}),
            )
            .unwrap();

        let mgr = DownloadStatsManager::new(storage);
        mgr.load().unwrap();

        let exported = mgr.export().await;
        let minutes: Vec<_> = exported["speed"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        assert_eq!(minutes, vec![BASE.to_string()]);
    }

    #[tokio::test]
    async fn clear_empties_every_listed_partition() {
        async fn record_two_partitions(mgr: &DownloadStatsManager) {
            mgr.record_minute(at(BASE, vec![task("a", 100, 10)]))
                .await
                .unwrap();
            mgr.record_minute(at(BASE + 40 * DAY, vec![task("a", 200, 20)]))
                .await
                .unwrap();
        }

        fn assert_cleared(storage: &dyn StorageBackend) {
            assert_eq!(stored(storage, STATS_STORE_KEY)["speedParts"], json!([]));
            assert_eq!(stored(storage, &part_key(656)), json!({}));
            assert_eq!(stored(storage, &part_key(657)), json!({}));
        }

        let dir = TempDir::new().unwrap();
        let storage = file_storage(&dir);
        let mgr = DownloadStatsManager::new(storage.clone());

        record_two_partitions(&mgr).await;
        mgr.clear().await.unwrap();
        assert_cleared(&*storage);

        record_two_partitions(&mgr).await;
        let fresh = DownloadStatsManager::new(storage.clone());
        fresh.load().unwrap();
        fresh.clear_sync().unwrap();
        assert_cleared(&*storage);
    }

    #[tokio::test]
    async fn a_downgrade_and_upgrade_keeps_earlier_history() {
        let now = BASE + 45 * DAY;
        let dir = TempDir::new().unwrap();
        let storage = file_storage(&dir);
        let writer = DownloadStatsManager::new(storage.clone());
        for (minute, completed) in [(BASE, 100), (BASE + 60, 150), (BASE + 40 * DAY, 300)] {
            writer
                .record_minute(at(minute, vec![task("a", completed, 10)]))
                .await
                .unwrap();
        }
        storage
            .save(
                STATS_STORE_KEY,
                &json!({
                    "data": {
                        "version": 1,
                        "baselines": {"a": {"completedLength": 300, "kind": "http"}},
                        "monthly": {"2023-11": {"http": 200}},
                        "speed": {now.to_string(): bucket_json(500, 50)}
                    }
                }),
            )
            .unwrap();

        let mgr = DownloadStatsManager::new(storage.clone());
        mgr.load_at(now).unwrap();

        let view = whole_view(&mgr).await;
        let minutes: Vec<_> = view["speed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|point| point["minute"].clone())
            .collect();
        assert_eq!(
            minutes,
            vec![
                json!(BASE),
                json!(BASE + 60),
                json!(BASE + 40 * DAY),
                json!(now)
            ]
        );
        assert_eq!(
            stored(&*storage, STATS_STORE_KEY)["speedParts"],
            json!([656, 657])
        );

        let reloaded = DownloadStatsManager::new(storage);
        reloaded.load_at(now).unwrap();
        assert_eq!(whole_view(&reloaded).await, view);
    }

    #[tokio::test]
    async fn clear_reaches_partitions_the_main_file_does_not_list() {
        let now = BASE + 45 * DAY;
        let dir = TempDir::new().unwrap();
        let storage = file_storage(&dir);
        let writer = DownloadStatsManager::new(storage.clone());
        for minute in [BASE, BASE + 40 * DAY] {
            writer
                .record_minute(at(minute, vec![task("a", 100, 10)]))
                .await
                .unwrap();
        }
        storage
            .save(
                STATS_STORE_KEY,
                &json!({"data": {"version": 2, "baselines": 5, "monthly": {}, "speed": {}}}),
            )
            .unwrap();

        let mgr = DownloadStatsManager::new(storage.clone());
        assert!(mgr.load_at(now).is_err());
        mgr.clear_sync_at(now).unwrap();

        assert_eq!(stored(&*storage, &part_key(656)), json!({}));
        assert_eq!(stored(&*storage, &part_key(657)), json!({}));
    }
}
