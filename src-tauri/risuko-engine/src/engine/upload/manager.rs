use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{watch, Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::traits::{EventSink, StorageBackend};

use super::ftp::FtpSink;
use super::rules::{select_rule_sink, RuleInput, UploadRule};
use super::s3::S3Sink;
use super::sftp::SftpSink;
use super::sink::{
    BoxedSink, PostUploadAction, SinkConfig, UploadControl, UploadFile, UploadProgress,
    UploadSinkRecord,
};
use super::webdav::WebdavSink;

const UPLOAD_STORE_KEY: &str = "upload-sinks";

const MAX_TERMINAL_JOBS: usize = 200;

const MAX_UPLOAD_ATTEMPTS: u32 = 3;

use crate::engine::util::now_secs;

#[derive(Default, Serialize, Deserialize)]
struct UploadStore {
    #[serde(default)]
    sinks: Vec<UploadSinkRecord>,
    #[serde(default)]
    rules: Vec<UploadRule>,
    #[serde(default)]
    default_sink_id: Option<String>,
    #[serde(default = "default_concurrency")]
    max_concurrency: usize,
    #[serde(default)]
    secret_fallback: HashMap<String, Value>,
}

fn default_concurrency() -> usize {
    2
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum JobStatus {
    Queued,
    Active,
    Complete,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadJob {
    pub id: String,
    pub gid: String,
    pub sink_id: String,
    pub local_path: PathBuf,
    pub remote_relative: String,
    pub size: u64,
    pub uploaded: u64,
    pub status: JobStatus,
    pub error: Option<String>,
    pub created_at: u64,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
}

pub struct UploadSinkManager {
    store: Arc<RwLock<UploadStore>>,
    storage: Arc<dyn StorageBackend>,
    event_sink: StdMutex<Arc<dyn EventSink>>,
    jobs: Arc<Mutex<HashMap<String, UploadJob>>>,
    active: Arc<Mutex<HashMap<String, watch::Receiver<UploadProgress>>>>,
    cancels: Arc<Mutex<HashMap<String, CancellationToken>>>,
    semaphore: Arc<RwLock<Arc<Semaphore>>>,
    sink_runtimes: StdMutex<HashMap<String, BoxedSink>>,
}

impl UploadSinkManager {
    pub fn new(storage: Arc<dyn StorageBackend>, event_sink: Arc<dyn EventSink>) -> Self {
        Self {
            store: Arc::new(RwLock::new(UploadStore {
                max_concurrency: default_concurrency(),
                ..Default::default()
            })),
            storage,
            event_sink: StdMutex::new(event_sink),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            active: Arc::new(Mutex::new(HashMap::new())),
            cancels: Arc::new(Mutex::new(HashMap::new())),
            semaphore: Arc::new(RwLock::new(Arc::new(Semaphore::new(default_concurrency())))),
            sink_runtimes: StdMutex::new(HashMap::new()),
        }
    }

    pub fn set_event_sink(&self, sink: Arc<dyn EventSink>) {
        if let Ok(mut s) = self.event_sink.lock() {
            *s = sink;
        }
    }

    pub fn load(&self) -> Result<(), String> {
        if let Some(val) = self.storage.load(UPLOAD_STORE_KEY)? {
            if let Some(data_val) = val.get("data").cloned() {
                let data: UploadStore = serde_json::from_value(data_val)
                    .map_err(|e| format!("Failed to parse upload sinks: {e}"))?;
                let new_concurrency = data.max_concurrency.clamp(1, 16);
                let mut s = self.store.blocking_write();
                *s = data;
                s.max_concurrency = new_concurrency;
                drop(s);
                let mut sem = self.semaphore.blocking_write();
                *sem = Arc::new(Semaphore::new(new_concurrency));
            }
        }
        Ok(())
    }

    pub async fn save(&self) -> Result<(), String> {
        let data = {
            let s = self.store.read().await;
            serde_json::to_value(&*s).map_err(|e| format!("Serialize upload sinks failed: {e}"))?
        };
        let wrapper = serde_json::json!({ "data": data });
        self.storage.save(UPLOAD_STORE_KEY, &wrapper)?;
        Ok(())
    }

    pub async fn get_sink_secret_fallback(&self, id: &str) -> Option<Value> {
        let s = self.store.read().await;
        s.secret_fallback.get(id).cloned()
    }

    pub async fn put_sink_secret_fallback(&self, id: &str, secrets: &Value) -> Result<(), String> {
        let mut s = self.store.write().await;
        s.secret_fallback.insert(id.to_string(), secrets.clone());
        drop(s);
        self.save().await
    }

    pub async fn remove_sink_secret_fallback(&self, id: &str) -> Result<(), String> {
        let mut s = self.store.write().await;
        s.secret_fallback.remove(id);
        drop(s);
        self.save().await
    }

    pub async fn list_sinks(&self) -> Vec<UploadSinkRecord> {
        self.store.read().await.sinks.clone()
    }

    pub async fn list_rules(&self) -> Vec<UploadRule> {
        self.store.read().await.rules.clone()
    }

    pub async fn default_sink_id(&self) -> Option<String> {
        self.store.read().await.default_sink_id.clone()
    }

    pub async fn list_jobs(&self) -> Vec<UploadJob> {
        let mut jobs: Vec<UploadJob> = self.jobs.lock().await.values().cloned().collect();
        let active = self.active.lock().await;
        for job in jobs.iter_mut() {
            if let Some(a) = active.get(&job.id) {
                job.uploaded = a.borrow().uploaded;
            }
        }
        jobs.sort_by_key(|j| std::cmp::Reverse(j.created_at));
        jobs
    }

    pub async fn add_sink(&self, mut record: UploadSinkRecord) -> Result<UploadSinkRecord, String> {
        if record.id.is_empty() {
            record.id = Uuid::new_v4().to_string();
        }
        record.created_at = now_secs();
        let _ = build_sink_runtime(&record.config)?;

        let mut s = self.store.write().await;
        if s.sinks.iter().any(|x| x.id == record.id) {
            return Err(format!("sink id already exists: {}", record.id));
        }
        s.sinks.push(record.clone());
        if s.default_sink_id.is_none() {
            s.default_sink_id = Some(record.id.clone());
        }
        drop(s);
        self.save().await?;
        Ok(record)
    }

    pub async fn update_sink(&self, mut record: UploadSinkRecord) -> Result<(), String> {
        let mut s = self.store.write().await;
        let slot = s
            .sinks
            .iter_mut()
            .find(|x| x.id == record.id)
            .ok_or_else(|| format!("unknown sink {}", record.id))?;
        merge_secrets(&mut record.config, &slot.config);
        let _ = build_sink_runtime(&record.config)?;
        *slot = record;
        self.invalidate_sink_runtime(&slot.id.clone());
        drop(s);
        self.save().await
    }

    pub async fn remove_sink(&self, id: &str) -> Result<(), String> {
        let mut s = self.store.write().await;
        let before = s.sinks.len();
        s.sinks.retain(|x| x.id != id);
        if s.sinks.len() == before {
            return Err(format!("unknown sink {id}"));
        }
        s.rules.retain(|r| r.sink_id != id);
        if s.default_sink_id.as_deref() == Some(id) {
            s.default_sink_id = s.sinks.first().map(|x| x.id.clone());
        }
        s.secret_fallback.remove(id);
        self.invalidate_sink_runtime(id);
        drop(s);
        self.save().await
    }

    pub async fn set_default_sink(&self, id: Option<String>) -> Result<(), String> {
        let mut s = self.store.write().await;
        if let Some(ref new_id) = id {
            if !s.sinks.iter().any(|x| &x.id == new_id) {
                return Err(format!("unknown sink {new_id}"));
            }
        }
        s.default_sink_id = id;
        drop(s);
        self.save().await
    }

    pub async fn set_max_concurrency(&self, n: usize) -> Result<(), String> {
        let n = n.clamp(1, 16);
        let prev = {
            let mut s = self.store.write().await;
            std::mem::replace(&mut s.max_concurrency, n)
        };
        let sem = self.semaphore.read().await.clone();
        if n > prev {
            sem.add_permits(n - prev);
        } else if n < prev {
            let retire = (prev - n) as u32;
            tokio::spawn(async move {
                if let Ok(permits) = sem.acquire_many_owned(retire).await {
                    permits.forget();
                }
            });
        }
        self.save().await
    }

    pub async fn add_rule(&self, mut rule: UploadRule) -> Result<UploadRule, String> {
        if rule.id.is_empty() {
            rule.id = Uuid::new_v4().to_string();
        }
        let mut s = self.store.write().await;
        if !s.sinks.iter().any(|x| x.id == rule.sink_id) {
            return Err(format!("rule references unknown sink {}", rule.sink_id));
        }
        if s.rules.iter().any(|r| r.id == rule.id) {
            return Err(format!("duplicate rule id {}", rule.id));
        }
        s.rules.push(rule.clone());
        drop(s);
        self.save().await?;
        Ok(rule)
    }

    pub async fn remove_rule(&self, id: &str) -> Result<(), String> {
        let mut s = self.store.write().await;
        let before = s.rules.len();
        s.rules.retain(|r| r.id != id);
        if s.rules.len() == before {
            return Err(format!("unknown rule {id}"));
        }
        drop(s);
        self.save().await
    }

    pub async fn update_rule(&self, rule: UploadRule) -> Result<(), String> {
        let mut s = self.store.write().await;
        if !s.sinks.iter().any(|x| x.id == rule.sink_id) {
            return Err(format!("rule references unknown sink {}", rule.sink_id));
        }
        let slot = s
            .rules
            .iter_mut()
            .find(|r| r.id == rule.id)
            .ok_or_else(|| format!("unknown rule {}", rule.id))?;
        *slot = rule;
        drop(s);
        self.save().await
    }

    pub async fn test_sink(&self, id: &str) -> Result<(), String> {
        let cfg = {
            let s = self.store.read().await;
            s.sinks
                .iter()
                .find(|x| x.id == id)
                .map(|x| x.config.clone())
                .ok_or_else(|| format!("unknown sink {id}"))?
        };
        let runtime = build_sink_runtime(&cfg)?;
        runtime.test().await
    }

    pub async fn cancel_job(&self, id: &str) -> Result<(), String> {
        let cancels = self.cancels.lock().await;
        let token = cancels
            .get(id)
            .ok_or_else(|| format!("job {id} not found"))?
            .clone();
        drop(cancels);
        token.cancel();
        Ok(())
    }

    pub async fn clear_history(&self) {
        let mut jobs = self.jobs.lock().await;
        jobs.retain(|_, j| matches!(j.status, JobStatus::Queued | JobStatus::Active));
    }

    pub async fn enqueue_for_file(
        self: &Arc<Self>,
        gid: &str,
        local_path: PathBuf,
        remote_relative: String,
        size: u64,
        category: Option<String>,
        task_kind: &str,
        override_sink_id: Option<String>,
    ) {
        let chosen_sink = self
            .pick_sink(
                &local_path,
                size,
                category.as_deref(),
                task_kind,
                override_sink_id,
            )
            .await;

        let Some(sink_id) = chosen_sink else {
            return;
        };

        let mut sink_record = {
            let s = self.store.read().await;
            match s.sinks.iter().find(|x| x.id == sink_id).cloned() {
                Some(r) => r,
                None => {
                    tracing::warn!("upload: sink {sink_id} disappeared before enqueue");
                    return;
                }
            }
        };
        if task_kind == "torrent" && sink_record.post_action != PostUploadAction::Keep {
            tracing::info!("upload: keeping local files of torrent {gid} despite post-action");
            sink_record.post_action = PostUploadAction::Keep;
        }

        let job_id = Uuid::new_v4().to_string();
        let job = UploadJob {
            id: job_id.clone(),
            gid: gid.to_string(),
            sink_id: sink_id.clone(),
            local_path: local_path.clone(),
            remote_relative: remote_relative.clone(),
            size,
            uploaded: 0,
            status: JobStatus::Queued,
            error: None,
            created_at: now_secs(),
            started_at: None,
            finished_at: None,
        };

        {
            let mut jobs = self.jobs.lock().await;
            jobs.insert(job_id.clone(), job.clone());
            prune_terminal_jobs(&mut jobs, MAX_TERMINAL_JOBS);
        }
        let cancel = CancellationToken::new();
        self.cancels
            .lock()
            .await
            .insert(job_id.clone(), cancel.clone());
        self.emit_event("engine:upload-queued", &job);

        let this = self.clone();
        tokio::spawn(async move {
            this.run_job(
                job_id,
                sink_record,
                local_path,
                remote_relative,
                size,
                cancel,
            )
            .await;
        });
    }

    async fn pick_sink(
        &self,
        local_path: &Path,
        size: u64,
        category: Option<&str>,
        task_kind: &str,
        override_sink_id: Option<String>,
    ) -> Option<String> {
        let s = self.store.read().await;

        if let Some(id) = override_sink_id {
            return if s.sinks.iter().any(|x| x.id == id) {
                Some(id)
            } else {
                None
            };
        }

        let input = RuleInput {
            file_path: local_path,
            size,
            category,
            task_kind,
        };
        if let Some(sink_id) = select_rule_sink(&s.rules, &input) {
            return Some(sink_id.to_string());
        }

        s.default_sink_id.clone()
    }

    fn invalidate_sink_runtime(&self, id: &str) {
        if let Ok(mut cache) = self.sink_runtimes.lock() {
            cache.remove(id);
        }
    }

    async fn sink_runtime_for(&self, record: &UploadSinkRecord) -> Result<BoxedSink, String> {
        let s = self.store.read().await;
        let Some(current) = s.sinks.iter().find(|x| x.id == record.id) else {
            return build_sink_runtime(&record.config);
        };
        if let Ok(cache) = self.sink_runtimes.lock() {
            if let Some(rt) = cache.get(&record.id) {
                return Ok(rt.clone());
            }
        }
        let rt = build_sink_runtime(&current.config)?;
        if let Ok(mut cache) = self.sink_runtimes.lock() {
            cache.insert(record.id.clone(), rt.clone());
        }
        Ok(rt)
    }

    async fn run_job(
        self: Arc<Self>,
        job_id: String,
        sink_record: UploadSinkRecord,
        local_path: PathBuf,
        remote_relative: String,
        size: u64,
        cancel: CancellationToken,
    ) {
        let permit = loop {
            let sem = self.semaphore.read().await.clone();
            tokio::select! {
                res = sem.clone().acquire_owned() => match res {
                    Ok(p) => break p,
                    Err(_) => {
                        if Arc::ptr_eq(&sem, &*self.semaphore.read().await) {
                            self.cancels.lock().await.remove(&job_id);
                            self.fail_job(&job_id, "engine shutdown").await;
                            return;
                        }
                        continue;
                    }
                },
                _ = cancel.cancelled() => {
                    self.cancels.lock().await.remove(&job_id);
                    self.cancel_job_state(&job_id).await;
                    return;
                }
            }
        };

        if cancel.is_cancelled() {
            drop(permit);
            self.cancels.lock().await.remove(&job_id);
            self.cancel_job_state(&job_id).await;
            return;
        }

        let (tx, rx) = watch::channel(UploadProgress {
            uploaded: 0,
            total: size,
        });
        self.active.lock().await.insert(job_id.clone(), rx);

        // Drop the `jobs` guard before emitting so the event sink can't re-enter the manager
        let snapshot = {
            let mut jobs = self.jobs.lock().await;
            jobs.get_mut(&job_id).map(|j| {
                j.status = JobStatus::Active;
                j.started_at = Some(now_secs());
                j.clone()
            })
        };
        if let Some(snap) = snapshot {
            self.emit_event("engine:upload-start", &snap);
        }

        let sink_runtime = match self.sink_runtime_for(&sink_record).await {
            Ok(r) => r,
            Err(e) => {
                drop(permit);
                self.cleanup_active(&job_id).await;
                self.fail_job(&job_id, &format!("sink init failed: {e}"))
                    .await;
                return;
            }
        };

        let file = UploadFile {
            local_path: local_path.clone(),
            remote_relative,
            size,
        };
        let ctl = UploadControl {
            cancel: cancel.clone(),
            progress: tx,
        };

        let mut attempt = 1;
        let result = loop {
            let res = sink_runtime.upload(&file, &ctl).await;
            match res {
                Err(e)
                    if attempt < MAX_UPLOAD_ATTEMPTS
                        && !cancel.is_cancelled()
                        && is_retryable_upload_error(&e) =>
                {
                    tracing::warn!("upload {job_id}: attempt {attempt} failed ({e}), retrying");
                    ctl.report(0, size);
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = tokio::time::sleep(upload_retry_delay(attempt)) => {}
                    }
                    attempt += 1;
                }
                other => break other,
            }
        };
        drop(permit);
        self.cleanup_active(&job_id).await;

        match result {
            Ok(_remote_url) => {
                if let Err(e) = run_post_action(
                    &local_path,
                    &file.remote_relative,
                    sink_record.post_action,
                    sink_record.move_target.as_deref(),
                )
                .await
                {
                    tracing::warn!("upload {job_id}: post-action failed: {e}");
                }
                self.complete_job(&job_id).await;
                self.touch_sink_used(&sink_record.id).await;
            }
            Err(e) => {
                if cancel.is_cancelled() {
                    self.cancel_job_state(&job_id).await;
                } else {
                    self.fail_job(&job_id, &e).await;
                }
            }
        }
    }

    async fn cleanup_active(&self, job_id: &str) {
        self.active.lock().await.remove(job_id);
        self.cancels.lock().await.remove(job_id);
    }

    async fn complete_job(&self, job_id: &str) {
        let mut jobs = self.jobs.lock().await;
        if let Some(j) = jobs.get_mut(job_id) {
            j.status = JobStatus::Complete;
            j.uploaded = j.size;
            j.finished_at = Some(now_secs());
            let snapshot = j.clone();
            drop(jobs);
            self.emit_event("engine:upload-complete", &snapshot);
        }
    }

    async fn fail_job(&self, job_id: &str, error: &str) {
        let mut jobs = self.jobs.lock().await;
        if let Some(j) = jobs.get_mut(job_id) {
            j.status = JobStatus::Failed;
            j.error = Some(error.to_string());
            j.finished_at = Some(now_secs());
            let snapshot = j.clone();
            drop(jobs);
            self.emit_event("engine:upload-error", &snapshot);
        }
    }

    async fn cancel_job_state(&self, job_id: &str) {
        let mut jobs = self.jobs.lock().await;
        if let Some(j) = jobs.get_mut(job_id) {
            j.status = JobStatus::Cancelled;
            j.finished_at = Some(now_secs());
            let snapshot = j.clone();
            drop(jobs);
            self.emit_event("engine:upload-cancelled", &snapshot);
        }
    }

    async fn touch_sink_used(&self, sink_id: &str) {
        let now = now_secs();
        let changed = {
            let mut s = self.store.write().await;
            match s.sinks.iter_mut().find(|x| x.id == sink_id) {
                Some(sink) if sink.last_used_at != Some(now) => {
                    sink.last_used_at = Some(now);
                    true
                }
                _ => false,
            }
        };
        if changed {
            let _ = self.save().await;
        }
    }

    fn emit_event(&self, name: &str, job: &UploadJob) {
        let sink = match self.event_sink.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        };
        match serde_json::to_value(job) {
            Ok(v) => sink.emit(name, v),
            Err(e) => tracing::warn!("upload event serialize failed: {e}"),
        }
    }
}

fn prune_terminal_jobs(jobs: &mut HashMap<String, UploadJob>, max_terminal: usize) {
    let mut terminal: Vec<(u64, String)> = jobs
        .values()
        .filter(|j| {
            matches!(
                j.status,
                JobStatus::Complete | JobStatus::Failed | JobStatus::Cancelled
            )
        })
        .map(|j| (j.created_at, j.id.clone()))
        .collect();
    if terminal.len() <= max_terminal {
        return;
    }
    terminal.sort_by_key(|(created_at, _)| *created_at);
    let to_remove = terminal.len() - max_terminal;
    for (_, id) in terminal.into_iter().take(to_remove) {
        jobs.remove(&id);
    }
}

pub fn build_sink_runtime(cfg: &SinkConfig) -> Result<BoxedSink, String> {
    match cfg {
        SinkConfig::Webdav(c) => Ok(Arc::new(WebdavSink::new(c.clone())?)),
        SinkConfig::S3(c) => Ok(Arc::new(S3Sink::new(c.clone())?)),
        SinkConfig::Sftp(c) => Ok(Arc::new(SftpSink::new(c.clone())?)),
        SinkConfig::Ftp(c) => Ok(Arc::new(FtpSink::new(c.clone())?)),
    }
}

fn move_destination(target: &Path, remote_relative: &str, path: &Path) -> Option<PathBuf> {
    let mut dst = target.to_path_buf();
    let mut any = false;
    for comp in Path::new(remote_relative).components() {
        if let std::path::Component::Normal(part) = comp {
            dst.push(part);
            any = true;
        }
    }
    if !any {
        dst.push(path.file_name()?);
    }
    Some(dst)
}

async fn free_destination(path: PathBuf) -> PathBuf {
    if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
        return path;
    }
    let stem = path
        .file_stem()
        .and_then(|v| v.to_str())
        .unwrap_or("file")
        .to_string();
    let ext = path
        .extension()
        .and_then(|v| v.to_str())
        .map(str::to_string);
    for index in 1..10_000u32 {
        let name = match &ext {
            Some(ext) => format!("{stem} ({index}).{ext}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = path.with_file_name(name);
        if !tokio::fs::try_exists(&candidate).await.unwrap_or(false) {
            return candidate;
        }
    }
    path.with_file_name(format!("{stem}.{}", Uuid::new_v4().simple()))
}

fn is_retryable_upload_error(error: &str) -> bool {
    let e = error.to_ascii_lowercase();
    ![
        "cancel",
        "auth",
        "login",
        "denied",
        "forbidden",
        "401",
        "403",
        "traversal",
        "no such file",
        "open ",
    ]
    .iter()
    .any(|marker| e.contains(marker))
}

fn upload_retry_delay(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_secs(2u64.saturating_mul(u64::from(attempt)).min(30))
}

async fn run_post_action(
    path: &Path,
    remote_relative: &str,
    action: PostUploadAction,
    move_target: Option<&Path>,
) -> Result<(), String> {
    match action {
        PostUploadAction::Keep => Ok(()),
        PostUploadAction::Trash => tokio::fs::remove_file(path)
            .await
            .map_err(|e| format!("remove {}: {e}", path.display())),
        PostUploadAction::Move => {
            let target = match move_target {
                Some(t) => t,
                None => return Err("move post-action requires move_target".into()),
            };
            tokio::fs::create_dir_all(target)
                .await
                .map_err(|e| format!("mkdir {}: {e}", target.display()))?;
            let dst = move_destination(target, remote_relative, path)
                .ok_or_else(|| "source has no file name".to_string())?;
            if let Some(parent) = dst.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
            }
            let dst = free_destination(dst).await;
            if let Err(e) = tokio::fs::rename(path, &dst).await {
                if !is_cross_device_error(&e) {
                    return Err(format!(
                        "rename {} -> {}: {e}",
                        path.display(),
                        dst.display()
                    ));
                }
                tokio::fs::copy(path, &dst)
                    .await
                    .map_err(|e| format!("copy {} -> {}: {e}", path.display(), dst.display()))?;
                tokio::fs::remove_file(path)
                    .await
                    .map_err(|e| format!("cleanup {}: {e}", path.display()))?;
            }
            Ok(())
        }
    }
}

fn is_cross_device_error(e: &std::io::Error) -> bool {
    if e.kind() == std::io::ErrorKind::CrossesDevices {
        return true;
    }
    match e.raw_os_error() {
        #[cfg(unix)]
        Some(18) => true,
        #[cfg(windows)]
        Some(17) => true,
        _ => false,
    }
}

fn merge_secrets(new: &mut SinkConfig, old: &SinkConfig) {
    match (new, old) {
        (SinkConfig::Webdav(n), SinkConfig::Webdav(o)) if n.password.is_empty() => {
            n.password = o.password.clone();
        }
        (SinkConfig::S3(n), SinkConfig::S3(o)) if n.secret_access_key.is_empty() => {
            n.secret_access_key = o.secret_access_key.clone();
        }
        (SinkConfig::Sftp(n), SinkConfig::Sftp(o)) => {
            if n.password.is_empty() {
                n.password = o.password.clone();
            }
            if n.private_key.is_empty() {
                n.private_key = o.private_key.clone();
            }
        }
        (SinkConfig::Ftp(n), SinkConfig::Ftp(o)) if n.password.is_empty() => {
            n.password = o.password.clone();
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::upload::sink::{FtpConfig, S3Config, SftpConfig, SinkConfig, WebdavConfig};
    use crate::traits::{FileStorage, NoopEventSink};
    use tempfile::TempDir;

    struct UploadTestCtx {
        _dir: TempDir,
        mgr: UploadSinkManager,
    }

    fn test_manager() -> UploadTestCtx {
        let dir = TempDir::new().unwrap();
        let storage: Arc<dyn crate::traits::StorageBackend> =
            Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let event_sink: Arc<dyn crate::traits::EventSink> = Arc::new(NoopEventSink);
        let mgr = UploadSinkManager::new(storage, event_sink);
        UploadTestCtx { _dir: dir, mgr }
    }

    fn sftp_config() -> SinkConfig {
        SinkConfig::Sftp(SftpConfig {
            host: "sftp.example.com".into(),
            port: 22,
            username: "u".into(),
            password: "p".into(),
            private_key: String::new(),
            base_path: "/uploads".into(),
        })
    }

    fn ftp_config() -> SinkConfig {
        SinkConfig::Ftp(FtpConfig {
            host: "ftp.example.com".into(),
            port: 21,
            username: "u".into(),
            password: "p".into(),
            base_path: "/uploads".into(),
            secure: false,
            insecure: false,
        })
    }

    #[test]
    fn add_sink_generates_id_and_created_at() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let record = UploadSinkRecord {
            id: String::new(),
            label: "Test".into(),
            config: sftp_config(),
            post_action: PostUploadAction::Keep,
            move_target: None,
            created_at: 0,
            last_used_at: None,
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let created = rt.block_on(mgr.add_sink(record)).unwrap();
        assert!(!created.id.is_empty());
        assert!(created.created_at > 0);
    }

    #[test]
    fn add_sink_sets_default_when_first() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let created = rt
            .block_on(mgr.add_sink(UploadSinkRecord {
                id: String::new(),
                label: "First".into(),
                config: sftp_config(),
                post_action: PostUploadAction::Keep,
                move_target: None,
                created_at: 0,
                last_used_at: None,
            }))
            .unwrap();
        let default_id = rt.block_on(mgr.default_sink_id());
        assert_eq!(default_id, Some(created.id));
    }

    #[test]
    fn add_sink_rejects_duplicate_id() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let created = rt
            .block_on(mgr.add_sink(UploadSinkRecord {
                id: String::new(),
                label: "First".into(),
                config: sftp_config(),
                post_action: PostUploadAction::Keep,
                move_target: None,
                created_at: 0,
                last_used_at: None,
            }))
            .unwrap();
        let duplicate = rt.block_on(mgr.add_sink(UploadSinkRecord {
            id: created.id.clone(),
            label: "Duplicate".into(),
            config: ftp_config(),
            post_action: PostUploadAction::Keep,
            move_target: None,
            created_at: 0,
            last_used_at: None,
        }));
        assert!(duplicate.is_err());
    }

    #[test]
    fn list_sinks_returns_added() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(mgr.add_sink(UploadSinkRecord {
            id: String::new(),
            label: "A".into(),
            config: sftp_config(),
            post_action: PostUploadAction::Keep,
            move_target: None,
            created_at: 0,
            last_used_at: None,
        }))
        .unwrap();
        let sinks = rt.block_on(mgr.list_sinks());
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].label, "A");
    }

    #[test]
    fn update_sink_modifies_label() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let created = rt
            .block_on(mgr.add_sink(UploadSinkRecord {
                id: String::new(),
                label: "Old".into(),
                config: sftp_config(),
                post_action: PostUploadAction::Keep,
                move_target: None,
                created_at: 0,
                last_used_at: None,
            }))
            .unwrap();
        let mut updated = created.clone();
        updated.label = "New".into();
        rt.block_on(mgr.update_sink(updated)).unwrap();
        let sinks = rt.block_on(mgr.list_sinks());
        assert_eq!(sinks[0].label, "New");
    }

    #[test]
    fn update_sink_inherits_secrets_when_empty() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let created = rt
            .block_on(mgr.add_sink(UploadSinkRecord {
                id: String::new(),
                label: "A".into(),
                config: sftp_config(),
                post_action: PostUploadAction::Keep,
                move_target: None,
                created_at: 0,
                last_used_at: None,
            }))
            .unwrap();
        let mut updated = created.clone();
        if let SinkConfig::Sftp(ref mut c) = updated.config {
            c.password.clear();
            c.private_key.clear();
        }
        rt.block_on(mgr.update_sink(updated)).unwrap();
        let sinks = rt.block_on(mgr.list_sinks());
        if let SinkConfig::Sftp(ref c) = sinks[0].config {
            assert_eq!(c.password, "p");
        } else {
            panic!("expected sftp");
        }
    }

    #[test]
    fn remove_sink_deletes_and_updates_default() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = rt
            .block_on(mgr.add_sink(UploadSinkRecord {
                id: String::new(),
                label: "A".into(),
                config: sftp_config(),
                post_action: PostUploadAction::Keep,
                move_target: None,
                created_at: 0,
                last_used_at: None,
            }))
            .unwrap();
        let b = rt
            .block_on(mgr.add_sink(UploadSinkRecord {
                id: String::new(),
                label: "B".into(),
                config: ftp_config(),
                post_action: PostUploadAction::Keep,
                move_target: None,
                created_at: 0,
                last_used_at: None,
            }))
            .unwrap();
        assert_eq!(rt.block_on(mgr.default_sink_id()), Some(a.id.clone()));
        rt.block_on(mgr.remove_sink(&a.id)).unwrap();
        let sinks = rt.block_on(mgr.list_sinks());
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].id, b.id);
        assert_eq!(rt.block_on(mgr.default_sink_id()), Some(b.id.clone()));
    }

    #[test]
    fn remove_sink_clears_default_when_last() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = rt
            .block_on(mgr.add_sink(UploadSinkRecord {
                id: String::new(),
                label: "A".into(),
                config: sftp_config(),
                post_action: PostUploadAction::Keep,
                move_target: None,
                created_at: 0,
                last_used_at: None,
            }))
            .unwrap();
        rt.block_on(mgr.remove_sink(&a.id)).unwrap();
        assert_eq!(rt.block_on(mgr.default_sink_id()), None);
    }

    #[test]
    fn set_default_sink_validates() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(mgr.set_default_sink(Some("nonexistent".into())));
        assert!(err.is_err());
    }

    #[test]
    fn set_max_concurrency_clamps_and_persists() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(mgr.set_max_concurrency(0)).unwrap();
        rt.block_on(mgr.set_max_concurrency(100)).unwrap();
    }

    #[test]
    fn load_and_save_round_trip() {
        let dir = TempDir::new().unwrap();
        let storage: Arc<dyn crate::traits::StorageBackend> =
            Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let event_sink: Arc<dyn crate::traits::EventSink> = Arc::new(NoopEventSink);
        let mgr = UploadSinkManager::new(storage.clone(), event_sink.clone());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let created = rt
            .block_on(mgr.add_sink(UploadSinkRecord {
                id: String::new(),
                label: "Persisted".into(),
                config: sftp_config(),
                post_action: PostUploadAction::Move,
                move_target: Some("/done".into()),
                created_at: 0,
                last_used_at: None,
            }))
            .unwrap();
        let mgr2 = UploadSinkManager::new(storage, event_sink);
        mgr2.load().unwrap();
        let sinks = rt.block_on(mgr2.list_sinks());
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].label, "Persisted");
        assert_eq!(sinks[0].post_action, PostUploadAction::Move);
        assert_eq!(rt.block_on(mgr2.default_sink_id()), Some(created.id));
    }

    #[test]
    fn build_sink_runtime_webdav_ok() {
        let cfg = SinkConfig::Webdav(WebdavConfig {
            endpoint: "https://dav.example.com".into(),
            base_path: String::new(),
            username: String::new(),
            password: String::new(),
            insecure: false,
        });
        assert!(build_sink_runtime(&cfg).is_ok());
    }

    #[test]
    fn build_sink_runtime_webdav_invalid_endpoint() {
        let cfg = SinkConfig::Webdav(WebdavConfig {
            endpoint: String::new(),
            base_path: String::new(),
            username: String::new(),
            password: String::new(),
            insecure: false,
        });
        assert!(build_sink_runtime(&cfg).is_err());
    }

    #[test]
    fn build_sink_runtime_s3_ok() {
        let cfg = SinkConfig::S3(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            bucket: "b".into(),
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            prefix: String::new(),
            force_path_style: false,
        });
        assert!(build_sink_runtime(&cfg).is_ok());
    }

    #[test]
    fn build_sink_runtime_s3_invalid_bucket() {
        let cfg = SinkConfig::S3(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            bucket: String::new(),
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            prefix: String::new(),
            force_path_style: false,
        });
        assert!(build_sink_runtime(&cfg).is_err());
    }

    #[test]
    fn build_sink_runtime_sftp_ok() {
        let cfg = SinkConfig::Sftp(SftpConfig {
            host: "h".into(),
            port: 22,
            username: "u".into(),
            password: "p".into(),
            private_key: String::new(),
            base_path: String::new(),
        });
        assert!(build_sink_runtime(&cfg).is_ok());
    }

    #[test]
    fn build_sink_runtime_sftp_no_credentials() {
        let cfg = SinkConfig::Sftp(SftpConfig {
            host: "h".into(),
            port: 22,
            username: "u".into(),
            password: String::new(),
            private_key: String::new(),
            base_path: String::new(),
        });
        assert!(build_sink_runtime(&cfg).is_err());
    }

    #[test]
    fn build_sink_runtime_ftp_ok() {
        let cfg = SinkConfig::Ftp(FtpConfig {
            host: "ftp.example.com".into(),
            port: 21,
            username: String::new(),
            password: String::new(),
            base_path: String::new(),
            secure: false,
            insecure: false,
        });
        assert!(build_sink_runtime(&cfg).is_ok());
    }

    #[test]
    fn build_sink_runtime_ftp_empty_host() {
        let cfg = SinkConfig::Ftp(FtpConfig {
            host: String::new(),
            port: 21,
            username: String::new(),
            password: String::new(),
            base_path: String::new(),
            secure: false,
            insecure: false,
        });
        assert!(build_sink_runtime(&cfg).is_err());
    }

    #[test]
    fn move_destination_preserves_subtree() {
        let t = Path::new("/dst");
        let src = Path::new("/src/a/ep01.mkv");
        assert_eq!(
            move_destination(t, "S01/ep01.mkv", src),
            Some(PathBuf::from("/dst/S01/ep01.mkv"))
        );
        assert_eq!(
            move_destination(t, "../../etc/x", src),
            Some(PathBuf::from("/dst/etc/x"))
        );
        assert_eq!(
            move_destination(t, "", src),
            Some(PathBuf::from("/dst/ep01.mkv"))
        );
    }

    #[tokio::test]
    async fn post_action_move_never_overwrites() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out");
        let src_a = dir.path().join("a.txt");
        let src_b = dir.path().join("b.txt");
        std::fs::write(&src_a, "A").unwrap();
        std::fs::write(&src_b, "B").unwrap();
        run_post_action(
            &src_a,
            "CD1/readme.txt",
            PostUploadAction::Move,
            Some(&target),
        )
        .await
        .unwrap();
        run_post_action(
            &src_b,
            "CD1/readme.txt",
            PostUploadAction::Move,
            Some(&target),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(target.join("CD1/readme.txt")).unwrap(),
            "A"
        );
        assert_eq!(
            std::fs::read_to_string(target.join("CD1/readme (1).txt")).unwrap(),
            "B"
        );
    }

    #[test]
    fn retry_skips_credential_and_cancel_errors() {
        assert!(is_retryable_upload_error("PUT failed: connection reset"));
        assert!(is_retryable_upload_error("PUT x returned 503: busy"));
        assert!(!is_retryable_upload_error("cancelled"));
        assert!(!is_retryable_upload_error("FTP login failed: 530"));
        assert!(!is_retryable_upload_error("PUT x returned 403 Forbidden"));
    }

    #[tokio::test]
    async fn resize_keeps_single_semaphore_within_limit() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let sem = mgr.semaphore.read().await.clone();
        let held = sem.clone().acquire_many_owned(2).await.unwrap();
        mgr.set_max_concurrency(1).await.unwrap();
        assert_eq!(sem.available_permits(), 0);
        drop(held);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while sem.available_permits() != 1 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        mgr.set_max_concurrency(4).await.unwrap();
        assert_eq!(sem.available_permits(), 4);
    }
}
