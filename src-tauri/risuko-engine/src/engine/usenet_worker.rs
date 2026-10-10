use crate::engine::archive_pipeline::{cleanup_after_success, is_archive_volume_name, CleanupMode};
use crate::engine::task::{DownloadTask, UsenetRepairFailure, UsenetTaskData, UsenetTaskFile};
use crate::engine::usenet::{
    deferred_par2_volumes, file_role, FileRole, NzbSegment, UsenetCredentialResolver,
    UsenetCredentials, UsenetProviderProfile,
};
use crate::engine::usenet_par2::{
    platform_limits, verify_or_repair_with_cancel, Par2Error, Par2InputFile, Par2RepairRequest,
};
use crate::engine::usenet_pipeline::{
    assemble_file_with_report_with_limits_at_offset, decode_yenc_part, mark_par2_repaired,
    partial_path, resume_sidecar_matches, resume_sidecar_path, ArticleFetchError, ArticleSource,
    AssemblyReport, DecodedYencPart, ResumeSidecar, YencAssemblyBudget, YencAssemblyLimits,
};
use crate::engine::usenet_transport::{
    canonical_message_id, NntpConnection, NntpError, ProviderConnectionCapacityRegistry,
    ProviderConnectionLease, ProviderPool,
};
use fs4::{FileExt, TryLockError};
use risuko_bt::limiter::Throttle;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct AnonymousCredentialResolver;

#[async_trait::async_trait]
impl UsenetCredentialResolver for AnonymousCredentialResolver {
    async fn resolve(&self, _profile_id: &str) -> Result<Option<UsenetCredentials>, String> {
        Ok(None)
    }
}

#[derive(Debug)]
pub(crate) struct UsenetDownloadError {
    message: String,
    repair_failure: Option<UsenetRepairFailure>,
}

impl UsenetDownloadError {
    fn from_par2_error(error: Par2Error, unavailable: &[String]) -> Self {
        let repair_failure = match &error {
            Par2Error::InsufficientRecovery { needed, available } => Some(UsenetRepairFailure {
                needed_blocks: *needed,
                available_blocks: *available,
                partials_retained: true,
            }),
            _ => None,
        };
        Self {
            message: format!("{error}; unavailable segments: {}", unavailable.join(", ")),
            repair_failure,
        }
    }

    pub(crate) fn repair_failure(&self) -> Option<&UsenetRepairFailure> {
        self.repair_failure.as_ref()
    }
}

impl From<String> for UsenetDownloadError {
    fn from(message: String) -> Self {
        Self {
            message,
            repair_failure: None,
        }
    }
}

impl fmt::Display for UsenetDownloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for UsenetDownloadError {}

struct NntpArticleSource {
    pool: Arc<ProviderPool>,
    resolver: Arc<dyn UsenetCredentialResolver>,
    connection_capacity: Arc<ProviderConnectionCapacityRegistry>,
    profile_sessions: ProfileSessionCache,
    in_flight: AtomicUsize,
    preferred_profile_id: Mutex<Option<String>>,
    active_time: ActiveTimeTracker,
    max_active_seconds: u64,
    cancel: CancellationToken,
    proxy: Option<risuko_http::ProxyConnector>,
    throttle: Throttle,
    pipeline_depth: usize,
}

pub(crate) const PIPELINE_DEPTH: usize = 3;

#[derive(Default)]
struct ProfileSessionCache {
    sessions: Mutex<HashMap<String, Arc<ProfileSession>>>,
}

#[derive(Default)]
struct ProfileSession {
    credentials: AsyncMutex<Option<Option<UsenetCredentials>>>,
    idle: Mutex<Vec<LeasedNntpConnection>>,
    returned: Notify,
    pipeline: Mutex<PipelineState>,
}

enum ArticleReply {
    Article(Vec<u8>),
    Failed(NntpError),
    Retry(NntpError),
    Requeue,
}

struct QueuedArticle {
    seq: u64,
    message_id: String,
    fresh_only: bool,
    reply: oneshot::Sender<ArticleReply>,
}

#[derive(Default)]
struct PipelineState {
    queue: VecDeque<QueuedArticle>,
    drivers: usize,
    inflight: usize,
    next_seq: u64,
}

impl ProfileSession {
    fn take_idle(&self) -> Option<LeasedNntpConnection> {
        self.idle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop()
    }

    fn put_idle(&self, connection: LeasedNntpConnection) {
        self.idle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(connection);
        self.returned.notify_one();
    }

    fn pipeline(&self) -> std::sync::MutexGuard<'_, PipelineState> {
        self.pipeline
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn enqueue(
        &self,
        message_id: &str,
        fresh_only: bool,
        depth: usize,
    ) -> (u64, bool, oneshot::Receiver<ArticleReply>) {
        let (reply, receiver) = oneshot::channel();
        let mut state = self.pipeline();
        let seq = state.next_seq;
        state.next_seq += 1;
        let has_room = state.drivers * depth > state.queue.len() + state.inflight;
        let needs_driver = fresh_only || !has_room;
        if needs_driver {
            state.drivers += 1;
        }
        state.queue.push_back(QueuedArticle {
            seq,
            message_id: message_id.to_string(),
            fresh_only,
            reply,
        });
        (seq, needs_driver, receiver)
    }

    fn withdraw(&self, seq: u64) -> bool {
        let mut state = self.pipeline();
        match state.queue.iter().position(|item| item.seq == seq) {
            Some(index) => {
                state.queue.remove(index);
                true
            }
            None => false,
        }
    }

    fn release_driver(&self, inflight: usize) {
        let stranded = {
            let mut state = self.pipeline();
            state.drivers = state.drivers.saturating_sub(1);
            state.inflight = state.inflight.saturating_sub(inflight);
            if state.drivers == 0 {
                std::mem::take(&mut state.queue)
            } else {
                VecDeque::new()
            }
        };
        for item in stranded {
            let _ = item.reply.send(ArticleReply::Requeue);
        }
    }

    fn take_batch(
        &self,
        room: usize,
        proven: bool,
        window_empty: bool,
        leased: &mut Option<LeasedNntpConnection>,
    ) -> Option<Vec<QueuedArticle>> {
        let mut state = self.pipeline();
        let mut batch = Vec::new();
        let mut index = 0;
        while batch.len() < room && index < state.queue.len() {
            if proven && state.queue[index].fresh_only {
                index += 1;
                continue;
            }
            if let Some(item) = state.queue.remove(index) {
                batch.push(item);
            }
        }
        state.inflight += batch.len();
        if batch.is_empty() && window_empty {
            state.drivers = state.drivers.saturating_sub(1);
            if let Some(connection) = leased.take() {
                self.put_idle(connection);
            }
            return None;
        }
        Some(batch)
    }

    fn clear_idle(&self) {
        let drained =
            std::mem::take(&mut *self.idle.lock().unwrap_or_else(|error| error.into_inner()));
        drop(drained);
    }
}

struct LeasedNntpConnection {
    connection: NntpConnection,
    _capacity_lease: ProviderConnectionLease,
}

impl ProfileSessionCache {
    fn session_for(&self, profile_id: &str) -> Arc<ProfileSession> {
        self.sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(profile_id.to_string())
            .or_default()
            .clone()
    }

    fn discard_connections_except(&self, profile_id: &str) {
        let other_sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter(|(id, _)| id.as_str() != profile_id)
            .map(|(_, session)| session.clone())
            .collect::<Vec<_>>();
        for session in other_sessions {
            session.clear_idle();
        }
    }
}

struct InFlightGuard<'a>(&'a AtomicUsize);

impl<'a> InFlightGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
enum ConnectionAdmission {
    Immediate,
    Wait,
}

#[derive(Clone)]
struct ActiveTimeTracker {
    started_at: Instant,
    credential_wait: Arc<Mutex<Duration>>,
}

impl ActiveTimeTracker {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            credential_wait: Arc::new(Mutex::new(Duration::ZERO)),
        }
    }

    fn active_elapsed(&self) -> Duration {
        let credential_wait = *self
            .credential_wait
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.started_at.elapsed().saturating_sub(credential_wait)
    }

    fn record_credential_wait(&self, elapsed: Duration) {
        let mut credential_wait = self
            .credential_wait
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *credential_wait = credential_wait
            .checked_add(elapsed)
            .unwrap_or(Duration::MAX);
    }
}

impl ArticleSource for NntpArticleSource {
    fn fetch<'a>(
        &'a self,
        message_id: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<DecodedYencPart, ArticleFetchError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            ensure_usenet_active_time(&self.active_time, self.max_active_seconds)
                .map_err(ArticleFetchError::Failed)?;
            let _in_flight = InFlightGuard::new(&self.in_flight);
            let message_id = message_id.to_string();
            let preferred_profile_id = self
                .preferred_profile_id
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            let result = self
                .fetch_with_failover(&message_id, preferred_profile_id.as_deref())
                .await;
            if let Ok((profile_id, _)) = &result {
                *self
                    .preferred_profile_id
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = Some(profile_id.clone());
            }
            ensure_usenet_active_time(&self.active_time, self.max_active_seconds)
                .map_err(ArticleFetchError::Failed)?;
            result
                .map(|(_, article)| article)
                .map_err(article_fetch_error)
        })
    }

    fn concurrency(&self) -> usize {
        let profiles = self.pool.ordered_profiles_with_preference(None);
        let top = profiles.first().map(|profile| profile.priority);
        profiles
            .iter()
            .filter(|profile| Some(profile.priority) == top)
            .map(|profile| profile.max_connections as usize)
            .sum::<usize>()
            .saturating_mul(self.pipeline_depth.max(1))
            .max(1)
    }
}

impl NntpArticleSource {
    async fn fetch_with_failover(
        &self,
        message_id: &str,
        preferred_profile_id: Option<&str>,
    ) -> Result<(String, DecodedYencPart), NntpError> {
        let candidates = self
            .pool
            .ordered_profiles_with_preference(preferred_profile_id);
        if candidates.is_empty() {
            return Err(NntpError::InvalidProfile(
                "no enabled Usenet providers".into(),
            ));
        }

        let mut capacity_waiters = Vec::new();
        let mut last_error = None;
        for profile in candidates {
            match self
                .fetch_from_profile(profile.clone(), message_id, ConnectionAdmission::Immediate)
                .await
            {
                Ok(part) => {
                    self.pool.mark_success(&profile.id);
                    return Ok((profile.id, part));
                }
                Err(NntpError::CapacityUnavailable) => capacity_waiters.push(profile),
                Err(error) => {
                    let class = error.failover_class();
                    self.pool.mark_failure(&profile.id, &error);
                    if class == super::usenet_transport::FailoverClass::Permanent {
                        return Err(error);
                    }
                    last_error = Some(error);
                }
            }
        }

        for profile in capacity_waiters {
            match self
                .fetch_from_profile(profile.clone(), message_id, ConnectionAdmission::Wait)
                .await
            {
                Ok(part) => {
                    self.pool.mark_success(&profile.id);
                    return Ok((profile.id, part));
                }
                Err(error) => {
                    let class = error.failover_class();
                    self.pool.mark_failure(&profile.id, &error);
                    if class == super::usenet_transport::FailoverClass::Permanent {
                        return Err(error);
                    }
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap_or(NntpError::CapacityUnavailable))
    }

    async fn fetch_from_profile(
        &self,
        profile: UsenetProviderProfile,
        message_id: &str,
        admission: ConnectionAdmission,
    ) -> Result<DecodedYencPart, NntpError> {
        let article = self
            .fetch_article_from_profile(profile, message_id, admission)
            .await?;
        tokio::task::spawn_blocking(move || decode_yenc_part(&article))
            .await
            .map_err(|error| NntpError::Io(std::io::Error::other(error)))?
            .map_err(|message| NntpError::ArticleCorrupt { message })
    }

    async fn fetch_article_from_profile(
        &self,
        profile: UsenetProviderProfile,
        message_id: &str,
        admission: ConnectionAdmission,
    ) -> Result<Vec<u8>, NntpError> {
        let session = self.profile_sessions.session_for(&profile.id);
        let depth = self.pipeline_depth.max(1);
        let mut retried_stale_connection = false;
        let mut fresh_only = false;

        loop {
            if self.cancel.is_cancelled() {
                return Err(NntpError::Cancelled);
            }
            let (seq, needs_driver, mut receiver) = session.enqueue(message_id, fresh_only, depth);
            let reply = if needs_driver {
                let checkout = self.checkout_connection(&session, &profile, admission, fresh_only);
                let checked_out = match admission {
                    ConnectionAdmission::Immediate => Ok(checkout.await),
                    ConnectionAdmission::Wait => tokio::select! {
                        biased;
                        reply = &mut receiver => Err(reply),
                        connection = checkout => Ok(connection),
                    },
                };
                match checked_out {
                    Ok(Ok((leased, reused))) => {
                        spawn_driver(session.clone(), leased, reused, self.cancel.clone(), depth);
                        receiver.await
                    }
                    Ok(Err(error)) => {
                        let withdrawn = session.withdraw(seq);
                        session.release_driver(0);
                        if withdrawn || matches!(error, NntpError::Cancelled) {
                            return Err(error);
                        }
                        receiver.await
                    }
                    Err(reply) => {
                        session.release_driver(0);
                        reply
                    }
                }
            } else {
                receiver.await
            };
            match reply {
                Ok(ArticleReply::Article(article)) => return Ok(article),
                Ok(ArticleReply::Failed(error)) => return Err(error),
                Ok(ArticleReply::Retry(error)) => {
                    if retried_stale_connection {
                        return Err(error);
                    }
                    retried_stale_connection = true;
                    fresh_only = true;
                }
                Ok(ArticleReply::Requeue) | Err(_) => {}
            }
        }
    }

    async fn checkout_connection(
        &self,
        session: &ProfileSession,
        profile: &UsenetProviderProfile,
        admission: ConnectionAdmission,
        fresh_only: bool,
    ) -> Result<(LeasedNntpConnection, bool), NntpError> {
        if !fresh_only {
            if let Some(connection) = session.take_idle() {
                return Ok((connection, true));
            }
        }
        let credentials = self.cached_credentials(session, &profile.id).await?;
        let capacity_lease = match admission {
            ConnectionAdmission::Immediate => {
                let lease = self.acquire_connection_capacity(profile, admission).await?;
                if self.in_flight.load(Ordering::Relaxed) <= 1 {
                    self.profile_sessions
                        .discard_connections_except(&profile.id);
                }
                lease
            }
            ConnectionAdmission::Wait => {
                self.profile_sessions
                    .discard_connections_except(&profile.id);
                let remaining = self.remaining_active_time()?;
                let acquire =
                    tokio::time::timeout(remaining, self.connection_capacity.acquire(profile));
                tokio::pin!(acquire);
                loop {
                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return Err(NntpError::Cancelled),
                        _ = session.returned.notified(), if !fresh_only => {
                            if let Some(connection) = session.take_idle() {
                                return Ok((connection, true));
                            }
                        }
                        lease = &mut acquire => break match lease {
                            Ok(result) => result?,
                            Err(_) => return Err(active_time_limit_error()),
                        },
                    }
                }
            }
        };
        let connection = self.connect_with_cancel(profile, credentials).await?;
        Ok((
            LeasedNntpConnection {
                connection,
                _capacity_lease: capacity_lease,
            },
            false,
        ))
    }

    async fn cached_credentials(
        &self,
        session: &ProfileSession,
        profile_id: &str,
    ) -> Result<Option<UsenetCredentials>, NntpError> {
        let mut cached = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(NntpError::Cancelled),
            cached = session.credentials.lock() => cached,
        };
        if let Some(credentials) = &*cached {
            return Ok(credentials.clone());
        }

        let credential_wait_started_at = Instant::now();
        let credentials = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                self.active_time
                    .record_credential_wait(credential_wait_started_at.elapsed());
                return Err(NntpError::Cancelled);
            }
            credentials = self.resolver.resolve(profile_id) => credentials,
        };
        self.active_time
            .record_credential_wait(credential_wait_started_at.elapsed());
        let credentials = credentials.map_err(|error| NntpError::Protocol {
            code: 0,
            message: error,
        })?;
        *cached = Some(credentials.clone());
        Ok(credentials)
    }

    async fn acquire_connection_capacity(
        &self,
        profile: &UsenetProviderProfile,
        admission: ConnectionAdmission,
    ) -> Result<ProviderConnectionLease, NntpError> {
        if self.cancel.is_cancelled() {
            return Err(NntpError::Cancelled);
        }
        match admission {
            ConnectionAdmission::Immediate => self
                .connection_capacity
                .try_acquire(profile)?
                .ok_or(NntpError::CapacityUnavailable),
            ConnectionAdmission::Wait => {
                let remaining = self.remaining_active_time()?;
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => Err(NntpError::Cancelled),
                    lease = tokio::time::timeout(
                        remaining,
                        self.connection_capacity.acquire(profile),
                    ) => match lease {
                        Ok(result) => result,
                        Err(_) => Err(active_time_limit_error()),
                    },
                }
            }
        }
    }

    fn remaining_active_time(&self) -> Result<Duration, NntpError> {
        let limit = Duration::from_secs(self.max_active_seconds);
        limit
            .checked_sub(self.active_time.active_elapsed())
            .ok_or_else(active_time_limit_error)
    }

    async fn connect_with_cancel(
        &self,
        profile: &UsenetProviderProfile,
        credentials: Option<UsenetCredentials>,
    ) -> Result<NntpConnection, NntpError> {
        let mut connection = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(NntpError::Cancelled),
            connection = NntpConnection::connect_with_proxy(
                profile,
                credentials,
                self.proxy.as_ref(),
            ) => connection?,
        };
        connection.set_throttle(self.throttle.clone());
        Ok(connection)
    }
}

fn spawn_driver(
    session: Arc<ProfileSession>,
    leased: LeasedNntpConnection,
    proven: bool,
    cancel: CancellationToken,
    depth: usize,
) {
    tokio::spawn(run_driver(session, leased, proven, cancel, depth));
}

async fn run_driver(
    session: Arc<ProfileSession>,
    leased: LeasedNntpConnection,
    mut proven: bool,
    cancel: CancellationToken,
    depth: usize,
) {
    tokio::task::yield_now().await;
    let mut leased = Some(leased);
    let mut window: VecDeque<QueuedArticle> = VecDeque::new();
    loop {
        let Some(batch) =
            session.take_batch(depth - window.len(), proven, window.is_empty(), &mut leased)
        else {
            return;
        };
        let mut fresh = Vec::with_capacity(batch.len());
        for item in batch {
            match canonical_message_id(&item.message_id) {
                Ok(_) => fresh.push(item),
                Err(error) => {
                    session.pipeline().inflight -= 1;
                    let _ = item.reply.send(ArticleReply::Failed(error));
                }
            }
        }
        let Some(connection) = leased.as_mut() else {
            return;
        };
        if !fresh.is_empty() {
            let ids = fresh
                .iter()
                .map(|item| item.message_id.clone())
                .collect::<Vec<_>>();
            window.extend(fresh);
            if let Err(error) = connection.connection.send_article_requests(&ids).await {
                break_connection(&session, &mut leased, window, error, proven);
                return;
            }
        }
        if window.is_empty() {
            continue;
        }
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(NntpError::Cancelled),
            article = connection.connection.read_article() => article,
        };
        match result {
            Ok(article) => {
                proven = true;
                finish_front(&session, &mut window, ArticleReply::Article(article));
            }
            Err(error @ NntpError::ArticleUnavailable { .. }) => {
                proven = true;
                finish_front(&session, &mut window, ArticleReply::Failed(error));
            }
            Err(error) => {
                break_connection(&session, &mut leased, window, error, proven);
                return;
            }
        }
    }
}

fn finish_front(
    session: &ProfileSession,
    window: &mut VecDeque<QueuedArticle>,
    reply: ArticleReply,
) {
    if let Some(item) = window.pop_front() {
        session.pipeline().inflight -= 1;
        let _ = item.reply.send(reply);
    }
}

fn break_connection(
    session: &ProfileSession,
    leased: &mut Option<LeasedNntpConnection>,
    window: VecDeque<QueuedArticle>,
    error: NntpError,
    proven: bool,
) {
    drop(leased.take());
    session.release_driver(window.len());
    let cancelled = matches!(error, NntpError::Cancelled);
    let mut error = Some(error);
    for item in window {
        let reply = match error.take() {
            Some(error) if cancelled => ArticleReply::Failed(error),
            Some(error) if proven && should_retry_stale_connection(&error) => {
                ArticleReply::Retry(error)
            }
            Some(error) => ArticleReply::Failed(error),
            None if cancelled => ArticleReply::Failed(NntpError::Cancelled),
            None => ArticleReply::Requeue,
        };
        let _ = item.reply.send(reply);
    }
}

fn should_retry_stale_connection(error: &NntpError) -> bool {
    matches!(
        error,
        NntpError::Io(_)
            | NntpError::Timeout
            | NntpError::ServiceUnavailable { code: 400, .. }
            | NntpError::AuthenticationFailed { code: 480, .. }
    )
}

fn ensure_usenet_active_time(
    active_time: &ActiveTimeTracker,
    max_active_seconds: u64,
) -> Result<(), String> {
    if active_time.active_elapsed() >= Duration::from_secs(max_active_seconds) {
        Err("archive limit: Usenet task exceeded the active-time limit".into())
    } else {
        Ok(())
    }
}

fn active_time_limit_error() -> NntpError {
    NntpError::Protocol {
        code: 0,
        message: "archive limit: Usenet task exceeded the active-time limit".into(),
    }
}

fn article_fetch_error(error: NntpError) -> ArticleFetchError {
    match error {
        NntpError::ArticleUnavailable { .. } | NntpError::ArticleCorrupt { .. } => {
            ArticleFetchError::Unavailable(error.to_string())
        }
        _ => ArticleFetchError::Failed(error.to_string()),
    }
}

struct AssembledTaskFile {
    index: usize,
    name: String,
    is_parity: bool,
    report: AssemblyReport,
    _reservation: OutputReservation,
}

struct OutputReservation {
    output: PathBuf,
    _lock_path: PathBuf,
    lock_file: Option<fs::File>,
}

impl Drop for OutputReservation {
    fn drop(&mut self) {
        drop(self.lock_file.take());
    }
}

type StageCallback = Arc<dyn Fn(&str) + Send + Sync>;

fn report_stage(stage: &Option<StageCallback>, value: &str) {
    if let Some(callback) = stage {
        callback(value);
    }
}

pub fn profiles_from_options(
    options: &Map<String, Value>,
) -> Result<Vec<UsenetProviderProfile>, String> {
    let value = options
        .get("usenet-profiles")
        .or_else(|| options.get("usenetProfiles"))
        .ok_or_else(|| "No Usenet provider profiles configured".to_string())?;
    serde_json::from_value(value.clone())
        .map_err(|error| format!("Invalid Usenet provider profiles: {error}"))
}

fn http_proxy_from_options(
    options: &Map<String, Value>,
) -> Result<Option<risuko_http::ProxyConnector>, String> {
    let server = options
        .get("all-proxy")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if server.is_empty() {
        return Ok(None);
    }
    let proxy = risuko_http::Proxy::all(server)
        .map_err(|error| format!("Invalid HTTP profile proxy: {error}"))?;
    let bypass = options
        .get("no-proxy")
        .and_then(Value::as_str)
        .unwrap_or("");
    Ok(Some(
        risuko_http::ProxyConnector::from_proxy(proxy)
            .with_no_proxy(risuko_http::NoProxy::parse(bypass)),
    ))
}

pub(crate) async fn run_usenet_download_with_resolver_and_capacity(
    task: &DownloadTask,
    options: &Map<String, Value>,
    completed: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
    cancel: CancellationToken,
    resolver: Arc<dyn UsenetCredentialResolver>,
    connection_capacity: Arc<ProviderConnectionCapacityRegistry>,
    throttle: Throttle,
    stage: Option<StageCallback>,
) -> Result<(PathBuf, Vec<(usize, PathBuf)>), UsenetDownloadError> {
    let metadata: &UsenetTaskData = task
        .usenet
        .as_ref()
        .ok_or_else(|| "Usenet task has no manifest metadata".to_string())?;
    let mut profiles = profiles_from_options(options)?;
    if let Some(selected) = metadata.options.profile_id.as_deref() {
        profiles.retain(|profile| profile.id == selected);
        if profiles.is_empty() {
            return Err(format!("Usenet provider profile {selected:?} is unavailable").into());
        }
    }
    let pool = Arc::new(ProviderPool::new(profiles).map_err(|error| error.to_string())?);
    let archive_limits = archive_limits_for_task(metadata, options)?;
    let proxy = http_proxy_from_options(options)?;
    let make_source = || NntpArticleSource {
        pool: pool.clone(),
        resolver: resolver.clone(),
        connection_capacity: connection_capacity.clone(),
        profile_sessions: ProfileSessionCache::default(),
        in_flight: AtomicUsize::new(0),
        preferred_profile_id: Mutex::new(None),
        active_time: ActiveTimeTracker::new(),
        max_active_seconds: u64::MAX,
        cancel: cancel.clone(),
        proxy: proxy.clone(),
        throttle: throttle.clone(),
        pipeline_depth: PIPELINE_DEPTH,
    };
    let assembly_limits = YencAssemblyLimits::new(
        archive_limits.max_entry_bytes,
        archive_limits.max_expanded_bytes,
    )?;
    let mut assembly_budget = YencAssemblyBudget::default();
    let destination = PathBuf::from(&task.dir);
    tokio::fs::create_dir_all(&destination)
        .await
        .map_err(|error| format!("create Usenet destination: {error}"))?;
    task_article_bytes(metadata)?;

    let names: Vec<String> = metadata
        .files
        .iter()
        .map(|file| crate::engine::util::safe_filename(&file.name, "download"))
        .collect();
    let roles: Vec<FileRole> = names.iter().map(|name| file_role(name)).collect();
    let mut fetch_now = Vec::new();
    let mut deferred = Vec::new();
    let policy_deferred = deferred_par2_volumes(&names);
    for (index, name) in names.iter().enumerate() {
        if policy_deferred.contains(&index) && !volume_has_local_state(&destination, name).await {
            deferred.push(index);
        } else {
            fetch_now.push(index);
        }
    }
    let file_bytes = |index: usize| -> u64 {
        metadata.files[index]
            .segments
            .iter()
            .fold(0u64, |sum, segment| sum.saturating_add(segment.bytes))
    };
    total.store(
        fetch_now
            .iter()
            .fold(0u64, |sum, index| sum.saturating_add(file_bytes(*index))),
        Ordering::Relaxed,
    );

    let mut assembled: Vec<AssembledTaskFile> = Vec::new();
    let mut promoted_outputs = Vec::new();
    report_stage(&stage, "fetching");
    let source = make_source();
    for index in fetch_now {
        assembled.push(
            fetch_task_file(
                index,
                &metadata.files[index],
                &destination,
                &source,
                &cancel,
                completed.as_ref(),
                assembly_limits,
                &mut assembly_budget,
                &stage,
            )
            .await?,
        );
    }
    drop(source);
    let active_time = ActiveTimeTracker::new();

    loop {
        let needs_repair = assembled
            .iter()
            .any(|file| !file.is_parity && !file.report.complete);
        let parity_files: Vec<PathBuf> = assembled
            .iter()
            .filter(|file| file.is_parity && file.report.complete)
            .map(|file| file.report.output.clone())
            .collect();
        let data_files: Vec<Par2InputFile> = assembled
            .iter()
            .filter(|file| !file.is_parity)
            .map(|file| Par2InputFile {
                manifest_name: file.name.clone(),
                source_path: file.report.source_path().to_path_buf(),
                output_path: file.report.output.clone(),
                expected_size: file.report.expected_size,
            })
            .collect();
        let unavailable = assembled
            .iter()
            .filter(|file| !file.report.unavailable_segments.is_empty())
            .map(|file| format!("{}:{:?}", file.name, file.report.unavailable_segments))
            .collect::<Vec<_>>();
        if parity_files.is_empty() {
            if needs_repair {
                let picks = pick_recovery_volumes(&deferred, &roles, 1);
                if !picks.is_empty() {
                    fetch_recovery_volumes(
                        &picks,
                        &mut deferred,
                        &mut assembled,
                        &make_source,
                        metadata,
                        &destination,
                        &cancel,
                        &completed,
                        &total,
                        assembly_limits,
                        &mut assembly_budget,
                        &stage,
                    )
                    .await?;
                    continue;
                }
                return Err(format!(
                    "PAR2 repair failed: no complete parity files are available; unavailable segments: {}",
                    unavailable.join(", ")
                )
                .into());
            }
            break;
        }
        if data_files.is_empty() {
            break;
        }
        report_stage(
            &stage,
            if needs_repair {
                "repairing"
            } else {
                "verifying"
            },
        );
        let request = Par2RepairRequest {
            destination: destination.clone(),
            data_files,
            parity_files,
            required_incomplete_names: assembled
                .iter()
                .filter(|file| !file.is_parity && !file.report.complete)
                .map(|file| file.name.clone())
                .collect::<BTreeSet<_>>(),
            limits: archive_limits,
            active_started_at: Some(Instant::now()),
            active_elapsed_before_repair: Some(active_time.active_elapsed()),
        };
        let repair_cancel = cancel.child_token();
        let repair_cancel_for_worker = repair_cancel.clone();
        let mut repair_task = tokio::task::spawn_blocking(move || {
            verify_or_repair_with_cancel(&request, Some(&repair_cancel_for_worker))
        });
        let remaining = Duration::from_secs(archive_limits.max_active_seconds)
            .checked_sub(active_time.active_elapsed())
            .ok_or_else(|| {
                "archive limit: Usenet task exceeded the active-time limit".to_string()
            })?;
        let outcome = tokio::select! {
            _ = cancel.cancelled() => {
                repair_cancel.cancel();
                let _ = (&mut repair_task).await;
                return Err(String::from("Download cancelled").into());
            }
            result = tokio::time::timeout(remaining, &mut repair_task) => {
                match result {
                    Ok(joined) => joined.map_err(|error| format!("PAR2 repair worker failed: {error}"))?,
                    Err(_) => {
                        repair_cancel.cancel();
                        let _ = (&mut repair_task).await;
                        return Err(String::from(
                            "archive limit: Usenet task exceeded the active-time limit",
                        )
                        .into());
                    }
                }
            }
        };
        let repair = match outcome {
            Ok(repair) => repair,
            Err(Par2Error::InsufficientRecovery { needed, available }) if !deferred.is_empty() => {
                let picks =
                    pick_recovery_volumes(&deferred, &roles, needed.saturating_sub(available));
                if picks.is_empty() {
                    return Err(UsenetDownloadError::from_par2_error(
                        Par2Error::InsufficientRecovery { needed, available },
                        &unavailable,
                    ));
                }
                fetch_recovery_volumes(
                    &picks,
                    &mut deferred,
                    &mut assembled,
                    &make_source,
                    metadata,
                    &destination,
                    &cancel,
                    &completed,
                    &total,
                    assembly_limits,
                    &mut assembly_budget,
                    &stage,
                )
                .await?;
                continue;
            }
            Err(error) => return Err(UsenetDownloadError::from_par2_error(error, &unavailable)),
        };
        persist_par2_repair_state(&assembled, &repair.promoted_outputs).await?;
        promoted_outputs = repair.promoted_outputs.clone();
        let unrepaired = assembled
            .iter()
            .filter(|file| {
                !file.is_parity
                    && !file.report.complete
                    && !repair.promoted_outputs.contains(&file.report.output)
            })
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>();
        if !unrepaired.is_empty() {
            return Err(format!(
                "PAR2 repair failed: no recovery set covers incomplete files {}; unavailable segments: {}",
                unrepaired.join(", "),
                unavailable.join(", ")
            )
            .into());
        }
        break;
    }
    ensure_usenet_active_time(&active_time, archive_limits.max_active_seconds)?;
    completed.store(total.load(Ordering::Relaxed), Ordering::Relaxed);
    let cleanup_mode = cleanup_mode_for_task(metadata, options);
    let (par2_inputs, archive_inputs) = cleanup_inputs(&assembled, false, &promoted_outputs);
    if let Err(error) = cleanup_after_success(cleanup_mode, true, &par2_inputs, &archive_inputs) {
        tracing::warn!(
            mode = ?cleanup_mode,
            %error,
            "Usenet cleanup after verified success was incomplete"
        );
    }
    report_stage(&stage, "complete");
    Ok((
        assembled
            .first()
            .map(|file| file.report.output.clone())
            .unwrap_or_else(|| destination.clone()),
        assembled
            .iter()
            .map(|file| (file.index, file.report.output.clone()))
            .collect(),
    ))
}

fn volume_blocks(role: FileRole) -> Option<u32> {
    match role {
        FileRole::Par2Volume { blocks } => blocks,
        _ => None,
    }
}

async fn volume_has_local_state(destination: &Path, name: &str) -> bool {
    let output = destination.join(name);
    for path in [resume_sidecar_path(&output), partial_path(&output), output] {
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return true;
        }
    }
    false
}

fn pick_recovery_volumes(deferred: &[usize], roles: &[FileRole], blocks_needed: u32) -> Vec<usize> {
    let mut counted: Vec<(usize, u32)> = Vec::new();
    let mut uncounted: Vec<usize> = Vec::new();
    for &index in deferred {
        match volume_blocks(roles[index]) {
            Some(blocks) => counted.push((index, blocks)),
            None => uncounted.push(index),
        }
    }
    let mut picks = Vec::new();
    let mut remaining = blocks_needed.max(1);
    while remaining > 0 && !counted.is_empty() {
        let covering = counted
            .iter()
            .enumerate()
            .filter(|(_, (_, blocks))| *blocks >= remaining)
            .min_by_key(|(_, (index, blocks))| (*blocks, *index))
            .map(|(position, _)| position);
        let position = covering.unwrap_or_else(|| {
            counted
                .iter()
                .enumerate()
                .max_by_key(|(_, (index, blocks))| (*blocks, std::cmp::Reverse(*index)))
                .map(|(position, _)| position)
                .unwrap_or(0)
        });
        let (index, blocks) = counted.remove(position);
        picks.push(index);
        remaining = remaining.saturating_sub(blocks);
    }
    if remaining > 0 {
        picks.extend(uncounted);
    }
    picks
}

#[allow(clippy::too_many_arguments)]
async fn fetch_recovery_volumes(
    picks: &[usize],
    deferred: &mut Vec<usize>,
    assembled: &mut Vec<AssembledTaskFile>,
    make_source: &impl Fn() -> NntpArticleSource,
    metadata: &UsenetTaskData,
    destination: &Path,
    cancel: &CancellationToken,
    completed: &AtomicU64,
    total: &AtomicU64,
    limits: YencAssemblyLimits,
    budget: &mut YencAssemblyBudget,
    stage: &Option<StageCallback>,
) -> Result<(), UsenetDownloadError> {
    let source = make_source();
    for &index in picks {
        let file = &metadata.files[index];
        let bytes = file
            .segments
            .iter()
            .fold(0u64, |sum, segment| sum.saturating_add(segment.bytes));
        total.fetch_add(bytes, Ordering::Relaxed);
        deferred.retain(|pending| *pending != index);
        assembled.push(
            fetch_task_file(
                index,
                file,
                destination,
                &source,
                cancel,
                completed,
                limits,
                budget,
                stage,
            )
            .await?,
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn fetch_task_file(
    index: usize,
    file: &UsenetTaskFile,
    destination: &Path,
    source: &NntpArticleSource,
    cancel: &CancellationToken,
    completed: &AtomicU64,
    limits: YencAssemblyLimits,
    budget: &mut YencAssemblyBudget,
    stage: &Option<StageCallback>,
) -> Result<AssembledTaskFile, UsenetDownloadError> {
    if cancel.is_cancelled() {
        return Err("Download cancelled".to_string().into());
    }
    let name = crate::engine::util::safe_filename(&file.name, "download");
    let segments: Vec<NzbSegment> = file
        .segments
        .iter()
        .map(|segment| NzbSegment {
            number: segment.number,
            bytes: segment.bytes,
            message_id: segment.message_id.clone(),
        })
        .collect();
    let reservation = reserve_output_path(destination, &name, &segments).await?;
    let output = reservation.output.clone();
    let local_before = completed.load(Ordering::Relaxed);
    report_stage(stage, "assembling");
    let report = assemble_file_with_report_with_limits_at_offset(
        &output,
        &segments,
        source,
        cancel,
        Some(completed),
        local_before,
        limits,
        budget,
    )
    .await?;
    Ok(AssembledTaskFile {
        index,
        is_parity: is_par2_name(&name),
        name,
        report,
        _reservation: reservation,
    })
}

fn cleanup_mode_for_task(metadata: &UsenetTaskData, options: &Map<String, Value>) -> CleanupMode {
    let configured = metadata
        .options
        .cleanup_mode
        .as_deref()
        .or_else(|| options.get("usenet-cleanup-mode").and_then(Value::as_str));
    let mode = CleanupMode::from_setting(configured);
    if mode == CleanupMode::DeletePar2AndVolumes {
        tracing::warn!(
            "Usenet archive-volume cleanup is unsupported because this worker does not verify extraction"
        );
        CleanupMode::DeletePar2
    } else {
        mode
    }
}

fn cleanup_inputs(
    assembled: &[AssembledTaskFile],
    archive_extraction_verified: bool,
    promoted_outputs: &[PathBuf],
) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut par2_inputs = Vec::new();
    let mut archive_inputs = Vec::new();
    for file in assembled {
        let verified_complete = file.report.complete
            || promoted_outputs
                .iter()
                .any(|output| output == &file.report.output);
        if !verified_complete {
            continue;
        }
        let target = if file.is_parity {
            &mut par2_inputs
        } else if archive_extraction_verified && is_archive_volume_name(&file.name) {
            &mut archive_inputs
        } else {
            continue;
        };
        for path in [&file.report.output, &file.report.part_path] {
            if !target.iter().any(|existing| existing == path) {
                target.push(path.clone());
            }
        }
    }
    (par2_inputs, archive_inputs)
}

fn is_par2_name(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("par2"))
}

fn task_article_bytes(metadata: &UsenetTaskData) -> Result<u64, String> {
    metadata
        .files
        .iter()
        .flat_map(|file| file.segments.iter())
        .try_fold(0u64, |total, segment| {
            total
                .checked_add(segment.bytes)
                .ok_or_else(|| "NZB article byte count overflowed".to_string())
        })
}

fn archive_limits_for_task(
    metadata: &UsenetTaskData,
    options: &Map<String, Value>,
) -> Result<crate::engine::archive_safety::ArchiveLimits, String> {
    let defaults = platform_limits();
    let value = metadata
        .options
        .archive_limits
        .as_ref()
        .or_else(|| options.get("usenet-archive-limits"));
    let Some(value) = value else {
        return Ok(defaults);
    };
    crate::engine::archive_safety::validate_limits_override_value(
        defaults,
        value,
        metadata.options.archive_limit_override_confirmed
            || options
                .get("usenet-archive-limit-override-confirmed")
                .and_then(Value::as_bool)
                .unwrap_or(false),
    )
    .map_err(|error| format!("Invalid Usenet archive limits: {error:?}"))
}

async fn reserve_output_path(
    destination: &Path,
    name: &str,
    segments: &[NzbSegment],
) -> Result<OutputReservation, String> {
    let path = Path::new(name);
    let stem = path
        .file_stem()
        .and_then(|v| v.to_str())
        .unwrap_or("download");
    let extension = path.extension().and_then(|v| v.to_str());
    for index in 0..10_000u32 {
        let candidate = if index == 0 {
            destination.join(name)
        } else {
            let renamed = match extension {
                Some(extension) => format!("{stem} ({index}).{extension}"),
                None => format!("{stem} ({index})"),
            };
            destination.join(renamed)
        };
        let is_resume = resume_sidecar_matches(&candidate, segments).await?;
        if is_resume || output_slot_is_available(&candidate).await? {
            if let Some(reservation) = try_reserve_output(&candidate).await? {
                if resume_sidecar_matches(&candidate, segments).await?
                    || output_slot_is_available(&candidate).await?
                {
                    return Ok(reservation);
                }
            }
        }
    }
    for _ in 0..100 {
        let candidate = destination.join(format!("{name}.{}", uuid::Uuid::new_v4().simple()));
        if output_slot_is_available(&candidate).await? {
            if let Some(reservation) = try_reserve_output(&candidate).await? {
                if output_slot_is_available(&candidate).await? {
                    return Ok(reservation);
                }
            }
        }
    }
    Err(format!(
        "could not allocate a Usenet output path for {name:?}"
    ))
}

async fn try_reserve_output(output: &Path) -> Result<Option<OutputReservation>, String> {
    let output = output.to_path_buf();
    let lock_path = output_lock_path(&output)?;
    tokio::task::spawn_blocking(move || {
        let lock_directory = lock_path.parent().ok_or_else(|| {
            format!(
                "Usenet output lock path has no parent: {}",
                lock_path.display()
            )
        })?;
        fs::create_dir_all(lock_directory).map_err(|error| {
            format!(
                "create Usenet output lock directory {}: {error}",
                lock_directory.display()
            )
        })?;
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| format!("open Usenet output lock {}: {error}", lock_path.display()))?;
        match FileExt::try_lock(&lock_file) {
            Ok(()) => Ok(Some(OutputReservation {
                output,
                _lock_path: lock_path,
                lock_file: Some(lock_file),
            })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(error)) => Err(format!(
                "lock Usenet output {}: {error}",
                lock_path.display()
            )),
        }
    })
    .await
    .map_err(|error| format!("reserve Usenet output: {error}"))?
}

fn output_lock_path(output: &Path) -> Result<PathBuf, String> {
    let parent = output
        .parent()
        .ok_or_else(|| format!("Usenet output has no parent: {}", output.display()))?;
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("Usenet output has no valid filename: {}", output.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(name.to_lowercase().as_bytes());
    Ok(parent
        .join(".risuko-usenet-locks")
        .join(format!("{}.lock", hex::encode(hasher.finalize()))))
}

async fn output_slot_is_available(output: &Path) -> Result<bool, String> {
    for path in [output.to_path_buf(), partial_path(output)] {
        match tokio::fs::symlink_metadata(&path).await {
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("inspect Usenet output {}: {error}", path.display())),
        }
    }

    let sidecar_path = resume_sidecar_path(output);
    match tokio::fs::symlink_metadata(&sidecar_path).await {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Ok(false);
            }
            match tokio::fs::read(&sidecar_path).await {
                Ok(bytes) if serde_json::from_slice::<ResumeSidecar>(&bytes).is_err() => Ok(true),
                Ok(_) => Ok(false),
                Err(error) => Err(format!(
                    "read Usenet resume metadata {}: {error}",
                    sidecar_path.display()
                )),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(format!(
            "inspect Usenet resume metadata {}: {error}",
            sidecar_path.display()
        )),
    }
}

async fn persist_par2_repair_state(
    assembled: &[AssembledTaskFile],
    promoted_outputs: &[PathBuf],
) -> Result<(), String> {
    for (index, output) in promoted_outputs.iter().enumerate() {
        let Some(file) = assembled.iter().find(|file| &file.report.output == output) else {
            continue;
        };
        if let Err(error) = mark_par2_repaired(&file.report).await {
            for output in &promoted_outputs[index..] {
                if let Some(unmarked) = assembled.iter().find(|file| &file.report.output == output)
                {
                    if !unmarked.report.complete {
                        let _ = tokio::fs::remove_file(&unmarked.report.output).await;
                    }
                }
            }
            return Err(format!("persist PAR2 repair resume metadata: {error}"));
        }
    }

    for output in promoted_outputs {
        let Some(file) = assembled.iter().find(|file| &file.report.output == output) else {
            continue;
        };
        if !file.report.complete && file.report.part_path != file.report.output {
            if let Err(error) = tokio::fs::remove_file(&file.report.part_path).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        path = %file.report.part_path.display(),
                        %error,
                        "repaired Usenet part retained because cleanup failed"
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn idle_disconnect_errors_trigger_a_fresh_connection() {
        assert!(should_retry_stale_connection(
            &NntpError::ServiceUnavailable {
                code: 400,
                message: "timeout".into(),
            }
        ));
        assert!(should_retry_stale_connection(
            &NntpError::AuthenticationFailed {
                code: 480,
                message: "auth".into(),
            }
        ));
        assert!(!should_retry_stale_connection(
            &NntpError::AuthenticationFailed {
                code: 481,
                message: "bad".into(),
            }
        ));
    }

    use super::*;
    use crate::engine::task::{UsenetTaskFile, UsenetTaskSegment};
    use crate::engine::usenet_pipeline::{manifest_sha256, ResumeSegment, ResumeSidecar};
    use sha2::{Digest, Sha256};
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

    const TEST_YENC_ARTICLE: &[u8] = b"=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n";

    struct RecordingResolver {
        credentials: HashMap<String, UsenetCredentials>,
        calls: Mutex<Vec<String>>,
    }

    impl RecordingResolver {
        fn calls_for(&self, profile_id: &str) -> usize {
            self.calls
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .filter(|id| id.as_str() == profile_id)
                .count()
        }
    }

    async fn wait_for_resolver_calls(
        resolver: &RecordingResolver,
        profile_id: &str,
        expected: usize,
    ) {
        timeout(Duration::from_secs(3), async {
            loop {
                if resolver.calls_for(profile_id) >= expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fake resolver did not receive the expected call");
    }

    #[async_trait::async_trait]
    impl UsenetCredentialResolver for RecordingResolver {
        async fn resolve(&self, profile_id: &str) -> Result<Option<UsenetCredentials>, String> {
            self.calls
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(profile_id.to_string());
            Ok(self.credentials.get(profile_id).cloned())
        }
    }

    fn profile(id: &str, port: u16, priority: i32) -> UsenetProviderProfile {
        UsenetProviderProfile {
            id: id.into(),
            name: id.into(),
            host: "127.0.0.1".into(),
            port,
            security_mode: "plain".into(),
            enabled: true,
            priority,
            max_connections: 1,
            allow_plain: true,
            deleted_at: None,
            updated_at: None,
        }
    }

    fn source_for_test(
        profiles: Vec<UsenetProviderProfile>,
        resolver: Arc<dyn UsenetCredentialResolver>,
    ) -> NntpArticleSource {
        source_for_test_with_capacity(
            profiles,
            resolver,
            Arc::new(ProviderConnectionCapacityRegistry::default()),
        )
    }

    fn source_for_test_with_capacity(
        profiles: Vec<UsenetProviderProfile>,
        resolver: Arc<dyn UsenetCredentialResolver>,
        connection_capacity: Arc<ProviderConnectionCapacityRegistry>,
    ) -> NntpArticleSource {
        NntpArticleSource {
            pool: Arc::new(ProviderPool::new(profiles).unwrap()),
            resolver,
            connection_capacity,
            profile_sessions: ProfileSessionCache::default(),
            in_flight: AtomicUsize::new(0),
            preferred_profile_id: Mutex::new(None),
            active_time: ActiveTimeTracker::new(),
            max_active_seconds: 60,
            cancel: CancellationToken::new(),
            proxy: None,
            throttle: Throttle::unlimited(),
            pipeline_depth: PIPELINE_DEPTH,
        }
    }

    async fn read_command(reader: &mut BufReader<TcpStream>) -> String {
        let mut line = String::new();
        let read = reader.read_line(&mut line).await.unwrap();
        assert_ne!(read, 0, "client closed the fake NNTP connection early");
        line.trim_end_matches(['\r', '\n']).to_string()
    }

    async fn greet_and_authenticate(
        reader: &mut BufReader<TcpStream>,
        username: &str,
        password: &str,
    ) {
        reader
            .get_mut()
            .write_all(b"200 fake NNTP server ready\r\n")
            .await
            .unwrap();
        assert_eq!(
            read_command(reader).await,
            format!("AUTHINFO USER {username}")
        );
        reader
            .get_mut()
            .write_all(b"381 password required\r\n")
            .await
            .unwrap();
        assert_eq!(
            read_command(reader).await,
            format!("AUTHINFO PASS {password}")
        );
        reader
            .get_mut()
            .write_all(b"281 authenticated\r\n")
            .await
            .unwrap();
    }

    async fn write_test_article(reader: &mut BufReader<TcpStream>) {
        reader
            .get_mut()
            .write_all(b"220 article follows\r\n")
            .await
            .unwrap();
        reader.get_mut().write_all(TEST_YENC_ARTICLE).await.unwrap();
        reader.get_mut().write_all(b".\r\n").await.unwrap();
    }

    fn credentials(username: &str, password: &str) -> UsenetCredentials {
        UsenetCredentials {
            username: Some(username.into()),
            password: Some(password.into()),
        }
    }

    #[tokio::test]
    async fn reuses_authenticated_connection_and_credentials() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_by_server = accepted.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accepted_by_server.fetch_add(1, Ordering::Relaxed);
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            for message_id in ["one@example", "two@example"] {
                assert_eq!(
                    read_command(&mut reader).await,
                    format!("ARTICLE <{message_id}>")
                );
                write_test_article(&mut reader).await;
            }
        });
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let source = source_for_test(vec![profile("primary", port, 0)], resolver.clone());

        timeout(Duration::from_secs(3), async {
            assert_eq!(source.fetch("one@example").await.unwrap().data, b"A");
            assert_eq!(source.fetch("two@example").await.unwrap().data, b"A");
            server.await.unwrap();
        })
        .await
        .expect("fake NNTP reuse test timed out");

        assert_eq!(accepted.load(Ordering::Relaxed), 1);
        assert_eq!(resolver.calls_for("primary"), 1);
    }

    #[tokio::test]
    async fn equal_priority_profiles_keep_the_warm_stream() {
        let primary = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let primary_port = primary.local_addr().unwrap().port();
        let secondary = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let secondary_port = secondary.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_by_server = accepted.clone();
        let primary_server = tokio::spawn(async move {
            let (stream, _) = primary.accept().await.unwrap();
            accepted_by_server.fetch_add(1, Ordering::Relaxed);
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            for message_id in ["one@example", "two@example"] {
                assert_eq!(
                    read_command(&mut reader).await,
                    format!("ARTICLE <{message_id}>")
                );
                write_test_article(&mut reader).await;
            }
        });
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([
                ("primary".into(), credentials("alice", "secret")),
                ("secondary".into(), credentials("bob", "secret")),
            ]),
            calls: Mutex::new(Vec::new()),
        });
        let source = source_for_test(
            vec![
                profile("primary", primary_port, 0),
                profile("secondary", secondary_port, 0),
            ],
            resolver.clone(),
        );

        timeout(Duration::from_secs(3), async {
            assert_eq!(source.fetch("one@example").await.unwrap().data, b"A");
            assert_eq!(source.fetch("two@example").await.unwrap().data, b"A");
            primary_server.await.unwrap();
        })
        .await
        .expect("equal-priority profiles did not keep the warm stream");

        assert_eq!(accepted.load(Ordering::Relaxed), 1);
        assert_eq!(resolver.calls_for("primary"), 1);
        assert_eq!(resolver.calls_for("secondary"), 0);
        assert!(
            timeout(Duration::from_millis(100), secondary.accept())
                .await
                .is_err(),
            "the equal-priority fallback was connected despite a healthy warm stream"
        );
    }

    #[tokio::test]
    async fn switching_profiles_releases_the_old_capacity_lease() {
        let first = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let first_port = first.local_addr().unwrap().port();
        let second = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let second_port = second.local_addr().unwrap().port();
        let first_server = tokio::spawn(async move {
            let (unavailable, _) = first.accept().await.unwrap();
            let mut unavailable_reader = BufReader::new(unavailable);
            greet_and_authenticate(&mut unavailable_reader, "alice", "first-secret").await;
            assert_eq!(
                read_command(&mut unavailable_reader).await,
                "ARTICLE <missing@example>"
            );
            unavailable_reader
                .get_mut()
                .write_all(b"430 article unavailable\r\n")
                .await
                .unwrap();

            let (available, _) = first.accept().await.unwrap();
            let mut available_reader = BufReader::new(available);
            greet_and_authenticate(&mut available_reader, "alice", "first-secret").await;
            assert_eq!(
                read_command(&mut available_reader).await,
                "ARTICLE <other@example>"
            );
            write_test_article(&mut available_reader).await;
        });
        let second_server = tokio::spawn(async move {
            let (stream, _) = second.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "bob", "second-secret").await;
            assert_eq!(read_command(&mut reader).await, "ARTICLE <missing@example>");
            write_test_article(&mut reader).await;
        });
        let registry = Arc::new(ProviderConnectionCapacityRegistry::default());
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([
                ("first".into(), credentials("alice", "first-secret")),
                ("second".into(), credentials("bob", "second-secret")),
            ]),
            calls: Mutex::new(Vec::new()),
        });
        let fallback_source = source_for_test_with_capacity(
            vec![
                profile("first", first_port, 0),
                profile("second", second_port, 1),
            ],
            resolver.clone(),
            registry.clone(),
        );

        assert_eq!(
            fallback_source.fetch("missing@example").await.unwrap().data,
            b"A"
        );

        let waiting_source = source_for_test_with_capacity(
            vec![profile("first", first_port, 0)],
            resolver,
            registry,
        );
        assert_eq!(
            timeout(
                Duration::from_secs(3),
                waiting_source.fetch("other@example")
            )
            .await
            .expect("fallback retained the previous provider capacity lease")
            .unwrap()
            .data,
            b"A"
        );
        first_server.await.unwrap();
        second_server.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_during_connection_releases_the_capacity_lease() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (first_connected_tx, first_connected_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stalled, _) = listener.accept().await.unwrap();
            let _stalled_reader = BufReader::new(stalled);
            first_connected_tx.send(()).unwrap();

            let (healthy, _) = listener.accept().await.unwrap();
            let mut healthy_reader = BufReader::new(healthy);
            greet_and_authenticate(&mut healthy_reader, "alice", "secret").await;
            assert_eq!(
                read_command(&mut healthy_reader).await,
                "ARTICLE <second@example>"
            );
            write_test_article(&mut healthy_reader).await;
        });
        let registry = Arc::new(ProviderConnectionCapacityRegistry::default());
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let cancel = CancellationToken::new();
        let mut stalled_source = source_for_test_with_capacity(
            vec![profile("primary", port, 0)],
            resolver.clone(),
            registry.clone(),
        );
        stalled_source.cancel = cancel.clone();
        let stalled_source = Arc::new(stalled_source);
        let source_for_fetch = stalled_source.clone();
        let stalled_fetch =
            tokio::spawn(async move { source_for_fetch.fetch("first@example").await });

        first_connected_rx.await.unwrap();
        cancel.cancel();
        let error = timeout(Duration::from_secs(3), stalled_fetch)
            .await
            .expect("cancelled connection stayed blocked")
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error,
            ArticleFetchError::Failed(message) if message.contains("Download cancelled")
        ));

        let healthy_source =
            source_for_test_with_capacity(vec![profile("primary", port, 0)], resolver, registry);
        assert_eq!(
            timeout(
                Duration::from_secs(3),
                healthy_source.fetch("second@example")
            )
            .await
            .expect("cancelled connection retained the capacity lease")
            .unwrap()
            .data,
            b"A"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_during_article_releases_the_capacity_lease() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (article_requested_tx, article_requested_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stalled, _) = listener.accept().await.unwrap();
            let mut stalled_reader = BufReader::new(stalled);
            greet_and_authenticate(&mut stalled_reader, "alice", "secret").await;
            assert_eq!(
                read_command(&mut stalled_reader).await,
                "ARTICLE <first@example>"
            );
            article_requested_tx.send(()).unwrap();

            let (healthy, _) = listener.accept().await.unwrap();
            let mut healthy_reader = BufReader::new(healthy);
            greet_and_authenticate(&mut healthy_reader, "alice", "secret").await;
            assert_eq!(
                read_command(&mut healthy_reader).await,
                "ARTICLE <second@example>"
            );
            write_test_article(&mut healthy_reader).await;
        });
        let registry = Arc::new(ProviderConnectionCapacityRegistry::default());
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let cancel = CancellationToken::new();
        let mut stalled_source = source_for_test_with_capacity(
            vec![profile("primary", port, 0)],
            resolver.clone(),
            registry.clone(),
        );
        stalled_source.cancel = cancel.clone();
        let stalled_source = Arc::new(stalled_source);
        let source_for_fetch = stalled_source.clone();
        let stalled_fetch =
            tokio::spawn(async move { source_for_fetch.fetch("first@example").await });

        article_requested_rx.await.unwrap();
        cancel.cancel();
        let error = timeout(Duration::from_secs(3), stalled_fetch)
            .await
            .expect("cancelled article read stayed blocked")
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error,
            ArticleFetchError::Failed(message) if message.contains("Download cancelled")
        ));

        let healthy_source =
            source_for_test_with_capacity(vec![profile("primary", port, 0)], resolver, registry);
        assert_eq!(
            timeout(
                Duration::from_secs(3),
                healthy_source.fetch("second@example")
            )
            .await
            .expect("cancelled article retained the capacity lease")
            .unwrap()
            .data,
            b"A"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnects_closed_session_without_reresolving_credentials() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_by_server = accepted.clone();
        let server = tokio::spawn(async move {
            for message_id in ["one@example", "two@example"] {
                let (stream, _) = listener.accept().await.unwrap();
                accepted_by_server.fetch_add(1, Ordering::Relaxed);
                let mut reader = BufReader::new(stream);
                greet_and_authenticate(&mut reader, "alice", "secret").await;
                assert_eq!(
                    read_command(&mut reader).await,
                    format!("ARTICLE <{message_id}>")
                );
                write_test_article(&mut reader).await;
            }
        });
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let source = source_for_test(vec![profile("primary", port, 0)], resolver.clone());

        timeout(Duration::from_secs(3), async {
            assert_eq!(source.fetch("one@example").await.unwrap().data, b"A");
            assert_eq!(source.fetch("two@example").await.unwrap().data, b"A");
            server.await.unwrap();
        })
        .await
        .expect("fake NNTP reconnect test timed out");

        assert_eq!(accepted.load(Ordering::Relaxed), 2);
        assert_eq!(resolver.calls_for("primary"), 1);
    }

    #[tokio::test]
    async fn unavailable_and_corrupt_articles_keep_the_authenticated_stream() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_by_server = accepted.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accepted_by_server.fetch_add(1, Ordering::Relaxed);
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;

            assert_eq!(read_command(&mut reader).await, "ARTICLE <missing@example>");
            reader
                .get_mut()
                .write_all(b"430 article unavailable\r\n")
                .await
                .unwrap();

            assert_eq!(read_command(&mut reader).await, "ARTICLE <corrupt@example>");
            reader
                .get_mut()
                .write_all(b"220 article follows\r\nnot yEnc\r\n.\r\n")
                .await
                .unwrap();

            assert_eq!(read_command(&mut reader).await, "ARTICLE <good@example>");
            write_test_article(&mut reader).await;
        });
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let source = source_for_test(vec![profile("primary", port, 0)], resolver.clone());

        timeout(Duration::from_secs(3), async {
            assert!(matches!(
                source.fetch("missing@example").await,
                Err(ArticleFetchError::Unavailable(_))
            ));
            assert!(matches!(
                source.fetch("corrupt@example").await,
                Err(ArticleFetchError::Unavailable(_))
            ));
            assert_eq!(source.fetch("good@example").await.unwrap().data, b"A");
            server.await.unwrap();
        })
        .await
        .expect("fake NNTP error-reuse test timed out");

        assert_eq!(accepted.load(Ordering::Relaxed), 1);
        assert_eq!(resolver.calls_for("primary"), 1);
    }

    #[tokio::test]
    async fn retained_streams_share_provider_capacity_across_tasks() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (second_accepted_tx, mut second_accepted_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first_reader = BufReader::new(first);
            greet_and_authenticate(&mut first_reader, "alice", "secret").await;
            assert_eq!(
                read_command(&mut first_reader).await,
                "ARTICLE <one@example>"
            );
            write_test_article(&mut first_reader).await;

            let (second, _) = listener.accept().await.unwrap();
            second_accepted_tx.send(()).unwrap();
            let mut second_reader = BufReader::new(second);
            greet_and_authenticate(&mut second_reader, "alice", "secret").await;
            assert_eq!(
                read_command(&mut second_reader).await,
                "ARTICLE <two@example>"
            );
            write_test_article(&mut second_reader).await;
        });
        let registry = Arc::new(ProviderConnectionCapacityRegistry::default());
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let primary = profile("primary", port, 0);
        let source_a = source_for_test_with_capacity(
            vec![primary.clone()],
            resolver.clone(),
            registry.clone(),
        );
        let source_b = Arc::new(source_for_test_with_capacity(
            vec![primary],
            resolver.clone(),
            registry,
        ));

        assert_eq!(source_a.fetch("one@example").await.unwrap().data, b"A");
        let source_b_for_fetch = source_b.clone();
        let second_fetch =
            tokio::spawn(async move { source_b_for_fetch.fetch("two@example").await });
        wait_for_resolver_calls(&resolver, "primary", 2).await;
        tokio::task::yield_now().await;
        assert_eq!(resolver.calls_for("primary"), 2);
        assert!(
            timeout(Duration::from_millis(100), &mut second_accepted_rx)
                .await
                .is_err(),
            "a second task opened a stream before the first task released its retained lease"
        );

        drop(source_a);
        assert_eq!(
            timeout(Duration::from_secs(3), second_fetch)
                .await
                .expect("second task remained blocked after capacity was released")
                .unwrap()
                .unwrap()
                .data,
            b"A"
        );
        second_accepted_rx.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn saturated_primary_uses_an_available_backup_before_waiting() {
        let primary_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let primary_port = primary_listener.local_addr().unwrap().port();
        let backup_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let backup_port = backup_listener.local_addr().unwrap().port();
        let primary_server = tokio::spawn(async move {
            let (stream, _) = primary_listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            assert!(matches!(
                read_command(&mut reader).await.as_str(),
                "ARTICLE <first@example>" | "ARTICLE <second@example>"
            ));
            write_test_article(&mut reader).await;
        });
        let backup_server = tokio::spawn(async move {
            let (stream, _) = backup_listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            assert!(matches!(
                read_command(&mut reader).await.as_str(),
                "ARTICLE <first@example>" | "ARTICLE <second@example>"
            ));
            write_test_article(&mut reader).await;
        });
        let registry = Arc::new(ProviderConnectionCapacityRegistry::default());
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([
                ("primary".into(), credentials("alice", "secret")),
                ("backup".into(), credentials("alice", "secret")),
            ]),
            calls: Mutex::new(Vec::new()),
        });
        let mut primary = profile("primary", primary_port, 0);
        primary.max_connections = 1;
        let mut backup = profile("backup", backup_port, 0);
        backup.max_connections = 1;
        let source_a = Arc::new(source_for_test_with_capacity(
            vec![primary.clone(), backup.clone()],
            resolver.clone(),
            registry.clone(),
        ));
        let source_b = Arc::new(source_for_test_with_capacity(
            vec![primary, backup],
            resolver,
            registry,
        ));

        let (first, second) = timeout(Duration::from_secs(3), async {
            tokio::join!(
                source_a.fetch("first@example"),
                source_b.fetch("second@example")
            )
        })
        .await
        .expect("a full primary provider blocked a task despite a free backup");

        assert_eq!(first.unwrap().data, b"A");
        assert_eq!(second.unwrap().data, b"A");
        primary_server.await.unwrap();
        backup_server.await.unwrap();
    }

    fn single_connection_source(port: u16) -> NntpArticleSource {
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        source_for_test(vec![profile("primary", port, 0)], resolver)
    }

    #[tokio::test]
    async fn pipelining_multiplies_the_fetch_window() {
        let source = single_connection_source(1);
        assert_eq!(source.concurrency(), PIPELINE_DEPTH);
    }

    #[tokio::test]
    async fn commands_are_written_ahead_of_the_first_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            let mut commands = Vec::new();
            for _ in 0..PIPELINE_DEPTH {
                commands.push(read_command(&mut reader).await);
            }
            for _ in 0..PIPELINE_DEPTH {
                write_test_article(&mut reader).await;
            }
            commands
        });
        let source = single_connection_source(port);

        let results = timeout(Duration::from_secs(5), async {
            tokio::join!(
                source.fetch("a@example"),
                source.fetch("b@example"),
                source.fetch("c@example")
            )
        })
        .await
        .expect("pipelined fetches stalled");

        assert_eq!(results.0.unwrap().data, b"A");
        assert_eq!(results.1.unwrap().data, b"A");
        assert_eq!(results.2.unwrap().data, b"A");
        let mut commands = server.await.unwrap();
        commands.sort();
        assert_eq!(
            commands,
            [
                "ARTICLE <a@example>",
                "ARTICLE <b@example>",
                "ARTICLE <c@example>"
            ]
        );
    }

    #[tokio::test]
    async fn missing_article_mid_window_fails_only_that_id() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_by_server = accepted.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                accepted_by_server.fetch_add(1, Ordering::SeqCst);
                let mut reader = BufReader::new(stream);
                greet_and_authenticate(&mut reader, "alice", "secret").await;
                let mut ids = Vec::new();
                for _ in 0..PIPELINE_DEPTH {
                    ids.push(read_command(&mut reader).await);
                }
                for id in &ids {
                    if id == "ARTICLE <gone@example>" {
                        reader
                            .get_mut()
                            .write_all(b"430 no such article\r\n")
                            .await
                            .unwrap();
                    } else {
                        write_test_article(&mut reader).await;
                    }
                }
                assert_eq!(read_command(&mut reader).await, "ARTICLE <later@example>");
                write_test_article(&mut reader).await;
            }
        });
        let source = single_connection_source(port);

        let (first, second, third) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                source.fetch("one@example"),
                source.fetch("gone@example"),
                source.fetch("two@example")
            )
        })
        .await
        .expect("pipelined fetches stalled");

        assert_eq!(first.unwrap().data, b"A");
        assert!(matches!(second, Err(ArticleFetchError::Unavailable(_))));
        assert_eq!(third.unwrap().data, b"A");
        assert_eq!(source.fetch("later@example").await.unwrap().data, b"A");
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn connection_drop_mid_window_requeues_unanswered_ids() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            let mut first_ids = Vec::new();
            for _ in 0..PIPELINE_DEPTH {
                first_ids.push(read_command(&mut reader).await);
            }
            write_test_article(&mut reader).await;
            drop(reader);

            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            let mut second_ids = Vec::new();
            for _ in 0..PIPELINE_DEPTH - 1 {
                second_ids.push(read_command(&mut reader).await);
            }
            for _ in &second_ids {
                write_test_article(&mut reader).await;
            }
            (first_ids, second_ids)
        });
        let source = single_connection_source(port);

        let results = timeout(Duration::from_secs(5), async {
            tokio::join!(
                source.fetch("a@example"),
                source.fetch("b@example"),
                source.fetch("c@example")
            )
        })
        .await
        .expect("requeued fetches stalled");

        assert_eq!(results.0.unwrap().data, b"A");
        assert_eq!(results.1.unwrap().data, b"A");
        assert_eq!(results.2.unwrap().data, b"A");
        let (first_ids, mut second_ids) = server.await.unwrap();
        assert_eq!(first_ids.len(), PIPELINE_DEPTH);
        second_ids.sort();
        let mut expected = first_ids[1..].to_vec();
        expected.sort();
        assert_eq!(second_ids, expected);
        assert_eq!(source.pool.ordered_profiles_with_preference(None).len(), 1);
    }

    #[tokio::test]
    async fn fresh_connection_loss_fails_only_the_oldest_request() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            let mut first_ids = Vec::new();
            for _ in 0..PIPELINE_DEPTH {
                first_ids.push(read_command(&mut reader).await);
            }
            drop(reader);

            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "secret").await;
            for _ in 0..PIPELINE_DEPTH - 1 {
                read_command(&mut reader).await;
            }
            for _ in 0..PIPELINE_DEPTH - 1 {
                write_test_article(&mut reader).await;
            }
            first_ids
        });
        let source = single_connection_source(port);

        let results = timeout(Duration::from_secs(5), async {
            tokio::join!(
                source.fetch("a@example"),
                source.fetch("b@example"),
                source.fetch("c@example")
            )
        })
        .await
        .expect("requeued fetches stalled");

        let outcomes = [results.0, results.1, results.2];
        let failed = outcomes.iter().filter(|result| result.is_err()).count();
        assert_eq!(failed, 1, "only the oldest request carries the error");
        assert!(outcomes
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .all(|part| part.data == b"A"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_fetches_use_separate_pooled_connections() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut readers = Vec::new();
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                greet_and_authenticate(&mut reader, "alice", "secret").await;
                readers.push(reader);
            }
            for reader in &mut readers {
                assert!(read_command(reader).await.starts_with("ARTICLE <"));
            }
            for reader in &mut readers {
                write_test_article(reader).await;
            }
        });
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let mut primary = profile("primary", port, 0);
        primary.max_connections = 2;
        let mut source = source_for_test(vec![primary], resolver);
        source.pipeline_depth = 1;
        assert_eq!(source.concurrency(), 2);

        let (first, second) = timeout(Duration::from_secs(3), async {
            tokio::join!(source.fetch("a@example"), source.fetch("b@example"))
        })
        .await
        .expect("concurrent fetches were serialized on one connection");

        assert_eq!(first.unwrap().data, b"A");
        assert_eq!(second.unwrap().data, b"A");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_while_waiting_for_capacity_releases_the_waiter() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first_reader = BufReader::new(first);
            greet_and_authenticate(&mut first_reader, "alice", "secret").await;
            assert_eq!(
                read_command(&mut first_reader).await,
                "ARTICLE <one@example>"
            );
            write_test_article(&mut first_reader).await;

            let (third, _) = listener.accept().await.unwrap();
            let mut third_reader = BufReader::new(third);
            greet_and_authenticate(&mut third_reader, "alice", "secret").await;
            assert_eq!(
                read_command(&mut third_reader).await,
                "ARTICLE <three@example>"
            );
            write_test_article(&mut third_reader).await;
        });
        let registry = Arc::new(ProviderConnectionCapacityRegistry::default());
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let primary = profile("primary", port, 0);
        let source_a = source_for_test_with_capacity(
            vec![primary.clone()],
            resolver.clone(),
            registry.clone(),
        );
        let wait_cancel = CancellationToken::new();
        let mut source_b = source_for_test_with_capacity(
            vec![primary.clone()],
            resolver.clone(),
            registry.clone(),
        );
        source_b.cancel = wait_cancel.clone();
        let source_b = Arc::new(source_b);

        assert_eq!(source_a.fetch("one@example").await.unwrap().data, b"A");
        let source_b_for_fetch = source_b.clone();
        let blocked_fetch =
            tokio::spawn(async move { source_b_for_fetch.fetch("two@example").await });
        wait_for_resolver_calls(&resolver, "primary", 2).await;
        tokio::task::yield_now().await;
        assert_eq!(resolver.calls_for("primary"), 2);
        wait_cancel.cancel();
        let error = timeout(Duration::from_secs(3), blocked_fetch)
            .await
            .expect("cancelled task remained queued for provider capacity")
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error, ArticleFetchError::Failed(message) if message.contains("Download cancelled"))
        );

        drop(source_b);
        drop(source_a);
        let source_c = source_for_test_with_capacity(vec![primary], resolver, registry);
        assert_eq!(
            timeout(Duration::from_secs(3), source_c.fetch("three@example"))
                .await
                .expect("cancelled waiter kept the provider capacity blocked")
                .unwrap()
                .data,
            b"A"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn failed_connect_releases_the_capacity_lease() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (failed, _) = listener.accept().await.unwrap();
            let mut failed_reader = BufReader::new(failed);
            failed_reader
                .get_mut()
                .write_all(b"400 temporarily unavailable\r\n")
                .await
                .unwrap();

            let (healthy, _) = listener.accept().await.unwrap();
            let mut healthy_reader = BufReader::new(healthy);
            greet_and_authenticate(&mut healthy_reader, "alice", "secret").await;
            assert_eq!(
                read_command(&mut healthy_reader).await,
                "ARTICLE <two@example>"
            );
            write_test_article(&mut healthy_reader).await;
        });
        let registry = Arc::new(ProviderConnectionCapacityRegistry::default());
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([("primary".into(), credentials("alice", "secret"))]),
            calls: Mutex::new(Vec::new()),
        });
        let primary = profile("primary", port, 0);
        let failed_source = source_for_test_with_capacity(
            vec![primary.clone()],
            resolver.clone(),
            registry.clone(),
        );
        let healthy_source = source_for_test_with_capacity(vec![primary], resolver, registry);

        assert!(matches!(
            failed_source.fetch("one@example").await,
            Err(ArticleFetchError::Failed(_))
        ));
        assert_eq!(
            timeout(Duration::from_secs(3), healthy_source.fetch("two@example"))
                .await
                .expect("failed connection kept provider capacity leased")
                .unwrap()
                .data,
            b"A"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn failover_keeps_cached_credentials_scoped_to_each_profile() {
        let first = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let first_port = first.local_addr().unwrap().port();
        let second = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let second_port = second.local_addr().unwrap().port();
        let first_server = tokio::spawn(async move {
            let (stream, _) = first.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "alice", "first-secret").await;
            assert_eq!(read_command(&mut reader).await, "ARTICLE <one@example>");
            reader
                .get_mut()
                .write_all(b"430 article unavailable\r\n")
                .await
                .unwrap();
        });
        let second_server = tokio::spawn(async move {
            let (stream, _) = second.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            greet_and_authenticate(&mut reader, "bob", "second-secret").await;
            assert_eq!(read_command(&mut reader).await, "ARTICLE <one@example>");
            write_test_article(&mut reader).await;
        });
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::from([
                ("first".into(), credentials("alice", "first-secret")),
                ("second".into(), credentials("bob", "second-secret")),
            ]),
            calls: Mutex::new(Vec::new()),
        });
        let source = source_for_test(
            vec![
                profile("first", first_port, 0),
                profile("second", second_port, 1),
            ],
            resolver.clone(),
        );

        timeout(Duration::from_secs(3), async {
            assert_eq!(source.fetch("one@example").await.unwrap().data, b"A");
            first_server.await.unwrap();
            second_server.await.unwrap();
        })
        .await
        .expect("fake NNTP failover test timed out");

        assert_eq!(resolver.calls_for("first"), 1);
        assert_eq!(resolver.calls_for("second"), 1);
    }

    #[test]
    fn corrupt_articles_become_repairable_holes_after_provider_failover() {
        let error = article_fetch_error(NntpError::ArticleCorrupt {
            message: "pcrc32 mismatch".into(),
        });

        assert!(
            matches!(error, ArticleFetchError::Unavailable(message) if message.contains("invalid yEnc"))
        );
    }

    #[test]
    fn insufficient_par2_recovery_keeps_a_structured_task_summary() {
        let unavailable = vec!["archive.part1.rar:[3, 6]".to_string()];
        let error = UsenetDownloadError::from_par2_error(
            Par2Error::InsufficientRecovery {
                needed: 184,
                available: 62,
            },
            &unavailable,
        );

        assert_eq!(
            error.repair_failure(),
            Some(&UsenetRepairFailure {
                needed_blocks: 184,
                available_blocks: 62,
                partials_retained: true,
            })
        );
        assert_eq!(
            error.to_string(),
            "PAR2 recovery is insufficient: need 184 blocks, have 62; unavailable segments: archive.part1.rar:[3, 6]"
        );
    }

    #[test]
    fn task_article_bytes_uses_the_manifest_segment_byte_unit() {
        let metadata = UsenetTaskData {
            files: vec![UsenetTaskFile {
                name: "sample.bin".into(),
                subject: "sample.bin yEnc".into(),
                groups: vec!["alt.binaries.example".into()],
                segments: vec![
                    UsenetTaskSegment {
                        number: 1,
                        bytes: 17,
                        message_id: "one".into(),
                    },
                    UsenetTaskSegment {
                        number: 2,
                        bytes: 19,
                        message_id: "two".into(),
                    },
                ],
            }],
            ..Default::default()
        };

        assert_eq!(task_article_bytes(&metadata).unwrap(), 36);
    }

    #[tokio::test]
    async fn resumes_a_finalized_output_before_applying_collision_naming() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 2,
            message_id: "one".into(),
        }];
        let mut sidecar = ResumeSidecar::new(manifest_sha256(&segments));
        sidecar.expected_size = Some(2);
        sidecar.completed_bytes = 2;
        sidecar.completed_segments.insert(1);
        sidecar.segment_receipts.insert(
            1,
            ResumeSegment {
                offset: 0,
                length: 2,
                sha256: hex::encode(Sha256::digest(b"OK")),
            },
        );
        sidecar
            .save_atomic(&resume_sidecar_path(&output))
            .await
            .unwrap();
        tokio::fs::write(&output, b"OK").await.unwrap();

        let reservation = reserve_output_path(dir.path(), "sample.bin", &segments)
            .await
            .unwrap();

        assert_eq!(reservation.output, output);
    }

    #[tokio::test]
    async fn malformed_resume_sidecar_is_replaced_by_a_fresh_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 2,
            message_id: "one".into(),
        }];
        tokio::fs::write(resume_sidecar_path(&output), b"{truncated")
            .await
            .unwrap();

        let reservation = reserve_output_path(dir.path(), "sample.bin", &segments)
            .await
            .unwrap();

        assert_eq!(reservation.output, output);
    }

    #[tokio::test]
    async fn resume_sidecar_directory_occupies_output_slot() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        tokio::fs::create_dir(resume_sidecar_path(&output))
            .await
            .unwrap();

        assert!(!output_slot_is_available(&output).await.unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn broken_resume_sidecar_symlink_occupies_output_slot() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let sidecar = resume_sidecar_path(&output);
        std::os::unix::fs::symlink(dir.path().join("missing-sidecar"), &sidecar).unwrap();

        assert!(!output_slot_is_available(&output).await.unwrap());
    }

    #[tokio::test]
    async fn reserves_a_distinct_output_for_concurrent_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 2,
            message_id: "one".into(),
        }];

        let first = reserve_output_path(dir.path(), "sample.bin", &segments)
            .await
            .unwrap();
        let second = reserve_output_path(dir.path(), "sample.bin", &segments)
            .await
            .unwrap();

        assert_eq!(first.output, dir.path().join("sample.bin"));
        assert_eq!(second.output, dir.path().join("sample (1).bin"));
    }

    #[tokio::test]
    async fn reserves_casefolded_names_as_one_output_slot() {
        let dir = tempfile::tempdir().unwrap();
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 2,
            message_id: "one".into(),
        }];

        let first = reserve_output_path(dir.path(), "Sample.bin", &segments)
            .await
            .unwrap();
        let second = reserve_output_path(dir.path(), "sample.bin", &segments)
            .await
            .unwrap();

        assert_eq!(first.output, dir.path().join("Sample.bin"));
        assert_eq!(second.output, dir.path().join("sample (1).bin"));
    }

    #[test]
    fn active_time_limit_is_checked_during_assembly() {
        let tracker = ActiveTimeTracker {
            started_at: Instant::now() - Duration::from_secs(2),
            credential_wait: Arc::new(Mutex::new(Duration::ZERO)),
        };

        assert!(ensure_usenet_active_time(&tracker, 1).is_err());
    }

    #[test]
    fn capacity_wait_reports_the_archive_limit_when_no_active_time_remains() {
        let resolver = Arc::new(RecordingResolver {
            credentials: HashMap::new(),
            calls: Mutex::new(Vec::new()),
        });
        let mut source = source_for_test(vec![profile("primary", 1, 0)], resolver);
        source.max_active_seconds = 1;
        source.active_time = ActiveTimeTracker {
            started_at: Instant::now() - Duration::from_secs(2),
            credential_wait: Arc::new(Mutex::new(Duration::ZERO)),
        };

        assert!(matches!(
            source.remaining_active_time(),
            Err(NntpError::Protocol { message, .. }) if message.contains("archive limit")
        ));
    }

    #[test]
    fn credential_wait_does_not_consume_active_time() {
        let tracker = ActiveTimeTracker::new();
        tracker.record_credential_wait(Duration::from_secs(60));

        assert_eq!(tracker.active_elapsed(), Duration::ZERO);
    }

    #[test]
    fn task_cleanup_override_wins_over_global_default() {
        let metadata = UsenetTaskData {
            options: crate::engine::task::UsenetTaskOptions {
                cleanup_mode: Some("delete-par2".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut options = Map::new();
        options.insert(
            "usenet-cleanup-mode".into(),
            Value::String("delete-par2-and-archives".into()),
        );

        assert_eq!(
            cleanup_mode_for_task(&metadata, &options),
            CleanupMode::DeletePar2
        );
    }

    #[test]
    fn archive_volume_cleanup_is_explicitly_unsupported_without_extraction_verification() {
        let metadata = UsenetTaskData::default();
        let mut options = Map::new();
        options.insert(
            "usenet-cleanup-mode".into(),
            Value::String("delete-par2-and-volumes".into()),
        );
        assert_eq!(
            cleanup_mode_for_task(&metadata, &options),
            CleanupMode::DeletePar2
        );
    }

    #[test]
    fn cleanup_inputs_include_final_and_partial_archive_paths_only() {
        let dir = tempfile::tempdir().unwrap();
        let par2_output = dir.path().join("release.vol00+01.par2");
        let rar_output = dir.path().join("release.part01.rar");
        let par2_part = partial_path(&par2_output);
        let rar_part = partial_path(&rar_output);
        let par2_sidecar = resume_sidecar_path(&par2_output);
        let rar_sidecar = resume_sidecar_path(&rar_output);
        let lock = fs::File::create(dir.path().join("lock")).unwrap();
        let assembled = vec![
            AssembledTaskFile {
                index: 0,
                name: "release.vol00+01.par2".into(),
                is_parity: true,
                report: AssemblyReport {
                    output: par2_output.clone(),
                    part_path: par2_part.clone(),
                    sidecar_path: par2_sidecar.clone(),
                    manifest_sha256: String::new(),
                    expected_size: None,
                    completed_bytes: 0,
                    unavailable_segments: Vec::new(),
                    complete: true,
                },
                _reservation: OutputReservation {
                    output: par2_output,
                    _lock_path: dir.path().join("lock"),
                    lock_file: Some(lock),
                },
            },
            AssembledTaskFile {
                index: 1,
                name: "release.part01.rar".into(),
                is_parity: false,
                report: AssemblyReport {
                    output: rar_output.clone(),
                    part_path: rar_part.clone(),
                    sidecar_path: rar_sidecar.clone(),
                    manifest_sha256: String::new(),
                    expected_size: None,
                    completed_bytes: 0,
                    unavailable_segments: Vec::new(),
                    complete: true,
                },
                _reservation: OutputReservation {
                    output: rar_output,
                    _lock_path: dir.path().join("lock-2"),
                    lock_file: Some(fs::File::create(dir.path().join("lock-2")).unwrap()),
                },
            },
        ];

        let (par2, archives) = cleanup_inputs(&assembled, true, &[]);
        assert!(par2.contains(&par2_part));
        assert!(archives.contains(&rar_part));
        assert!(!par2.contains(&par2_sidecar));
        assert!(!archives.contains(&rar_sidecar));
    }

    #[test]
    fn cleanup_inputs_keeps_archive_volumes_until_extraction_is_verified() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("release.part01.rar");
        let lock = fs::File::create(dir.path().join("lock")).unwrap();
        let assembled = vec![AssembledTaskFile {
            index: 0,
            name: "release.part01.rar".into(),
            is_parity: false,
            report: AssemblyReport {
                output: output.clone(),
                part_path: partial_path(&output),
                sidecar_path: resume_sidecar_path(&output),
                manifest_sha256: String::new(),
                expected_size: None,
                completed_bytes: 0,
                unavailable_segments: Vec::new(),
                complete: true,
            },
            _reservation: OutputReservation {
                output,
                _lock_path: dir.path().join("lock"),
                lock_file: Some(lock),
            },
        }];

        let (_, archives) = cleanup_inputs(&assembled, false, &[]);
        assert!(archives.is_empty());
    }

    #[test]
    fn cleanup_inputs_preserves_incomplete_parity_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("release.vol00+01.par2");
        let lock = fs::File::create(dir.path().join("lock")).unwrap();
        let assembled = vec![AssembledTaskFile {
            index: 0,
            name: "release.vol00+01.par2".into(),
            is_parity: true,
            report: AssemblyReport {
                output: output.clone(),
                part_path: partial_path(&output),
                sidecar_path: resume_sidecar_path(&output),
                manifest_sha256: String::new(),
                expected_size: None,
                completed_bytes: 0,
                unavailable_segments: vec![2],
                complete: false,
            },
            _reservation: OutputReservation {
                output,
                _lock_path: dir.path().join("lock"),
                lock_file: Some(lock),
            },
        }];

        let (par2, _) = cleanup_inputs(&assembled, true, &[]);
        assert!(par2.is_empty());
    }

    #[test]
    fn cleanup_inputs_accepts_durably_promoted_repaired_archive() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("release.part01.rar");
        let lock = fs::File::create(dir.path().join("lock")).unwrap();
        let assembled = vec![AssembledTaskFile {
            index: 0,
            name: "release.part01.rar".into(),
            is_parity: false,
            report: AssemblyReport {
                output: output.clone(),
                part_path: partial_path(&output),
                sidecar_path: resume_sidecar_path(&output),
                manifest_sha256: String::new(),
                expected_size: Some(10),
                completed_bytes: 4,
                unavailable_segments: vec![2],
                complete: false,
            },
            _reservation: OutputReservation {
                output: output.clone(),
                _lock_path: dir.path().join("lock"),
                lock_file: Some(lock),
            },
        }];

        let (_, archives) = cleanup_inputs(&assembled, true, &[output]);
        assert_eq!(archives.len(), 2);
    }

    #[test]
    fn output_reservation_keeps_lock_file_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("reservation.lock");
        let reservation = OutputReservation {
            output: dir.path().join("output"),
            _lock_path: lock_path.clone(),
            lock_file: Some(fs::File::create(&lock_path).unwrap()),
        };
        assert!(lock_path.exists());
        drop(reservation);
        assert!(lock_path.exists());
    }

    #[test]
    fn recovery_volume_picks_cover_the_need_with_the_fewest_blocks() {
        use crate::engine::usenet::file_role;
        let names = [
            "a.vol000+01.par2",
            "a.vol001+02.par2",
            "a.vol003+04.par2",
            "a.vol007+08.par2",
        ];
        let roles: Vec<FileRole> = names.iter().map(|name| file_role(name)).collect();
        let all = [0, 1, 2, 3];
        assert_eq!(pick_recovery_volumes(&all, &roles, 1), [0]);
        assert_eq!(pick_recovery_volumes(&all, &roles, 3), [2]);
        assert_eq!(pick_recovery_volumes(&all, &roles, 8), [3]);
        assert_eq!(pick_recovery_volumes(&all, &roles, 10), [3, 1]);
        assert_eq!(pick_recovery_volumes(&all, &roles, 100).len(), 4);
        assert!(pick_recovery_volumes(&[], &roles, 1).is_empty());
    }

    #[test]
    fn volumes_without_a_block_count_are_taken_only_after_counted_ones() {
        use crate::engine::usenet::file_role;
        let names = ["a.vol1.par2", "a.vol000+02.par2"];
        let roles: Vec<FileRole> = names.iter().map(|name| file_role(name)).collect();
        assert_eq!(pick_recovery_volumes(&[0, 1], &roles, 2), [1]);
        assert_eq!(pick_recovery_volumes(&[0, 1], &roles, 3), [1, 0]);
    }

    #[tokio::test]
    async fn volume_local_state_counts_as_requested() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("a.vol0+1.par2");
        assert!(!volume_has_local_state(dir.path(), "a.vol0+1.par2").await);
        fs::write(partial_path(&output), b"x").unwrap();
        assert!(volume_has_local_state(dir.path(), "a.vol0+1.par2").await);
    }

    fn yenc_article(name: &str, data: &[u8], part: Option<(u64, u64, u64)>) -> Vec<u8> {
        let mut out = Vec::new();
        match part {
            Some((begin, end, total)) => {
                out.extend_from_slice(
                    format!("=ybegin part=1 total=2 line=128 size={total} name={name}\r\n")
                        .as_bytes(),
                );
                out.extend_from_slice(format!("=ypart begin={begin} end={end}\r\n").as_bytes());
            }
            None => out.extend_from_slice(
                format!("=ybegin line=128 size={} name={name}\r\n", data.len()).as_bytes(),
            ),
        }
        let mut line = Vec::new();
        for byte in data {
            let encoded = byte.wrapping_add(42);
            let mut piece = Vec::new();
            if matches!(encoded, 0 | 10 | 13 | 61) {
                piece.push(b'=');
                piece.push(encoded.wrapping_add(64));
            } else {
                piece.push(encoded);
            }
            if line.len() + piece.len() > 128 {
                flush_yenc_line(&mut out, &mut line);
            }
            line.extend(piece);
        }
        flush_yenc_line(&mut out, &mut line);
        match part {
            Some(_) => {
                out.extend_from_slice(format!("=yend size={} part=1\r\n", data.len()).as_bytes())
            }
            None => out.extend_from_slice(format!("=yend size={}\r\n", data.len()).as_bytes()),
        }
        out
    }

    fn flush_yenc_line(out: &mut Vec<u8>, line: &mut Vec<u8>) {
        if line.is_empty() {
            return;
        }
        if line[0] == b'.' {
            out.push(b'.');
        }
        out.extend_from_slice(line);
        out.extend_from_slice(b"\r\n");
        line.clear();
    }

    async fn spawn_article_server(
        articles: HashMap<String, Vec<u8>>,
    ) -> (u16, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requested = Arc::new(Mutex::new(Vec::new()));
        let seen = requested.clone();
        let articles = Arc::new(articles);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let articles = articles.clone();
                let seen = seen.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    reader
                        .get_mut()
                        .write_all(b"200 fake NNTP server ready\r\n")
                        .await
                        .ok();
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                            break;
                        }
                        let command = line.trim_end();
                        if let Some(id) = command
                            .strip_prefix("ARTICLE <")
                            .and_then(|rest| rest.strip_suffix('>'))
                        {
                            seen.lock().unwrap().push(id.to_string());
                            match articles.get(id) {
                                Some(body) => {
                                    let stream = reader.get_mut();
                                    stream.write_all(b"220 article follows\r\n").await.ok();
                                    stream.write_all(body).await.ok();
                                    stream.write_all(b".\r\n").await.ok();
                                }
                                None => {
                                    reader
                                        .get_mut()
                                        .write_all(b"430 no such article\r\n")
                                        .await
                                        .ok();
                                }
                            }
                        } else if command.starts_with("QUIT") {
                            reader.get_mut().write_all(b"205 bye\r\n").await.ok();
                            break;
                        } else {
                            reader
                                .get_mut()
                                .write_all(b"500 unknown command\r\n")
                                .await
                                .ok();
                        }
                    }
                });
            }
        });
        (port, requested)
    }

    struct Par2Scenario {
        task: DownloadTask,
        options: Map<String, Value>,
        articles: HashMap<String, Vec<u8>>,
        index_bytes: u64,
        volume_bytes: u64,
    }

    fn par2_scenario(dir: &Path, lose_second_half: bool, port_hint: u16) -> Par2Scenario {
        let parity = tempfile::tempdir().unwrap();
        let data = b"ABCDEFGH";
        let parity_files =
            crate::engine::usenet_par2::tests::write_fixture(parity.path(), "data.bin", data, true);
        let index = fs::read(&parity_files[0]).unwrap();
        let volume = fs::read(&parity_files[1]).unwrap();
        let mut articles = HashMap::new();
        articles.insert(
            "data-1@t".to_string(),
            yenc_article("data.bin", &data[..4], Some((1, 4, 8))),
        );
        if !lose_second_half {
            articles.insert(
                "data-2@t".to_string(),
                yenc_article("data.bin", &data[4..], Some((5, 8, 8))),
            );
        }
        articles.insert(
            "index-1@t".to_string(),
            yenc_article("sample.par2", &index, None),
        );
        articles.insert(
            "volume-1@t".to_string(),
            yenc_article("sample.vol00+1.par2", &volume, None),
        );
        let file = |name: &str, segments: &[(u32, u64, &str)]| UsenetTaskFile {
            name: name.into(),
            subject: name.into(),
            groups: Vec::new(),
            segments: segments
                .iter()
                .map(|(number, bytes, id)| UsenetTaskSegment {
                    number: *number,
                    bytes: *bytes,
                    message_id: (*id).into(),
                })
                .collect(),
        };
        let metadata = UsenetTaskData {
            files: vec![
                file("data.bin", &[(1, 4, "data-1@t"), (2, 4, "data-2@t")]),
                file("sample.par2", &[(1, index.len() as u64, "index-1@t")]),
                file(
                    "sample.vol00+1.par2",
                    &[(1, volume.len() as u64, "volume-1@t")],
                ),
            ],
            ..Default::default()
        };
        let task = DownloadTask {
            dir: dir.to_string_lossy().into_owned(),
            usenet: Some(metadata),
            ..Default::default()
        };
        let mut options = Map::new();
        options.insert(
            "usenet-profiles".into(),
            serde_json::to_value(vec![profile("primary", port_hint, 0)]).unwrap(),
        );
        Par2Scenario {
            task,
            options,
            articles,
            index_bytes: index.len() as u64,
            volume_bytes: volume.len() as u64,
        }
    }

    async fn run_scenario(
        lose_second_half: bool,
    ) -> (
        Result<(PathBuf, Vec<(usize, PathBuf)>), UsenetDownloadError>,
        Vec<String>,
        u64,
        tempfile::TempDir,
        Par2Scenario,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut scenario = par2_scenario(dir.path(), lose_second_half, 0);
        let (port, requested) = spawn_article_server(scenario.articles.clone()).await;
        scenario.options.insert(
            "usenet-profiles".into(),
            serde_json::to_value(vec![profile("primary", port, 0)]).unwrap(),
        );
        let completed = Arc::new(AtomicU64::new(0));
        let total = Arc::new(AtomicU64::new(0));
        let result = timeout(
            Duration::from_secs(20),
            run_usenet_download_with_resolver_and_capacity(
                &scenario.task,
                &scenario.options,
                completed,
                total.clone(),
                CancellationToken::new(),
                Arc::new(AnonymousCredentialResolver),
                Arc::new(ProviderConnectionCapacityRegistry::default()),
                Throttle::unlimited(),
                None,
            ),
        )
        .await
        .expect("usenet run timed out");
        let requested = requested.lock().unwrap().clone();
        (
            result,
            requested,
            total.load(Ordering::Relaxed),
            dir,
            scenario,
        )
    }

    #[tokio::test]
    async fn undamaged_downloads_never_request_recovery_volumes() {
        let (result, requested, total, dir, scenario) = run_scenario(false).await;
        let (_, outputs) = result.unwrap_or_else(|error| panic!("{error}"));

        assert!(
            !requested.iter().any(|id| id == "volume-1@t"),
            "{requested:?}"
        );
        assert_eq!(total, 8 + scenario.index_bytes);
        assert_eq!(fs::read(dir.path().join("data.bin")).unwrap(), b"ABCDEFGH");
        assert!(!dir.path().join("sample.vol00+1.par2").exists());
        assert_eq!(outputs.len(), 2);
    }

    #[tokio::test]
    async fn damage_fetches_the_recovery_volume_and_counts_it_once_requested() {
        let (result, requested, total, dir, scenario) = run_scenario(true).await;
        result.unwrap_or_else(|error| panic!("{error}"));

        assert!(
            requested.iter().any(|id| id == "volume-1@t"),
            "{requested:?}"
        );
        assert_eq!(total, 8 + scenario.index_bytes + scenario.volume_bytes);
        assert_eq!(fs::read(dir.path().join("data.bin")).unwrap(), b"ABCDEFGH");
    }
}
