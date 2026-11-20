use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::broadcast::Receiver;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::events::{EngineEvent, EventBroadcaster};
use super::manager::TaskManager;
use super::options::json_bool;
use super::torrent::disk_component;
use crate::config::defaults;
use crate::standalone::load_config;

const DEFAULT_RETRY_INTERVAL_SECS: u64 = 5;
const MAX_RETRY_INTERVAL_SECS: u64 = 300;
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const MAX_ATTEMPTS_LIMIT: u32 = 1000;
const MIN_RETRY_DELAY_MS: u64 = 1000;
const MAX_RETRY_DELAY_MS: u64 = 15 * 60 * 1000;
const DEFAULT_SCRIPT_TIMEOUT_MS: u64 = 30_000;
const MIN_SCRIPT_TIMEOUT_MS: u64 = 1_000;
const MAX_SCRIPT_TIMEOUT_MS: u64 = 300_000;
const MAX_OUTPUT_BYTES: usize = 8 * 1024;
const TEMP_DOWNLOAD_SUFFIX: &str = ".part";
const PERMANENT_ERROR_CODES: &[&str] = &[
    "300", "301", "302", "400", "401", "502", "530", "531", "533", "534", "540", "541", "542",
    "550", "551", "552", "553", "554", "315",
];
const STATUS_KEYS: &[&str] = &[
    "status",
    "dir",
    "files",
    "bittorrent",
    "infoHash",
    "errorCode",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetrySettings {
    enabled: bool,
    interval_secs: u64,
    exponential: bool,
    max_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScriptSettings {
    enabled: bool,
    command: String,
    args_template: String,
    timeout_ms: u64,
}

impl ScriptSettings {
    fn configured(&self) -> bool {
        self.enabled && !self.command.is_empty()
    }
}

fn setting_bool(user: &Map<String, Value>, key: &str) -> bool {
    user.get(key).and_then(json_bool).unwrap_or(false)
}

fn setting_number(user: &Map<String, Value>, key: &str) -> Option<f64> {
    let number = match user.get(key)? {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => s.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    number.is_finite().then_some(number)
}

fn setting_str<'a>(user: &'a Map<String, Value>, key: &str) -> &'a str {
    user.get(key).and_then(Value::as_str).unwrap_or_default()
}

pub(crate) fn retry_settings(user: &Map<String, Value>) -> RetrySettings {
    let interval_secs = setting_number(user, "auto-retry-interval")
        .map_or(DEFAULT_RETRY_INTERVAL_SECS, |n| n.floor() as u64)
        .clamp(1, MAX_RETRY_INTERVAL_SECS);
    let max_attempts = match setting_number(user, "auto-retry-max-attempts") {
        Some(n) if n >= 0.0 => (n.floor() as u64).min(u64::from(MAX_ATTEMPTS_LIMIT)) as u32,
        _ => DEFAULT_MAX_ATTEMPTS,
    };
    RetrySettings {
        enabled: setting_bool(user, "auto-retry"),
        interval_secs,
        exponential: setting_str(user, "auto-retry-strategy")
            .trim()
            .eq_ignore_ascii_case("exponential"),
        max_attempts,
    }
}

pub(crate) fn script_settings(user: &Map<String, Value>) -> ScriptSettings {
    ScriptSettings {
        enabled: setting_bool(user, "completion-script-enabled"),
        command: setting_str(user, "completion-script-command")
            .trim()
            .to_string(),
        args_template: setting_str(user, "completion-script-args").to_string(),
        timeout_ms: setting_number(user, "completion-script-timeout-ms")
            .map_or(DEFAULT_SCRIPT_TIMEOUT_MS, |n| n as u64)
            .clamp(MIN_SCRIPT_TIMEOUT_MS, MAX_SCRIPT_TIMEOUT_MS),
    }
}

pub(crate) fn is_permanent_error(code: Option<&str>) -> bool {
    code.map(str::trim)
        .is_some_and(|code| PERMANENT_ERROR_CODES.contains(&code))
}

pub(crate) fn retry_delay(settings: &RetrySettings, next_attempt: u32) -> Duration {
    let base_ms = settings
        .interval_secs
        .saturating_mul(1000)
        .max(MIN_RETRY_DELAY_MS);
    let computed = if settings.exponential {
        let exponent = next_attempt.saturating_sub(1).min(62);
        u128::from(base_ms).saturating_mul(1u128 << exponent)
    } else {
        u128::from(base_ms)
    };
    let ms = computed.clamp(
        u128::from(MIN_RETRY_DELAY_MS),
        u128::from(MAX_RETRY_DELAY_MS),
    ) as u64;
    Duration::from_millis(ms)
}

pub(crate) fn should_retry(
    settings: &RetrySettings,
    error_code: Option<&str>,
    attempts_so_far: u32,
) -> bool {
    settings.enabled
        && !is_permanent_error(error_code)
        && (settings.max_attempts == 0 || attempts_so_far < settings.max_attempts)
}

pub(crate) fn script_args(template: &str, path: &str, hash: &str, status: &str) -> Vec<String> {
    template
        .split_whitespace()
        .map(|token| {
            token
                .replace("{hash}", hash)
                .replace("{status}", status)
                .replace("{path}", path)
        })
        .collect()
}

pub(crate) fn completed_path(status: &Value) -> String {
    task_path(status, true)
}

pub fn task_path_from_status(status: &Value, finished: bool) -> String {
    task_path(status, finished)
}

fn task_path(status: &Value, finished: bool) -> String {
    let dir = status
        .get("dir")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let bittorrent = status.get("bittorrent");
    if bittorrent.is_some_and(|bt| bt.get("info").is_none()) {
        return dir.to_string();
    }
    let files = status
        .get("files")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let torrent_name = bittorrent
        .and_then(|bt| bt.pointer("/info/name"))
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty());
    if let Some(name) = torrent_name.filter(|_| files.len() > 1) {
        return join_path(dir, &disk_component(name));
    }
    let path = files
        .first()
        .and_then(|file| file.get("path"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    match path {
        "" => dir.to_string(),
        path if finished => strip_temp_suffix(path).to_string(),
        path => path.to_string(),
    }
}

fn join_path(dir: &str, name: &str) -> String {
    let dir = dir.trim_end_matches(['/', '\\']);
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

fn strip_temp_suffix(path: &str) -> &str {
    let Some(cut) = path.len().checked_sub(TEMP_DOWNLOAD_SUFFIX.len()) else {
        return path;
    };
    match path.get(cut..) {
        Some(tail) if tail.eq_ignore_ascii_case(TEMP_DOWNLOAD_SUFFIX) => &path[..cut],
        _ => path,
    }
}

pub(crate) fn script_status_for(event: &EngineEvent, already_ran: bool) -> Option<&'static str> {
    match event {
        EngineEvent::DownloadError { .. } => Some("error"),
        EngineEvent::BtDownloadComplete { seeding: true, .. }
        | EngineEvent::DownloadComplete { seeded: false, .. }
            if !already_ran =>
        {
            Some("complete")
        }
        _ => None,
    }
}

#[derive(Default)]
struct CappedOutput {
    buf: Vec<u8>,
    truncated: bool,
}

impl CappedOutput {
    async fn drain<R: AsyncRead + Unpin>(&mut self, pipe: Option<R>) {
        let Some(mut pipe) = pipe else {
            return;
        };
        let mut chunk = [0u8; 4096];
        while let Ok(read) = pipe.read(&mut chunk).await {
            if read == 0 {
                break;
            }
            self.push(&chunk[..read]);
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        let room = MAX_OUTPUT_BYTES.saturating_sub(self.buf.len());
        let take = bytes.len().min(room);
        self.buf.extend_from_slice(&bytes[..take]);
        self.truncated |= take < bytes.len();
    }

    fn into_string(self) -> String {
        let mut text = String::from_utf8_lossy(&self.buf).into_owned();
        if self.truncated {
            text.push_str("\n...[truncated]");
        }
        text
    }
}

#[derive(Debug)]
pub(crate) struct ScriptOutcome {
    success: bool,
    exit_code: Option<i32>,
    timed_out: bool,
    message: Option<String>,
    stdout: String,
    stderr: String,
    duration_ms: u128,
}

impl ScriptOutcome {
    fn failed(
        start: std::time::Instant,
        timed_out: bool,
        message: String,
        stdout: CappedOutput,
        stderr: CappedOutput,
    ) -> Self {
        Self {
            success: false,
            exit_code: None,
            timed_out,
            message: Some(message),
            stdout: stdout.into_string(),
            stderr: stderr.into_string(),
            duration_ms: start.elapsed().as_millis(),
        }
    }

    fn log(&self, path: &str) {
        if self.success {
            tracing::info!(
                "[completion-script] ok exit={:?} dur={}ms path={}",
                self.exit_code,
                self.duration_ms,
                path
            );
        } else {
            tracing::warn!(
                "[completion-script] failed exit={:?} timed_out={} dur={}ms msg={:?} stderr={}",
                self.exit_code,
                self.timed_out,
                self.duration_ms,
                self.message,
                self.stderr
            );
        }
        if !self.stdout.is_empty() {
            tracing::debug!("[completion-script] stdout={}", self.stdout);
        }
    }
}

pub(crate) async fn run_script(
    command: &str,
    args: Vec<String>,
    envs: Vec<(&'static str, String)>,
    timeout: Duration,
) -> ScriptOutcome {
    let start = std::time::Instant::now();
    let spawned = tokio::process::Command::new(command)
        .args(&args)
        .envs(envs)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn();
    let mut child = match spawned {
        Ok(child) => child,
        Err(e) => {
            return ScriptOutcome::failed(
                start,
                false,
                format!("spawn failed: {e}"),
                CappedOutput::default(),
                CappedOutput::default(),
            )
        }
    };
    let (stdout_pipe, stderr_pipe) = (child.stdout.take(), child.stderr.take());
    let mut stdout = CappedOutput::default();
    let mut stderr = CappedOutput::default();
    let waited = tokio::time::timeout(timeout, async {
        let (_, _, status) = tokio::join!(
            stdout.drain(stdout_pipe),
            stderr.drain(stderr_pipe),
            child.wait()
        );
        status
    })
    .await;
    match waited {
        Ok(Ok(status)) => ScriptOutcome {
            success: status.success(),
            exit_code: status.code(),
            timed_out: false,
            message: None,
            stdout: stdout.into_string(),
            stderr: stderr.into_string(),
            duration_ms: start.elapsed().as_millis(),
        },
        Ok(Err(e)) => {
            ScriptOutcome::failed(start, false, format!("wait failed: {e}"), stdout, stderr)
        }
        Err(_) => {
            if let Err(e) = child.kill().await {
                tracing::warn!("[completion-script] failed to kill timed out script: {e}");
            }
            ScriptOutcome::failed(
                start,
                true,
                format!("script timed out after {}ms", timeout.as_millis()),
                stdout,
                stderr,
            )
        }
    }
}

fn program_name(command: &str) -> String {
    Path::new(command)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| command.to_string())
}

pub(crate) struct PolicyTask {
    stop: CancellationToken,
    handle: JoinHandle<()>,
}

impl PolicyTask {
    pub(crate) async fn stop(self) {
        self.stop.cancel();
        if let Err(e) = self.handle.await {
            tracing::error!("headless policy task failed: {e}");
        }
    }
}

pub(crate) fn spawn(
    manager: Arc<TaskManager>,
    events: &EventBroadcaster,
    config_dir: &Path,
) -> PolicyTask {
    let user = load_config(&config_dir.join("user.json"), defaults::user_defaults());
    let retry = retry_settings(&user);
    let script = script_settings(&user);
    tracing::info!(
        "headless policies: auto-retry is {}",
        if retry.enabled { "on" } else { "off" }
    );
    if script.configured() {
        tracing::info!(
            "headless policies: completion script runs {}",
            program_name(&script.command)
        );
    } else {
        tracing::info!("headless policies: no completion script configured");
    }
    let receiver = events.subscribe();
    let stop = CancellationToken::new();
    let policy = Policy {
        manager,
        retry,
        script,
        status_keys: STATUS_KEYS.iter().map(|key| key.to_string()).collect(),
        attempts: HashMap::new(),
        self_started: HashSet::new(),
        script_ran: HashSet::new(),
        retries: HashMap::new(),
        scripts: JoinSet::new(),
    };
    let handle = tokio::spawn(policy.run(receiver, stop.clone()));
    PolicyTask { stop, handle }
}

struct Policy {
    manager: Arc<TaskManager>,
    retry: RetrySettings,
    script: ScriptSettings,
    status_keys: Vec<String>,
    attempts: HashMap<String, u32>,
    self_started: HashSet<String>,
    script_ran: HashSet<String>,
    retries: HashMap<String, (Instant, u32)>,
    scripts: JoinSet<()>,
}

async fn sleep_until_due(due: Option<Instant>) {
    match due {
        Some(due) => tokio::time::sleep_until(due).await,
        None => std::future::pending().await,
    }
}

impl Policy {
    async fn run(mut self, mut events: Receiver<EngineEvent>, stop: CancellationToken) {
        loop {
            let next_retry = self.retries.values().map(|(due, _)| *due).min();
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                received = events.recv() => match received {
                    Ok(event) => self.handle(event).await,
                    Err(RecvError::Lagged(missed)) => {
                        tracing::warn!("headless policies lagged by {missed} events");
                    }
                    Err(RecvError::Closed) => break,
                },
                _ = sleep_until_due(next_retry) => self.fire_due_retries().await,
                Some(_) = self.scripts.join_next(), if !self.scripts.is_empty() => {}
            }
        }
        loop {
            match events.try_recv() {
                Ok(event) => self.handle(event).await,
                Err(TryRecvError::Lagged(missed)) => {
                    tracing::warn!("headless policies lagged by {missed} events");
                }
                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
            }
        }
        self.retries.clear();
        while self.scripts.join_next().await.is_some() {}
    }

    async fn handle(&mut self, event: EngineEvent) {
        let gid = event.gid();
        let script_status = script_status_for(&event, self.script_ran.contains(gid))
            .filter(|_| self.script.configured());
        let is_error = matches!(event, EngineEvent::DownloadError { .. });
        match &event {
            EngineEvent::DownloadStart { .. } => {
                if !self.self_started.remove(gid) {
                    self.attempts.remove(gid);
                }
            }
            EngineEvent::DownloadStop { .. } | EngineEvent::DownloadComplete { .. } => {
                self.forget(gid);
                self.script_ran.remove(gid);
            }
            EngineEvent::BtDownloadComplete { seeding, .. } => {
                self.forget(gid);
                if *seeding && self.script.configured() {
                    self.script_ran.insert(gid.to_string());
                }
            }
            EngineEvent::DownloadPause { .. } | EngineEvent::DownloadError { .. } => {}
        }
        if script_status.is_none() && !(is_error && self.retry.enabled) {
            return;
        }
        let status = match self.manager.tell_status(gid, &self.status_keys).await {
            Ok(status) => status,
            Err(e) => {
                tracing::debug!("headless policies: {gid} is gone: {e}");
                return;
            }
        };
        if let Some(script_status) = script_status {
            self.start_script(&status, script_status);
        }
        if is_error {
            self.plan_retry(gid, &status);
        }
    }

    fn forget(&mut self, gid: &str) {
        self.attempts.remove(gid);
        self.self_started.remove(gid);
        self.retries.remove(gid);
    }

    fn start_script(&mut self, status: &Value, script_status: &'static str) {
        let path = if script_status == "complete" {
            completed_path(status)
        } else {
            task_path(status, false)
        };
        let hash = status
            .get("infoHash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let args = script_args(&self.script.args_template, &path, &hash, script_status);
        let envs = vec![
            ("RISUKO_PATH", path.clone()),
            ("RISUKO_HASH", hash),
            ("RISUKO_STATUS", script_status.to_string()),
        ];
        let command = self.script.command.clone();
        let timeout = Duration::from_millis(self.script.timeout_ms);
        self.scripts.spawn(async move {
            run_script(&command, args, envs, timeout).await.log(&path);
        });
    }

    fn plan_retry(&mut self, gid: &str, status: &Value) {
        let code = status.get("errorCode").and_then(Value::as_str);
        let attempts = self.attempts.get(gid).copied().unwrap_or(0);
        if should_retry(&self.retry, code, attempts) {
            let next = attempts.saturating_add(1);
            self.attempts.insert(gid.to_string(), next);
            let due = Instant::now() + retry_delay(&self.retry, next);
            self.retries.insert(gid.to_string(), (due, next));
        } else if self.retry.enabled && !is_permanent_error(code) {
            tracing::info!("[auto-retry] gave up on {gid} after {attempts} attempts");
        }
    }

    async fn fire_due_retries(&mut self) {
        let now = Instant::now();
        let due: Vec<(String, u32)> = self
            .retries
            .iter()
            .filter(|(_, (at, _))| *at <= now)
            .map(|(gid, (_, attempt))| (gid.clone(), *attempt))
            .collect();
        for (gid, attempt) in due {
            self.retries.remove(&gid);
            let still_failed = self
                .manager
                .tell_status(&gid, &self.status_keys[..1])
                .await
                .is_ok_and(|status| status.get("status").and_then(Value::as_str) == Some("error"));
            if !still_failed {
                continue;
            }
            self.self_started.insert(gid.clone());
            match self.manager.unpause(&gid).await {
                Ok(()) => tracing::info!("[auto-retry] resuming {gid}, attempt {attempt}"),
                Err(e) => {
                    self.self_started.remove(&gid);
                    tracing::warn!("[auto-retry] resuming {gid} failed: {e}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::options::EngineOptions;
    use crate::engine::session::SessionManager;
    use crate::engine::task::{DownloadTask, TaskKind, TaskStatus};
    use serde_json::json;
    use std::path::PathBuf;

    fn user(value: Value) -> Map<String, Value> {
        value.as_object().cloned().unwrap()
    }

    #[test]
    fn retry_settings_follow_the_defaults() {
        assert_eq!(
            retry_settings(&defaults::user_defaults()),
            RetrySettings {
                enabled: false,
                interval_secs: 5,
                exponential: false,
                max_attempts: 5,
            }
        );
    }

    #[test]
    fn retry_settings_accept_strings_and_clamp() {
        let settings = retry_settings(&user(json!({
            "auto-retry": "true",
            "auto-retry-interval": "7",
            "auto-retry-strategy": "Exponential",
            "auto-retry-max-attempts": "0",
        })));
        assert_eq!(
            settings,
            RetrySettings {
                enabled: true,
                interval_secs: 7,
                exponential: true,
                max_attempts: 0,
            }
        );

        let settings = retry_settings(&user(json!({
            "auto-retry": true,
            "auto-retry-interval": 0,
            "auto-retry-strategy": "static",
            "auto-retry-max-attempts": 5000,
        })));
        assert_eq!(
            (
                settings.interval_secs,
                settings.exponential,
                settings.max_attempts
            ),
            (1, false, 1000)
        );

        let settings = retry_settings(&user(json!({
            "auto-retry": "off",
            "auto-retry-interval": 9000,
            "auto-retry-max-attempts": "many",
        })));
        assert_eq!(
            (
                settings.enabled,
                settings.interval_secs,
                settings.max_attempts
            ),
            (false, 300, 5)
        );

        let settings = retry_settings(&user(json!({
            "auto-retry-interval": "soon",
            "auto-retry-max-attempts": -2,
        })));
        assert_eq!((settings.interval_secs, settings.max_attempts), (5, 5));
    }

    #[test]
    fn script_settings_accept_strings_and_clamp_the_timeout() {
        let settings = script_settings(&defaults::user_defaults());
        assert!(!settings.configured());
        assert_eq!(settings.timeout_ms, 30_000);

        let settings = script_settings(&user(json!({
            "completion-script-enabled": "true",
            "completion-script-command": "  /usr/local/bin/notify  ",
            "completion-script-args": "--out={path} {status}",
            "completion-script-timeout-ms": "500",
        })));
        assert_eq!(
            settings,
            ScriptSettings {
                enabled: true,
                command: "/usr/local/bin/notify".into(),
                args_template: "--out={path} {status}".into(),
                timeout_ms: 1_000,
            }
        );
        assert!(settings.configured());

        let settings = script_settings(&user(json!({
            "completion-script-enabled": true,
            "completion-script-command": "   ",
            "completion-script-timeout-ms": 900000,
        })));
        assert!(!settings.configured());
        assert_eq!(settings.timeout_ms, 300_000);

        let settings = script_settings(&user(json!({
            "completion-script-enabled": false,
            "completion-script-command": "/usr/local/bin/notify",
        })));
        assert!(!settings.configured());
    }

    #[test]
    fn permanent_errors_are_never_retried() {
        for code in [
            "300", "301", "302", "400", "401", "502", "530", "531", "533", "534", "540", "541",
            "542", "550", "551", "552", "553", "554", "315", " 401 ",
        ] {
            assert!(is_permanent_error(Some(code)), "{code}");
        }
        for code in [
            None,
            Some(""),
            Some("1"),
            Some("2"),
            Some("503"),
            Some("3150"),
        ] {
            assert!(!is_permanent_error(code), "{code:?}");
        }
    }

    #[test]
    fn retry_delay_doubles_with_the_exponential_strategy_up_to_the_cap() {
        let fixed = RetrySettings {
            enabled: true,
            interval_secs: 5,
            exponential: false,
            max_attempts: 5,
        };
        let doubling = RetrySettings {
            exponential: true,
            ..fixed.clone()
        };
        let slow = RetrySettings {
            interval_secs: 300,
            ..doubling.clone()
        };
        let secs = |settings: &RetrySettings, attempt| retry_delay(settings, attempt).as_secs();

        assert_eq!([1, 2, 3].map(|n| secs(&fixed, n)), [5, 5, 5]);
        assert_eq!([1, 2, 3].map(|n| secs(&doubling, n)), [5, 10, 20]);
        assert_eq!([1, 2, 3, 4].map(|n| secs(&slow, n)), [300, 600, 900, 900]);
        assert_eq!(secs(&doubling, u32::MAX), 900);
        assert_eq!(
            retry_delay(
                &RetrySettings {
                    interval_secs: 0,
                    ..fixed
                },
                1
            ),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn should_retry_needs_the_setting_a_transient_error_and_budget() {
        let settings = RetrySettings {
            enabled: true,
            interval_secs: 1,
            exponential: false,
            max_attempts: 2,
        };
        assert!(should_retry(&settings, Some("2"), 0));
        assert!(should_retry(&settings, None, 1));
        assert!(!should_retry(&settings, Some("2"), 2));
        assert!(!should_retry(&settings, Some("401"), 0));
        assert!(!should_retry(&settings, Some("315"), 0));
        assert!(!should_retry(
            &RetrySettings {
                enabled: false,
                ..settings.clone()
            },
            Some("2"),
            0
        ));
        let unlimited = RetrySettings {
            max_attempts: 0,
            ..settings
        };
        assert!(should_retry(&unlimited, Some("2"), 10_000));
    }

    #[test]
    fn script_args_replace_placeholders_inside_tokens() {
        assert_eq!(
            script_args(
                "--out={path}  {hash}\t{status} plain",
                "/dl/a b.iso",
                "abc",
                "complete"
            ),
            vec!["--out=/dl/a b.iso", "abc", "complete", "plain"]
        );
        assert!(script_args("   ", "p", "h", "s").is_empty());
    }

    #[test]
    fn completed_path_covers_torrent_folders_files_and_bare_dirs() {
        let multi = json!({
            "dir": "/dl/",
            "files": [{"path": "/dl/Show/a.mkv"}, {"path": "/dl/Show/b.mkv"}],
            "bittorrent": {"info": {"name": "Show: S01"}},
        });
        assert_eq!(
            completed_path(&multi),
            format!("/dl/{}", disk_component("Show: S01"))
        );

        let single = json!({"dir": "/dl", "files": [{"path": "/dl/file.iso.PART"}]});
        assert_eq!(completed_path(&single), "/dl/file.iso");
        assert_eq!(task_path(&single, false), "/dl/file.iso.PART");

        let single_torrent = json!({
            "dir": "/dl",
            "files": [{"path": "/dl/one.iso"}],
            "bittorrent": {"info": {"name": "one.iso"}},
        });
        assert_eq!(completed_path(&single_torrent), "/dl/one.iso");

        assert_eq!(
            completed_path(&json!({"dir": "/dl", "files": [{"path": ""}]})),
            "/dl"
        );
        assert_eq!(completed_path(&json!({"dir": "/dl"})), "/dl");
        assert_eq!(
            completed_path(&json!({
                "dir": "/dl",
                "files": [{"path": "/dl/x"}, {"path": "/dl/y"}],
                "bittorrent": {},
            })),
            "/dl"
        );
    }

    #[test]
    fn scripts_run_once_per_finished_download_and_on_every_error() {
        let bt = |seeding| EngineEvent::BtDownloadComplete {
            gid: "g".into(),
            seeding,
        };
        let done = |seeded| EngineEvent::DownloadComplete {
            gid: "g".into(),
            seeded,
        };
        let error = EngineEvent::DownloadError { gid: "g".into() };

        assert_eq!(script_status_for(&bt(true), false), Some("complete"));
        assert_eq!(script_status_for(&bt(true), true), None);
        assert_eq!(script_status_for(&bt(false), false), None);
        assert_eq!(script_status_for(&done(false), false), Some("complete"));
        assert_eq!(script_status_for(&done(false), true), None);
        assert_eq!(script_status_for(&done(true), false), None);
        assert_eq!(script_status_for(&error, false), Some("error"));
        assert_eq!(script_status_for(&error, true), Some("error"));
        assert_eq!(
            script_status_for(&EngineEvent::DownloadStart { gid: "g".into() }, false),
            None
        );
    }

    #[test]
    fn captured_output_keeps_only_the_cap() {
        let mut output = CappedOutput::default();
        output.push(&[b'a'; MAX_OUTPUT_BYTES - 1]);
        output.push(b"bc");
        output.push(b"d");
        let text = output.into_string();
        assert_eq!(
            text,
            format!("{}b\n...[truncated]", "a".repeat(MAX_OUTPUT_BYTES - 1))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_script_reports_success_and_output() {
        let outcome = run_script(
            "/bin/sh",
            vec!["-c".into(), "echo \"$RISUKO_STATUS\"; exit 0".into()],
            vec![("RISUKO_STATUS", "complete".into())],
            Duration::from_secs(5),
        )
        .await;
        assert!(outcome.success, "{outcome:?}");
        assert_eq!(outcome.exit_code, Some(0));
        assert_eq!(outcome.stdout, "complete\n");
        assert!(!outcome.timed_out);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_script_reports_a_non_zero_exit() {
        let outcome = run_script(
            "/bin/sh",
            vec!["-c".into(), "echo broken >&2; exit 3".into()],
            Vec::new(),
            Duration::from_secs(5),
        )
        .await;
        assert!(!outcome.success);
        assert_eq!(outcome.exit_code, Some(3));
        assert_eq!(outcome.stderr, "broken\n");
        assert!(!outcome.timed_out);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_script_kills_a_script_that_outlives_its_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("late");
        let outcome = run_script(
            "/bin/sh",
            vec![
                "-c".into(),
                format!("sleep 2; touch '{}'", marker.display()),
            ],
            Vec::new(),
            Duration::from_secs(1),
        )
        .await;
        assert!(!outcome.success);
        assert!(outcome.timed_out);
        assert!(outcome.duration_ms < 1900, "{outcome:?}");
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(!marker.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_script_reports_a_missing_program() {
        let outcome = run_script(
            "/nonexistent/risuko-missing-program",
            Vec::new(),
            Vec::new(),
            Duration::from_secs(1),
        )
        .await;
        assert!(!outcome.success);
        assert_eq!(outcome.exit_code, None);
        assert!(!outcome.timed_out);
        assert!(outcome
            .message
            .as_deref()
            .is_some_and(|m| m.starts_with("spawn failed")));
    }

    struct Harness {
        _dir: tempfile::TempDir,
        root: PathBuf,
        manager: Arc<TaskManager>,
        events: EventBroadcaster,
    }

    impl Harness {
        async fn new(task: DownloadTask) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().to_path_buf();
            SessionManager::new(&root).save(&[task]).unwrap();
            let mut system = Map::new();
            system.insert(
                "dir".into(),
                json!(root.join("downloads").to_string_lossy()),
            );
            for key in [
                "bt-enable-upnp",
                "bt-enable-lsd",
                "ed2k-enable-kad",
                "ed2k-enable-upload",
            ] {
                system.insert(key.into(), json!(false));
            }
            let options = EngineOptions::from_config(&system, &Map::new());
            let events = EventBroadcaster::default();
            let manager = Arc::new(
                TaskManager::new(&root, options, events.clone())
                    .await
                    .unwrap(),
            );
            manager.shutdown().await;
            Self {
                _dir: dir,
                root,
                manager,
                events,
            }
        }

        fn write_user(&self, user: Value) {
            std::fs::write(self.root.join("user.json"), user.to_string()).unwrap();
        }

        fn spawn(&self) -> PolicyTask {
            spawn(self.manager.clone(), &self.events, &self.root)
        }

        async fn status(&self, gid: &str) -> String {
            self.manager
                .tell_status(gid, &["status".to_string()])
                .await
                .unwrap()["status"]
                .as_str()
                .unwrap()
                .to_string()
        }

        async fn status_within(&self, gid: &str, wait: Duration) -> String {
            let deadline = Instant::now() + wait;
            loop {
                let status = self.status(gid).await;
                if status != "error" || Instant::now() >= deadline {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        #[cfg(unix)]
        fn record_script_runs(&self, delay_secs: u32) -> PathBuf {
            let script = self.root.join("record.sh");
            let marker = self.root.join("runs.txt");
            std::fs::write(
                &script,
                format!(
                    "sleep {delay_secs}\nprintf '%s %s\\n' \"$RISUKO_STATUS\" \"$1\" >> '{}'\n",
                    marker.display()
                ),
            )
            .unwrap();
            self.write_user(json!({
                "completion-script-enabled": true,
                "completion-script-command": "/bin/sh",
                "completion-script-args": format!("{} {{path}}", script.display()),
            }));
            marker
        }
    }

    fn failed_task(gid: &str, code: &str) -> DownloadTask {
        DownloadTask {
            gid: gid.into(),
            status: TaskStatus::Error,
            kind: TaskKind::Http,
            uris: vec!["http://127.0.0.1:9/file.bin".into()],
            dir: "/srv/downloads".into(),
            error_code: Some(code.into()),
            error_message: Some("connection refused".into()),
            ..Default::default()
        }
    }

    #[cfg(unix)]
    fn finished_task(gid: &str) -> DownloadTask {
        DownloadTask {
            gid: gid.into(),
            status: TaskStatus::Complete,
            kind: TaskKind::Http,
            uris: vec!["http://127.0.0.1:9/done.iso".into()],
            dir: "/srv/downloads".into(),
            files: vec![crate::engine::task::DownloadFile {
                index: "1".into(),
                path: "/srv/downloads/done.iso.part".into(),
                length: "4".into(),
                completed_length: "4".into(),
                selected: "true".into(),
                uris: Vec::new(),
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_failed_task_is_resumed_after_the_interval() {
        let harness = Harness::new(failed_task("f", "2")).await;
        harness.write_user(json!({"auto-retry": true, "auto-retry-interval": 1}));
        let policy = harness.spawn();

        harness
            .events
            .send(EngineEvent::DownloadError { gid: "f".into() });

        assert_eq!(
            harness.status_within("f", Duration::from_secs(5)).await,
            "waiting"
        );
        policy.stop().await;
    }

    #[tokio::test]
    async fn a_permanent_error_is_not_retried() {
        let harness = Harness::new(failed_task("p", "401")).await;
        harness.write_user(json!({"auto-retry": "true", "auto-retry-interval": "1"}));
        let policy = harness.spawn();

        harness
            .events
            .send(EngineEvent::DownloadError { gid: "p".into() });

        assert_eq!(
            harness
                .status_within("p", Duration::from_millis(2500))
                .await,
            "error"
        );
        policy.stop().await;
    }

    #[tokio::test]
    async fn settings_written_after_start_have_no_effect() {
        let harness = Harness::new(failed_task("r", "2")).await;
        harness.write_user(json!({"auto-retry": false, "auto-retry-interval": 1}));
        let policy = harness.spawn();
        harness.write_user(json!({"auto-retry": true, "auto-retry-interval": 1}));

        harness
            .events
            .send(EngineEvent::DownloadError { gid: "r".into() });

        assert_eq!(
            harness
                .status_within("r", Duration::from_millis(2500))
                .await,
            "error"
        );
        policy.stop().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_completion_just_before_stop_still_runs_its_script() {
        let harness = Harness::new(finished_task("d")).await;
        let runs = harness.record_script_runs(1);
        let policy = harness.spawn();

        harness.events.send(EngineEvent::DownloadComplete {
            gid: "d".into(),
            seeded: false,
        });
        tokio::time::timeout(Duration::from_secs(10), policy.stop())
            .await
            .expect("stop waits only for the running script");

        assert_eq!(
            std::fs::read_to_string(&runs).ok().as_deref(),
            Some("complete /srv/downloads/done.iso\n")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_repeated_bt_completion_runs_the_script_once() {
        let harness = Harness::new(finished_task("s")).await;
        let runs = harness.record_script_runs(0);
        let policy = harness.spawn();

        for _ in 0..2 {
            harness.events.send(EngineEvent::BtDownloadComplete {
                gid: "s".into(),
                seeding: true,
            });
        }
        harness.events.send(EngineEvent::DownloadComplete {
            gid: "s".into(),
            seeded: true,
        });
        tokio::time::timeout(Duration::from_secs(10), policy.stop())
            .await
            .expect("stop waits only for the running script");

        assert_eq!(
            std::fs::read_to_string(&runs).ok().as_deref(),
            Some("complete /srv/downloads/done.iso\n")
        );
    }
}
