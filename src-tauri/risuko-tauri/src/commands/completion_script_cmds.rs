use std::time::Duration;

use risuko_engine::engine;
use serde::Serialize;
use serde_json::Value;
use tauri::{AppHandle, State};
use tauri_plugin_shell::process::CommandEvent;
use tauri_plugin_shell::ShellExt;
use tokio::time::timeout;

use super::completion_script_approval::{self as approval, Requester, ScriptPair};
use crate::state::AppState;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MAX_TIMEOUT_MS: u64 = 5 * 60_000;
const MAX_OUTPUT_BYTES: usize = 8 * 1024;

#[derive(Default)]
struct ScriptConfig {
    enabled: bool,
    command: String,
    args_template: String,
    timeout_ms: u64,
}

fn read_script_config(state: &AppState) -> Result<ScriptConfig, String> {
    let cfg = state.config.lock().map_err(|e| e.to_string())?;
    let user = cfg.get_user_config();

    let enabled = user
        .get("completion-script-enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let command = user
        .get("completion-script-command")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let args_template = user
        .get("completion-script-args")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let timeout_ms = user
        .get("completion-script-timeout-ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1_000, MAX_TIMEOUT_MS);

    Ok(ScriptConfig {
        enabled,
        command,
        args_template,
        timeout_ms,
    })
}

fn replace_placeholders(token: &str, path: &str, hash: &str, status: &str) -> String {
    token
        .replace("{hash}", hash)
        .replace("{status}", status)
        .replace("{path}", path)
}

fn build_args(template: &str, path: &str, hash: &str, status: &str) -> Vec<String> {
    template
        .split_whitespace()
        .map(|tok| replace_placeholders(tok, path, hash, status))
        .collect()
}

#[derive(Serialize)]
pub struct ScriptRunResult {
    success: bool,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    duration_ms: u128,
    timed_out: bool,
    message: Option<String>,
}

#[derive(Default)]
struct CappedOutput {
    buf: Vec<u8>,
    truncated: bool,
}

impl CappedOutput {
    fn push_line(&mut self, line: &[u8]) {
        let room = MAX_OUTPUT_BYTES.saturating_sub(self.buf.len());
        if room == 0 {
            self.truncated = true;
            return;
        }
        let take = line.len().min(room);
        self.buf.extend_from_slice(&line[..take]);
        if take < room {
            self.buf.push(b'\n');
        } else {
            self.truncated = true;
        }
    }

    fn into_string(self) -> String {
        let mut text = String::from_utf8_lossy(&self.buf).into_owned();
        if self.truncated {
            text.push_str("\n...[truncated]");
        }
        text
    }
}

fn failure_result(
    start: std::time::Instant,
    timed_out: bool,
    message: String,
    stdout: String,
    stderr: String,
) -> ScriptRunResult {
    ScriptRunResult {
        success: false,
        exit_code: None,
        stdout,
        stderr,
        duration_ms: start.elapsed().as_millis(),
        timed_out,
        message: Some(message),
    }
}

async fn execute(
    handle: &AppHandle,
    command: &str,
    args: Vec<String>,
    env_vars: Vec<(String, String)>,
    timeout_ms: u64,
) -> ScriptRunResult {
    let start = std::time::Instant::now();
    let shell = handle.shell();
    let mut cmd = shell.command(command).args(args);
    for (k, v) in env_vars {
        cmd = cmd.env(k, v);
    }

    let (mut rx, child) = match cmd.spawn() {
        Ok(spawned) => spawned,
        Err(e) => {
            return failure_result(
                start,
                false,
                format!("spawn failed: {e}"),
                String::new(),
                String::new(),
            )
        }
    };

    let mut stdout = CappedOutput::default();
    let mut stderr = CappedOutput::default();
    let mut code = None;
    let drain = async {
        while let Some(event) = rx.recv().await {
            match event {
                CommandEvent::Stdout(line) => stdout.push_line(&line),
                CommandEvent::Stderr(line) => stderr.push_line(&line),
                CommandEvent::Terminated(payload) => code = payload.code,
                _ => {}
            }
        }
    };
    if timeout(Duration::from_millis(timeout_ms), drain)
        .await
        .is_err()
    {
        if let Err(e) = child.kill() {
            tracing::warn!("[completion-script] failed to kill timed out script: {e}");
        }
        return failure_result(
            start,
            true,
            format!("script timed out after {timeout_ms}ms"),
            stdout.into_string(),
            stderr.into_string(),
        );
    }

    ScriptRunResult {
        success: code == Some(0),
        exit_code: code,
        stdout: stdout.into_string(),
        stderr: stderr.into_string(),
        duration_ms: start.elapsed().as_millis(),
        timed_out: false,
        message: None,
    }
}

const COMMAND_KEY: &str = "risuko-completion-script-command";
const ARGS_KEY: &str = "risuko-completion-script-args";
const ENABLED_KEY: &str = "risuko-completion-script-enabled";
const TIMEOUT_KEY: &str = "risuko-completion-script-timeout-ms";

#[derive(Default, Debug, PartialEq)]
struct CompletionScriptOverrides {
    enabled: Option<bool>,
    command: Option<String>,
    args: Option<String>,
    timeout_ms: Option<u64>,
}

fn option_enabled(value: &Value) -> bool {
    match value {
        Value::Bool(flag) => *flag,
        Value::String(text) => text == "true",
        Value::Number(number) => number.as_f64() == Some(1.0),
        _ => false,
    }
}

fn option_timeout_ms(value: &Value) -> Option<u64> {
    let parsed = match value {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    (parsed.is_finite() && parsed > 0.0).then_some(parsed as u64)
}

fn overrides_from_options(options: &Value) -> Option<CompletionScriptOverrides> {
    let options = options.as_object()?;
    let command = options.get(COMMAND_KEY);
    let args = options.get(ARGS_KEY);
    let enabled = options.get(ENABLED_KEY);
    let timeout = options.get(TIMEOUT_KEY);
    if command.is_none() && args.is_none() && enabled.is_none() && timeout.is_none() {
        return None;
    }
    Some(CompletionScriptOverrides {
        enabled: enabled.map(option_enabled),
        command: command.and_then(Value::as_str).map(str::to_string),
        args: args.and_then(Value::as_str).map(str::to_string),
        timeout_ms: timeout.and_then(option_timeout_ms),
    })
}

const TASK_STATUS_KEYS: &[&str] = &["status", "seeder", "dir", "files", "bittorrent", "infoHash"];

struct TaskFacts {
    overrides: Option<CompletionScriptOverrides>,
    status: Value,
}

async fn lookup_task(gid: &str) -> Option<TaskFacts> {
    let manager = engine::get_manager().await?;
    let keys: Vec<String> = TASK_STATUS_KEYS.iter().map(|key| key.to_string()).collect();
    let status = manager.tell_status(gid, &keys).await.ok()?;
    let overrides = manager
        .get_option(gid)
        .await
        .ok()
        .and_then(|options| overrides_from_options(&options));
    Some(TaskFacts { overrides, status })
}

fn merge_with_overrides(
    mut cfg: ScriptConfig,
    overrides: Option<CompletionScriptOverrides>,
) -> ScriptConfig {
    let Some(o) = overrides else {
        return cfg;
    };
    if let Some(enabled) = o.enabled {
        cfg.enabled = enabled;
    }
    if let Some(command) = o.command {
        let trimmed = command.trim().to_string();
        if !trimmed.is_empty() {
            cfg.command = trimmed;
            if o.enabled.is_none() {
                cfg.enabled = true;
            }
        }
    }
    if let Some(args) = o.args {
        cfg.args_template = args;
    }
    if let Some(timeout_ms) = o.timeout_ms {
        cfg.timeout_ms = timeout_ms.clamp(1_000, MAX_TIMEOUT_MS);
    }
    cfg
}

#[derive(Debug)]
struct ScriptRun {
    pair: ScriptPair,
    path: String,
    hash: String,
    timeout_ms: u64,
}

#[derive(Debug, PartialEq)]
enum Skip {
    Disabled,
    TaskMissing,
    StateMismatch(String),
}

fn state_fits(status: &Value, outcome: &str) -> bool {
    let field = |key: &str| status.get(key).and_then(Value::as_str);
    match (outcome, field("status")) {
        ("error", Some("error" | "paused")) | ("complete", Some("complete")) => true,
        ("complete", Some("active")) => field("seeder") == Some("true"),
        _ => false,
    }
}

fn prepare_run(
    base: ScriptConfig,
    task: Option<TaskFacts>,
    outcome: &str,
) -> Result<ScriptRun, Skip> {
    let Some(task) = task else {
        return Err(if base.enabled && !base.command.is_empty() {
            Skip::TaskMissing
        } else {
            Skip::Disabled
        });
    };
    let cfg = merge_with_overrides(base, task.overrides);
    if !cfg.enabled || cfg.command.is_empty() {
        return Err(Skip::Disabled);
    }
    if !state_fits(&task.status, outcome) {
        let state = task.status.get("status").and_then(Value::as_str);
        return Err(Skip::StateMismatch(state.unwrap_or_default().to_string()));
    }
    let hash = task
        .status
        .get("infoHash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(ScriptRun {
        pair: ScriptPair::new(&cfg.command, &cfg.args_template),
        path: engine::task_path_from_status(&task.status, outcome == "complete"),
        hash,
        timeout_ms: cfg.timeout_ms,
    })
}

#[tauri::command]
pub async fn run_completion_script(
    handle: AppHandle,
    state: State<'_, AppState>,
    gid: String,
    status: String,
) -> Result<(), String> {
    if status != "complete" && status != "error" {
        return Err(format!("unsupported status: {status}"));
    }
    let base = read_script_config(&state)?;
    let run = match prepare_run(base, lookup_task(&gid).await, &status) {
        Ok(run) => run,
        Err(Skip::Disabled) => return Ok(()),
        Err(Skip::TaskMissing) => {
            tracing::warn!("[completion-script] task {gid} was not found, script not run");
            return Ok(());
        }
        Err(Skip::StateMismatch(state)) => {
            tracing::warn!(
                "[completion-script] task {gid} is {state:?}, which does not fit {status}, script not run"
            );
            return Ok(());
        }
    };

    let args = build_args(&run.pair.args, &run.path, &run.hash, &status);
    let env = vec![
        ("RISUKO_PATH".into(), run.path.clone()),
        ("RISUKO_HASH".into(), run.hash.clone()),
        ("RISUKO_STATUS".into(), status),
    ];

    tauri::async_runtime::spawn(async move {
        if !approval::ensure_approved(&handle, &run.pair, Requester::App).await {
            tracing::warn!(
                "[completion-script] task {gid}: {}, script not run",
                run.pair.refusal()
            );
            return;
        }
        let result = execute(&handle, &run.pair.command, args, env, run.timeout_ms).await;
        if result.success {
            tracing::info!(
                "[completion-script] ok exit={:?} dur={}ms path={}",
                result.exit_code,
                result.duration_ms,
                run.path
            );
        } else {
            tracing::warn!(
                "[completion-script] failed exit={:?} timed_out={} dur={}ms msg={:?} stderr={}",
                result.exit_code,
                result.timed_out,
                result.duration_ms,
                result.message,
                result.stderr
            );
        }
    });

    Ok(())
}

#[tauri::command]
pub async fn test_completion_script(
    handle: AppHandle,
    command: String,
    args: String,
    timeout_ms: Option<u64>,
) -> Result<ScriptRunResult, String> {
    let pair = ScriptPair::new(&command, &args);
    if pair.command.is_empty() {
        return Err("command is empty".into());
    }
    if !approval::ensure_approved(&handle, &pair, Requester::User).await {
        return Err(pair.refusal());
    }
    let timeout_ms = timeout_ms
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1_000, MAX_TIMEOUT_MS);
    let path = "/tmp/risuko-test-file".to_string();
    let hash = "0000000000000000000000000000000000000000".to_string();
    let status = "complete".to_string();
    let parsed_args = build_args(&pair.args, &path, &hash, &status);
    let env = vec![
        ("RISUKO_PATH".into(), path),
        ("RISUKO_HASH".into(), hash),
        ("RISUKO_STATUS".into(), status),
    ];
    Ok(execute(&handle, &pair.command, parsed_args, env, timeout_ms).await)
}

#[tauri::command]
pub async fn approve_completion_script(
    handle: AppHandle,
    command: String,
    args: String,
) -> Result<bool, String> {
    let pair = ScriptPair::new(&command, &args);
    if pair.command.is_empty() {
        return Err("command is empty".into());
    }
    let approved = approval::ensure_approved(&handle, &pair, Requester::User).await;
    if !approved && pair.too_long() {
        return Err(pair.refusal());
    }
    Ok(approved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_args_substitutes_placeholders() {
        let args = build_args(
            "--path {path} --hash {hash} --status {status}",
            "/tmp/file.iso",
            "abc123",
            "complete",
        );
        assert_eq!(
            args,
            vec![
                "--path",
                "/tmp/file.iso",
                "--hash",
                "abc123",
                "--status",
                "complete",
            ]
        );
    }

    #[test]
    fn capped_output_drops_bytes_past_the_cap() {
        let mut out = CappedOutput::default();
        let line = vec![b'a'; 1000];
        for _ in 0..100 {
            out.push_line(&line);
        }
        assert!(out.buf.len() <= MAX_OUTPUT_BYTES);
        assert!(out.truncated);
        assert!(out.into_string().ends_with("...[truncated]"));
    }

    #[test]
    fn capped_output_keeps_short_output_whole() {
        let mut out = CappedOutput::default();
        out.push_line(b"one");
        out.push_line(b"two");
        assert_eq!(out.into_string(), "one\ntwo\n");
    }

    #[test]
    fn build_args_handles_empty_template() {
        assert!(build_args("", "p", "h", "s").is_empty());
    }

    fn cfg(enabled: bool, command: &str, args: &str, timeout_ms: u64) -> ScriptConfig {
        ScriptConfig {
            enabled,
            command: command.to_string(),
            args_template: args.to_string(),
            timeout_ms,
        }
    }

    #[test]
    fn override_command_implies_enabled_when_global_disabled() {
        let merged = merge_with_overrides(
            cfg(false, "/global", "", 30_000),
            Some(CompletionScriptOverrides {
                enabled: None,
                command: Some("/per-task".into()),
                args: None,
                timeout_ms: None,
            }),
        );
        assert!(merged.enabled);
        assert_eq!(merged.command, "/per-task");
    }

    #[test]
    fn override_can_explicitly_disable_per_task() {
        let merged = merge_with_overrides(
            cfg(true, "/global", "--g", 30_000),
            Some(CompletionScriptOverrides {
                enabled: Some(false),
                command: Some("/per-task".into()),
                args: None,
                timeout_ms: None,
            }),
        );
        assert!(!merged.enabled);
    }

    #[test]
    fn override_args_and_timeout_replace_global() {
        let merged = merge_with_overrides(
            cfg(true, "/global", "--global", 5_000),
            Some(CompletionScriptOverrides {
                enabled: None,
                command: None,
                args: Some("--task {path}".into()),
                timeout_ms: Some(10_000),
            }),
        );
        assert_eq!(merged.args_template, "--task {path}");
        assert_eq!(merged.timeout_ms, 10_000);
        assert_eq!(merged.command, "/global");
    }

    #[test]
    fn exact_keys_produce_the_script_overrides() {
        assert_eq!(
            overrides_from_options(&json!({
                "risuko-completion-script-command": "/usr/bin/notify",
                "risuko-completion-script-args": "{path}",
                "risuko-completion-script-enabled": "true",
                "risuko-completion-script-timeout-ms": "30000",
            })),
            Some(CompletionScriptOverrides {
                enabled: Some(true),
                command: Some("/usr/bin/notify".into()),
                args: Some("{path}".into()),
                timeout_ms: Some(30_000),
            })
        );
    }

    #[test]
    fn respelled_keys_are_ignored() {
        assert_eq!(
            overrides_from_options(&json!({
                "risukoCompletionScriptCommand": "sh",
                "risuko_completion_script_command": "sh",
                "RISUKO-COMPLETION-SCRIPT-COMMAND": "sh",
                "risuko.completion.script.command": "sh",
            })),
            None
        );
    }

    #[test]
    fn a_respelled_command_does_not_ride_along_with_an_exact_enabled_key() {
        assert_eq!(
            overrides_from_options(&json!({
                "risuko-completion-script-enabled": false,
                "risukoCompletionScriptCommand": "sh",
            })),
            Some(CompletionScriptOverrides {
                enabled: Some(false),
                ..Default::default()
            })
        );
    }

    #[test]
    fn a_non_string_command_and_a_non_positive_timeout_are_dropped() {
        assert_eq!(
            overrides_from_options(&json!({
                "risuko-completion-script-command": 42,
                "risuko-completion-script-timeout-ms": 0,
            })),
            Some(CompletionScriptOverrides::default())
        );
        assert_eq!(
            overrides_from_options(&json!({
                "risuko-completion-script-args": "{path}",
                "risuko-completion-script-timeout-ms": -5,
            })),
            Some(CompletionScriptOverrides {
                args: Some("{path}".into()),
                ..Default::default()
            })
        );
    }

    #[test]
    fn missing_options_or_no_script_keys_give_none() {
        assert_eq!(overrides_from_options(&Value::Null), None);
        assert_eq!(overrides_from_options(&json!("sh")), None);
        assert_eq!(overrides_from_options(&json!([])), None);
        assert_eq!(overrides_from_options(&json!({})), None);
        assert_eq!(overrides_from_options(&json!({"dir": "/tmp"})), None);
    }

    #[test]
    fn a_timeout_given_as_a_string_converts() {
        let parsed = |value: Value| {
            overrides_from_options(&json!({ "risuko-completion-script-timeout-ms": value }))
                .and_then(|o| o.timeout_ms)
        };
        assert_eq!(parsed(json!("5000")), Some(5_000));
        assert_eq!(parsed(json!(" 5000 ")), Some(5_000));
        assert_eq!(parsed(json!(5000)), Some(5_000));
        assert_eq!(parsed(json!("")), None);
        assert_eq!(parsed(json!("abc")), None);
        assert_eq!(parsed(json!("Infinity")), None);
        assert_eq!(parsed(json!("NaN")), None);
        assert_eq!(parsed(json!("-5")), None);
        assert_eq!(parsed(json!(null)), None);
        assert_eq!(parsed(json!(true)), None);
    }

    #[test]
    fn a_fractional_or_huge_timeout_still_converts() {
        let parsed = |value: Value| {
            overrides_from_options(&json!({ "risuko-completion-script-timeout-ms": value }))
                .and_then(|o| o.timeout_ms)
        };
        assert_eq!(parsed(json!(1500.5)), Some(1_500));
        assert_eq!(parsed(json!("1500.5")), Some(1_500));
        assert_eq!(parsed(json!(1e30)), Some(u64::MAX));
    }

    #[test]
    fn enabled_is_true_only_for_true_the_string_true_and_one() {
        let parsed = |value: Value| {
            overrides_from_options(&json!({ "risuko-completion-script-enabled": value }))
                .and_then(|o| o.enabled)
        };
        assert_eq!(parsed(json!(true)), Some(true));
        assert_eq!(parsed(json!("true")), Some(true));
        assert_eq!(parsed(json!(1)), Some(true));
        assert_eq!(parsed(json!(1.0)), Some(true));
        assert_eq!(parsed(json!("1")), Some(false));
        assert_eq!(parsed(json!("True")), Some(false));
        assert_eq!(parsed(json!("false")), Some(false));
        assert_eq!(parsed(json!(false)), Some(false));
        assert_eq!(parsed(json!(0)), Some(false));
        assert_eq!(parsed(json!(2)), Some(false));
        assert_eq!(parsed(json!(null)), Some(false));
    }

    #[test]
    fn a_non_string_command_or_args_is_ignored() {
        assert_eq!(
            overrides_from_options(&json!({
                "risuko-completion-script-command": ["sh"],
                "risuko-completion-script-args": 7,
                "risuko-completion-script-enabled": true,
            })),
            Some(CompletionScriptOverrides {
                enabled: Some(true),
                ..Default::default()
            })
        );
    }

    fn facts(status: Value, overrides: Option<CompletionScriptOverrides>) -> Option<TaskFacts> {
        Some(TaskFacts { overrides, status })
    }

    fn enabled() -> ScriptConfig {
        cfg(true, "/usr/bin/notify", "{path}", 30_000)
    }

    #[test]
    fn the_path_comes_from_the_task_in_its_finished_or_unfinished_form() {
        let plain = |state: &str| json!({"status": state, "dir": "/dl", "files": [{"path": "/dl/file.iso.part"}]});
        let run = prepare_run(enabled(), facts(plain("complete"), None), "complete").unwrap();
        assert_eq!(run.path, "/dl/file.iso");
        assert_eq!(run.pair, ScriptPair::new("/usr/bin/notify", "{path}"));
        assert_eq!(run.hash, "");
        assert_eq!(
            prepare_run(enabled(), facts(plain("error"), None), "error")
                .unwrap()
                .path,
            "/dl/file.iso.part"
        );

        let seeding = json!({
            "status": "active",
            "seeder": "true",
            "dir": "/dl",
            "files": [{"path": "/dl/Show/a.mkv"}, {"path": "/dl/Show/b.mkv"}],
            "bittorrent": {"info": {"name": "Show"}},
            "infoHash": "abc123",
        });
        let run = prepare_run(enabled(), facts(seeding, None), "complete").unwrap();
        assert_eq!(run.path, "/dl/Show");
        assert_eq!(run.hash, "abc123");

        let single_torrent = json!({
            "status": "complete",
            "dir": "/dl",
            "files": [{"path": "/dl/one.iso"}],
            "bittorrent": {"info": {"name": "one.iso"}},
        });
        assert_eq!(
            prepare_run(enabled(), facts(single_torrent, None), "complete")
                .unwrap()
                .path,
            "/dl/one.iso"
        );

        let magnet = json!({"status": "error", "dir": "/dl", "files": [], "bittorrent": {}});
        assert_eq!(
            prepare_run(enabled(), facts(magnet, None), "error")
                .unwrap()
                .path,
            "/dl"
        );
    }

    #[test]
    fn a_task_whose_state_does_not_fit_the_outcome_is_skipped() {
        let task = |state: &str, seeder: &str| {
            facts(
                json!({"status": state, "seeder": seeder, "dir": "/dl"}),
                None,
            )
        };
        for (state, seeder, outcome) in [
            ("active", "false", "complete"),
            ("paused", "true", "complete"),
            ("waiting", "false", "complete"),
            ("removed", "false", "complete"),
            ("error", "false", "complete"),
            ("complete", "false", "error"),
            ("active", "true", "error"),
            ("waiting", "false", "error"),
        ] {
            assert_eq!(
                prepare_run(enabled(), task(state, seeder), outcome).unwrap_err(),
                Skip::StateMismatch(state.to_string()),
                "{state} {seeder} {outcome}"
            );
        }
        assert!(prepare_run(enabled(), task("complete", "false"), "complete").is_ok());
        assert!(prepare_run(enabled(), task("active", "true"), "complete").is_ok());
        assert!(prepare_run(enabled(), task("error", "false"), "error").is_ok());
        assert!(prepare_run(enabled(), task("paused", "false"), "error").is_ok());
        assert_eq!(
            prepare_run(enabled(), facts(json!({}), None), "complete").unwrap_err(),
            Skip::StateMismatch(String::new())
        );
        assert_eq!(
            prepare_run(enabled(), None, "complete").unwrap_err(),
            Skip::TaskMissing
        );
    }

    #[test]
    fn a_disabled_script_is_skipped_quietly_whatever_the_task() {
        let mismatched = || facts(json!({"status": "active"}), None);
        assert_eq!(
            prepare_run(cfg(false, "/usr/bin/notify", "", 30_000), None, "complete").unwrap_err(),
            Skip::Disabled
        );
        assert_eq!(
            prepare_run(cfg(true, "", "", 30_000), None, "error").unwrap_err(),
            Skip::Disabled
        );
        assert_eq!(
            prepare_run(
                cfg(false, "/usr/bin/notify", "", 30_000),
                mismatched(),
                "complete"
            )
            .unwrap_err(),
            Skip::Disabled
        );
        let off = Some(CompletionScriptOverrides {
            enabled: Some(false),
            ..Default::default()
        });
        assert_eq!(
            prepare_run(
                enabled(),
                facts(json!({"status": "complete"}), off),
                "complete"
            )
            .unwrap_err(),
            Skip::Disabled
        );
    }

    #[tokio::test]
    async fn a_task_script_runs_only_when_its_merged_pair_is_allowed() {
        use super::super::completion_script_approval::{Answer, ApprovalStore};
        use std::future::ready;
        use std::sync::Mutex;

        let dir = tempfile::tempdir().unwrap();
        let store = ApprovalStore::open(dir.path().join("approvals.json"));
        let task = facts(
            json!({"status": "complete", "dir": "/dl", "files": [{"path": "/dl/a.iso"}]}),
            Some(CompletionScriptOverrides {
                command: Some(" /per-task ".into()),
                args: Some("--file {path}".into()),
                ..Default::default()
            }),
        );
        let run = prepare_run(cfg(false, "/global", "--g", 30_000), task, "complete").unwrap();
        assert_eq!(run.pair, ScriptPair::new("/per-task", "--file {path}"));
        assert_eq!(
            build_args(&run.pair.args, &run.path, &run.hash, "complete"),
            vec!["--file", "/dl/a.iso"]
        );

        let asked = Mutex::new(Vec::new());
        let answer = |answer: Answer| {
            let asked = &asked;
            move |pair: ScriptPair| {
                asked.lock().unwrap().push(pair);
                ready(answer)
            }
        };
        assert!(
            !store
                .ensure_approved(&run.pair, Requester::App, answer(Answer::Declined))
                .await
        );
        assert!(
            !store
                .ensure_approved(&run.pair, Requester::App, answer(Answer::Allowed))
                .await
        );
        assert!(
            store
                .ensure_approved(&run.pair, Requester::User, answer(Answer::Allowed))
                .await
        );
        assert!(
            store
                .ensure_approved(&run.pair, Requester::App, answer(Answer::Declined))
                .await
        );
        assert_eq!(
            *asked.lock().unwrap(),
            vec![run.pair.clone(), run.pair.clone()]
        );
    }
}
