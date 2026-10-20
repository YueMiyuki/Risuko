use std::time::Duration;

use serde::Serialize;
use tauri::{AppHandle, State};
use tauri_plugin_shell::process::CommandEvent;
use tauri_plugin_shell::ShellExt;
use tokio::time::timeout;

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

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionScriptOverrides {
    pub enabled: Option<bool>,
    pub command: Option<String>,
    pub args: Option<String>,
    pub timeout_ms: Option<u64>,
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

#[tauri::command]
pub async fn run_completion_script(
    handle: AppHandle,
    state: State<'_, AppState>,
    path: String,
    hash: Option<String>,
    status: String,
    overrides: Option<CompletionScriptOverrides>,
) -> Result<(), String> {
    let base = read_script_config(&state)?;
    let cfg = merge_with_overrides(base, overrides);
    if !cfg.enabled || cfg.command.is_empty() {
        return Ok(());
    }

    let hash = hash.unwrap_or_default();
    let args = build_args(&cfg.args_template, &path, &hash, &status);
    let env = vec![
        ("RISUKO_PATH".into(), path.clone()),
        ("RISUKO_HASH".into(), hash.clone()),
        ("RISUKO_STATUS".into(), status.clone()),
    ];

    let handle_clone = handle.clone();
    let command = cfg.command.clone();
    let timeout_ms = cfg.timeout_ms;
    tauri::async_runtime::spawn(async move {
        let result = execute(&handle_clone, &command, args, env, timeout_ms).await;
        if result.success {
            tracing::info!(
                "[completion-script] ok exit={:?} dur={}ms path={}",
                result.exit_code,
                result.duration_ms,
                path
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
    let command = command.trim().to_string();
    if command.is_empty() {
        return Err("command is empty".into());
    }
    let timeout_ms = timeout_ms
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1_000, MAX_TIMEOUT_MS);
    let path = "/tmp/risuko-test-file".to_string();
    let hash = "0000000000000000000000000000000000000000".to_string();
    let status = "complete".to_string();
    let parsed_args = build_args(&args, &path, &hash, &status);
    let env = vec![
        ("RISUKO_PATH".into(), path),
        ("RISUKO_HASH".into(), hash),
        ("RISUKO_STATUS".into(), status),
    ];
    Ok(execute(&handle, &command, parsed_args, env, timeout_ms).await)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
