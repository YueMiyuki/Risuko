use serde_json::{json, Value};
use std::io::Write;
use std::time::{Duration, Instant};

use crate::rpc_client::RpcClient;

const POLL_KEYS: &[&str] = &[
    "gid",
    "status",
    "totalLength",
    "completedLength",
    "downloadSpeed",
    "uploadSpeed",
    "errorMessage",
];

const POLL_INTERVAL: Duration = Duration::from_secs(1);

pub async fn watch_download(
    client: &RpcClient,
    gid: &str,
    json_output: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut name_known = false;
    let mut cached_name = String::from("unknown");

    loop {
        let with_files = json_output || !name_known;
        let keys: Vec<Value> = POLL_KEYS
            .iter()
            .chain(with_files.then_some(&"files"))
            .map(|k| json!(k))
            .collect();
        let status = client
            .call("risuko.tellStatus", vec![json!(gid), json!(keys)])
            .await?;

        let task_status = status
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let total: u64 = parse_num(&status, "totalLength");
        let completed: u64 = parse_num(&status, "completedLength");
        let speed: u64 = parse_num(&status, "downloadSpeed");
        if with_files {
            let name = extract_filename(&status, "");
            if !name.is_empty() {
                cached_name = name;
                name_known = true;
            }
        }
        let name = cached_name.clone();

        if json_output {
            println!("{}", serde_json::to_string(&status)?);
        } else {
            print_progress(&name, task_status, total, completed, speed);
        }

        match task_status {
            "complete" => {
                if !json_output {
                    println!();
                    print_summary(&name, total, start.elapsed());
                }
                return Ok(());
            }
            "error" => {
                if !json_output {
                    println!();
                }
                let msg = status
                    .get("errorMessage")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown error");
                return Err(format!("Download failed: {}", msg).into());
            }
            "removed" => {
                if !json_output {
                    println!();
                }
                return Err("Download was removed".into());
            }
            _ => {}
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub fn parse_num(val: &Value, key: &str) -> u64 {
    val.get(key)
        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or(v.as_u64()))
        .unwrap_or(0)
}

pub fn extract_filename(status: &Value, fallback: &str) -> String {
    status
        .get("files")
        .and_then(|f| f.as_array())
        .and_then(|arr| arr.first())
        .and_then(|f| f.get("path"))
        .and_then(|p| p.as_str())
        .and_then(|p| {
            std::path::Path::new(p)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
        })
        .map(|n| n.strip_suffix(".part").unwrap_or(&n).to_string())
        .unwrap_or_else(|| fallback.into())
}

fn print_progress(name: &str, status: &str, total: u64, completed: u64, speed: u64) {
    let pct = if total > 0 {
        (completed as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    let bar_width = 30;
    let filled = if total > 0 {
        ((completed as f64 / total as f64 * bar_width as f64) as usize).min(bar_width)
    } else {
        0
    };
    let empty = bar_width - filled;

    let eta = if speed > 0 && total > completed {
        let remaining = total - completed;
        let secs = remaining / speed;
        format_duration(Duration::from_secs(secs))
    } else {
        "-".to_string()
    };

    let display_name = if name.chars().count() > 30 {
        let truncated: String = name.chars().take(27).collect();
        format!("{}...", truncated)
    } else {
        name.to_string()
    };

    print!(
        "\r{} [{}{}] {:>5.1}%  {}  ETA: {}  [{}]",
        display_name,
        "█".repeat(filled),
        "░".repeat(empty),
        pct,
        format_size_speed(speed),
        eta,
        status,
    );
    let _ = std::io::stdout().flush();
}

fn print_summary(name: &str, total: u64, elapsed: Duration) {
    let avg_speed = if elapsed.as_secs() > 0 {
        total / elapsed.as_secs()
    } else {
        total
    };

    println!("Download complete: {}", name);
    println!(
        "  Size: {}  Time: {}  Avg speed: {}",
        format_size(total),
        format_duration(elapsed),
        format_size_speed(avg_speed),
    );
}

pub fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

pub fn format_size_speed(bytes_per_sec: u64) -> String {
    if bytes_per_sec == 0 {
        return "-".to_string();
    }
    format!("{}/s", format_size(bytes_per_sec))
}

pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let s = secs % 60;
    if hours > 0 {
        format!("{:02}:{:02}:{:02}", hours, mins, s)
    } else {
        format!("{:02}:{:02}", mins, s)
    }
}
