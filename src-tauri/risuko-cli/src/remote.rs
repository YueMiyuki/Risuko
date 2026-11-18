use std::path::Path;

use serde_json::Value;

use crate::progress::{extract_filename, format_size, format_size_speed, parse_num};
use crate::rpc_client::RpcClient;

type CliResult<T> = Result<T, Box<dyn std::error::Error>>;

pub fn resolve_rpc_secret(config_dir: &Path, explicit: Option<String>) -> Option<String> {
    explicit.filter(|secret| !secret.is_empty()).or_else(|| {
        let secret = risuko_engine::standalone::load_engine_options(config_dir).rpc_secret();
        (!secret.is_empty()).then_some(secret)
    })
}

pub fn rpc_client(config_dir: &Path, port: u16, secret: Option<String>) -> RpcClient {
    let host = risuko_engine::standalone::load_engine_options(config_dir).rpc_host();
    RpcClient::new_with_host(&host, port, secret)
}

pub async fn require_engine(client: &RpcClient, start_hint: &str) -> CliResult<()> {
    let status = client.engine_status().await;
    if let Some(problem) = status.problem() {
        return Err(problem.into());
    }
    if !status.is_present() {
        return Err(format!("No Risuko instance running. {start_hint}").into());
    }
    Ok(())
}

pub async fn connect(
    config_dir: &Path,
    port: u16,
    explicit_secret: Option<String>,
    start_hint: &str,
) -> CliResult<RpcClient> {
    let secret = resolve_rpc_secret(config_dir, explicit_secret);
    let client = rpc_client(config_dir, port, secret);
    require_engine(&client, start_hint).await?;
    Ok(client)
}

fn percent(completed: u64, total: u64) -> String {
    if total > 0 {
        format!("{:.1}%", completed as f64 / total as f64 * 100.0)
    } else {
        "0.0%".into()
    }
}

pub fn print_task_table(tasks: &[Value], gid_chars: usize) {
    let gid_width = gid_chars + 2;
    println!(
        "{:<gid_width$} {:<10} {:<30} {:>9} {:>12} {:>10}",
        "GID", "Status", "Name", "Progress", "Speed", "Size"
    );
    println!("{}", "-".repeat(gid_width + 75));

    for task in tasks {
        let gid = task.get("gid").and_then(|v| v.as_str()).unwrap_or("-");
        let gid: String = gid.chars().take(gid_chars).collect();
        let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("-");
        let total = parse_num(task, "totalLength");
        let completed = parse_num(task, "completedLength");
        let speed = parse_num(task, "downloadSpeed");
        let name = extract_filename(task, "-");

        let display_name = if name.chars().count() > 28 {
            let truncated: String = name.chars().take(25).collect();
            format!("{}...", truncated)
        } else {
            name
        };

        let speed_str = if speed > 0 {
            format_size_speed(speed)
        } else {
            "-".into()
        };

        println!(
            "{:<gid_width$} {:<10} {:<30} {:>9} {:>12} {:>10}",
            gid,
            status,
            display_name,
            percent(completed, total),
            speed_str,
            format_size(total),
        );
    }
}

pub fn print_task_detail(task: &Value) {
    let gid = task.get("gid").and_then(|v| v.as_str()).unwrap_or("-");
    let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("-");
    let total = parse_num(task, "totalLength");
    let completed = parse_num(task, "completedLength");
    let dl_speed = parse_num(task, "downloadSpeed");
    let ul_speed = parse_num(task, "uploadSpeed");
    let name = extract_filename(task, "-");

    println!("GID:       {}", gid);
    println!("Name:      {}", name);
    println!("Status:    {}", status);
    println!("Size:      {}", format_size(total));
    println!(
        "Completed: {} ({})",
        format_size(completed),
        percent(completed, total)
    );
    println!("DL Speed:  {}", format_size_speed(dl_speed));
    println!("UL Speed:  {}", format_size_speed(ul_speed));

    if let Some(err) = task.get("errorMessage").and_then(|v| v.as_str()) {
        if !err.is_empty() {
            println!("Error:     {}", err);
        }
    }
}
