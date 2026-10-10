use std::path::PathBuf;

use serde_json::{json, Map, Value};

use risuko_engine::config::defaults;
use risuko_engine::engine::options::EngineOptions;
use risuko_engine::standalone::{
    load_config, standalone_config_dir, StandaloneConfig, StandaloneEngine,
};

use crate::{
    ConfigAction, ConfigCommand, DownloadArgs, GidArgs, PauseArgs, RemoveArgs, ResumeArgs, RpcArgs,
    RssAction, RssCommand, ServeArgs, StatusArgs,
};
use risuko_cli::progress::{self, extract_filename, format_size, format_size_speed, parse_num};
use risuko_cli::rpc_client::RpcClient;

fn resolve_rpc_secret(explicit: Option<String>) -> Option<String> {
    explicit
        .filter(|s| !s.is_empty())
        .or_else(read_secret_from_config)
}

fn rpc_client(port: u16, secret: Option<String>) -> RpcClient {
    let host = resolve_rpc_host();
    RpcClient::new_with_host(&host, port, secret)
}

fn resolve_rpc_host() -> String {
    read_options_from_config().rpc_host()
}

fn read_secret_from_config() -> Option<String> {
    let secret = read_options_from_config().rpc_secret();
    if secret.is_empty() {
        None
    } else {
        Some(secret)
    }
}

fn read_options_from_config() -> EngineOptions {
    let config_dir = get_config_dir();
    let system = load_config(&config_dir.join("system.json"), defaults::system_defaults());
    let user = load_config(&config_dir.join("user.json"), defaults::user_defaults());
    EngineOptions::from_config(&system, &user)
}

pub async fn download(args: DownloadArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let mut client = rpc_client(args.rpc_port, secret.clone());
    let mut headless_engine = None;

    let status = client.engine_status().await;
    if let Some(problem) = status.problem() {
        return Err(problem.into());
    }
    if !status.is_present() {
        eprintln!("No running Risuko instance found. Starting headless engine...");
        let engine = start_headless_engine(args.rpc_port).await?;
        if secret.is_none() {
            client = rpc_client(args.rpc_port, engine.rpc_secret.clone());
        }
        headless_engine = Some(engine);
    }

    // Ctrl+C must still reach the shutdown below so an embedded engine saves its session
    let result = tokio::select! {
        r = do_download(&client, &args) => r,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\nInterrupted");
            Err("Interrupted".into())
        }
    };

    if let Some(engine) = headless_engine {
        engine.stop().await;
    }

    result
}

async fn do_download(
    client: &RpcClient,
    args: &DownloadArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut options = serde_json::Map::new();

    options.insert("split".into(), json!(args.threads.to_string()));

    if let Some(ref dir) = args.dir {
        options.insert("dir".into(), json!(dir));
    }
    if let Some(ref out) = args.out {
        options.insert("out".into(), json!(out));
    }
    if let Some(ref ua) = args.user_agent {
        options.insert("user-agent".into(), json!(ua));
    }
    if let Some(ref proxy) = args.proxy {
        options.insert("all-proxy".into(), json!(proxy));
    }
    if let Some(ref referer) = args.referer {
        options.insert("referer".into(), json!(referer));
    }
    if let Some(ref cookie) = args.cookie {
        if !args
            .headers
            .iter()
            .any(|h| h.to_lowercase().starts_with("cookie:"))
        {
            if let Some(arr) = options
                .entry("header".to_string())
                .or_insert_with(|| json!([]))
                .as_array_mut()
            {
                arr.push(json!(format!("Cookie: {}", cookie)));
            }
        }
    }
    if let Some(ref media_format) = args.media_format {
        options.insert("media-format".into(), json!(media_format));
    }
    if args.force_ytdlp {
        options.insert("force-ytdlp".into(), json!(true));
    }
    if let Some(ratio) = args.seed_ratio {
        options.insert("seed-ratio".into(), json!(ratio.to_string()));
    }
    if let Some(time) = args.seed_time {
        options.insert("seed-time".into(), json!(time.to_string()));
    }

    if !args.headers.is_empty() {
        let existing = options
            .get("header")
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();
        let mut all_headers: Vec<Value> = existing;
        for h in &args.headers {
            all_headers.push(json!(h));
        }
        options.insert("header".into(), json!(all_headers));
    }

    let primary = args.urls.first().map(String::as_str).unwrap_or_default();
    let is_torrent = primary.ends_with(".torrent") && std::path::Path::new(primary).exists();
    let is_media = risuko_engine::engine::media::is_media_uri(primary.trim());

    if args.urls.len() > 1 {
        let any_special = args.force_ytdlp
            || args.urls.iter().any(|u| {
                (u.ends_with(".torrent") && std::path::Path::new(u).exists())
                    || risuko_engine::engine::media::is_media_uri(u.trim())
            });
        if any_special {
            return Err(
                "Torrent, media, and yt-dlp downloads accept only one input; \
                 multiple URLs are supported only as HTTP mirrors"
                    .into(),
            );
        }
    }

    let gid = if is_torrent {
        let torrent_data = std::fs::read(primary)?;
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &torrent_data);
        let result = client
            .call(
                "risuko.addTorrent",
                vec![json!(b64), json!([]), json!(options)],
            )
            .await?;
        result.as_str().unwrap_or("").to_string()
    } else if is_media || args.force_ytdlp {
        let result = client
            .call("risuko.addMedia", vec![json!(primary), json!(options)])
            .await?;
        result.as_str().unwrap_or("").to_string()
    } else {
        let result = client
            .call("risuko.addUri", vec![json!(&args.urls), json!(options)])
            .await?;
        result.as_str().unwrap_or("").to_string()
    };

    if gid.is_empty() {
        return Err("Failed to get task GID from engine".into());
    }

    if !args.json {
        eprintln!("Download started (GID: {})", gid);
    }

    progress::watch_download(client, &gid, args.json).await
}

pub async fn status(args: StatusArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;

    if let Some(ref gid) = args.gid {
        let status = client.call("risuko.tellStatus", vec![json!(gid)]).await?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&status)?);
        } else {
            print_task_detail(&status);
        }
    } else {
        let all_keys = json!([
            "gid",
            "status",
            "totalLength",
            "completedLength",
            "downloadSpeed",
            "uploadSpeed",
            "files"
        ]);
        let active = client
            .call("risuko.tellActive", vec![all_keys.clone()])
            .await?;
        let waiting = client
            .call(
                "risuko.tellWaiting",
                vec![json!(0), json!(100), all_keys.clone()],
            )
            .await?;
        let stopped = client
            .call("risuko.tellStopped", vec![json!(0), json!(100), all_keys])
            .await?;

        let mut tasks: Vec<Value> = Vec::new();
        for list in [&active, &waiting, &stopped] {
            if let Some(arr) = list.as_array() {
                tasks.extend(arr.iter().cloned());
            }
        }

        if args.json {
            println!("{}", serde_json::to_string_pretty(&tasks)?);
        } else if tasks.is_empty() {
            println!("No downloads.");
        } else {
            print_task_table(&tasks);
        }
    }

    Ok(())
}

pub async fn pause(args: PauseArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    client.call("risuko.pause", vec![json!(args.gid)]).await?;
    println!("Paused: {}", args.gid);
    Ok(())
}

pub async fn resume(args: ResumeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    client.call("risuko.unpause", vec![json!(args.gid)]).await?;
    println!("Resumed: {}", args.gid);
    Ok(())
}

pub async fn remove(args: RemoveArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    for gid in &args.gids {
        match client.call("risuko.remove", vec![json!(gid)]).await {
            Ok(_) => println!("Removed: {gid}"),
            Err(e) => eprintln!("Failed to remove {gid}: {e}"),
        }
    }
    Ok(())
}

pub async fn pause_all(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    client.call("risuko.pauseAll", vec![]).await?;
    println!("All downloads paused.");
    Ok(())
}

pub async fn resume_all(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    client.call("risuko.unpauseAll", vec![]).await?;
    println!("All downloads resumed.");
    Ok(())
}

pub async fn global_stat(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    let stat = client.call("risuko.getGlobalStat", vec![]).await?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&stat)?);
    } else {
        let dl = parse_num(&stat, "downloadSpeed");
        let ul = parse_num(&stat, "uploadSpeed");
        let active = parse_num(&stat, "numActive");
        let waiting = parse_num(&stat, "numWaiting");
        let stopped = parse_num(&stat, "numStopped");

        println!(
            "Download: {}   Upload: {}",
            format_size_speed(dl),
            format_size_speed(ul)
        );
        println!(
            "Active: {}   Waiting: {}   Stopped: {}",
            active, waiting, stopped
        );
    }

    Ok(())
}

pub async fn files(args: GidArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    let result = client
        .call("risuko.getFiles", vec![json!(args.gid)])
        .await?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else if let Some(files) = result.as_array() {
        println!("{:<4} {:<10} {:<10} Path", "Idx", "Size", "Done");
        println!("{}", "-".repeat(60));
        for (i, f) in files.iter().enumerate() {
            let length = parse_num(f, "length");
            let completed = parse_num(f, "completedLength");
            let path = f.get("path").and_then(|v| v.as_str()).unwrap_or("-");
            println!(
                "{:<4} {:<10} {:<10} {}",
                i + 1,
                format_size(length),
                format_size(completed),
                path,
            );
        }
    }

    Ok(())
}

pub async fn peers(args: GidArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    let result = client
        .call("risuko.getPeers", vec![json!(args.gid)])
        .await?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else if let Some(peers) = result.as_array() {
        if peers.is_empty() {
            println!("No peers.");
        } else {
            println!("{:<22} {:<12} {:<12} Peer ID", "IP", "DL Speed", "UL Speed");
            println!("{}", "-".repeat(60));
            for p in peers {
                let ip = p.get("ip").and_then(|v| v.as_str()).unwrap_or("-");
                let port = p.get("port").and_then(|v| v.as_str()).unwrap_or("");
                let dl = parse_num(p, "downloadSpeed");
                let ul = parse_num(p, "uploadSpeed");
                let peer_id = p.get("peerId").and_then(|v| v.as_str()).unwrap_or("-");
                println!(
                    "{:<22} {:<12} {:<12} {}",
                    format!("{}:{}", ip, port),
                    format_size_speed(dl),
                    format_size_speed(ul),
                    peer_id,
                );
            }
        }
    }

    Ok(())
}

pub async fn purge(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    client.call("risuko.purgeDownloadResult", vec![]).await?;
    println!("Purged completed/error/removed download results.");
    Ok(())
}

pub async fn config(cmd: ConfigCommand) -> Result<(), Box<dyn std::error::Error>> {
    match cmd.action {
        ConfigAction::Get { key } => {
            let config_dir = get_config_dir();
            let system = load_config(&config_dir.join("system.json"), defaults::system_defaults());
            let user = load_config(&config_dir.join("user.json"), defaults::user_defaults());
            let mut merged = system;
            for (k, v) in user {
                merged.insert(k, v);
            }
            match merged.get(&key) {
                Some(val) => println!("{}", serde_json::to_string_pretty(val)?),
                None => println!("Key '{}' not found", key),
            }
            Ok(())
        }
        ConfigAction::Set { key, value } => {
            let config_dir = get_config_dir();
            let path = config_dir.join("user.json");
            let mut config = load_user_config_for_edit(&path)?;
            let parsed: Value =
                serde_json::from_str(&value).unwrap_or_else(|_| Value::String(value.clone()));
            config.insert(key.clone(), parsed);
            std::fs::create_dir_all(&config_dir)?;
            write_atomically(&path, serde_json::to_string_pretty(&config)?.as_bytes())?;
            println!("Set {} = {}", key, value);
            Ok(())
        }
        ConfigAction::List { json } => {
            let config_dir = get_config_dir();
            let system = load_config(&config_dir.join("system.json"), defaults::system_defaults());
            let user = load_config(&config_dir.join("user.json"), defaults::user_defaults());
            let mut merged = system;
            for (k, v) in user {
                merged.insert(k, v);
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&merged)?);
            } else {
                let mut keys: Vec<&String> = merged.keys().collect();
                keys.sort();
                for k in keys {
                    println!("{}: {}", k, merged[k]);
                }
            }
            Ok(())
        }
    }
}

pub async fn rss(cmd: RssCommand) -> Result<(), Box<dyn std::error::Error>> {
    match cmd.action {
        RssAction::Add {
            url,
            rpc_port,
            rpc_secret,
        } => {
            let secret = resolve_rpc_secret(rpc_secret);
            let client = rpc_client(rpc_port, secret);
            require_engine(&client).await?;
            let result = client.call("risuko.addRssFeed", vec![json!(url)]).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        RssAction::List {
            rpc_port,
            rpc_secret,
            json,
        } => {
            let secret = resolve_rpc_secret(rpc_secret);
            let client = rpc_client(rpc_port, secret);
            require_engine(&client).await?;
            let result = client.call("risuko.getRssFeeds", vec![]).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else if let Some(feeds) = result.as_array() {
                if feeds.is_empty() {
                    println!("No RSS feeds.");
                } else {
                    for feed in feeds {
                        let id = feed.get("id").and_then(|v| v.as_str()).unwrap_or("-");
                        let url = feed.get("url").and_then(|v| v.as_str()).unwrap_or("-");
                        let title = feed.get("title").and_then(|v| v.as_str()).unwrap_or("-");
                        println!("[{}] {} ({})", id, title, url);
                    }
                }
            }
            Ok(())
        }
        RssAction::Refresh {
            rpc_port,
            rpc_secret,
        } => {
            let secret = resolve_rpc_secret(rpc_secret);
            let client = rpc_client(rpc_port, secret);
            require_engine(&client).await?;
            client.call("risuko.refreshAllRssFeeds", vec![]).await?;
            println!("RSS feeds refreshed.");
            Ok(())
        }
        RssAction::Remove {
            id,
            rpc_port,
            rpc_secret,
        } => {
            let secret = resolve_rpc_secret(rpc_secret);
            let client = rpc_client(rpc_port, secret);
            require_engine(&client).await?;
            client.call("risuko.removeRssFeed", vec![json!(id)]).await?;
            println!("Removed RSS feed: {}", id);
            Ok(())
        }
    }
}

pub async fn serve(args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Starting Risuko engine on port {}", args.rpc_port);
    let engine = start_headless_engine(args.rpc_port).await?;
    tracing::info!("Risuko engine running, press Ctrl+C to stop");

    let signal = engine.shutdown_signal();
    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            res?;
            tracing::info!("Received Ctrl+C, shutting down...");
        }
        _ = signal.notified() => {
            tracing::info!("Shutdown requested via RPC, shutting down...");
        }
    }
    engine.stop().await;
    Ok(())
}

pub async fn shutdown(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;
    client.call("risuko.shutdown", vec![]).await?;
    println!("Shutdown request sent.");
    Ok(())
}

async fn start_headless_engine(
    rpc_port: u16,
) -> Result<StandaloneEngine, Box<dyn std::error::Error>> {
    Ok(StandaloneEngine::start(StandaloneConfig {
        config_dir: get_config_dir(),
        rpc_port: Some(rpc_port),
        enable_rpc: true,
        require_download_dir: false,
    })
    .await?)
}

async fn require_engine(client: &RpcClient) -> Result<(), Box<dyn std::error::Error>> {
    let status = client.engine_status().await;
    if let Some(problem) = status.problem() {
        return Err(problem.into());
    }
    if !status.is_present() {
        return Err(
            "No Risuko instance running. Start the app or use `risuko serve` first.".into(),
        );
    }
    Ok(())
}

fn get_config_dir() -> PathBuf {
    standalone_config_dir()
}

fn load_user_config_for_edit(
    path: &std::path::Path,
) -> Result<Map<String, Value>, Box<dyn std::error::Error>> {
    let data = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Map::new()),
        Err(e) => return Err(format!("Cannot read {}: {}", path.display(), e).into()),
    };
    match serde_json::from_str(&data) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(_) => Err(format!("{} is not a JSON object; fix it first", path.display()).into()),
        Err(e) => Err(format!("{} is not valid JSON ({}); fix it first", path.display(), e).into()),
    }
}

fn write_atomically(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

fn print_task_table(tasks: &[Value]) {
    println!(
        "{:<18} {:<10} {:<30} {:>9} {:>12} {:>10}",
        "GID", "Status", "Name", "Progress", "Speed", "Size"
    );
    println!("{}", "-".repeat(93));

    for task in tasks {
        let gid = task.get("gid").and_then(|v| v.as_str()).unwrap_or("-");
        let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("-");
        let total = parse_num(task, "totalLength");
        let completed = parse_num(task, "completedLength");
        let speed = parse_num(task, "downloadSpeed");
        let name = extract_filename(task, "-");

        let pct = if total > 0 {
            format!("{:.1}%", completed as f64 / total as f64 * 100.0)
        } else {
            "0.0%".into()
        };

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
            "{:<18} {:<10} {:<30} {:>9} {:>12} {:>10}",
            gid,
            status,
            display_name,
            pct,
            speed_str,
            format_size(total),
        );
    }
}

fn print_task_detail(task: &Value) {
    let gid = task.get("gid").and_then(|v| v.as_str()).unwrap_or("-");
    let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("-");
    let total = parse_num(task, "totalLength");
    let completed = parse_num(task, "completedLength");
    let dl_speed = parse_num(task, "downloadSpeed");
    let ul_speed = parse_num(task, "uploadSpeed");
    let name = extract_filename(task, "-");

    let pct = if total > 0 {
        format!("{:.1}%", completed as f64 / total as f64 * 100.0)
    } else {
        "0.0%".into()
    };

    println!("GID:       {}", gid);
    println!("Name:      {}", name);
    println!("Status:    {}", status);
    println!("Size:      {}", format_size(total));
    println!("Completed: {} ({})", format_size(completed), pct);
    println!("DL Speed:  {}", format_size_speed(dl_speed));
    println!("UL Speed:  {}", format_size_speed(ul_speed));

    if let Some(err) = task.get("errorMessage").and_then(|v| v.as_str()) {
        if !err.is_empty() {
            println!("Error:     {}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_edit_refuses_malformed_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user.json");
        assert!(load_user_config_for_edit(&path).unwrap().is_empty());

        std::fs::write(&path, "{ \"a\": 1,").unwrap();
        assert!(load_user_config_for_edit(&path).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{ \"a\": 1,");

        write_atomically(&path, b"{\"a\":1}").unwrap();
        assert_eq!(load_user_config_for_edit(&path).unwrap()["a"], 1);
        assert!(!dir.path().join("user.json.tmp").exists());
    }
}
