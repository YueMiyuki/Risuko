use serde_json::{json, Value};

use super::headless;
use super::progress::{self, extract_filename, format_size, format_size_speed, parse_num};
use super::rpc_client::RpcClient;
use super::{
    DownloadArgs, ExtractCookiesArgs, PauseArgs, RemoveArgs, ResumeArgs, ServeArgs, StatusArgs,
};
use risuko_engine::engine::media::is_media_uri;
use risuko_engine::engine::options::EngineOptions;

fn resolve_rpc_secret(explicit: Option<String>) -> Option<String> {
    explicit
        .filter(|s| !s.is_empty())
        .or_else(read_secret_from_config)
}

fn rpc_client(port: u16, secret: Option<String>) -> RpcClient {
    RpcClient::new_with_host(&resolve_rpc_host(), port, secret)
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
    risuko_engine::standalone::load_engine_options(&headless::get_config_dir())
}

pub async fn download(args: DownloadArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut secret = resolve_rpc_secret(args.rpc_secret.clone());
    let client = rpc_client(args.rpc_port, secret.clone());
    let mut headless_engine = None;

    let status = client.engine_status().await;
    if let Some(problem) = status.problem() {
        return Err(problem.into());
    }
    if !status.is_present() {
        eprintln!("No running Risuko instance found. Starting headless engine...");
        let engine = headless::start_headless_engine(args.rpc_port).await?;
        if secret.is_none() {
            secret = engine.rpc_secret().map(|s| s.to_string());
        }
        headless_engine = Some(engine);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let client = rpc_client(args.rpc_port, secret);
    let result = do_download(&client, &args).await;

    if let Some(engine) = headless_engine {
        engine.shutdown().await;
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
    let mut previous_doh: Option<serde_json::Map<String, Value>> = None;
    let mut doh_global = serde_json::Map::new();
    if let Some(ref doh_url) = args.doh_url {
        doh_global.insert("doh-enable".into(), json!(true));
        doh_global.insert("doh-url".into(), json!(doh_url));
    }
    if let Some(ref doh_bootstrap) = args.doh_bootstrap {
        doh_global.insert("doh-bootstrap".into(), json!(doh_bootstrap));
    } else if args.doh_url.is_some() {
        doh_global.insert("doh-bootstrap".into(), json!(""));
    }
    if !doh_global.is_empty() {
        let global_opts = client
            .call("risuko.getGlobalOption", vec![])
            .await
            .ok()
            .and_then(|v| v.as_object().cloned());
        if let Some(opts) = global_opts {
            let mut saved = serde_json::Map::new();
            for key in [
                "doh-enable",
                "doh-url",
                "doh-bootstrap",
                "doh-fallback",
                "doh-provider",
            ] {
                if let Some(val) = opts.get(key) {
                    saved.insert(key.to_string(), val.clone());
                }
            }
            if !saved.contains_key("doh-enable") {
                saved.insert("doh-enable".into(), json!(false));
            }
            previous_doh = Some(saved);
        }
        client
            .call("risuko.changeGlobalOption", vec![json!(doh_global)])
            .await?;
    }

    let result = do_download_inner(client, args, &mut options).await;

    if let Some(ref saved) = previous_doh {
        let _ = client
            .call("risuko.changeGlobalOption", vec![json!(saved)])
            .await;
    }

    result
}

async fn do_download_inner(
    client: &RpcClient,
    args: &DownloadArgs,
    options: &mut serde_json::Map<String, Value>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let is_media = is_media_uri(primary.trim());

    if args.urls.len() > 1 {
        let any_special = args.force_ytdlp
            || args.urls.iter().any(|u| {
                (u.ends_with(".torrent") && std::path::Path::new(u).exists())
                    || is_media_uri(u.trim())
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
    let secret = resolve_rpc_secret(args.rpc_secret);
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

        let mut tasks: Vec<Value> = Vec::new();
        if let Some(arr) = active.as_array() {
            tasks.extend(arr.iter().cloned());
        }

        let page_size = 100;
        for method in ["risuko.tellWaiting", "risuko.tellStopped"] {
            let mut offset = 0;
            loop {
                let page = client
                    .call(
                        method,
                        vec![json!(offset), json!(page_size), all_keys.clone()],
                    )
                    .await?;
                let arr = page.as_array();
                let count = arr.map(|a| a.len()).unwrap_or(0);
                if let Some(items) = arr {
                    tasks.extend(items.iter().cloned());
                }
                if count < page_size {
                    break;
                }
                offset += page_size;
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
    let secret = resolve_rpc_secret(args.rpc_secret);
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;

    client.call("risuko.pause", vec![json!(args.gid)]).await?;
    println!("Paused: {}", args.gid);
    Ok(())
}

pub async fn resume(args: ResumeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret);
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;

    client.call("risuko.unpause", vec![json!(args.gid)]).await?;
    println!("Resumed: {}", args.gid);
    Ok(())
}

pub async fn remove(args: RemoveArgs) -> Result<(), Box<dyn std::error::Error>> {
    let secret = resolve_rpc_secret(args.rpc_secret);
    let client = rpc_client(args.rpc_port, secret);
    require_engine(&client).await?;

    client.call("risuko.remove", vec![json!(args.gid)]).await?;
    println!("Removed: {}", args.gid);
    Ok(())
}

async fn require_engine(client: &RpcClient) -> Result<(), Box<dyn std::error::Error>> {
    let status = client.engine_status().await;
    if let Some(problem) = status.problem() {
        return Err(problem.into());
    }
    if !status.is_present() {
        return Err("No Risuko instance running. Start the app or run a download first.".into());
    }
    Ok(())
}

fn print_task_table(tasks: &[Value]) {
    println!(
        "{:<12} {:<10} {:<30} {:>9} {:>12} {:>10}",
        "GID", "Status", "Name", "Progress", "Speed", "Size"
    );
    println!("{}", "-".repeat(87));

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

        let gid_short: String = gid.chars().take(10).collect();
        println!(
            "{:<12} {:<10} {:<30} {:>9} {:>12} {:>10}",
            gid_short,
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

pub async fn serve(args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Starting Risuko engine on port {}...", args.rpc_port);
    let engine = headless::start_headless_engine(args.rpc_port).await?;
    eprintln!("Risuko engine running. Press Ctrl+C to stop.");

    tokio::select! {
        res = tokio::signal::ctrl_c() => { res?; }
        _ = engine.shutdown_requested() => {
            eprintln!("Shutdown requested via RPC.");
        }
    }
    eprintln!("\nShutting down...");
    engine.shutdown().await;
    Ok(())
}

pub async fn extract_cookies(args: ExtractCookiesArgs) -> Result<(), Box<dyn std::error::Error>> {
    let host_cookies = risuko_cookies::cookies_for_url(&args.browser, &args.url).await?;
    let json = serde_json::to_string(&host_cookies)?;
    match args.out {
        Some(path) => write_secret_file(path.as_ref(), json.as_bytes())?,
        None => println!("{json}"),
    }
    Ok(())
}

fn write_secret_file(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(data)
}

#[cfg(all(test, unix))]
mod secret_file_tests {
    use super::write_secret_file;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn existing_secret_output_is_made_private_before_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cookies.json");
        std::fs::write(&path, b"old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        write_secret_file(&path, b"secret").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"secret");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
