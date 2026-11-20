use std::path::{Path, PathBuf};

use serde_json::{json, Map, Value};

use risuko_engine::standalone::{StandaloneConfig, StandaloneEngine};

use crate::args::{DownloadArgs, PauseArgs, RemoveArgs, ResumeArgs, ServeArgs, StatusArgs};
use crate::progress;
use crate::remote;
use crate::rpc_client::RpcClient;

type CliResult<T> = Result<T, Box<dyn std::error::Error>>;

const START_HINT: &str = "Start the app or use `risuko serve` first.";
const GID_CHARS: usize = 16;
const DOH_KEYS: [&str; 5] = [
    "doh-enable",
    "doh-url",
    "doh-bootstrap",
    "doh-fallback",
    "doh-provider",
];

pub struct EngineHost {
    pub config_dir: fn() -> PathBuf,
    pub require_download_dir: bool,
    pub before_start: fn(),
}

pub fn resolve_rpc_secret(host: &EngineHost, explicit: Option<String>) -> Option<String> {
    remote::resolve_rpc_secret(&(host.config_dir)(), explicit)
}

pub fn rpc_client(host: &EngineHost, port: u16, secret: Option<String>) -> RpcClient {
    remote::rpc_client(&(host.config_dir)(), port, secret)
}

pub async fn connect(host: &EngineHost, port: u16, secret: Option<String>) -> CliResult<RpcClient> {
    remote::connect(&(host.config_dir)(), port, secret, START_HINT).await
}

pub async fn start_headless_engine(
    host: &EngineHost,
    rpc_port: u16,
) -> CliResult<StandaloneEngine> {
    (host.before_start)();
    tracing::info!("Starting Risuko engine on port {}", rpc_port);
    Ok(StandaloneEngine::start(StandaloneConfig {
        config_dir: (host.config_dir)(),
        rpc_port: Some(rpc_port),
        enable_rpc: true,
        require_download_dir: host.require_download_dir,
        headless_policies: true,
    })
    .await?)
}

pub async fn connect_or_start_engine(
    host: &EngineHost,
    rpc_port: u16,
    rpc_secret: Option<String>,
) -> CliResult<(RpcClient, Option<StandaloneEngine>)> {
    let secret = resolve_rpc_secret(host, rpc_secret);
    let client = rpc_client(host, rpc_port, secret.clone());
    let status = client.engine_status().await;
    if let Some(problem) = status.problem() {
        return Err(problem.into());
    }
    if status.is_present() {
        return Ok((client, None));
    }
    eprintln!("No running Risuko instance found. Starting headless engine...");
    let engine = start_headless_engine(host, rpc_port).await?;
    engine
        .manager
        .change_global_option(own_engine_options())
        .await;
    let client = match secret {
        Some(_) => client,
        None => rpc_client(host, rpc_port, engine.rpc_secret.clone()),
    };
    Ok((client, Some(engine)))
}

pub async fn add_torrent(
    client: &RpcClient,
    torrent: &[u8],
    options: &Map<String, Value>,
) -> CliResult<String> {
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, torrent);
    let result = client
        .call(
            "risuko.addTorrent",
            vec![json!(b64), json!([]), json!(options)],
        )
        .await?;
    Ok(result.as_str().unwrap_or("").to_string())
}

pub async fn download(host: &EngineHost, args: DownloadArgs) -> CliResult<()> {
    let (client, headless_engine) =
        connect_or_start_engine(host, args.rpc_port, args.rpc_secret.clone()).await?;

    let mut previous_doh = None;
    // Ctrl+C must still reach the shutdown below so an embedded engine saves its session
    let result = tokio::select! {
        r = do_download(&client, &args, &mut previous_doh) => r,
        _ = crate::signal::shutdown_signal() => {
            eprintln!("\nInterrupted");
            Err("Interrupted".into())
        }
    };

    if let Some(saved) = previous_doh {
        let _ = client
            .call("risuko.changeGlobalOption", vec![json!(saved)])
            .await;
    }
    if let Some(engine) = headless_engine {
        engine.stop().await;
    }

    result
}

async fn do_download(
    client: &RpcClient,
    args: &DownloadArgs,
    previous_doh: &mut Option<Map<String, Value>>,
) -> CliResult<()> {
    let mut options = serde_json::Map::new();

    options.insert("split".into(), json!(args.threads.to_string()));

    if let Some(ref dir) = args.dir {
        options.insert("dir".into(), json!(absolute_dir(dir)));
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

    let doh = doh_overrides(args.doh_url.as_deref(), args.doh_bootstrap.as_deref());
    if !doh.is_empty() {
        *previous_doh = client
            .call("risuko.getGlobalOption", vec![])
            .await
            .ok()
            .and_then(|v| v.as_object().map(doh_restore));
        client
            .call("risuko.changeGlobalOption", vec![json!(doh)])
            .await?;
    }

    let gid = if is_torrent {
        add_torrent(client, &std::fs::read(primary)?, &options).await?
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

fn absolute_dir(dir: &str) -> String {
    if Path::new(dir).is_absolute() {
        return dir.to_string();
    }
    std::path::absolute(dir)
        .ok()
        .and_then(|path| path.into_os_string().into_string().ok())
        .unwrap_or_else(|| dir.to_string())
}

fn own_engine_options() -> Map<String, Value> {
    let mut options = Map::new();
    options.insert(
        risuko_engine::engine::rpc_paths::ALLOW_ANY_DIR_KEY.into(),
        json!(true),
    );
    options
}

fn doh_overrides(url: Option<&str>, bootstrap: Option<&str>) -> Map<String, Value> {
    let mut doh = Map::new();
    if let Some(url) = url {
        doh.insert("doh-enable".into(), json!(true));
        doh.insert("doh-url".into(), json!(url));
    }
    if let Some(bootstrap) = bootstrap {
        doh.insert("doh-bootstrap".into(), json!(bootstrap));
    } else if url.is_some() {
        doh.insert("doh-bootstrap".into(), json!(""));
    }
    doh
}

fn doh_restore(current: &Map<String, Value>) -> Map<String, Value> {
    let mut saved: Map<String, Value> = DOH_KEYS
        .iter()
        .filter_map(|key| Some((key.to_string(), current.get(*key)?.clone())))
        .collect();
    saved.entry("doh-enable").or_insert(json!(false));
    saved
}

pub async fn status(host: &EngineHost, args: StatusArgs) -> CliResult<()> {
    let client = connect(host, args.rpc_port, args.rpc_secret.clone()).await?;

    if let Some(ref gid) = args.gid {
        let status = client.call("risuko.tellStatus", vec![json!(gid)]).await?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&status)?);
        } else {
            remote::print_task_detail(&status);
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
            remote::print_task_table(&tasks, GID_CHARS);
        }
    }

    Ok(())
}

pub async fn pause(host: &EngineHost, args: PauseArgs) -> CliResult<()> {
    let client = connect(host, args.rpc_port, args.rpc_secret.clone()).await?;
    client.call("risuko.pause", vec![json!(args.gid)]).await?;
    println!("Paused: {}", args.gid);
    Ok(())
}

pub async fn resume(host: &EngineHost, args: ResumeArgs) -> CliResult<()> {
    let client = connect(host, args.rpc_port, args.rpc_secret.clone()).await?;
    client.call("risuko.unpause", vec![json!(args.gid)]).await?;
    println!("Resumed: {}", args.gid);
    Ok(())
}

pub async fn remove(host: &EngineHost, args: RemoveArgs) -> CliResult<()> {
    let client = connect(host, args.rpc_port, args.rpc_secret.clone()).await?;
    let mut failed = 0usize;
    for gid in &args.gids {
        match client.call("risuko.remove", vec![json!(gid)]).await {
            Ok(_) => println!("Removed: {gid}"),
            Err(e) => {
                failed += 1;
                eprintln!("Failed to remove {gid}: {e}");
            }
        }
    }
    if failed > 0 {
        return Err(format!("{failed} removal(s) failed").into());
    }
    Ok(())
}

pub async fn serve(host: &EngineHost, args: ServeArgs) -> CliResult<()> {
    let engine = start_headless_engine(host, args.rpc_port).await?;
    tracing::info!("Risuko engine running, press Ctrl+C to stop");

    let signal = engine.shutdown_signal();
    tokio::select! {
        res = crate::signal::shutdown_signal() => {
            res?;
            tracing::info!("Received stop signal, shutting down...");
        }
        _ = signal.notified() => {
            tracing::info!("Shutdown requested via RPC, shutting down...");
        }
    }
    engine.stop().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_own_engine_lets_the_command_choose_its_folder() {
        assert_eq!(
            Value::Object(own_engine_options()),
            json!({"rpc-allow-any-dir": true})
        );
    }

    #[test]
    fn a_relative_dir_is_sent_as_an_absolute_one() {
        let here = std::env::current_dir().unwrap();
        assert_eq!(PathBuf::from(absolute_dir("saved")), here.join("saved"));
        assert_eq!(
            PathBuf::from(absolute_dir("saved/deeper")),
            here.join("saved").join("deeper")
        );
        assert_eq!(PathBuf::from(absolute_dir(".")), here);
        assert!(Path::new(&absolute_dir("../saved")).is_absolute());

        let absolute = std::env::temp_dir().join("a").join(".").join("b");
        let absolute = absolute.to_str().unwrap();
        assert_eq!(absolute_dir(absolute), absolute);
        assert_eq!(absolute_dir(""), "");
    }

    #[test]
    fn doh_overrides_follow_the_flags() {
        assert!(doh_overrides(None, None).is_empty());
        assert_eq!(
            Value::Object(doh_overrides(Some("https://dns.example/q"), None)),
            json!({"doh-enable": true, "doh-url": "https://dns.example/q", "doh-bootstrap": ""})
        );
        assert_eq!(
            Value::Object(doh_overrides(
                Some("https://dns.example/q"),
                Some("1.1.1.1")
            )),
            json!({"doh-enable": true, "doh-url": "https://dns.example/q", "doh-bootstrap": "1.1.1.1"})
        );
        assert_eq!(
            Value::Object(doh_overrides(None, Some("1.1.1.1"))),
            json!({"doh-bootstrap": "1.1.1.1"})
        );
    }

    #[test]
    fn doh_restore_keeps_only_the_doh_options() {
        let current = json!({
            "doh-enable": "true",
            "doh-url": "https://dns.example/q",
            "doh-bootstrap": "",
            "doh-fallback": "true",
            "doh-provider": "custom",
            "split": "16",
            "dir": "/tmp"
        });
        assert_eq!(
            Value::Object(doh_restore(current.as_object().unwrap())),
            json!({
                "doh-enable": "true",
                "doh-url": "https://dns.example/q",
                "doh-bootstrap": "",
                "doh-fallback": "true",
                "doh-provider": "custom"
            })
        );
    }

    #[test]
    fn doh_restore_turns_doh_off_when_it_was_unset() {
        let current = json!({"split": "16", "doh-url": "https://dns.example/q"});
        assert_eq!(
            Value::Object(doh_restore(current.as_object().unwrap())),
            json!({"doh-enable": false, "doh-url": "https://dns.example/q"})
        );
    }
}
