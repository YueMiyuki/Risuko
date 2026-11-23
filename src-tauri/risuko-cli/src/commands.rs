use std::path::PathBuf;

use serde_json::{json, Map, Value};

use risuko_engine::config::defaults;
use risuko_engine::config::keys::{self, KeyScope};
use risuko_engine::standalone::{load_config, standalone_config_dir, StandaloneEngine};

use crate::{ConfigAction, ConfigCommand, CreateTorrentArgs, GidArgs, RpcArgs, TorrentFormat};
use risuko_cli::progress::{format_size, format_size_speed, parse_num};
use risuko_cli::rpc_client::RpcClient;
use risuko_cli::shared::{self, add_torrent, connect_or_start_engine, EngineHost};
use risuko_cli::signal::exit_on_next_signal;

pub const HOST: EngineHost = EngineHost {
    config_dir: get_config_dir,
    require_download_dir: false,
    before_start: || {},
};

async fn connect(
    port: u16,
    secret: Option<String>,
) -> Result<RpcClient, Box<dyn std::error::Error>> {
    shared::connect(&HOST, port, secret).await
}

pub async fn pause_all(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect(args.rpc_port, args.rpc_secret.clone()).await?;
    client.call("risuko.pauseAll", vec![]).await?;
    println!("All downloads paused.");
    Ok(())
}

pub async fn resume_all(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect(args.rpc_port, args.rpc_secret.clone()).await?;
    client.call("risuko.unpauseAll", vec![]).await?;
    println!("All downloads resumed.");
    Ok(())
}

pub async fn global_stat(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect(args.rpc_port, args.rpc_secret.clone()).await?;
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
    let client = connect(args.rpc_port, args.rpc_secret.clone()).await?;
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
    let client = connect(args.rpc_port, args.rpc_secret.clone()).await?;
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
    let client = connect(args.rpc_port, args.rpc_secret.clone()).await?;
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
            let file = set_config_value(&get_config_dir(), &key, &value)?;
            println!("Set {} = {} ({})", key, value, file);
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

pub async fn shutdown(args: RpcArgs) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect(args.rpc_port, args.rpc_secret.clone()).await?;
    client.call("risuko.shutdown", vec![]).await?;
    println!("Shutdown request sent.");
    Ok(())
}

pub async fn create_torrent(args: CreateTorrentArgs) -> Result<(), Box<dyn std::error::Error>> {
    use risuko_bt::core::MetaVersion;
    use risuko_bt::create::{plan, throttled, CreateOptions};
    use risuko_bt::torrent::split_tracker_list;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let source =
        std::fs::canonicalize(&args.path).map_err(|e| format!("{}: {e}", args.path.display()))?;
    let name = source
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or("cannot make a torrent of a filesystem root")?;
    let output = std::path::absolute(
        args.output
            .unwrap_or_else(|| PathBuf::from(format!("{name}.torrent"))),
    )?;
    if output.exists() && !args.force {
        return Err(format!(
            "{} already exists; pass --force to overwrite it",
            output.display()
        )
        .into());
    }
    let defaults = CreateOptions::default();
    let opts = CreateOptions {
        version: match args.format {
            TorrentFormat::V1 => MetaVersion::V1,
            TorrentFormat::V2 => MetaVersion::V2,
            TorrentFormat::Hybrid => MetaVersion::Hybrid,
        },
        piece_length: args
            .piece_size
            .as_deref()
            .map(parse_piece_size)
            .transpose()?,
        trackers: args
            .trackers
            .iter()
            .map(|tier| split_tracker_list(tier).map(str::to_string).collect())
            .collect(),
        web_seeds: args.web_seeds,
        comment: args.comment,
        source: args.source,
        private: args.private,
        creation_date: if args.no_date {
            None
        } else {
            defaults.creation_date
        },
        exclude: vec![output.clone()],
        ..defaults
    };

    let cancel = Arc::new(AtomicBool::new(false));
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    let show_progress = !args.json;
    let mut hashing = tokio::task::spawn_blocking({
        let cancel = cancel.clone();
        move || {
            let planned = plan(&source, &opts)?;
            let padding = planned.hybrid_padding();
            if opts.version == MetaVersion::Hybrid && padding > planned.total_length {
                eprintln!(
                    "Warning: hybrid pads these files with {} of zeros for {} of data; --format v2 needs no padding",
                    format_size(padding),
                    format_size(planned.total_length)
                );
            }
            planned.create(
                &opts,
                throttled(std::time::Duration::from_millis(200), |done, total| {
                    if show_progress {
                        eprint!(
                            "\rHashing {:5.1}%  {} / {}   ",
                            done as f64 * 100.0 / total.max(1) as f64,
                            format_size(done),
                            format_size(total)
                        );
                    }
                }),
                &cancel,
            )
        }
    });
    let created = tokio::select! {
        result = &mut hashing => result?,
        _ = &mut interrupted => {
            cancel.store(true, Ordering::Relaxed);
            let _ = hashing.await;
            if show_progress {
                eprintln!();
            }
            return Err("Interrupted".into());
        }
    };
    if show_progress {
        eprintln!();
    }
    let created = created?;
    std::fs::write(&output, &created.bytes).map_err(|e| format!("{}: {e}", output.display()))?;

    let (gid, headless_engine) = if args.seed {
        let (gid, engine) = seed_created_torrent(
            args.rpc_port,
            args.rpc_secret,
            &created.bytes,
            &created.seed_dir,
        )
        .await?;
        (Some(gid), engine)
    } else {
        (None, None)
    };

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "torrent": output.to_string_lossy(),
                "name": created.name,
                "infoHash": created.info_hash_v1.map(|h| h.to_hex()),
                "infoHashV2": created.info_hash_v2.map(|h| h.to_hex()),
                "pieceLength": created.piece_length,
                "totalLength": created.total_length,
                "files": created.file_count,
                "magnet": created.magnet,
                "gid": gid,
            }))?
        );
    } else {
        println!("Created {}", output.display());
        println!(
            "  {} file(s), {}, {} pieces",
            created.file_count,
            format_size(created.total_length),
            format_size(created.piece_length.into())
        );
        if let Some(v1) = created.info_hash_v1 {
            println!("  Info hash v1: {}", v1.to_hex());
        }
        if let Some(v2) = created.info_hash_v2 {
            println!("  Info hash v2: {}", v2.to_hex());
        }
        println!("  {}", created.magnet);
        if let Some(gid) = &gid {
            println!("Seeding (GID: {gid})");
        }
    }

    if let Some(engine) = headless_engine {
        eprintln!("Seeding from a headless engine, press Ctrl+C to stop");
        let signal = engine.shutdown_signal();
        tokio::select! {
            _ = &mut interrupted => {}
            _ = signal.notified() => {}
        }
        exit_on_next_signal();
        engine.stop().await;
    }
    Ok(())
}

async fn seed_created_torrent(
    rpc_port: u16,
    rpc_secret: Option<String>,
    torrent: &[u8],
    dir: &std::path::Path,
) -> Result<(String, Option<StandaloneEngine>), Box<dyn std::error::Error>> {
    let dir = dir.to_string_lossy();
    let (client, headless_engine) =
        connect_or_start_engine(&HOST, rpc_port, rpc_secret, Some(&dir)).await?;
    let options = risuko_engine::engine::manager::TaskManager::seed_in_place_options(&dir);
    let added = match add_torrent(&client, torrent, &options).await {
        Ok(gid) if gid.is_empty() => Err("Failed to get task GID from engine".into()),
        Ok(gid) => ensure_task_ok(&client, &gid).await.map(|()| gid),
        Err(e) => Err(e),
    };
    match added {
        Ok(gid) => Ok((gid, headless_engine)),
        Err(e) => {
            if let Some(engine) = headless_engine {
                exit_on_next_signal();
                engine.stop().await;
            }
            Err(e)
        }
    }
}

async fn ensure_task_ok(client: &RpcClient, gid: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = client
        .call(
            "risuko.tellStatus",
            vec![json!(gid), json!(["status", "errorMessage"])],
        )
        .await?;
    match status["status"].as_str() {
        Some("error") => Err(status["errorMessage"].as_str().unwrap_or("error").into()),
        _ => Ok(()),
    }
}

fn parse_piece_size(text: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let lower = text.trim().to_ascii_lowercase();
    let lower = lower.trim_end_matches("ib").trim_end_matches('b');
    let (digits, unit) = match lower.char_indices().last() {
        Some((i, 'k')) => (&lower[..i], 1024u64),
        Some((i, 'm')) => (&lower[..i], 1024 * 1024),
        _ => (lower, 1),
    };
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(unit))
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| format!("invalid piece size: {text}").into())
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

fn config_target(key: &str) -> (&'static str, bool) {
    match keys::scope_of(key) {
        Some(KeyScope::System) => ("system.json", true),
        Some(KeyScope::User) => ("user.json", true),
        None if defaults::system_defaults().contains_key(key) => ("system.json", true),
        None => ("user.json", defaults::user_defaults().contains_key(key)),
    }
}

fn set_config_value(
    config_dir: &std::path::Path,
    key: &str,
    value: &str,
) -> Result<&'static str, Box<dyn std::error::Error>> {
    let (file, known) = config_target(key);
    if !known {
        eprintln!("'{key}' is not a known setting; stored in {file}");
    }
    let path = config_dir.join(file);
    let mut config = load_user_config_for_edit(&path)?;
    let parsed: Value =
        serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()));
    config.insert(key.to_string(), parsed);
    risuko_engine::traits::write_file_atomically(
        &path,
        serde_json::to_string_pretty(&config)?.as_bytes(),
    )?;
    Ok(file)
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

        risuko_engine::traits::write_file_atomically(&path, b"{\"a\":1}").unwrap();
        assert_eq!(load_user_config_for_edit(&path).unwrap()["a"], 1);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn config_target_follows_the_registry() {
        assert_eq!(
            config_target("max-concurrent-downloads"),
            ("system.json", true)
        );
        assert_eq!(config_target("dir"), ("system.json", true));
        assert_eq!(config_target("keep-seeding"), ("user.json", true));
        assert_eq!(config_target("ftp-user"), ("system.json", true));
        assert_eq!(
            config_target("completion-script-command"),
            ("user.json", true)
        );
        assert_eq!(config_target("rpc-host"), ("user.json", true));
        assert_eq!(config_target("bt-upload-rate-limit"), ("system.json", true));
        assert_eq!(config_target("no-such-setting"), ("user.json", false));
    }

    #[test]
    fn config_set_writes_each_key_to_its_own_file() {
        let dir = tempfile::tempdir().unwrap();
        let system = dir.path().join("system.json");
        let user = dir.path().join("user.json");

        let file = set_config_value(dir.path(), "max-concurrent-downloads", "10").unwrap();
        assert_eq!(file, "system.json");
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&system).unwrap()).unwrap();
        assert_eq!(written, json!({"max-concurrent-downloads": 10}));
        assert!(!user.exists());

        let system_before = std::fs::read_to_string(&system).unwrap();
        let file = set_config_value(dir.path(), "keep-seeding", "true").unwrap();
        assert_eq!(file, "user.json");
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&user).unwrap()).unwrap();
        assert_eq!(written, json!({"keep-seeding": true}));
        assert_eq!(std::fs::read_to_string(&system).unwrap(), system_before);

        set_config_value(dir.path(), "split", "8").unwrap();
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&system).unwrap()).unwrap();
        assert_eq!(written, json!({"max-concurrent-downloads": 10, "split": 8}));

        let file = set_config_value(dir.path(), "ftp-user", "alice").unwrap();
        assert_eq!(file, "system.json");
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&system).unwrap()).unwrap();
        assert_eq!(
            written,
            json!({"max-concurrent-downloads": 10, "split": 8, "ftp-user": "alice"})
        );
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&user).unwrap()).unwrap();
        assert_eq!(written, json!({"keep-seeding": true}));
    }

    #[test]
    fn piece_sizes_accept_binary_suffixes() {
        assert_eq!(parse_piece_size("16384").unwrap(), 16_384);
        assert_eq!(parse_piece_size("256K").unwrap(), 256 * 1024);
        assert_eq!(parse_piece_size("256kib").unwrap(), 256 * 1024);
        assert_eq!(parse_piece_size("4M").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_piece_size(" 4 MiB ").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_piece_size("1mb").unwrap(), 1024 * 1024);
        for bad in ["", "K", "abc", "-1M", "99999999M"] {
            assert!(parse_piece_size(bad).is_err(), "{bad}");
        }
    }
}
