use serde_json::{json, Map, Value};

pub fn system_defaults() -> Map<String, Value> {
    let downloads_dir = default_download_dir().to_string_lossy().to_string();
    let file_allocation = if cfg!(target_os = "android") {
        "none"
    } else {
        "falloc"
    };

    let mut m = Map::new();
    m.insert("all-proxy".into(), json!(""));
    m.insert("p2p-proxy".into(), json!(""));
    m.insert("p2p-udp-proxy".into(), json!(""));
    m.insert("auto-file-renaming".into(), json!(true));
    m.insert("bt-force-encryption".into(), json!(false));
    m.insert("bt-load-saved-metadata".into(), json!(true));
    m.insert("bt-save-metadata".into(), json!(true));
    m.insert("bt-create-subfolder".into(), json!(true));
    m.insert("bt-tracker".into(), json!(""));
    m.insert("bt-max-peers-per-torrent".into(), json!(100));
    m.insert("bt-max-outstanding-per-peer".into(), json!(0));
    m.insert("bt-max-connections".into(), json!(400));
    m.insert("bt-ban-corrupt-peers".into(), json!(true));
    m.insert("bt-ban-corrupt-strikes".into(), json!(3));
    m.insert("bt-upload-rate-limit".into(), json!(0));
    m.insert("bt-enable-upnp".into(), json!(true));
    m.insert("bt-upnp-lease".into(), json!(300));
    m.insert("bt-enable-lsd".into(), json!(true));
    m.insert("bt-encryption-policy".into(), json!("prefer"));
    m.insert("bt-listen-v6".into(), json!(false));
    m.insert("dht-listen-port".into(), json!(26701));
    m.insert("dir".into(), json!(downloads_dir));
    m.insert("doh-enable".into(), json!(false));
    m.insert("doh-url".into(), json!(""));
    m.insert("doh-bootstrap".into(), json!(""));
    m.insert("doh-fallback".into(), json!(true));
    m.insert("ed2k-enable-kad".into(), json!(true));
    m.insert("ed2k-kad-port".into(), json!(4672));
    m.insert("ed2k-server".into(), json!("176.123.5.89:4725,45.82.80.155:5687,85.239.33.123:4232,91.208.162.87:4232,145.239.2.134:4661"));
    m.insert("file-allocation".into(), json!(file_allocation));
    m.insert("gift-enabled".into(), json!(false));
    m.insert("gift-host".into(), json!("127.0.0.1"));
    m.insert("gift-port".into(), json!(1213));
    m.insert("listen-port".into(), json!(21301));
    m.insert("max-concurrent-downloads".into(), json!(5));
    m.insert("max-download-limit".into(), json!(0));
    m.insert("max-overall-download-limit".into(), json!(0));
    m.insert("max-overall-upload-limit".into(), json!(0));
    m.insert("no-proxy".into(), json!(""));
    m.insert("p2p-no-proxy".into(), json!(""));
    m.insert("p2p-udp-no-proxy".into(), json!(""));
    m.insert("rpc-listen-port".into(), json!(16800));
    m.insert("rpc-secret".into(), json!(""));
    m.insert("rpc-secret-initialized".into(), json!(false));
    m.insert("pbh-enable".into(), json!(false));
    m.insert("pbh-listen-port".into(), json!(16801));
    m.insert("pbh-rpc-secret".into(), json!(""));
    m.insert("remote-time".into(), json!(false));
    m.insert("seed-ratio".into(), json!(0));
    m.insert("seed-time".into(), json!(0));
    m.insert("split".into(), json!(16));
    m.insert(
        "user-agent".into(),
        json!("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    );
    m
}

fn default_download_dir() -> std::path::PathBuf {
    #[cfg(target_os = "android")]
    {
        let public_downloads = std::path::PathBuf::from("/storage/emulated/0/Download/Risuko");
        if let Some(parent) = public_downloads.parent() {
            if is_writable_dir(parent) {
                return public_downloads;
            }
        }
        let app_external = std::path::PathBuf::from(
            "/storage/emulated/0/Android/data/app.risuko.mobile/files/Download",
        );
        if let Some(parent) = app_external.parent() {
            if is_writable_dir(parent) {
                return app_external;
            }
        }
        if let Some(home) = dirs::home_dir() {
            return home.join("Download");
        }
    }

    dirs::download_dir()
        .or_else(|| dirs::home_dir().map(|p| p.join("Downloads")))
        .or_else(|| std::env::current_dir().ok().map(|p| p.join("Downloads")))
        .unwrap_or_else(|| std::env::temp_dir().join("Downloads"))
}

#[cfg(target_os = "android")]
fn is_writable_dir(path: &std::path::Path) -> bool {
    if !path.is_dir() {
        return false;
    }
    let probe = path.join(".risuko-write-test");
    match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(probe);
            true
        }
        Err(_) => false,
    }
}

pub fn user_defaults() -> Map<String, Value> {
    let is_macos = cfg!(target_os = "macos");
    let is_not_macos = !is_macos;

    let mut m = Map::new();
    m.insert("auto-detect-low-speed-tasks".into(), json!(false));
    m.insert("auto-check-update".into(), json!(false));
    m.insert("auto-hide-window".into(), json!(false));
    m.insert("auto-retry".into(), json!(false));
    m.insert("auto-retry-interval".into(), json!(5));
    m.insert("auto-retry-strategy".into(), json!("static"));
    m.insert("auto-sync-tracker".into(), json!(true));
    m.insert("doh-provider".into(), json!("cloudflare"));
    m.insert("favorite-directories".into(), json!([]));
    m.insert("font-family".into(), json!("system"));
    m.insert("font-size".into(), json!("default"));
    m.insert("hide-app-menu".into(), json!(is_not_macos));
    m.insert("history-directories".into(), json!([]));
    m.insert("keep-seeding".into(), json!(false));
    m.insert("keep-window-state".into(), json!(false));
    m.insert("last-check-update-time".into(), json!(0));
    m.insert("last-sync-tracker-time".into(), json!(0));
    m.insert("locale".into(), json!("auto"));
    m.insert("log-dir-override".into(), json!(""));
    m.insert("log-level".into(), json!("warn"));
    m.insert("low-speed-threshold".into(), json!(20));
    m.insert("new-task-show-downloading".into(), json!(true));
    m.insert("no-confirm-before-delete-task".into(), json!(false));
    m.insert("open-at-login".into(), json!(false));
    m.insert(
        "protocols".into(),
        json!({"magnet": true, "thunder": false}),
    );
    m.insert(
        "proxy".into(),
        json!({
            "http": {
                "enable": false,
                "server": "",
                "bypass": "",
                "scope": ["download", "update-app", "update-trackers"]
            },
            "p2p": {
                "enable": false,
                "server": "",
                "bypass": "",
                "udp": {
                    "server": "",
                    "bypass": ""
                }
            }
        }),
    );
    m.insert("rpc-host".into(), json!("127.0.0.1"));
    m.insert("purge-record-on-start".into(), json!(false));
    m.insert("resume-all-when-app-launched".into(), json!(false));
    m.insert("run-mode".into(), json!(1));
    m.insert("show-progress-bar".into(), json!(true));
    m.insert("task-notification".into(), json!(true));
    m.insert("legal-accepted".into(), json!(false));
    #[cfg(not(target_os = "android"))]
    {
        m.insert("clipboard-watch".into(), json!(true));
        m.insert("clipboard-watch-notice-seen".into(), json!(false));
        m.insert(
            "clipboard-watch-extensions".into(),
            json!([
                "zip", "7z", "rar", "tar", "gz", "tgz", "bz2", "xz", "zst", "iso", "img", "dmg",
                "pkg", "exe", "msi", "apk", "apks", "xapk", "deb", "rpm", "appimage", "bin", "mp4",
                "mkv", "avi", "mov", "webm", "flv", "wmv", "m4v", "mp3", "flac", "wav", "m4a",
                "aac", "ogg", "opus", "pdf", "epub", "mobi", "azw3", "docx", "xlsx", "pptx", "odt",
                "jar", "torrent", "meta4", "metalink"
            ]),
        );
    }
    m.insert("theme".into(), json!("auto"));
    m.insert(
        "tracker-source".into(),
        json!([
            "https://cdn.jsdelivr.net/gh/ngosang/trackerslist/trackers_best_ip.txt",
            "https://cdn.jsdelivr.net/gh/ngosang/trackerslist/trackers_best.txt"
        ]),
    );
    m.insert("tray-theme".into(), json!("auto"));
    m.insert("tray-speedometer".into(), json!(is_macos));
    m.insert("m3u8-output-format".into(), json!("ts"));
    m.insert("update-channel".into(), json!("latest"));
    m.insert("window-state".into(), json!({}));
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_defaults_required_keys() {
        let sys = system_defaults();
        let required = [
            "dir",
            "max-concurrent-downloads",
            "rpc-listen-port",
            "rpc-secret",
            "pbh-enable",
            "pbh-listen-port",
            "pbh-rpc-secret",
            "seed-ratio",
            "seed-time",
            "split",
            "user-agent",
            "listen-port",
            "doh-enable",
            "doh-url",
            "doh-bootstrap",
            "doh-fallback",
            "ed2k-enable-kad",
            "ed2k-kad-port",
        ];
        for key in required {
            assert!(sys.contains_key(key), "missing system key: {key}");
        }
    }

    #[test]
    fn system_defaults_drop_unread_aria2_keys_and_add_gift() {
        let sys = system_defaults();
        for key in [
            "allow-overwrite",
            "bt-exclude-tracker",
            "bt-enable-lpd",
            "enable-dht",
            "enable-dht6",
            "enable-peer-exchange",
            "continue",
            "follow-torrent",
        ] {
            assert!(!sys.contains_key(key), "unread key still defaulted: {key}");
        }
        assert_eq!(sys.get("gift-enabled").unwrap(), false);
        assert_eq!(sys.get("gift-host").unwrap(), "127.0.0.1");
        assert_eq!(sys.get("gift-port").unwrap(), 1213);
    }

    #[test]
    fn system_defaults_sensible_values() {
        let sys = system_defaults();
        assert_eq!(sys.get("max-concurrent-downloads").unwrap(), 5);
        assert_eq!(sys.get("rpc-listen-port").unwrap(), 16800);
        assert_eq!(sys.get("split").unwrap(), 16);
        assert_eq!(sys.get("rpc-secret").unwrap(), "");
        assert_eq!(sys.get("pbh-enable").unwrap(), false);
        assert_eq!(sys.get("pbh-listen-port").unwrap(), 16801);
        assert_eq!(sys.get("bt-enable-upnp").unwrap(), true);
        assert_eq!(sys.get("bt-max-connections").unwrap(), 400);
        assert_eq!(sys.get("bt-ban-corrupt-peers").unwrap(), true);
        assert_eq!(sys.get("bt-ban-corrupt-strikes").unwrap(), 3);
        assert!(sys.contains_key("bt-upnp-lease"), "missing bt-upnp-lease");
        assert_eq!(sys.get("bt-upnp-lease").unwrap(), 300);
        assert_eq!(sys.get("bt-enable-lsd").unwrap(), true);
        assert_eq!(sys.get("bt-encryption-policy").unwrap(), "prefer");
        assert_eq!(sys.get("bt-listen-v6").unwrap(), false);
        assert_eq!(sys.get("doh-enable").unwrap(), false);
        assert_eq!(sys.get("doh-url").unwrap(), "");
        assert_eq!(sys.get("doh-bootstrap").unwrap(), "");
        assert_eq!(sys.get("doh-fallback").unwrap(), true);
        assert_eq!(sys.get("ed2k-enable-kad").unwrap(), true);
        assert_eq!(sys.get("ed2k-kad-port").unwrap(), 4672);
    }

    #[test]
    fn system_defaults_dir_is_valid_path() {
        let sys = system_defaults();
        let dir = sys.get("dir").unwrap().as_str().unwrap();
        let path = std::path::Path::new(dir);
        assert!(!dir.is_empty(), "dir should not be empty");
        assert!(path.is_absolute(), "dir should be absolute, got: {dir}");
    }

    #[test]
    fn user_defaults_required_keys() {
        let user = user_defaults();
        let required = [
            "theme",
            "font-family",
            "font-size",
            "locale",
            "keep-seeding",
            "auto-check-update",
            "rpc-host",
            "m3u8-output-format",
            "tray-theme",
            "log-level",
            "doh-provider",
        ];
        for key in required {
            assert!(user.contains_key(key), "missing user key: {key}");
        }
    }

    #[test]
    fn user_defaults_sensible_values() {
        let user = user_defaults();
        assert_eq!(user.get("theme").unwrap(), "auto");
        assert_eq!(user.get("font-family").unwrap(), "system");
        assert_eq!(user.get("font-size").unwrap(), "default");
        assert_eq!(user.get("locale").unwrap(), "auto");
        assert_eq!(user.get("keep-seeding").unwrap(), false);
        assert_eq!(user.get("rpc-host").unwrap(), "127.0.0.1");
        assert_eq!(user.get("m3u8-output-format").unwrap(), "ts");
        assert_eq!(user.get("purge-record-on-start").unwrap(), false);
        assert_eq!(user.get("doh-provider").unwrap(), "cloudflare");
    }

    #[test]
    fn user_defaults_platform_specific() {
        let user = user_defaults();
        assert_eq!(user.get("auto-check-update").unwrap(), false);
        if cfg!(target_os = "macos") {
            assert_eq!(user.get("hide-app-menu").unwrap(), false);
            assert_eq!(user.get("tray-speedometer").unwrap(), true);
        } else {
            assert_eq!(user.get("hide-app-menu").unwrap(), true);
            assert_eq!(user.get("tray-speedometer").unwrap(), false);
        }
    }
}
