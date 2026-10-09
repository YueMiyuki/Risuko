mod bridge;
pub mod cli;
mod commands;
mod managers;
mod state;
mod utils;

pub use risuko_engine::config;
pub use risuko_engine::engine;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tauri::Manager;
#[cfg(not(target_os = "android"))]
use tauri::{Emitter, Listener};

#[cfg(not(target_os = "android"))]
use tauri_plugin_autostart::{MacosLauncher, ManagerExt};

use risuko_engine::engine::rss::RssManager;

#[cfg(not(target_os = "android"))]
fn with_desktop_plugins<R: tauri::Runtime>(builder: tauri::Builder<R>) -> tauri::Builder<R> {
    builder
        .plugin(tauri_plugin_autostart::init(
            MacosLauncher::LaunchAgent,
            Some(vec!["--opened-at-login=1"]),
        ))
        .plugin(tauri_plugin_single_instance::init(|app, args, _cwd| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.set_focus();

                // A relaunched second instance may carry leading flags (e.g. --opened-at-login=1), so scan all args for the first path that exists rather than assuming it is at a fixed index
                if let Some(path) = args
                    .iter()
                    .skip(1)
                    .find(|arg| !arg.starts_with('-') && std::path::Path::new(arg).exists())
                {
                    tracing::info!("Single instance received file: {}", path);
                    let _ = window.emit("open-file", path);
                }
            }
        }))
        .plugin(tauri_plugin_clipboard::init())
        .plugin(tauri_plugin_notification::init())
}

#[cfg(target_os = "android")]
fn with_desktop_plugins<R: tauri::Runtime>(builder: tauri::Builder<R>) -> tauri::Builder<R> {
    builder
        .plugin(risuko_webview_upgrade::init())
        .plugin(tauri_plugin_barcode_scanner::init())
}

#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
fn with_updater_plugins<R: tauri::Runtime>(builder: tauri::Builder<R>) -> tauri::Builder<R> {
    builder
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
}

#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
fn with_updater_plugins<R: tauri::Runtime>(builder: tauri::Builder<R>) -> tauri::Builder<R> {
    builder
}

fn resolve_log_dir(
    config: &risuko_engine::config::ConfigManager,
    default_log_dir: &std::path::Path,
) -> std::path::PathBuf {
    let override_value = config
        .get_user_config()
        .get("log-dir-override")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if override_value.is_empty() {
        return default_log_dir.to_path_buf();
    }
    let candidate = std::path::PathBuf::from(&override_value);
    if !candidate.is_absolute() {
        eprintln!(
            "log-dir-override must be an absolute path; got '{}'. Falling back to default.",
            override_value
        );
        return default_log_dir.to_path_buf();
    }
    if let Err(e) = std::fs::create_dir_all(&candidate) {
        eprintln!(
            "log-dir-override '{}' is not writable ({}). Falling back to default.",
            candidate.display(),
            e
        );
        return default_log_dir.to_path_buf();
    }
    let probe = candidate.join(".risuko-log-write-test");
    if let Err(e) = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&probe)
    {
        eprintln!(
            "log-dir-override '{}' is not writable ({}). Falling back to default.",
            candidate.display(),
            e
        );
        return default_log_dir.to_path_buf();
    }
    let _ = std::fs::remove_file(probe);
    candidate
}

fn build_daily_log_appender(
    log_dir: &std::path::Path,
) -> Result<tracing_appender::rolling::RollingFileAppender, tracing_appender::rolling::InitError> {
    tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("risuko")
        .filename_suffix("log")
        .build(log_dir)
}

fn init_logging(
    log_dir: &std::path::Path,
    log_level: &str,
) -> tracing_appender::non_blocking::WorkerGuard {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    if let Err(e) = std::fs::create_dir_all(log_dir) {
        eprintln!("Failed to create log directory {:?}: {}", log_dir, e);
    }

    let log_writer: Box<dyn std::io::Write + Send> = match build_daily_log_appender(log_dir) {
        Ok(appender) => Box::new(appender),
        Err(e) => {
            eprintln!("Failed to configure rolling log appender: {e}");
            let fallback_dir = std::env::temp_dir().join("risuko-logs");
            if let Err(directory_error) = std::fs::create_dir_all(&fallback_dir) {
                eprintln!(
                    "Failed to create fallback log directory {:?}: {}. Logging to stderr.",
                    fallback_dir, directory_error
                );
                Box::new(std::io::stderr())
            } else {
                match build_daily_log_appender(&fallback_dir) {
                    Ok(appender) => Box::new(appender),
                    Err(fallback_error) => {
                        eprintln!(
                            "Failed to configure fallback rolling log appender in {:?}: {}. Logging to stderr.",
                            fallback_dir, fallback_error
                        );
                        Box::new(std::io::stderr())
                    }
                }
            }
        }
    };
    let (file_writer, guard) = tracing_appender::non_blocking(log_writer);

    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("warn"));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(file_writer),
        )
        .init();

    guard
}

/// Apply environment workarounds for known WebKitGTK/Linux rendering issues before any GTK/WebKit code initializes. Only sets variables not already defined, so users can override
#[cfg(target_os = "linux")]
fn apply_linux_webkit_workarounds() {
    // WebKitGTK's DMA-BUF renderer (default since 2.42) frequently fails to initialize EGL/GBM on NVIDIA, virtualized GPUs, Wayland sessions without a working GBM backend, or older Mesa stacks. The failure manifests as: "Could not create GBM EGL display: EGL_NOT_INITIALIZED. Aborting..." followed by SIGABRT before the window is shown. Disabling DMA-BUF forces the legacy GLES path which is far more compatible. See: https://github.com/tauri-apps/tauri/issues/9304
    // `__NV_DISABLE_EXPLICIT_SYNC` is read by the NVIDIA driver only; Tauri documents it for the Wayland "Error 71" crash and reports no performance cost, so it is safe to carry for every GPU. See: https://v2.tauri.app/develop/debug/linux-graphics/
    for (key, value) in [
        ("WEBKIT_DISABLE_DMABUF_RENDERER", "1"),
        ("WEBKIT_DISABLE_COMPOSITING_MODE", "1"),
        ("__NV_DISABLE_EXPLICIT_SYNC", "1"),
    ] {
        if std::env::var_os(key).is_none() {
            std::env::set_var(key, value);
        }
    }
}

/// Log the app, webview and (on Linux) display environment once per launch so freeze and rendering reports carry the facts that decide the WebKitGTK code path (session type, GDK backend, IM modules, renderer overrides) without a follow-up round trip
fn log_runtime_diagnostics(app: &tauri::App) {
    let webview = tauri::webview_version().unwrap_or_else(|e| format!("unknown ({e})"));
    tracing::info!(
        "Risuko {} starting (webview {})",
        app.package_info().version,
        webview
    );

    #[cfg(target_os = "linux")]
    {
        // AppImage launchers force GDK_BACKEND=x11, so a Wayland session can still run through XWayland; logging both the session and the backend keeps that visible
        const KEYS: [&str; 17] = [
            "XDG_SESSION_TYPE",
            "XDG_CURRENT_DESKTOP",
            "DESKTOP_SESSION",
            "WAYLAND_DISPLAY",
            "DISPLAY",
            "GDK_BACKEND",
            "GTK_IM_MODULE",
            "QT_IM_MODULE",
            "XMODIFIERS",
            "APPIMAGE",
            "WEBKIT_DISABLE_DMABUF_RENDERER",
            "WEBKIT_DISABLE_COMPOSITING_MODE",
            "WEBKIT_DMABUF_RENDERER_FORCE_SHM",
            "WEBKIT_USE_SKIA_FOR_COMPOSITION",
            "__NV_DISABLE_EXPLICIT_SYNC",
            "__GLX_VENDOR_LIBRARY_NAME",
            "LIBGL_ALWAYS_SOFTWARE",
        ];
        let snapshot = KEYS
            .iter()
            .map(|key| match std::env::var_os(key) {
                Some(value) => format!("{key}={}", value.to_string_lossy()),
                None => format!("{key}=<unset>"),
            })
            .collect::<Vec<_>>()
            .join(" ");
        tracing::info!("Linux display environment: {snapshot}");
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    #[cfg(target_os = "linux")]
    apply_linux_webkit_workarounds();

    let app = with_updater_plugins(with_desktop_plugins(
        tauri::Builder::default()
            .plugin(tauri_plugin_store::Builder::default().build())
            .plugin(tauri_plugin_dialog::init())
            .plugin(tauri_plugin_shell::init()),
    ))
    .plugin(tauri_plugin_clipboard_manager::init())
    .plugin(tauri_plugin_deep_link::init())
    .setup(|app| {
        let handle = app.handle();

        // Create Tauri-backed trait implementations
        let config_dir_provider = bridge::TauriConfigDir::new(handle);
        let event_sink: Arc<dyn risuko_engine::EventSink> =
            Arc::new(bridge::TauriEventSink::new(handle));
        let storage: Arc<dyn risuko_engine::StorageBackend> =
            Arc::new(bridge::TauriStorage::new(handle));

        let default_log_dir = handle.path().app_log_dir().unwrap_or_else(|_| {
            handle
                .path()
                .app_config_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("."))
                .join("logs")
        });
        let config = risuko_engine::config::ConfigManager::new(&config_dir_provider)
            .map_err(|e| e.to_string())?;
        let log_level = config
            .get_user_config()
            .get("log-level")
            .and_then(|v| v.as_str())
            .unwrap_or("warn")
            .to_string();
        let log_dir = resolve_log_dir(&config, &default_log_dir);

        let log_guard = init_logging(&log_dir, &log_level);
        if log_dir != default_log_dir {
            tracing::info!(
                "Log directory: {} (override of {})",
                log_dir.display(),
                default_log_dir.display()
            );
        } else {
            tracing::info!("Log directory: {}", log_dir.display());
        }
        log_runtime_diagnostics(app);

        let app_state = state::AppState::new(config, storage, log_dir, log_guard)?;
        app.manage(app_state);
        sync_open_at_login_setting(app);

        // P2P file sharing
        {
            let (share_tx, mut share_rx) =
                tokio::sync::mpsc::unbounded_channel::<risuko_share::ShareEnvelope>();
            let share_dir = handle
                .path()
                .app_cache_dir()
                .unwrap_or_else(|_| {
                    handle
                        .path()
                        .app_config_dir()
                        .unwrap_or_else(|_| std::path::PathBuf::from("."))
                })
                .join("share");
            let share_manager = risuko_share::ShareManager::new(share_dir, share_tx);
            app.manage(commands::share_cmds::ShareState::new(share_manager));

            let emit_handle = handle.clone();
            tauri::async_runtime::spawn(async move {
                use tauri::Emitter;
                while let Some(envelope) = share_rx.recv().await {
                    if let Err(e) = emit_handle.emit("share:event", &envelope) {
                        tracing::warn!("Failed to emit share event: {}", e);
                    }
                }
            });
        }

        // Windows/Linux use a custom title bar, so disable native decorations; macOS keeps decorations and uses `titleBarStyle: Overlay`
        #[cfg(not(any(target_os = "macos", target_os = "android")))]
        if let Some(window) = app.get_webview_window("main") {
            let _ = window.set_decorations(false);
        }

        let opened_at_login = std::env::args().any(|arg| arg == "--opened-at-login=1");
        if opened_at_login {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.hide();
            }
        }

        let config_guard = app.state::<state::AppState>();
        let should_start = {
            let config = config_guard
                .config
                .lock()
                .map_err(|_| "Configuration lock poisoned")?;
            risuko_engine::engine::should_start_embedded_engine(&config)
        };

        let event_sink_clone = event_sink.clone();
        let upload_mgr = app.state::<state::AppState>().upload_sinks.clone();
        upload_mgr.set_event_sink(event_sink_clone.clone());
        let vault = app.state::<state::AppState>().vault.clone();
        let config_dir = app
            .state::<state::AppState>()
            .config
            .lock()
            .map_err(|_| "Configuration lock poisoned")?
            .config_dir()
            .to_path_buf();
        tauri::async_runtime::block_on(risuko_engine::engine::set_usenet_credential_resolver(
            std::sync::Arc::new(
                commands::usenet_cmds::VaultCredentialResolver::with_config_dir(vault, config_dir),
            ),
        ));

        {
            let vault = app.state::<state::AppState>().vault.clone();
            tauri::async_runtime::block_on(commands::upload_cmds::rehydrate_upload_sinks(
                &upload_mgr,
                &vault,
            ));
        }

        if should_start {
            let config_ref = config_guard
                .config
                .lock()
                .map_err(|_| "Configuration lock poisoned")?;
            let config_dir = config_ref.config_dir().to_path_buf();
            drop(config_ref);

            tauri::async_runtime::spawn(async move {
                let config = match risuko_engine::config::ConfigManager::with_dir(config_dir) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("Failed to create ConfigManager: {}", e);
                        return;
                    }
                };
                if let Err(e) =
                    risuko_engine::engine::start_engine(&config, event_sink_clone, Some(upload_mgr))
                        .await
                {
                    tracing::error!("Failed to start engine: {}", e);
                }
            });
        }

        managers::menu::setup_menu(app)?;

        // On non-macOS, respect the hide-app-menu user preference
        #[cfg(not(any(target_os = "macos", target_os = "android")))]
        {
            let hide_menu = app
                .state::<state::AppState>()
                .config
                .lock()
                .ok()
                .and_then(|cfg| {
                    cfg.get_user_config()
                        .get("hide-app-menu")
                        .and_then(|v| v.as_bool())
                })
                .unwrap_or(true);
            if hide_menu {
                let _ = app.handle().remove_menu();
            }
        }

        managers::tray::setup_tray(app)?;

        managers::flyout::setup_flyout(app)?;

        managers::clip_prompt::setup_clip_prompt(app)?;

        #[cfg(not(target_os = "android"))]
        {
            let watch_handle = app.handle().clone();
            app.listen_any(
                "plugin:clipboard://clipboard-monitor/update",
                move |_event| {
                    commands::clipboard_cmds::on_clipboard_update(&watch_handle);
                },
            );
            let state = app.state::<state::AppState>();
            if commands::clipboard_cmds::watch_enabled(&state) {
                use tauri_plugin_clipboard::Clipboard;
                let clipboard = app.state::<Clipboard>();
                if let Err(e) = clipboard.start_monitor(app.handle().clone()) {
                    tracing::warn!("[Risuko] failed to start clipboard monitor: {}", e);
                }
            }
        }

        let rss = app.state::<state::AppState>().rss.clone();
        // `RssManager::start_polling` uses `tokio::spawn`, so enter Tauri's async runtime before invoking it from this synchronous setup hook
        tauri::async_runtime::spawn(async move {
            drop(RssManager::start_polling(rss));
        });

        Ok(())
    })
    .on_window_event(|window, event| {
        if let tauri::WindowEvent::CloseRequested { api, .. } = event {
            #[cfg(not(target_os = "android"))]
            if window.label() == managers::flyout::FLYOUT_LABEL {
                api.prevent_close();
                let _ = window.hide();
                return;
            }
            if window.label() != "main" {
                return;
            }
            let quitting = window
                .app_handle()
                .state::<state::AppState>()
                .is_quitting
                .load(Ordering::SeqCst);
            if quitting {
                return;
            }
            api.prevent_close();
            let _ = commands::app_cmds::hide_main_window(window.app_handle());
        }

        if let tauri::WindowEvent::Focused(false) = event {
            #[cfg(not(target_os = "android"))]
            if window.label() == managers::flyout::FLYOUT_LABEL {
                // blur also fires when the flyout hands focus to the main window it just opened; demoting then would strand the visible UI
                #[cfg(target_os = "macos")]
                managers::flyout::demote_if_ui_hidden(window.app_handle());
                let _ = window.hide();
            }

            #[cfg(not(target_os = "android"))]
            if window.label() == managers::clip_prompt::CLIP_PROMPT_LABEL {
                managers::clip_prompt::hide_clip_prompt(window.app_handle());
            }
        }
    })
    .invoke_handler(tauri::generate_handler![
        commands::config_cmds::get_app_config,
        commands::config_cmds::save_preference,
        commands::config_cmds::prepare_preference_patch,
        commands::config_cmds::resolve_configured_proxy,
        commands::config_cmds::is_signed_updater_available,
        commands::config_cmds::fetch_tracker_sources,
        commands::clipboard_cmds::mark_clipboard_self_write,
        commands::clipboard_cmds::start_clipboard_watch,
        commands::clipboard_cmds::stop_clipboard_watch,
        commands::clipboard_cmds::get_clip_prompt_uri,
        commands::clipboard_cmds::clip_prompt_accept,
        commands::clipboard_cmds::clip_prompt_dismiss,
        commands::app_cmds::relaunch_app,
        commands::app_cmds::quit_app,
        commands::app_cmds::show_window,
        commands::app_cmds::hide_window,
        commands::app_cmds::log_frontend,
        commands::app_cmds::factory_reset,
        commands::app_cmds::reset_session,
        commands::app_cmds::toggle_app_menu,
        commands::app_cmds::is_opened_at_login,
        commands::app_cmds::set_android_system_bars,
        commands::app_cmds::ensure_android_storage_access,
        commands::app_cmds::update_android_download_notification,
        commands::app_cmds::clear_android_download_notification,
        commands::app_cmds::shutdown_system,
        commands::file_cmds::reveal_in_folder,
        commands::file_cmds::select_android_directory,
        commands::file_cmds::stage_android_share_paths,
        commands::file_cmds::open_path,
        commands::file_cmds::trash_item,
        commands::file_cmds::rename_path,
        commands::file_cmds::resolve_torrent_path,
        commands::file_cmds::cleanup_generated_torrent_sidecars_for_task,
        commands::engine_cmds::restart_engine,
        commands::health_cmds::run_health_checks,
        commands::health_cmds::list_log_files,
        commands::health_cmds::read_log_file,
        commands::engine_cmds::decode_thunder_uri,
        commands::engine_cmds::add_uri,
        commands::engine_cmds::add_media,
        commands::engine_cmds::get_media_info,
        commands::engine_cmds::add_torrent_by_path,
        commands::engine_cmds::add_torrents_by_paths,
        commands::engine_cmds::add_metalinks_by_paths,
        commands::engine_cmds::add_nzbs_by_paths,
        commands::engine_cmds::resolve_magnet,
        commands::engine_cmds::evaluate_low_speed_tasks,
        commands::engine_cmds::plan_auto_retry,
        commands::engine_cmds::reorder_tasks,
        commands::engine_cmds::set_task_schedule,
        commands::engine_cmds::start_task_now,
        commands::engine_cmds::tell_status,
        commands::engine_cmds::tell_active,
        commands::engine_cmds::tell_waiting,
        commands::engine_cmds::tell_stopped,
        commands::engine_cmds::tell_scheduled,
        commands::engine_cmds::pause_task,
        commands::engine_cmds::unpause_task,
        commands::engine_cmds::remove_task,
        commands::engine_cmds::change_option,
        commands::engine_cmds::update_task,
        commands::engine_cmds::change_global_option_engine,
        commands::engine_cmds::get_option_engine,
        commands::engine_cmds::get_global_option_engine,
        commands::engine_cmds::get_global_stat,
        commands::engine_cmds::save_session,
        commands::engine_cmds::get_version,
        commands::engine_cmds::pause_all_tasks,
        commands::engine_cmds::unpause_all_tasks,
        commands::engine_cmds::purge_download_result,
        commands::engine_cmds::remove_download_result,
        commands::stats_cmds::record_download_stats_minute,
        commands::stats_cmds::get_download_stats,
        commands::stats_cmds::export_download_stats,
        commands::stats_cmds::merge_download_stats,
        commands::stats_cmds::clear_download_stats,
        commands::engine_cmds::get_peers,
        commands::engine_cmds::multicall_engine,
        commands::engine_cmds::infer_out_from_uri,
        commands::engine_cmds::resolve_file_category,
        commands::cookie_cmds::list_browsers_cmd,
        commands::cookie_cmds::import_browser_cookies,
        commands::cookie_cmds::import_browser_cookies_elevated,
        commands::cookie_cmds::list_cookie_entries,
        commands::cookie_cmds::delete_cookie_entry,
        commands::cookie_cmds::clear_cookie_entries,
        commands::cookie_cmds::retry_with_cookies,
        commands::cookie_cmds::capture_user_agent,
        commands::engine_cmds::list_routing_rules,
        commands::engine_cmds::add_routing_rule,
        commands::engine_cmds::update_routing_rule,
        commands::engine_cmds::remove_routing_rule,
        commands::engine_cmds::resolve_routing,
        commands::event_cmds::on_download_status_change,
        commands::event_cmds::on_speed_change,
        commands::event_cmds::on_progress_change,
        commands::event_cmds::on_task_download_complete,
        commands::completion_script_cmds::run_completion_script,
        commands::completion_script_cmds::test_completion_script,
        commands::event_cmds::update_tray,
        commands::event_cmds::update_tray_menu_labels,
        commands::event_cmds::update_app_menu_labels,
        commands::rss_cmds::add_rss_feed,
        commands::rss_cmds::remove_rss_feed,
        commands::rss_cmds::refresh_rss_feed,
        commands::rss_cmds::refresh_all_rss_feeds,
        commands::rss_cmds::get_rss_feeds,
        commands::rss_cmds::get_rss_items,
        commands::rss_cmds::update_rss_feed_settings,
        commands::rss_cmds::add_rss_rule,
        commands::rss_cmds::update_rss_rule,
        commands::rss_cmds::reorder_rss_rules,
        commands::rss_cmds::dry_run_rss_rule,
        commands::rss_cmds::parse_rss_item_title,
        commands::rss_cmds::remove_rss_rule,
        commands::rss_cmds::get_rss_rules,
        commands::rss_cmds::delete_rss_items,
        commands::rss_cmds::mark_rss_downloaded,
        commands::rss_cmds::clear_rss_download,
        commands::rss_cmds::read_rss_download,
        commands::rss_cmds::download_rss_item_tracked,
        commands::rss_cmds::mark_rss_item_read,
        commands::rss_cmds::mark_rss_items_read,
        commands::upload_cmds::list_upload_sinks,
        commands::upload_cmds::add_upload_sink,
        commands::upload_cmds::update_upload_sink,
        commands::upload_cmds::remove_upload_sink,
        commands::upload_cmds::test_upload_sink,
        commands::upload_cmds::get_default_upload_sink,
        commands::upload_cmds::set_default_upload_sink,
        commands::upload_cmds::set_upload_max_concurrency,
        commands::upload_cmds::list_upload_rules,
        commands::upload_cmds::add_upload_rule,
        commands::upload_cmds::update_upload_rule,
        commands::upload_cmds::remove_upload_rule,
        commands::upload_cmds::list_upload_jobs,
        commands::upload_cmds::cancel_upload_job,
        commands::upload_cmds::clear_upload_history,
        commands::vault_cmds::vault_status,
        commands::vault_cmds::vault_put_credential,
        commands::vault_cmds::vault_get_credential,
        commands::vault_cmds::vault_remove_credential,
        commands::usenet_cmds::usenet_save_credentials,
        commands::usenet_cmds::usenet_remove_credentials,
        commands::usenet_cmds::usenet_has_credentials,
        commands::usenet_cmds::usenet_test_profile,
        commands::share_cmds::share_start_send,
        commands::share_cmds::share_start_receive,
        commands::share_cmds::share_cancel,
    ])
    .build(tauri::generate_context!())
    .expect("error while building Risuko");

    app.run(|_, event| {
        if matches!(
            event,
            tauri::RunEvent::ExitRequested { .. } | tauri::RunEvent::Exit
        ) {
            commands::event_cmds::cleanup_download_inhibit();
        }
    });
}

fn sync_open_at_login_setting(app: &tauri::App) {
    #[cfg(target_os = "android")]
    {
        let _ = app;
        return;
    }
    #[cfg(not(target_os = "android"))]
    {
        let desired = app
            .state::<state::AppState>()
            .config
            .lock()
            .ok()
            .and_then(|cfg| {
                cfg.get_user_config()
                    .get("open-at-login")
                    .and_then(|v| v.as_bool())
            });

        let Some(desired) = desired else {
            return;
        };

        let autolaunch = app.autolaunch();
        let needs_update = match autolaunch.is_enabled() {
            Ok(current) => current != desired,
            Err(_) => true,
        };

        if !needs_update {
            return;
        }

        let result = if desired {
            autolaunch.enable()
        } else {
            autolaunch.disable()
        };

        if let Err(err) = result {
            tracing::warn!("Failed to sync open-at-login setting: {}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daily_log_appender_uses_the_new_filename_format() {
        let directory = tempfile::tempdir().expect("create temporary log directory");
        let _appender = build_daily_log_appender(directory.path()).expect("build log appender");
        let filenames = std::fs::read_dir(directory.path())
            .expect("list log directory")
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect::<Vec<_>>();

        assert!(filenames.iter().any(|name| {
            name.len() == "risuko.YYYY-MM-DD.log".len()
                && name.starts_with("risuko.")
                && name.ends_with(".log")
        }));
    }
}
