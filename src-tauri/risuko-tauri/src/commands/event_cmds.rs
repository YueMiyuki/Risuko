use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::process::Stdio;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::process::{Child, Command};
use std::sync::Mutex;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::sync::OnceLock;
#[cfg(target_os = "windows")]
use std::{
    ffi::c_void,
    os::windows::ffi::OsStrExt,
    path::Path,
    sync::{mpsc, OnceLock},
    thread,
};
use tauri::AppHandle;
#[cfg(not(target_os = "android"))]
use tauri::Manager;

#[cfg_attr(target_os = "android", allow(dead_code))]
fn changed<T: PartialEq>(slot: &Mutex<Option<T>>, new: T) -> bool {
    let mut last = slot.lock().unwrap_or_else(|e| e.into_inner());
    if last.as_ref() == Some(&new) {
        return false;
    }
    *last = Some(new);
    true
}

#[tauri::command(async)]
pub fn on_download_status_change(
    downloading: bool,
    _state: tauri::State<'_, crate::state::AppState>,
) -> Result<(), String> {
    apply_download_inhibit(downloading);
    Ok(())
}

#[tauri::command]
pub fn on_speed_change(
    handle: AppHandle,
    upload_speed: u64,
    download_speed: u64,
    show_tray_speed: bool,
    app_name: Option<String>,
    download_label: Option<String>,
    upload_label: Option<String>,
) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        let _ = (
            handle,
            upload_speed,
            download_speed,
            show_tray_speed,
            app_name,
            download_label,
            upload_label,
        );
        return Ok(());
    }
    #[cfg(not(target_os = "android"))]
    {
        let app_name = app_name
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "Risuko".to_string());
        let download_label = download_label
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "Download".to_string());
        let upload_label = upload_label
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "Upload".to_string());

        if let Some(tray) = handle.tray_by_id("main") {
            let tooltip = if show_tray_speed && (upload_speed > 0 || download_speed > 0) {
                format!(
                    "{}\n{}: {}/s  {}: {}/s",
                    app_name,
                    download_label,
                    crate::cli::progress::format_size(download_speed),
                    upload_label,
                    crate::cli::progress::format_size(upload_speed)
                )
            } else {
                app_name
            };
            static LAST_TOOLTIP: Mutex<Option<String>> = Mutex::new(None);
            if changed(&LAST_TOOLTIP, tooltip.clone()) {
                let _ = tray.set_tooltip(Some(&tooltip));
            }
        }
        Ok(())
    }
}

#[tauri::command]
pub fn on_progress_change(
    handle: AppHandle,
    progress: f64,
    show_progress_bar: bool,
) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        let _ = (handle, progress, show_progress_bar);
        return Ok(());
    }
    #[cfg(not(target_os = "android"))]
    {
        if let Some(window) = handle.get_webview_window("main") {
            let prog = (show_progress_bar && (0.0..=1.0).contains(&progress))
                .then_some((progress * 100.0) as u64);
            static LAST_PROGRESS: Mutex<Option<Option<u64>>> = Mutex::new(None);
            if !changed(&LAST_PROGRESS, prog) {
                return Ok(());
            }
            let status = if prog.is_some() {
                tauri::window::ProgressBarStatus::Normal
            } else {
                tauri::window::ProgressBarStatus::None
            };
            let _ = window.set_progress_bar(tauri::window::ProgressBarState {
                status: Some(status),
                progress: prog,
            });
        }
        Ok(())
    }
}

#[tauri::command]
pub fn on_task_download_complete(_handle: AppHandle, path: String) -> Result<(), String> {
    add_recent_document(&path)?;
    Ok(())
}

#[tauri::command]
pub fn update_tray(
    handle: AppHandle,
    image_data: Vec<u8>,
    width: u32,
    height: u32,
) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        let _ = (handle, image_data, width, height);
        return Ok(());
    }
    #[cfg(not(target_os = "android"))]
    {
        if let Some(tray) = handle.tray_by_id("main") {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            (&image_data, width, height).hash(&mut hasher);
            static LAST_ICON: Mutex<Option<u64>> = Mutex::new(None);
            if !changed(&LAST_ICON, hasher.finish()) {
                return Ok(());
            }
            let image = tauri::image::Image::new_owned(image_data, width, height);
            let _ = tray.set_icon_with_as_template(Some(image), true);
        }
        Ok(())
    }
}

#[tauri::command]
pub fn update_tray_menu_labels(
    handle: AppHandle,
    labels: HashMap<String, String>,
) -> Result<(), String> {
    crate::managers::tray::update_tray_menu_labels(&handle, &labels)
}

#[tauri::command]
pub fn update_app_menu_labels(
    handle: AppHandle,
    labels: HashMap<String, String>,
) -> Result<(), String> {
    crate::managers::menu::update_menu_labels(&handle, &labels)
}

fn apply_download_inhibit(downloading: bool) {
    static DESIRED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    static APPLY: std::sync::Mutex<()> = std::sync::Mutex::new(());
    use std::sync::atomic::Ordering::SeqCst;
    DESIRED.store(downloading, SeqCst);
    let _guard = APPLY.lock().unwrap_or_else(|e| e.into_inner());
    loop {
        let want = DESIRED.load(SeqCst);
        apply_download_inhibit_inner(want);
        if DESIRED.load(SeqCst) == want {
            break;
        }
    }
}

fn apply_download_inhibit_inner(downloading: bool) {
    #[cfg(target_os = "windows")]
    let active = windows_apply_download_inhibit(downloading);

    #[cfg(target_os = "macos")]
    let active = macos_apply_download_inhibit(downloading);

    #[cfg(target_os = "linux")]
    let active = linux_apply_download_inhibit(downloading);

    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    let active = {
        let _ = downloading;
        false
    };

    SLEEP_INHIBIT_ACTIVE.store(active, std::sync::atomic::Ordering::Relaxed);
}

static SLEEP_INHIBIT_ACTIVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn sleep_inhibit_active() -> bool {
    SLEEP_INHIBIT_ACTIVE.load(std::sync::atomic::Ordering::Relaxed)
}

fn add_recent_document(path: &str) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    {
        windows_add_recent_document(path)?;
    }

    #[cfg(target_os = "macos")]
    {
        let _ = path;
    }

    #[cfg(target_os = "linux")]
    {
        let _ = path;
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    {
        let _ = path;
    }

    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn inhibit_child_slot() -> &'static Mutex<Option<Child>> {
    static SLOT: OnceLock<Mutex<Option<Child>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn inhibit_child_running(slot: &mut Option<Child>) -> bool {
    let running = match slot.as_mut() {
        Some(child) => matches!(child.try_wait(), Ok(None)),
        None => return false,
    };
    if !running {
        *slot = None;
    }
    running
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn clear_inhibit_child(slot: &mut Option<Child>) {
    if let Some(mut child) = slot.take() {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: plain kill(2) on a pid we spawned; a stale pid only yields ESRCH
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGTERM);
            }
        }
        let _ = child.kill();
        let _ = child.wait();
    }
}

#[cfg(target_os = "macos")]
fn caffeinate_args(pid: u32) -> Vec<String> {
    vec!["-ims".into(), "-w".into(), pid.to_string()]
}

#[cfg(target_os = "macos")]
fn macos_apply_download_inhibit(downloading: bool) -> bool {
    let Ok(mut child_guard) = inhibit_child_slot().lock() else {
        return false;
    };

    if !downloading {
        clear_inhibit_child(&mut child_guard);
        return false;
    }
    if inhibit_child_running(&mut child_guard) {
        return true;
    }
    match Command::new("caffeinate")
        .args(caffeinate_args(std::process::id()))
        .spawn()
    {
        Ok(child) => {
            *child_guard = Some(child);
            true
        }
        Err(err) => {
            tracing::warn!("Failed to spawn caffeinate: {}", err);
            false
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_inhibit_cookie_slot() -> &'static Mutex<Option<u32>> {
    static SLOT: OnceLock<Mutex<Option<u32>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

#[cfg(target_os = "linux")]
fn parse_dbus_uint32(output: &[u8]) -> Option<u32> {
    let stdout = String::from_utf8_lossy(output);
    stdout
        .split_whitespace()
        .find_map(|part| part.parse::<u32>().ok())
}

#[cfg(target_os = "linux")]
fn systemd_inhibit_args(pid: u32) -> Vec<String> {
    [
        "--what=sleep:idle",
        "--who=Risuko",
        "--why=Downloading active tasks",
        "--mode=block",
        "sh",
        "-c",
        "while kill -0 \"$0\" 2>/dev/null; do sleep 2; done",
    ]
    .into_iter()
    .map(String::from)
    .chain(std::iter::once(pid.to_string()))
    .collect()
}

#[cfg(target_os = "linux")]
const DBUS_REPLY_TIMEOUT_MS: u32 = 3000;

#[cfg(target_os = "linux")]
fn screensaver_args(method: &str, extra: &[String]) -> Vec<String> {
    let mut args = vec![
        "--session".to_string(),
        "--dest=org.freedesktop.ScreenSaver".to_string(),
        "--type=method_call".to_string(),
        "--print-reply".to_string(),
        format!("--reply-timeout={DBUS_REPLY_TIMEOUT_MS}"),
        "/org/freedesktop/ScreenSaver".to_string(),
        format!("org.freedesktop.ScreenSaver.{method}"),
    ];
    args.extend_from_slice(extra);
    args
}

#[cfg(target_os = "linux")]
fn spawn_logind_inhibit() -> Option<Child> {
    use std::os::unix::process::CommandExt;
    let mut child = Command::new("systemd-inhibit")
        .args(systemd_inhibit_args(std::process::id()))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .ok()?;
    std::thread::sleep(std::time::Duration::from_millis(150));
    if matches!(child.try_wait(), Ok(None)) {
        Some(child)
    } else {
        None
    }
}

#[cfg(target_os = "linux")]
fn screensaver_inhibit(cookie_guard: &mut Option<u32>) -> bool {
    let extra = [
        "string:Risuko".to_string(),
        "string:Downloading active tasks".to_string(),
    ];
    match Command::new("dbus-send")
        .args(screensaver_args("Inhibit", &extra))
        .output()
    {
        Ok(output) if output.status.success() => {
            if let Some(cookie) = parse_dbus_uint32(&output.stdout) {
                *cookie_guard = Some(cookie);
                true
            } else {
                tracing::warn!("Failed to parse DBus inhibit cookie from response");
                false
            }
        }
        Ok(output) => {
            tracing::warn!(
                "DBus Inhibit call failed with status {:?}",
                output.status.code()
            );
            false
        }
        Err(err) => {
            tracing::warn!("Failed to invoke DBus Inhibit call: {}", err);
            false
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_apply_download_inhibit(downloading: bool) -> bool {
    let (Ok(mut child_guard), Ok(mut cookie_guard)) = (
        inhibit_child_slot().lock(),
        linux_inhibit_cookie_slot().lock(),
    ) else {
        return false;
    };

    if downloading {
        if inhibit_child_running(&mut child_guard) || cookie_guard.is_some() {
            return true;
        }
        if let Some(child) = spawn_logind_inhibit() {
            *child_guard = Some(child);
            return true;
        }
        return screensaver_inhibit(&mut cookie_guard);
    }

    clear_inhibit_child(&mut child_guard);
    if let Some(cookie) = cookie_guard.take() {
        let extra = [format!("uint32:{cookie}")];
        match Command::new("dbus-send")
            .args(screensaver_args("UnInhibit", &extra))
            .output()
        {
            Ok(output) if output.status.success() => {}
            Ok(output) => tracing::warn!(
                "DBus UnInhibit call failed with status {:?} for cookie {}",
                output.status.code(),
                cookie
            ),
            Err(err) => tracing::warn!(
                "Failed to invoke DBus UnInhibit call for cookie {}: {}",
                cookie,
                err
            ),
        }
    }
    false
}

#[cfg(target_os = "windows")]
struct WindowsInhibitWorker {
    tx: mpsc::Sender<bool>,
}

#[cfg(target_os = "windows")]
fn windows_inhibit_worker() -> &'static WindowsInhibitWorker {
    const ES_CONTINUOUS: u32 = 0x8000_0000;
    const ES_SYSTEM_REQUIRED: u32 = 0x0000_0001;

    static WORKER: OnceLock<WindowsInhibitWorker> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<bool>();

        thread::spawn(move || {
            let mut inhibited = false;
            while let Ok(downloading) = rx.recv() {
                if downloading == inhibited {
                    continue;
                }

                let flags = if downloading {
                    ES_CONTINUOUS | ES_SYSTEM_REQUIRED
                } else {
                    ES_CONTINUOUS
                };

                unsafe {
                    // SAFETY: All SetThreadExecutionState calls happen on this dedicated worker thread
                    let _ = SetThreadExecutionState(flags);
                }

                inhibited = downloading;
            }
        });

        WindowsInhibitWorker { tx }
    })
}

#[cfg(target_os = "windows")]
fn windows_apply_download_inhibit(downloading: bool) -> bool {
    let worker = windows_inhibit_worker();
    if let Err(err) = worker.tx.send(downloading) {
        tracing::warn!("Failed to update Windows sleep inhibit state: {}", err);
        return false;
    }
    downloading
}

pub fn cleanup_download_inhibit() {
    #[cfg(target_os = "windows")]
    {
        windows_apply_download_inhibit(false);
    }

    #[cfg(target_os = "macos")]
    {
        macos_apply_download_inhibit(false);
    }

    #[cfg(target_os = "linux")]
    {
        linux_apply_download_inhibit(false);
    }
}

#[cfg(test)]
mod tests {
    use super::changed;
    use crate::cli::progress::format_size;
    use std::sync::Mutex;

    #[test]
    fn changed_reports_only_new_values() {
        let slot = Mutex::new(None);
        assert!(changed(&slot, "a".to_string()));
        assert!(!changed(&slot, "a".to_string()));
        assert!(changed(&slot, "b".to_string()));
        assert!(changed(&slot, "a".to_string()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn caffeinate_is_tied_to_our_pid() {
        let args = super::caffeinate_args(4242);
        assert_eq!(args, ["-ims", "-w", "4242"]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn dbus_calls_carry_a_reply_timeout() {
        let args = super::screensaver_args("UnInhibit", &["uint32:7".to_string()]);
        assert!(args.iter().any(|a| a.starts_with("--reply-timeout=")));
        assert_eq!(args.last().map(String::as_str), Some("uint32:7"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn systemd_inhibit_watches_our_pid() {
        let args = super::systemd_inhibit_args(99);
        assert_eq!(args.last().map(String::as_str), Some("99"));
        assert!(args.contains(&"--what=sleep:idle".to_string()));
    }

    #[test]
    fn format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn format_size_kb() {
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1536), "1.5 KB");
        assert_eq!(format_size(1024 * 1024 - 1), "1024.0 KB");
    }

    #[test]
    fn format_size_mb() {
        assert_eq!(format_size(1024 * 1024), "1.0 MB");
        assert_eq!(format_size(1024 * 1024 * 15), "15.0 MB");
    }

    #[test]
    fn format_size_gb() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_size(1024 * 1024 * 1024 * 3), "3.0 GB");
    }
}

#[cfg(target_os = "windows")]
fn windows_add_recent_document(path: &str) -> Result<(), String> {
    let path = Path::new(path);
    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    const SHARD_PATHW: u32 = 0x0000_0003;
    unsafe {
        // SAFETY: SHAddToRecentDocs reads a null-terminated wide path pointer for SHARD_PATHW
        SHAddToRecentDocs(SHARD_PATHW, wide.as_ptr() as *const c_void);
    }
    Ok(())
}

#[cfg(target_os = "windows")]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn SetThreadExecutionState(es_flags: u32) -> u32;
}

#[cfg(target_os = "windows")]
#[link(name = "shell32")]
unsafe extern "system" {
    fn SHAddToRecentDocs(u_flags: u32, pv: *const c_void);
}
