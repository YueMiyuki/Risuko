use std::collections::HashMap;
#[cfg(not(target_os = "android"))]
use std::sync::Mutex;

#[cfg(not(target_os = "android"))]
use tauri::{
    menu::{AboutMetadataBuilder, Menu, MenuBuilder, MenuItemBuilder, Submenu, SubmenuBuilder},
    App, AppHandle, Emitter, Manager,
};

#[cfg(target_os = "android")]
use tauri::{App, AppHandle};

#[cfg(not(target_os = "android"))]
use super::{emit_command, show_and_emit};

#[cfg(not(target_os = "android"))]
static MENU_HIDDEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(not(target_os = "android"))]
pub fn set_menu_hidden(hidden: bool) {
    MENU_HIDDEN.store(hidden, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(target_os = "android"))]
static CACHED_LABELS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

#[cfg(not(target_os = "android"))]
pub(super) fn get_menu_text(labels: &HashMap<String, String>, id: &str, fallback: &str) -> String {
    labels
        .get(id)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

#[cfg(not(target_os = "android"))]
pub fn setup_menu(app: &App) -> Result<(), Box<dyn std::error::Error>> {
    let handle = app.handle();
    let menu = build_menu(handle, &HashMap::new())?;
    app.set_menu(menu)?;
    setup_menu_event_handler(app);
    Ok(())
}

#[cfg(target_os = "android")]
pub fn setup_menu(_app: &App) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[cfg(not(target_os = "android"))]
pub fn update_menu_labels(
    handle: &AppHandle,
    labels: &HashMap<String, String>,
) -> Result<(), String> {
    if let Ok(mut cached) = CACHED_LABELS.lock() {
        *cached = Some(labels.clone());
    }
    if MENU_HIDDEN.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }
    let menu = build_menu(handle, labels).map_err(|e| e.to_string())?;
    handle.set_menu(menu).map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(target_os = "android")]
pub fn update_menu_labels(
    _handle: &AppHandle,
    _labels: &HashMap<String, String>,
) -> Result<(), String> {
    Ok(())
}

#[cfg(not(target_os = "android"))]
pub fn toggle_app_menu(handle: &AppHandle, hidden: bool) -> Result<(), String> {
    set_menu_hidden(hidden);
    if hidden {
        handle.remove_menu().map_err(|e| e.to_string())?;
    } else {
        let labels = CACHED_LABELS
            .lock()
            .ok()
            .and_then(|c| c.clone())
            .unwrap_or_default();
        let menu = build_menu(handle, &labels).map_err(|e| e.to_string())?;
        handle.set_menu(menu).map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[cfg(target_os = "android")]
pub fn toggle_app_menu(_handle: &AppHandle, _hidden: bool) -> Result<(), String> {
    Ok(())
}

#[cfg(not(target_os = "android"))]
fn build_menu(
    handle: &AppHandle,
    labels: &HashMap<String, String>,
) -> Result<Menu<tauri::Wry>, Box<dyn std::error::Error>> {
    if cfg!(target_os = "macos") {
        build_macos_menu(handle, labels)
    } else {
        build_default_menu(handle, labels)
    }
}

#[cfg(not(target_os = "android"))]
fn build_macos_menu(
    handle: &AppHandle,
    labels: &HashMap<String, String>,
) -> Result<Menu<tauri::Wry>, Box<dyn std::error::Error>> {
    let mi = |id: &str, fb: &str| MenuItemBuilder::with_id(id, get_menu_text(labels, id, fb));

    #[allow(unused_mut)]
    let mut app_menu = SubmenuBuilder::new(handle, get_menu_text(labels, "menu-app", "Risuko"))
        .about(Some(
            AboutMetadataBuilder::new()
                .name(Some("Risuko"))
                .version(Some(env!("CARGO_PKG_VERSION")))
                .build(),
        ))
        .separator()
        .item(
            &mi("preferences", "Preferences...")
                .accelerator("CmdOrCtrl+,")
                .build(handle)?,
        );

    #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
    {
        app_menu = app_menu.item(&mi("check-for-updates", "Check for Updates...").build(handle)?);
    }

    let app_menu = app_menu
        .separator()
        .hide()
        .hide_others()
        .show_all()
        .separator()
        .item(
            &mi("quit", "Quit Risuko")
                .accelerator("CmdOrCtrl+Q")
                .build(handle)?,
        )
        .build()?;

    let task_menu = build_task_submenu(handle, true, labels)?;
    let edit_menu = build_edit_submenu(handle, labels)?;

    let window_menu = SubmenuBuilder::new(handle, get_menu_text(labels, "menu-window", "Window"))
        .item(
            &mi("reload", "Reload")
                .accelerator("CmdOrCtrl+R")
                .build(handle)?,
        )
        .close_window()
        .minimize()
        .maximize()
        .fullscreen()
        .separator()
        .item(&mi("front", "Bring All to Front").build(handle)?)
        .build()?;

    let help_menu = build_help_submenu(handle, labels)?;

    let menu = MenuBuilder::new(handle)
        .items(&[&app_menu, &task_menu, &edit_menu, &window_menu, &help_menu])
        .build()?;

    Ok(menu)
}

#[cfg(not(target_os = "android"))]
fn build_default_menu(
    handle: &AppHandle,
    labels: &HashMap<String, String>,
) -> Result<Menu<tauri::Wry>, Box<dyn std::error::Error>> {
    let mi = |id: &str, fb: &str| MenuItemBuilder::with_id(id, get_menu_text(labels, id, fb));

    #[allow(unused_mut)]
    let mut file_menu = SubmenuBuilder::new(handle, get_menu_text(labels, "menu-file", "File"))
        .item(&mi("about", "About Risuko").build(handle)?)
        .separator()
        .item(
            &mi("preferences", "Preferences...")
                .accelerator("CmdOrCtrl+,")
                .build(handle)?,
        );

    #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
    {
        file_menu = file_menu.item(&mi("check-for-updates", "Check for Updates...").build(handle)?);
    }

    let file_menu = file_menu
        .item(&mi("show-window", "Show Risuko").build(handle)?)
        .separator()
        .item(
            &mi("quit", "Quit Risuko")
                .accelerator("CmdOrCtrl+Q")
                .build(handle)?,
        )
        .build()?;

    let task_menu = build_task_submenu(handle, false, labels)?;
    let edit_menu = build_edit_submenu(handle, labels)?;

    let window_menu = SubmenuBuilder::new(handle, get_menu_text(labels, "menu-window", "Window"))
        .item(
            &mi("reload", "Reload")
                .accelerator("CmdOrCtrl+R")
                .build(handle)?,
        )
        .close_window()
        .minimize()
        .fullscreen()
        .build()?;

    let help_menu = build_help_submenu(handle, labels)?;

    let menu = MenuBuilder::new(handle)
        .items(&[&file_menu, &task_menu, &edit_menu, &window_menu, &help_menu])
        .build()?;

    Ok(menu)
}

#[cfg(not(target_os = "android"))]
fn build_task_submenu(
    handle: &tauri::AppHandle,
    include_clear_recent: bool,
    labels: &HashMap<String, String>,
) -> Result<Submenu<tauri::Wry>, Box<dyn std::error::Error>> {
    let mi = |id: &str, fb: &str| MenuItemBuilder::with_id(id, get_menu_text(labels, id, fb));

    let mut builder = SubmenuBuilder::new(handle, get_menu_text(labels, "menu-task", "Task"))
        .item(
            &mi("new-task", "New Task")
                .accelerator("CmdOrCtrl+N")
                .build(handle)?,
        )
        .item(
            &mi("new-bt-task", "New BT Task")
                .accelerator("CmdOrCtrl+Shift+N")
                .build(handle)?,
        )
        .item(
            &mi("open-file", "Open Torrent File...")
                .accelerator("CmdOrCtrl+O")
                .build(handle)?,
        )
        .separator()
        .item(
            &mi("task-list", "Task List")
                .accelerator("CmdOrCtrl+L")
                .build(handle)?,
        )
        .item(&mi("pause-task", "Pause Task").build(handle)?)
        .item(&mi("resume-task", "Resume Task").build(handle)?)
        .item(&mi("delete-task", "Delete Task").build(handle)?)
        .separator()
        .item(
            &mi("pause-all-task", "Pause All Tasks")
                .accelerator("CmdOrCtrl+Shift+P")
                .build(handle)?,
        )
        .item(
            &mi("resume-all-task", "Resume All Tasks")
                .accelerator("CmdOrCtrl+Shift+R")
                .build(handle)?,
        )
        .item(
            &mi("select-all-task", "Select All Tasks")
                .accelerator("CmdOrCtrl+Shift+A")
                .build(handle)?,
        );

    if include_clear_recent {
        builder = builder
            .separator()
            .item(&mi("clear-recent-tasks", "Clear Recent Tasks").build(handle)?);
    }

    Ok(builder.build()?)
}

#[cfg(not(target_os = "android"))]
fn build_edit_submenu(
    handle: &tauri::AppHandle,
    labels: &HashMap<String, String>,
) -> Result<Submenu<tauri::Wry>, Box<dyn std::error::Error>> {
    Ok(
        SubmenuBuilder::new(handle, get_menu_text(labels, "menu-edit", "Edit"))
            .undo()
            .redo()
            .separator()
            .cut()
            .copy()
            .paste()
            .select_all()
            .build()?,
    )
}

#[cfg(not(target_os = "android"))]
fn build_help_submenu(
    handle: &tauri::AppHandle,
    labels: &HashMap<String, String>,
) -> Result<Submenu<tauri::Wry>, Box<dyn std::error::Error>> {
    let mi = |id: &str, fb: &str| MenuItemBuilder::with_id(id, get_menu_text(labels, id, fb));

    let mut builder = SubmenuBuilder::new(handle, get_menu_text(labels, "menu-help", "Help"))
        .item(&mi("official-website", "Official Website").build(handle)?)
        .item(&mi("manual", "Manual").build(handle)?)
        .item(&mi("release-notes", "Release Notes").build(handle)?)
        .separator()
        .item(&mi("report-problem", "Report Problem").build(handle)?);

    if cfg!(debug_assertions) {
        builder = builder.separator().item(
            &mi("toggle-dev-tools", "Toggle Developer Tools")
                .accelerator("F12")
                .build(handle)?,
        );
    }

    Ok(builder.build()?)
}

#[cfg(not(target_os = "android"))]
fn setup_menu_event_handler(app: &App) {
    app.on_menu_event(move |app, event| {
        let id = event.id().as_ref();
        match id {
            "new-task" => show_and_emit(app, "application:new-task"),
            "new-bt-task" => show_and_emit(app, "application:new-bt-task"),
            "open-file" => show_and_emit(app, "application:open-file"),
            "task-list" => emit_command(app, "application:task-list"),
            "pause-task" => emit_command(app, "application:pause-task"),
            "resume-task" => emit_command(app, "application:resume-task"),
            "delete-task" => emit_command(app, "application:delete-task"),
            "pause-all-task" => emit_command(app, "application:pause-all-task"),
            "resume-all-task" => emit_command(app, "application:resume-all-task"),
            "select-all-task" => emit_command(app, "application:select-all-task"),
            "clear-recent-tasks" => emit_command(app, "application:clear-recent-tasks"),
            "preferences" => emit_command(app, "application:preferences"),
            "about" => emit_command(app, "application:about"),
            #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
            "check-for-updates" => show_and_emit(app, "application:check-for-updates"),
            "show-window" => {
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.show();
                    let _ = window.set_focus();
                }
            }
            "official-website" => {
                let _ = open::that("https://risuko.app");
            }
            "manual" => {
                let _ = open::that("https://risuko.app/docs");
            }
            "release-notes" => {
                let _ = open::that("https://github.com/YueMiyuki/Risuko/releases");
            }
            "report-problem" => {
                let _ = open::that("https://github.com/YueMiyuki/Risuko/issues");
            }
            "reload" => {
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.eval("window.location.reload()");
                }
            }
            "front" => {
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.show();
                    let _ = window.set_focus();
                }
            }
            "toggle-dev-tools" =>
            {
                #[cfg(debug_assertions)]
                if let Some(window) = app.get_webview_window("main") {
                    {
                        if window.is_devtools_open() {
                            window.close_devtools();
                        } else {
                            window.open_devtools();
                        }
                    }
                }
            }
            "quit" => {
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.show();
                    let _ = window.set_focus();
                }
                let _ = app.emit("confirm-quit", ());
            }
            _ => {}
        }
    });
}
