use std::collections::HashMap;

#[cfg(not(target_os = "android"))]
use tauri::{
    image::Image,
    menu::{Menu, MenuBuilder, MenuItemBuilder, PredefinedMenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
    App, AppHandle, Emitter,
};

#[cfg(target_os = "android")]
use tauri::{App, AppHandle};

#[cfg(not(target_os = "android"))]
use super::{flyout, menu::get_menu_text, show_and_emit};

#[cfg(not(target_os = "android"))]
fn tray_event_rect(event: &TrayIconEvent) -> Option<(&tauri::Rect, (f64, f64))> {
    match event {
        TrayIconEvent::Click { rect, position, .. }
        | TrayIconEvent::DoubleClick { rect, position, .. }
        | TrayIconEvent::Enter { rect, position, .. }
        | TrayIconEvent::Move { rect, position, .. }
        | TrayIconEvent::Leave { rect, position, .. } => Some((rect, (position.x, position.y))),
        _ => None,
    }
}

#[cfg(not(target_os = "android"))]
fn build_tray_menu(
    handle: &AppHandle,
    labels: &HashMap<String, String>,
) -> Result<Menu<tauri::Wry>, Box<dyn std::error::Error>> {
    let item = |id: &str, fb: &str| {
        MenuItemBuilder::with_id(id, get_menu_text(labels, id, fb)).build(handle)
    };

    let new_task = item("tray-new-task", "New Task")?;
    let new_bt_task = item("tray-new-bt-task", "New BT Task")?;
    let open_file = item("tray-open-file", "Open Torrent File...")?;
    let sep1 = PredefinedMenuItem::separator(handle)?;
    let show = item("tray-show", "Show Risuko")?;
    let quick_panel = item("tray-quick-panel", "Quick Panel")?;
    let manual = item("tray-manual", "Manual")?;
    let sep2 = PredefinedMenuItem::separator(handle)?;
    let task_list = item("tray-task-list", "Task List")?;
    let preferences = item("tray-preferences", "Preferences...")?;
    let sep3 = PredefinedMenuItem::separator(handle)?;
    let quit = item("tray-quit", "Quit")?;

    #[allow(unused_mut)]
    let mut builder = MenuBuilder::new(handle).items(&[
        &new_task,
        &new_bt_task,
        &open_file,
        &sep1,
        &show,
        &quick_panel,
        &manual,
    ]);

    #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
    {
        let check_updates = item("tray-check-updates", "Check for Updates...")?;
        builder = builder.item(&check_updates);
    }

    let menu = builder
        .items(&[&sep2, &task_list, &preferences, &sep3, &quit])
        .build()?;

    Ok(menu)
}

#[cfg(not(target_os = "android"))]
pub fn setup_tray(app: &App) -> Result<(), Box<dyn std::error::Error>> {
    let handle = app.handle();
    let menu = build_tray_menu(handle, &HashMap::new())?;

    let icon_bytes = include_bytes!("../../icons/icon.png");
    let icon = Image::from_bytes(icon_bytes).expect("Failed to load tray icon");

    let _tray = TrayIconBuilder::with_id("main")
        .icon(icon)
        .menu(&menu)
        .tooltip("Risuko")
        .icon_as_template(true)
        .show_menu_on_left_click(false)
        .on_tray_icon_event(|tray, event| {
            let app = tray.app_handle();
            if let Some((rect, point)) = tray_event_rect(&event) {
                let scale = app
                    .monitor_from_point(point.0, point.1)
                    .ok()
                    .flatten()
                    .map(|m| m.scale_factor())
                    .unwrap_or(1.0);
                flyout::cache_tray_rect(app, rect, scale);
            }

            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                flyout::toggle_flyout(app);
            }
        })
        .on_menu_event(move |app, event| {
            let id = event.id().as_ref();
            match id {
                "tray-new-task" => show_and_emit(app, "application:new-task"),
                "tray-new-bt-task" => show_and_emit(app, "application:new-bt-task"),
                "tray-open-file" => show_and_emit(app, "application:open-file"),
                "tray-show" => {
                    let _ = crate::commands::app_cmds::show_main_window(app);
                }
                "tray-quick-panel" => flyout::toggle_flyout(app),
                "tray-manual" => {
                    let _ = open::that("https://risuko.app/docs");
                }
                #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
                "tray-check-updates" => show_and_emit(app, "application:check-for-updates"),
                "tray-task-list" => show_and_emit(app, "application:task-list"),
                "tray-preferences" => show_and_emit(app, "application:preferences"),
                "tray-quit" => {
                    let _ = crate::commands::app_cmds::show_main_window(app);
                    let _ = app.emit("confirm-quit", ());
                }
                _ => {}
            }
        })
        .build(app)?;

    Ok(())
}

#[cfg(target_os = "android")]
pub fn setup_tray(_app: &App) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[cfg(not(target_os = "android"))]
pub fn update_tray_menu_labels(
    handle: &AppHandle,
    labels: &HashMap<String, String>,
) -> Result<(), String> {
    let Some(tray) = handle.tray_by_id("main") else {
        return Ok(());
    };

    let menu = build_tray_menu(handle, labels).map_err(|e| e.to_string())?;
    tray.set_menu(Some(menu)).map_err(|e| e.to_string())
}

#[cfg(target_os = "android")]
pub fn update_tray_menu_labels(
    _handle: &AppHandle,
    _labels: &HashMap<String, String>,
) -> Result<(), String> {
    Ok(())
}
