pub mod clip_prompt;
pub mod flyout;
pub mod menu;
#[cfg(not(target_os = "android"))]
pub mod panel_gate;
pub mod tray;
pub mod vault;

#[cfg(not(target_os = "android"))]
use tauri::Emitter;

#[cfg(not(target_os = "android"))]
pub fn emit_command(app: &tauri::AppHandle, command: &str) {
    let _ = app.emit("command", serde_json::json!({ "command": command }));
}

#[cfg(not(target_os = "android"))]
pub fn show_and_emit(app: &tauri::AppHandle, command: &str) {
    let _ = crate::commands::app_cmds::show_main_window(app);
    emit_command(app, command);
}

#[cfg(not(target_os = "android"))]
fn popup_origin(
    icon: (f64, f64, f64, f64),
    work: (f64, f64, f64, f64),
    win: (f64, f64),
) -> (f64, f64) {
    let (icon_x, icon_y, icon_w, icon_h) = icon;
    let (wa_x, wa_y, wa_w, wa_h) = work;
    let (win_w, win_h) = win;
    let icon_center_x = icon_x + icon_w / 2.0;
    let icon_center_y = icon_y + icon_h / 2.0;

    let in_top_half = icon_center_y < wa_y + wa_h / 2.0;
    let y = if in_top_half {
        icon_y + icon_h
    } else {
        icon_y - win_h
    };
    let x = icon_center_x - win_w / 2.0;

    let max_x = (wa_x + wa_w - win_w).max(wa_x);
    let max_y = (wa_y + wa_h - win_h).max(wa_y);
    (x.clamp(wa_x, max_x), y.clamp(wa_y, max_y))
}

#[cfg(not(target_os = "android"))]
pub(crate) fn position_near_tray(
    app: &tauri::AppHandle,
    window: &tauri::WebviewWindow,
    width: f64,
    height: f64,
) {
    use tauri::{Manager, PhysicalPosition};

    let anchor = app
        .try_state::<crate::state::AppState>()
        .and_then(|state| state.tray_anchor.lock().ok().and_then(|guard| *guard));

    let icon = match anchor {
        Some(rect) => rect,
        None => match app.cursor_position() {
            Ok(pos) => (pos.x, pos.y, 0.0, 0.0),
            Err(_) => (0.0, 0.0, 0.0, 0.0),
        },
    };

    let monitor = app
        .monitor_from_point(icon.0 + icon.2 / 2.0, icon.1 + icon.3 / 2.0)
        .ok()
        .flatten()
        .or_else(|| app.primary_monitor().ok().flatten());

    let Some(monitor) = monitor else {
        let _ = window.set_position(PhysicalPosition::new(icon.0 as i32, icon.1 as i32));
        return;
    };

    let scale = monitor.scale_factor();
    let work = monitor.work_area();
    let (x, y) = popup_origin(
        icon,
        (
            work.position.x as f64,
            work.position.y as f64,
            work.size.width as f64,
            work.size.height as f64,
        ),
        (width * scale, height * scale),
    );
    let _ = window.set_position(PhysicalPosition::new(x.round() as i32, y.round() as i32));
}

#[cfg(all(test, not(target_os = "android")))]
mod tests {
    use super::popup_origin;

    const WORK: (f64, f64, f64, f64) = (0.0, 0.0, 1000.0, 800.0);

    #[test]
    fn top_tray_opens_below_icon_centred() {
        let (x, y) = popup_origin((480.0, 0.0, 40.0, 24.0), WORK, (200.0, 300.0));
        assert_eq!((x, y), (400.0, 24.0));
    }

    #[test]
    fn bottom_tray_opens_above_icon() {
        let (_, y) = popup_origin((480.0, 776.0, 40.0, 24.0), WORK, (200.0, 300.0));
        assert_eq!(y, 476.0);
    }

    #[test]
    fn clamps_into_work_area() {
        let (x, _) = popup_origin((990.0, 0.0, 10.0, 24.0), WORK, (200.0, 300.0));
        assert_eq!(x, 800.0);
        let (x, _) = popup_origin((0.0, 0.0, 10.0, 24.0), WORK, (200.0, 300.0));
        assert_eq!(x, 0.0);
    }

    #[test]
    fn oversized_window_pins_to_origin() {
        let (x, y) = popup_origin((500.0, 400.0, 10.0, 10.0), WORK, (2000.0, 2000.0));
        assert_eq!((x, y), (0.0, 0.0));
    }
}
