#[tauri::command]
pub async fn panel_ready(window: tauri::WebviewWindow) {
    #[cfg(not(target_os = "android"))]
    {
        use crate::managers::{clip_prompt, flyout};
        use tauri::Manager;
        let app = window.app_handle().clone();
        match window.label() {
            flyout::FLYOUT_LABEL => flyout::on_ready(&app),
            clip_prompt::CLIP_PROMPT_LABEL => clip_prompt::on_ready(&app),
            _ => {}
        }
    }
    #[cfg(target_os = "android")]
    let _ = window;
}
