#[cfg(mobile)]
#[tauri::mobile_entry_point]
pub fn run() {
    risuko_tauri::run(tauri::generate_context!());
}
