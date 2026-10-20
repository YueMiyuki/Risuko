use serde::Serialize;
use serde_json::Value;
use tauri::State;

use crate::state::AppState;

#[derive(Serialize)]
pub struct VaultStatus {
    pub enabled: bool,
    pub backend: &'static str,
}

#[tauri::command(async)]
pub fn vault_status(state: State<'_, AppState>) -> VaultStatus {
    VaultStatus {
        enabled: state.vault.enabled(),
        backend: "os-keychain",
    }
}

#[tauri::command(async)]
pub fn vault_put_credential(
    state: State<'_, AppState>,
    id: String,
    secrets: Value,
) -> Result<(), String> {
    state.vault.put(&id, &secrets)
}

#[tauri::command(async)]
pub fn vault_get_credential(
    state: State<'_, AppState>,
    id: String,
) -> Result<Option<Value>, String> {
    state.vault.get(&id)
}

#[tauri::command(async)]
pub fn vault_remove_credential(state: State<'_, AppState>, id: String) -> Result<(), String> {
    state.vault.remove(&id)
}
