use std::sync::Arc;

use dbx_core::models::connection::{DatabaseType, TransportLayerConfig};
use tauri::State;

use super::connection::AppState;
use crate::file_manager::FileOperatorRegistry;

#[tauri::command]
pub async fn load_tunnel_profiles(state: State<'_, Arc<AppState>>) -> Result<Vec<TransportLayerConfig>, String> {
    state.storage.load_tunnel_profiles().await
}

#[tauri::command]
pub async fn save_tunnel_profiles(
    state: State<'_, Arc<AppState>>,
    file_registry: State<'_, FileOperatorRegistry>,
    profiles: Vec<TransportLayerConfig>,
) -> Result<(), String> {
    state.storage.save_tunnel_profiles(&profiles).await?;
    let file_ids: Vec<String> = state
        .storage
        .load_connections()
        .await?
        .into_iter()
        .filter(|config| config.db_type == DatabaseType::FileManager)
        .map(|config| config.id)
        .collect();
    file_registry.drop_connections(state.inner(), file_ids.iter().map(String::as_str)).await;
    Ok(())
}

#[tauri::command]
pub async fn test_tunnel_profile(
    state: State<'_, Arc<AppState>>,
    profile: TransportLayerConfig,
) -> Result<String, String> {
    state.test_tunnel_profile(&profile).await
}
