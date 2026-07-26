use std::sync::Arc;

use dbx_core::connection::AppState;
use tauri::State;

use crate::file_manager;
use crate::file_manager::models::{
    FileConnection, FileEntry, FileManagerError, SaveFileConnectionRequest, TestFileConnectionRequest,
};

#[tauri::command]
pub async fn list_file_connections(state: State<'_, Arc<AppState>>) -> Result<Vec<FileConnection>, FileManagerError> {
    file_manager::service::list_connections(&state.storage).await
}

#[tauri::command]
pub async fn save_file_connection(
    state: State<'_, Arc<AppState>>,
    request: SaveFileConnectionRequest,
) -> Result<FileConnection, FileManagerError> {
    file_manager::service::save_connection(&state.storage, request).await
}

#[tauri::command]
pub async fn delete_file_connection(state: State<'_, Arc<AppState>>, id: String) -> Result<(), FileManagerError> {
    file_manager::service::delete_connection(&state.storage, &id).await
}

#[tauri::command]
pub async fn test_file_connection(
    state: State<'_, Arc<AppState>>,
    request: TestFileConnectionRequest,
) -> Result<(), FileManagerError> {
    file_manager::service::test_connection(&state.storage, request).await
}

#[tauri::command]
pub async fn stat_file_path(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    path: String,
) -> Result<FileEntry, FileManagerError> {
    file_manager::service::stat_path(&state.storage, &connection_id, &path).await
}

#[tauri::command]
pub async fn list_file_path(
    state: State<'_, Arc<AppState>>,
    connection_id: String,
    path: String,
) -> Result<Vec<FileEntry>, FileManagerError> {
    file_manager::service::list_path(&state.storage, &connection_id, &path).await
}
