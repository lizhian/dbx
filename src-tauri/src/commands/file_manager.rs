use std::sync::Arc;

use dbx_core::connection::AppState;
use tauri::State;

use crate::file_manager;
use crate::file_manager::models::{
    FileConnection, FileManagerError, SaveFileConnectionRequest, TestFileConnectionRequest,
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
