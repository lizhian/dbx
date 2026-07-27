use std::sync::Arc;

use dbx_core::connection::AppState;
use tauri::State;

use crate::file_manager;
use crate::file_manager::models::{
    FileConnection, FileEntry, FileManagerError, FileRemoteOperationRequest, FileSecretStatus, FileTransferRequest,
    SaveFileConnectionRequest, TestFileConnectionRequest,
};
use crate::file_manager::FileTransferState;

#[tauri::command]
pub async fn list_file_connections(state: State<'_, Arc<AppState>>) -> Result<Vec<FileConnection>, FileManagerError> {
    file_manager::service::list_connections(&state.storage).await
}

#[tauri::command]
pub async fn file_connection_secret_status(
    state: State<'_, Arc<AppState>>,
    id: String,
) -> Result<FileSecretStatus, FileManagerError> {
    state
        .storage
        .file_connection_secret_status(&id)
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connection credential status"))
}

#[tauri::command]
pub async fn save_file_connection(
    state: State<'_, Arc<AppState>>,
    request: SaveFileConnectionRequest,
) -> Result<FileConnection, FileManagerError> {
    file_manager::service::save_connection(&state.storage, request).await
}

#[tauri::command]
pub async fn delete_file_connection(
    state: State<'_, Arc<AppState>>,
    transfer_state: State<'_, FileTransferState>,
    id: String,
) -> Result<(), FileManagerError> {
    file_manager::service::delete_connection(&state.storage, &id).await?;
    transfer_state.forget_connection(&id).await;
    Ok(())
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

#[tauri::command]
pub async fn upload_file(
    state: State<'_, Arc<AppState>>,
    transfer_state: State<'_, FileTransferState>,
    request: FileTransferRequest,
) -> Result<u64, FileManagerError> {
    file_manager::transfer::upload(&state.storage, &transfer_state, request).await
}

#[tauri::command]
pub async fn download_file(
    state: State<'_, Arc<AppState>>,
    transfer_state: State<'_, FileTransferState>,
    request: FileTransferRequest,
) -> Result<u64, FileManagerError> {
    file_manager::transfer::download(&state.storage, &transfer_state, request).await
}

#[tauri::command]
pub async fn delete_file_path(
    state: State<'_, Arc<AppState>>,
    transfer_state: State<'_, FileTransferState>,
    connection_id: String,
    path: String,
) -> Result<(), FileManagerError> {
    file_manager::transfer::delete(&state.storage, &transfer_state, &connection_id, &path).await
}

#[tauri::command]
pub async fn copy_file_path(
    state: State<'_, Arc<AppState>>,
    transfer_state: State<'_, FileTransferState>,
    request: FileRemoteOperationRequest,
) -> Result<(), FileManagerError> {
    file_manager::operations::copy(&state.storage, &transfer_state, request).await
}

#[tauri::command]
pub async fn rename_file_path(
    state: State<'_, Arc<AppState>>,
    transfer_state: State<'_, FileTransferState>,
    request: FileRemoteOperationRequest,
) -> Result<(), FileManagerError> {
    file_manager::operations::rename(&state.storage, &transfer_state, request).await
}
