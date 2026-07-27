use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dbx_core::connection::AppState;
use dbx_core::file_connection_config::{FILE_SECRET_KEYS, FILE_SECRET_PREFIX};
use dbx_core::models::connection::DatabaseType;
use tauri::{ipc::Channel, State};

use crate::file_manager;
use crate::file_manager::models::{
    FileConnection, FileEntry, FileManagerError, FileRemoteOperationRequest, FileSecretStatus, FileTransferProgress,
    FileTransferRequest,
};
use crate::file_manager::{FileOperatorRegistry, FileTransferState};

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
pub async fn export_file_connection_secrets(
    state: State<'_, Arc<AppState>>,
    connection_ids: Vec<String>,
) -> Result<HashMap<String, HashMap<String, String>>, FileManagerError> {
    let requested: HashSet<&str> = connection_ids.iter().map(String::as_str).collect();
    let file_ids: Vec<String> = state
        .storage
        .load_connections()
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connections"))?
        .into_iter()
        .filter(|config| config.db_type == DatabaseType::FileManager && requested.contains(config.id.as_str()))
        .map(|config| config.id)
        .collect();
    let mut result = HashMap::new();
    for connection_id in file_ids {
        let mut secrets = HashMap::new();
        for key in FILE_SECRET_KEYS {
            if let Some(value) =
                state
                    .storage
                    .get_secret(&connection_id, &format!("{FILE_SECRET_PREFIX}{key}"))
                    .await
                    .map_err(|_| FileManagerError::new("storage", "Failed to read file connection credentials"))?
            {
                secrets.insert(key.to_string(), value);
            }
        }
        if !secrets.is_empty() {
            result.insert(connection_id, secrets);
        }
    }
    Ok(result)
}

#[tauri::command]
pub async fn stat_file_path(
    state: State<'_, Arc<AppState>>,
    registry: State<'_, FileOperatorRegistry>,
    connection_id: String,
    path: String,
) -> Result<FileEntry, FileManagerError> {
    file_manager::service::stat_path_cached(state.inner(), registry.inner(), &connection_id, &path).await
}

#[tauri::command]
pub async fn list_file_path(
    state: State<'_, Arc<AppState>>,
    registry: State<'_, FileOperatorRegistry>,
    connection_id: String,
    path: String,
) -> Result<Vec<FileEntry>, FileManagerError> {
    file_manager::service::list_path_cached(state.inner(), registry.inner(), &connection_id, &path).await
}

#[tauri::command]
pub async fn upload_file(
    state: State<'_, Arc<AppState>>,
    registry: State<'_, FileOperatorRegistry>,
    transfer_state: State<'_, FileTransferState>,
    request: FileTransferRequest,
) -> Result<u64, FileManagerError> {
    file_manager::transfer::upload_cached(state.inner(), registry.inner(), &transfer_state, request).await
}

#[tauri::command]
pub async fn download_file(
    state: State<'_, Arc<AppState>>,
    registry: State<'_, FileOperatorRegistry>,
    transfer_state: State<'_, FileTransferState>,
    request: FileTransferRequest,
    on_progress: Channel<FileTransferProgress>,
) -> Result<u64, FileManagerError> {
    file_manager::transfer::download_cached(
        state.inner(),
        registry.inner(),
        &transfer_state,
        request,
        move |bytes_transferred, total_bytes| {
            let _ = on_progress.send(FileTransferProgress { bytes_transferred, total_bytes });
        },
    )
    .await
}

#[tauri::command]
pub async fn delete_file_path(
    state: State<'_, Arc<AppState>>,
    registry: State<'_, FileOperatorRegistry>,
    transfer_state: State<'_, FileTransferState>,
    connection_id: String,
    path: String,
) -> Result<(), FileManagerError> {
    file_manager::transfer::delete_cached(state.inner(), registry.inner(), &transfer_state, &connection_id, &path).await
}

#[tauri::command]
pub async fn copy_file_path(
    state: State<'_, Arc<AppState>>,
    registry: State<'_, FileOperatorRegistry>,
    transfer_state: State<'_, FileTransferState>,
    request: FileRemoteOperationRequest,
) -> Result<(), FileManagerError> {
    file_manager::operations::copy_cached(state.inner(), registry.inner(), &transfer_state, request).await
}

#[tauri::command]
pub async fn rename_file_path(
    state: State<'_, Arc<AppState>>,
    registry: State<'_, FileOperatorRegistry>,
    transfer_state: State<'_, FileTransferState>,
    request: FileRemoteOperationRequest,
) -> Result<(), FileManagerError> {
    file_manager::operations::rename_cached(state.inner(), registry.inner(), &transfer_state, request).await
}
