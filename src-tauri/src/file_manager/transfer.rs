use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use dbx_core::connection::AppState;
#[cfg(test)]
use dbx_core::storage::Storage;
use opendal::Operator;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};
use uuid::Uuid;

use super::adapter::map_operation_error;
use super::models::{FileManagerError, FileTransferRequest};
use super::registry::FileOperatorRegistry;
#[cfg(test)]
use super::service::operator_for_connection;
use super::service::{ensure_writable_connection, operator_lease_for_connection, validate_remote_path};

const TRANSFER_BUFFER_SIZE: usize = 64 * 1024;
#[cfg(test)]
pub(crate) const TRANSFER_TIMEOUT_SECS: u64 = 60;
const GLOBAL_TRANSFER_LIMIT: usize = 8;
const CONNECTION_TRANSFER_LIMIT: usize = 2;

pub struct FileTransferState {
    global: Arc<Semaphore>,
    connections: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl Default for FileTransferState {
    fn default() -> Self {
        Self { global: Arc::new(Semaphore::new(GLOBAL_TRANSFER_LIMIT)), connections: Mutex::new(HashMap::new()) }
    }
}

impl FileTransferState {
    pub(crate) async fn acquire(
        &self,
        connection_id: &str,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), FileManagerError> {
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| FileManagerError::new("unavailable", "File transfers are unavailable"))?;
        let connection = {
            let mut connections = self.connections.lock().await;
            connections
                .entry(connection_id.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(CONNECTION_TRANSFER_LIMIT)))
                .clone()
        }
        .acquire_owned()
        .await
        .map_err(|_| FileManagerError::new("unavailable", "This file connection is unavailable"))?;
        Ok((global, connection))
    }

    pub async fn forget_connection(&self, connection_id: &str) {
        self.connections.lock().await.remove(connection_id);
    }
}

#[cfg(test)]
pub async fn upload(
    storage: &Storage,
    state: &FileTransferState,
    request: FileTransferRequest,
) -> Result<u64, FileManagerError> {
    ensure_writable_connection(storage, &request.connection_id).await?;
    let operator = operator_for_connection(storage, &request.connection_id).await?;
    upload_with_operator(state, request, operator, Some(Duration::from_secs(TRANSFER_TIMEOUT_SECS))).await
}

pub async fn upload_cached(
    app_state: &AppState,
    registry: &FileOperatorRegistry,
    state: &FileTransferState,
    request: FileTransferRequest,
) -> Result<u64, FileManagerError> {
    ensure_writable_connection(&app_state.storage, &request.connection_id).await?;
    let lease = operator_lease_for_connection(app_state, registry, &request.connection_id).await?;
    upload_with_operator(state, request, (*lease.operator).clone(), lease.operation_timeout).await
}

async fn upload_with_operator(
    state: &FileTransferState,
    request: FileTransferRequest,
    operator: Operator,
    timeout: Option<Duration>,
) -> Result<u64, FileManagerError> {
    let remote_path = non_root_remote_path(&request.remote_path)?;
    let local_path = absolute_local_path(&request.local_path)?;
    let metadata = tokio::fs::metadata(&local_path)
        .await
        .map_err(|_| FileManagerError::new("local_not_found", "The local upload file does not exist"))?;
    if !metadata.is_file() {
        return Err(FileManagerError::configuration("The upload source must be a file"));
    }
    let _permits = state.acquire(&request.connection_id).await?;

    transfer_with_configured_timeout(timeout, async {
        if !request.replace && operator.exists(&remote_path).await.map_err(map_operation_error)? {
            return Err(FileManagerError::new("already_exists", "The remote destination already exists"));
        }
        let source = File::open(&local_path)
            .await
            .map_err(|_| FileManagerError::new("local_read", "Failed to open the local upload file"))?;
        let writer = if !request.replace && operator.info().full_capability().write_with_if_not_exists {
            operator.writer_with(&remote_path).if_not_exists(true).await
        } else {
            operator.writer(&remote_path).await
        }
        .map_err(map_operation_error)?
        .into_futures_async_write()
        .compat_write();
        copy_with_fixed_buffer(source, writer)
            .await
            .map_err(|_| FileManagerError::new("transfer", "The upload did not complete"))
    })
    .await
}

#[cfg(test)]
pub async fn download(
    storage: &Storage,
    state: &FileTransferState,
    request: FileTransferRequest,
) -> Result<u64, FileManagerError> {
    let operator = operator_for_connection(storage, &request.connection_id).await?;
    download_with_operator(state, request, operator, Some(Duration::from_secs(TRANSFER_TIMEOUT_SECS))).await
}

pub async fn download_cached(
    app_state: &AppState,
    registry: &FileOperatorRegistry,
    state: &FileTransferState,
    request: FileTransferRequest,
) -> Result<u64, FileManagerError> {
    let lease = operator_lease_for_connection(app_state, registry, &request.connection_id).await?;
    download_with_operator(state, request, (*lease.operator).clone(), lease.operation_timeout).await
}

async fn download_with_operator(
    state: &FileTransferState,
    request: FileTransferRequest,
    operator: Operator,
    timeout: Option<Duration>,
) -> Result<u64, FileManagerError> {
    let remote_path = non_root_remote_path(&request.remote_path)?;
    let local_path = absolute_local_path(&request.local_path)?;
    validate_download_target(&local_path, request.replace).await?;
    let _permits = state.acquire(&request.connection_id).await?;
    let (temporary_path, temporary_file) = create_download_temporary(&local_path).await?;

    let transfer_result = transfer_with_configured_timeout(timeout, async {
        let reader = operator
            .reader(&remote_path)
            .await
            .map_err(map_operation_error)?
            .into_futures_async_read(..)
            .await
            .map_err(map_operation_error)?
            .compat();
        let mut temporary_file = temporary_file;
        let bytes = copy_with_fixed_buffer(reader, &mut temporary_file)
            .await
            .map_err(|_| FileManagerError::new("transfer", "The download did not complete"))?;
        temporary_file
            .sync_all()
            .await
            .map_err(|_| FileManagerError::new("local_write", "Failed to flush the downloaded file"))?;
        Ok(bytes)
    })
    .await;

    let bytes = match transfer_result {
        Ok(bytes) => bytes,
        Err(error) => {
            let _ = tokio::fs::remove_file(&temporary_path).await;
            return Err(error);
        }
    };
    if let Err(error) = publish_download(&temporary_path, &local_path, request.replace).await {
        let _ = tokio::fs::remove_file(&temporary_path).await;
        return Err(error);
    }
    Ok(bytes)
}

#[cfg(test)]
pub async fn delete(
    storage: &Storage,
    state: &FileTransferState,
    connection_id: &str,
    path: &str,
) -> Result<(), FileManagerError> {
    ensure_writable_connection(storage, connection_id).await?;
    let operator = operator_for_connection(storage, connection_id).await?;
    delete_with_operator(state, connection_id, path, operator, Some(Duration::from_secs(TRANSFER_TIMEOUT_SECS))).await
}

pub async fn delete_cached(
    app_state: &AppState,
    registry: &FileOperatorRegistry,
    state: &FileTransferState,
    connection_id: &str,
    path: &str,
) -> Result<(), FileManagerError> {
    ensure_writable_connection(&app_state.storage, connection_id).await?;
    let lease = operator_lease_for_connection(app_state, registry, connection_id).await?;
    delete_with_operator(state, connection_id, path, (*lease.operator).clone(), lease.operation_timeout).await
}

async fn delete_with_operator(
    state: &FileTransferState,
    connection_id: &str,
    path: &str,
    operator: Operator,
    timeout: Option<Duration>,
) -> Result<(), FileManagerError> {
    let path = non_root_remote_path(path)?;
    let _permits = state.acquire(connection_id).await?;
    transfer_with_configured_timeout(timeout, async {
        let metadata = operator.stat(&path).await.map_err(map_operation_error)?;
        if metadata.is_dir() {
            let directory = format!("{path}/");
            let entries = operator.list(&directory).await.map_err(map_operation_error)?;
            if entries.iter().any(|entry| entry.path() != directory) {
                return Err(FileManagerError::new("directory_not_empty", "Only empty directories can be deleted"));
            }
        }
        let delete_path = if metadata.is_dir() { format!("{path}/") } else { path };
        operator.delete(&delete_path).await.map_err(map_operation_error)
    })
    .await
}

pub(crate) async fn transfer_with_configured_timeout<T>(
    timeout: Option<Duration>,
    operation: impl std::future::Future<Output = Result<T, FileManagerError>>,
) -> Result<T, FileManagerError> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, operation)
            .await
            .map_err(|_| FileManagerError::new("timeout", "The file transfer timed out"))?,
        None => operation.await,
    }
}

pub(crate) async fn copy_with_fixed_buffer<R, W>(reader: R, mut writer: W) -> std::io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut reader = BufReader::with_capacity(TRANSFER_BUFFER_SIZE, reader);
    let bytes = tokio::io::copy(&mut reader, &mut writer).await?;
    writer.shutdown().await?;
    Ok(bytes)
}

pub(crate) fn non_root_remote_path(path: &str) -> Result<String, FileManagerError> {
    let path = validate_remote_path(path)?;
    if path.is_empty() {
        return Err(FileManagerError::configuration("This operation cannot target the connection root"));
    }
    Ok(path)
}

fn absolute_local_path(path: &str) -> Result<PathBuf, FileManagerError> {
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        return Err(FileManagerError::configuration("Local file paths must be absolute"));
    }
    Ok(path)
}

async fn validate_download_target(path: &Path, replace: bool) -> Result<(), FileManagerError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(FileManagerError::new("unsafe_local_target", "The download target cannot be a symbolic link"))
        }
        Ok(metadata) if metadata.is_dir() => {
            Err(FileManagerError::configuration("The download target must be a file path"))
        }
        Ok(_) if !replace => Err(FileManagerError::new("already_exists", "The local destination already exists")),
        Ok(_) | Err(_) => Ok(()),
    }
}

async fn create_download_temporary(target: &Path) -> Result<(PathBuf, File), FileManagerError> {
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| FileManagerError::configuration("The download target has no parent directory"))?;
    let parent_metadata = tokio::fs::metadata(parent)
        .await
        .map_err(|_| FileManagerError::new("local_not_found", "The download directory does not exist"))?;
    if !parent_metadata.is_dir() {
        return Err(FileManagerError::configuration("The download parent path is not a directory"));
    }
    for _ in 0..8 {
        let path = parent.join(format!(".dbx-download-{}.tmp", Uuid::new_v4()));
        match OpenOptions::new().write(true).create_new(true).open(&path).await {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => {
                return Err(FileManagerError::new("local_write", "Failed to create the download temporary file"));
            }
        }
    }
    Err(FileManagerError::new("local_write", "Failed to allocate a download temporary file"))
}

async fn publish_download(temporary: &Path, target: &Path, replace: bool) -> Result<(), FileManagerError> {
    validate_download_target(target, replace).await?;
    if replace {
        tokio::fs::rename(temporary, target)
            .await
            .map_err(|_| FileManagerError::new("local_write", "Failed to publish the downloaded file"))
    } else {
        tokio::fs::hard_link(temporary, target).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                FileManagerError::new("already_exists", "The local destination already exists")
            } else {
                FileManagerError::new("local_write", "Failed to publish the downloaded file")
            }
        })?;
        let _ = tokio::fs::remove_file(temporary).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use dbx_core::storage::Storage;
    use uuid::Uuid;

    use super::{
        copy_with_fixed_buffer, delete, download, upload, FileTransferRequest, FileTransferState, TRANSFER_BUFFER_SIZE,
    };
    use crate::file_manager::models::{
        FileConnectionConfig, FileSecretUpdates, SaveFileConnectionRequest, SecretUpdate,
    };
    use crate::file_manager::service::save_connection;

    struct GeneratedReader {
        remaining: usize,
    }

    impl AsyncRead for GeneratedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let count = self.remaining.min(buffer.remaining());
            buffer.initialize_unfilled_to(count).fill(b'x');
            buffer.advance(count);
            self.remaining -= count;
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Default)]
    struct BoundedWriter {
        bytes: usize,
        largest_write: usize,
    }

    impl AsyncWrite for BoundedWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.bytes += buffer.len();
            self.largest_write = self.largest_write.max(buffer.len());
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn streaming_buffer_does_not_scale_with_file_size() {
        let file_size = TRANSFER_BUFFER_SIZE * 64;
        let reader = GeneratedReader { remaining: file_size };
        let mut writer = BoundedWriter::default();
        let copied = copy_with_fixed_buffer(reader, &mut writer).await.unwrap();
        assert_eq!(copied, file_size as u64);
        assert_eq!(writer.bytes, file_size);
        assert!(writer.largest_write <= TRANSFER_BUFFER_SIZE);
    }

    #[tokio::test]
    #[ignore = "requires deploy/file-manager FTP service"]
    async fn ftp_transfer_delete_and_no_clobber_contract() {
        let suffix = Uuid::new_v4();
        let database = std::env::temp_dir().join(format!("dbx-file-transfer-{suffix}.db"));
        let storage = Storage::open(&database).await.unwrap();
        save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "ftp-transfer".to_string(),
                name: "FTP Transfer".to_string(),
                config: FileConnectionConfig::Ftp {
                    endpoint: "127.0.0.1".to_string(),
                    port: 2121,
                    root: "/ftp/dbx/".to_string(),
                    username: "dbx".to_string(),
                },
                secrets: FileSecretUpdates {
                    password: SecretUpdate::Set("dbx-password".to_string()),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
        let state = FileTransferState::default();
        let local_source = std::env::temp_dir().join(format!("dbx-upload-{suffix}.bin"));
        let local_download = std::env::temp_dir().join(format!("dbx-download-{suffix}.bin"));
        let directory = format!("transfer-{suffix}");
        let remote_path = format!("{directory}/source.bin");
        tokio::fs::write(&local_source, vec![b'x'; TRANSFER_BUFFER_SIZE * 32]).await.unwrap();

        let request = FileTransferRequest {
            connection_id: "ftp-transfer".to_string(),
            remote_path: remote_path.clone(),
            local_path: local_source.to_string_lossy().to_string(),
            replace: false,
        };
        upload(&storage, &state, request.clone()).await.unwrap();
        assert_eq!(upload(&storage, &state, request).await.unwrap_err().code, "already_exists");

        tokio::fs::write(&local_source, b"replacement").await.unwrap();
        upload(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "ftp-transfer".to_string(),
                remote_path: remote_path.clone(),
                local_path: local_source.to_string_lossy().to_string(),
                replace: true,
            },
        )
        .await
        .unwrap();
        let download_request = FileTransferRequest {
            connection_id: "ftp-transfer".to_string(),
            remote_path: remote_path.clone(),
            local_path: local_download.to_string_lossy().to_string(),
            replace: false,
        };
        download(&storage, &state, download_request.clone()).await.unwrap();
        assert_eq!(tokio::fs::read(&local_download).await.unwrap(), b"replacement");
        assert_eq!(download(&storage, &state, download_request).await.unwrap_err().code, "already_exists");

        assert_eq!(delete(&storage, &state, "ftp-transfer", &directory).await.unwrap_err().code, "directory_not_empty");
        delete(&storage, &state, "ftp-transfer", &remote_path).await.unwrap();
        delete(&storage, &state, "ftp-transfer", &directory).await.unwrap();

        let _ = tokio::fs::remove_file(local_source).await;
        let _ = tokio::fs::remove_file(local_download).await;
        let _ = tokio::fs::remove_file(database).await;
    }
}
