use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dbx_core::connection::AppState;
use dbx_core::storage::FileTransferStorageRecord;
use futures::io::AsyncRead as FuturesAsyncRead;
use futures::io::AsyncReadExt as FuturesAsyncReadExt;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager, Runtime, State, WebviewWindow};
use tauri_plugin_fs::FsExt;
use tempfile::{Builder as TempFileBuilder, TempPath};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::file_manager::{validate_remote_relative_path, FileManagerRuntime, PreparedFileOperation};

const DOWNLOAD_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const GLOBAL_TRANSFER_LIMIT: usize = 8;
const CONNECTION_TRANSFER_LIMIT: usize = 4;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);
const GLOBAL_PROGRESS_INTERVAL: Duration = Duration::from_millis(50);
const IO_PROGRESS_WATCHDOG: Duration = Duration::from_secs(30);
const DOWNLOAD_OPERATION_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const TRANSFER_EVENT: &str = "file-transfer-progress";

pub struct FileTransferRuntime {
    global_limit: Arc<Semaphore>,
    connection_limits: Mutex<HashMap<String, Arc<Semaphore>>>,
    active: Mutex<HashMap<String, ActiveTransfer>>,
    recovery: OnceCell<()>,
    last_progress_event: Mutex<Option<Instant>>,
}

struct ActiveTransfer {
    connection_id: String,
    cancellation: CancellationToken,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartDownloadInput {
    pub connection_id: String,
    pub remote_path: String,
    pub local_path: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartTransferResult {
    pub transfer_id: String,
}

#[derive(Debug)]
struct TransferFailure {
    status: &'static str,
    message: String,
    invalidate_operator: bool,
}

struct DownloadOutcome {
    bytes_transferred: i64,
    total_bytes: Option<i64>,
}

struct TransferProgressSnapshot {
    bytes_transferred: AtomicI64,
    total_bytes: AtomicI64,
}

impl TransferProgressSnapshot {
    fn new() -> Self {
        Self { bytes_transferred: AtomicI64::new(0), total_bytes: AtomicI64::new(-1) }
    }

    fn record_total(&self, total_bytes: Option<i64>) {
        self.total_bytes.store(total_bytes.unwrap_or(-1), Ordering::Release);
    }

    fn record_bytes(&self, bytes_transferred: i64) {
        self.bytes_transferred.fetch_max(bytes_transferred, Ordering::AcqRel);
    }

    fn bytes(&self) -> i64 {
        self.bytes_transferred.load(Ordering::Acquire)
    }

    fn total(&self) -> Option<i64> {
        let total = self.total_bytes.load(Ordering::Acquire);
        (total >= 0).then_some(total)
    }
}

impl FileTransferRuntime {
    fn new() -> Self {
        Self {
            global_limit: Arc::new(Semaphore::new(GLOBAL_TRANSFER_LIMIT)),
            connection_limits: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
            recovery: OnceCell::new(),
            last_progress_event: Mutex::new(None),
        }
    }

    fn connection_limit(&self, connection_id: &str) -> Arc<Semaphore> {
        self.connection_limits
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(connection_id.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(CONNECTION_TRANSFER_LIMIT)))
            .clone()
    }

    fn register(&self, transfer_id: String, connection_id: String, cancellation: CancellationToken) {
        self.active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(transfer_id, ActiveTransfer { connection_id, cancellation });
    }

    fn cancellation(&self, transfer_id: &str) -> Option<CancellationToken> {
        self.active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(transfer_id)
            .map(|transfer| transfer.cancellation.clone())
    }

    fn unregister(&self, transfer_id: &str) {
        let connection_id = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(transfer_id)
            .map(|transfer| transfer.connection_id);
        let Some(connection_id) = connection_id else {
            return;
        };
        let still_active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(|transfer| transfer.connection_id == connection_id);
        if !still_active {
            self.connection_limits.lock().unwrap_or_else(|error| error.into_inner()).remove(&connection_id);
        }
    }

    fn should_emit_progress(&self) -> bool {
        let now = Instant::now();
        let mut last = self.last_progress_event.lock().unwrap_or_else(|error| error.into_inner());
        if last.is_some_and(|last| now.duration_since(last) < GLOBAL_PROGRESS_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }

    async fn ensure_recovered(&self, state: &AppState) -> Result<(), String> {
        self.recovery
            .get_or_try_init(|| async {
                let interrupted = state.storage.recover_interrupted_file_transfers().await?;
                for transfer in interrupted {
                    recover_owned_temp_file(state, &transfer).await?;
                }
                Ok(())
            })
            .await
            .map(|_| ())
    }
}

impl Default for FileTransferRuntime {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) async fn recover_interrupted_downloads(app: AppHandle) {
    let state = app.state::<Arc<AppState>>().inner().clone();
    let runtime = app.state::<FileTransferRuntime>();
    if let Err(error) = runtime.ensure_recovered(&state).await {
        log::error!("Failed to recover interrupted file downloads at startup: {error}");
    }
}

#[tauri::command]
pub async fn start_download(
    app: AppHandle,
    window: WebviewWindow,
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    input: StartDownloadInput,
) -> Result<StartTransferResult, String> {
    runtime.ensure_recovered(&state).await?;
    let remote_path = validate_remote_relative_path(&input.remote_path)?;
    let local_path = validate_local_destination(Path::new(&input.local_path)).await?;
    let fs_scope = window
        .try_fs_scope()
        .ok_or_else(|| "File-system authorization is unavailable; choose the destination again".to_string())?;
    validate_local_authorization(&fs_scope, &local_path)?;
    if state.storage.load_file_connection(&input.connection_id).await?.is_none() {
        return Err("File connection not found".to_string());
    }

    let transfer_id = Uuid::new_v4().to_string();
    let record = state
        .storage
        .create_file_transfer(
            transfer_id.clone(),
            input.connection_id.clone(),
            "download".to_string(),
            remote_path,
            local_path.to_string_lossy().into_owned(),
        )
        .await?;
    let cancellation = CancellationToken::new();
    runtime.register(transfer_id.clone(), input.connection_id.clone(), cancellation.clone());
    emit_transfer(&app, &record);
    let worker_id = transfer_id.clone();
    let connection_id = input.connection_id;
    tokio::spawn(async move {
        run_download_worker(app, worker_id, connection_id, cancellation).await;
    });

    Ok(StartTransferResult { transfer_id })
}

#[tauri::command]
pub async fn get_file_transfer(
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    transfer_id: String,
) -> Result<FileTransferStorageRecord, String> {
    runtime.ensure_recovered(&state).await?;
    state.storage.get_file_transfer(&transfer_id).await?.ok_or_else(|| "File transfer not found".to_string())
}

#[tauri::command]
pub async fn list_file_transfers(
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    connection_id: Option<String>,
) -> Result<Vec<FileTransferStorageRecord>, String> {
    runtime.ensure_recovered(&state).await?;
    state.storage.list_file_transfers(connection_id.as_deref(), 100).await
}

#[tauri::command]
pub async fn cancel_file_transfer(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    transfer_id: String,
) -> Result<FileTransferStorageRecord, String> {
    runtime.ensure_recovered(&state).await?;
    let record = state.storage.request_file_transfer_cancel(&transfer_id).await?;
    if record.status == "cancelling" {
        emit_transfer(&app, &record);
        if let Some(cancellation) = runtime.cancellation(&transfer_id) {
            cancellation.cancel();
        }
    }
    Ok(record)
}

async fn run_download_worker<R: Runtime>(
    app: AppHandle<R>,
    transfer_id: String,
    connection_id: String,
    cancellation: CancellationToken,
) {
    let state = app.state::<Arc<AppState>>().inner().clone();
    let runtime = app.state::<FileTransferRuntime>();
    let file_manager = app.state::<FileManagerRuntime>();
    let progress = Arc::new(TransferProgressSnapshot::new());

    let result = async {
        let initial = transfer_record_for_worker(&state, &transfer_id).await?;
        let (_connection_permit, _global_permit) =
            acquire_transfer_permits(&runtime, &connection_id, &cancellation).await?;
        let current = transfer_record_for_worker(&state, &transfer_id).await?;
        let remote_path = if current.remote_path == initial.remote_path {
            current.remote_path
        } else {
            return Err(local_failure("File transfer remote path changed while queued"));
        };
        let prepared = tokio::select! {
            _ = cancellation.cancelled() => return Err(cancelled_failure()),
            prepared = file_manager.prepare_file_operation(&state, &connection_id, &remote_path) => {
                prepared.map_err(remote_failure)?
            }
        };
        let connection_cancellation = prepared.cancellation.clone();
        let commit_started = Arc::new(AtomicBool::new(false));
        let operation =
            execute_download(&app, &state, &runtime, &transfer_id, &prepared, commit_started.clone(), progress.clone());
        tokio::pin!(operation);
        let operation_deadline = tokio::time::sleep(DOWNLOAD_OPERATION_TIMEOUT);
        tokio::pin!(operation_deadline);
        let operation_result = tokio::select! {
            result = &mut operation => result,
            _ = &mut operation_deadline => {
                if commit_started.load(Ordering::Acquire) {
                    operation.await
                } else {
                    Err(TransferFailure {
                        status: "failed",
                        message: "Download operation timed out".to_string(),
                        invalidate_operator: true,
                    })
                }
            },
            _ = cancellation.cancelled() => {
                if commit_started.load(Ordering::Acquire) {
                    operation.await
                } else {
                    Err(cancelled_active_failure())
                }
            },
            _ = connection_cancellation.cancelled() => {
                if commit_started.load(Ordering::Acquire) {
                    operation.await
                } else {
                    Err(TransferFailure {
                        status: "cancelled",
                        message: "The file connection was removed while the download was running".to_string(),
                        invalidate_operator: true,
                    })
                }
            }
        };
        operation_result.map_err(|failure| {
            if failure.invalidate_operator {
                file_manager.evict_revision(&connection_id, prepared.revision);
            }
            failure
        })
    }
    .await;

    let latest = state.storage.get_file_transfer(&transfer_id).await.ok().flatten();
    let (status, bytes_transferred, total_bytes, error) = match result {
        Ok(outcome) => ("completed", outcome.bytes_transferred, outcome.total_bytes, None),
        Err(failure) => (
            failure.status,
            progress.bytes().max(latest.as_ref().map_or(0, |record| record.bytes_transferred)),
            progress.total().or_else(|| latest.as_ref().and_then(|record| record.total_bytes)),
            Some(sanitize_error(&failure.message)),
        ),
    };
    match state
        .storage
        .update_file_transfer(&transfer_id, status.to_string(), bytes_transferred, total_bytes, None, error, true)
        .await
    {
        Ok(record) => emit_transfer(&app, &record),
        Err(error) => log::error!("Failed to persist terminal file transfer state: {error}"),
    }
    runtime.unregister(&transfer_id);
}

async fn transfer_record_for_worker(
    state: &AppState,
    transfer_id: &str,
) -> Result<FileTransferStorageRecord, TransferFailure> {
    let record = state
        .storage
        .get_file_transfer(transfer_id)
        .await
        .map_err(local_failure)?
        .ok_or_else(|| local_failure("File transfer not found"))?;
    match record.status.as_str() {
        "queued" | "running" => Ok(record),
        "cancelling" => Err(cancelled_failure()),
        _ => Err(local_failure(format!("File transfer is already {}", record.status))),
    }
}

async fn acquire_transfer_permits(
    runtime: &FileTransferRuntime,
    connection_id: &str,
    cancellation: &CancellationToken,
) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), TransferFailure> {
    let connection_limit = runtime.connection_limit(connection_id);
    let connection_permit = tokio::select! {
        _ = cancellation.cancelled() => return Err(cancelled_failure()),
        permit = connection_limit.acquire_owned() => permit.map_err(|_| local_failure("Connection transfer limiter closed"))?,
    };
    let global_limit = runtime.global_limit.clone();
    let global_permit = tokio::select! {
        _ = cancellation.cancelled() => return Err(cancelled_failure()),
        permit = global_limit.acquire_owned() => permit.map_err(|_| local_failure("Global transfer limiter closed"))?,
    };
    Ok((connection_permit, global_permit))
}

async fn execute_download<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    runtime: &FileTransferRuntime,
    transfer_id: &str,
    prepared: &PreparedFileOperation,
    commit_started: Arc<AtomicBool>,
    progress_snapshot: Arc<TransferProgressSnapshot>,
) -> Result<DownloadOutcome, TransferFailure> {
    let record = state
        .storage
        .get_file_transfer(transfer_id)
        .await
        .map_err(local_failure)?
        .ok_or_else(|| local_failure("File transfer not found"))?;
    let local_path = PathBuf::from(&record.local_path);
    validate_local_destination(&local_path).await.map_err(local_failure)?;

    let metadata =
        watched_remote(prepared.operator.stat(&prepared.remote_path), "Remote file metadata timed out").await?;
    if !metadata.mode().is_file() {
        return Err(remote_failure("The remote path is not a file"));
    }
    let total_bytes = i64::try_from(metadata.content_length()).ok();
    progress_snapshot.record_total(total_bytes);

    let parent =
        local_path.parent().ok_or_else(|| local_failure("Local destination parent is required"))?.to_path_buf();
    let temp_prefix = format!(".dbx-download-{transfer_id}-");
    let (std_file, temp_path) = tokio::task::spawn_blocking(move || {
        TempFileBuilder::new().prefix(&temp_prefix).suffix(".part").tempfile_in(parent).map(|file| file.into_parts())
    })
    .await
    .map_err(|error| local_failure(error.to_string()))?
    .map_err(|error| local_failure(format!("Failed to create download temporary file: {error}")))?;
    let persisted_temp_path = temp_path.to_string_lossy().into_owned();

    let running = state
        .storage
        .update_file_transfer(
            transfer_id,
            "running".to_string(),
            0,
            total_bytes,
            Some(persisted_temp_path.clone()),
            None,
            false,
        )
        .await
        .map_err(local_failure)?;
    emit_transfer(app, &running);

    let mut output = tokio::fs::File::from_std(std_file);
    let reader_future = prepared.operator.reader_with(&prepared.remote_path).concurrent(1).chunk(DOWNLOAD_BUFFER_SIZE);
    let reader = watched_remote(async { reader_future.await }, "Opening the remote file timed out").await?;
    let mut reader =
        watched_remote(reader.into_futures_async_read(..), "Preparing the remote stream timed out").await?;
    let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_SIZE];
    let mut bytes_transferred = 0_i64;
    let mut last_progress = Instant::now();

    loop {
        let count = transfer_one_chunk(&mut reader, &mut output, &mut buffer, IO_PROGRESS_WATCHDOG).await?;
        if count == 0 {
            break;
        }
        bytes_transferred = bytes_transferred.saturating_add(i64::try_from(count).unwrap_or(i64::MAX));
        progress_snapshot.record_bytes(bytes_transferred);

        if last_progress.elapsed() >= PROGRESS_INTERVAL {
            let progress = state
                .storage
                .update_file_transfer(
                    transfer_id,
                    "running".to_string(),
                    bytes_transferred,
                    total_bytes,
                    Some(persisted_temp_path.clone()),
                    None,
                    false,
                )
                .await
                .map_err(local_failure)?;
            if progress.status == "running" && runtime.should_emit_progress() {
                emit_transfer(app, &progress);
            }
            last_progress = Instant::now();
        }
    }

    tokio::time::timeout(IO_PROGRESS_WATCHDOG, output.flush())
        .await
        .map_err(|_| local_failure("Flushing the download timed out"))?
        .map_err(|error| local_failure(format!("Failed to flush the download: {error}")))?;
    tokio::time::timeout(IO_PROGRESS_WATCHDOG, output.sync_all())
        .await
        .map_err(|_| local_failure("Synchronizing the download timed out"))?
        .map_err(|error| local_failure(format!("Failed to synchronize the download: {error}")))?;
    drop(output);

    // After this point cancellation must not report "cancelled": the
    // no-clobber atomic publish may already have installed the destination.
    commit_started.store(true, Ordering::Release);
    publish_temp_file(temp_path, local_path.clone()).await?;
    sync_parent_directory(local_path.parent().unwrap_or(Path::new("/"))).await?;
    Ok(DownloadOutcome { bytes_transferred, total_bytes })
}

async fn transfer_one_chunk<R, W>(
    reader: &mut R,
    output: &mut W,
    buffer: &mut [u8],
    watchdog: Duration,
) -> Result<usize, TransferFailure>
where
    R: FuturesAsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let count = tokio::time::timeout(watchdog, FuturesAsyncReadExt::read(reader, buffer))
        .await
        .map_err(|_| remote_failure("Remote read made no progress before the I/O watchdog expired"))?
        .map_err(|error| remote_failure(error.to_string()))?;
    if count == 0 {
        return Ok(0);
    }
    tokio::time::timeout(watchdog, output.write_all(&buffer[..count]))
        .await
        .map_err(|_| local_failure("Local write made no progress before the I/O watchdog expired"))?
        .map_err(|error| local_failure(format!("Failed to write the download: {error}")))?;
    Ok(count)
}

async fn publish_temp_file(temp_path: TempPath, local_path: PathBuf) -> Result<(), TransferFailure> {
    tokio::task::spawn_blocking(move || temp_path.persist_noclobber(local_path))
        .await
        .map_err(|error| local_failure(error.to_string()))?
        .map_err(|error| {
            local_failure(format!(
                "Failed to atomically publish the download without replacing the destination: {error}"
            ))
        })
}

#[cfg(unix)]
async fn sync_parent_directory(parent: &Path) -> Result<(), TransferFailure> {
    let parent = parent.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
        .await
        .map_err(|error| local_failure(error.to_string()))?
        .map_err(|error| local_failure(format!("Failed to synchronize the destination directory: {error}")))
}

#[cfg(not(unix))]
async fn sync_parent_directory(_parent: &Path) -> Result<(), TransferFailure> {
    Ok(())
}

async fn watched_remote<T>(
    future: impl std::future::Future<Output = Result<T, opendal::Error>>,
    timeout_message: &'static str,
) -> Result<T, TransferFailure> {
    tokio::time::timeout(IO_PROGRESS_WATCHDOG, future)
        .await
        .map_err(|_| remote_failure(timeout_message))?
        .map_err(|error| remote_failure(error.to_string()))
}

async fn validate_local_destination(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("Local download destination must be an absolute path".to_string());
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::CurDir | std::path::Component::ParentDir))
    {
        return Err("Local download destination cannot contain '.' or '..' path segments".to_string());
    }
    if path.file_name().is_none() {
        return Err("Local download destination must name a file".to_string());
    }
    let parent = path.parent().ok_or_else(|| "Local download destination parent is required".to_string())?;
    let parent_metadata = tokio::fs::metadata(parent)
        .await
        .map_err(|error| format!("Local download destination parent is unavailable: {error}"))?;
    if !parent_metadata.is_dir() {
        return Err("Local download destination parent is not a directory".to_string());
    }
    reject_symlink_ancestors(parent).await?;
    match tokio::fs::symlink_metadata(path).await {
        Ok(_) => return Err("Local download destination already exists".to_string()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("Failed to inspect local download destination: {error}")),
    }
    Ok(path.to_path_buf())
}

fn validate_local_authorization(scope: &tauri::fs::Scope, path: &Path) -> Result<(), String> {
    if scope.is_allowed(path) {
        Ok(())
    } else {
        Err("Local download destination is not authorized; choose it with the save dialog".to_string())
    }
}

async fn reject_symlink_ancestors(path: &Path) -> Result<(), String> {
    for ancestor in path.ancestors() {
        let metadata = tokio::fs::symlink_metadata(ancestor)
            .await
            .map_err(|error| format!("Failed to inspect local path: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err("Local download destination cannot traverse a symbolic link".to_string());
        }
    }
    Ok(())
}

async fn recover_owned_temp_file(state: &AppState, transfer: &FileTransferStorageRecord) -> Result<(), String> {
    let Some(temp_path) = transfer.temp_path.as_deref() else {
        return Ok(());
    };
    if !is_owned_temp_path(transfer, Path::new(temp_path)) {
        state
            .storage
            .update_file_transfer(
                &transfer.id,
                "failed".to_string(),
                transfer.bytes_transferred,
                transfer.total_bytes,
                Some(temp_path.to_string()),
                Some("Interrupted transfer has an invalid temporary-file path; no file was removed".to_string()),
                true,
            )
            .await?;
        return Ok(());
    }
    match tokio::fs::remove_file(temp_path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            state
                .storage
                .update_file_transfer(
                    &transfer.id,
                    "failed".to_string(),
                    transfer.bytes_transferred,
                    transfer.total_bytes,
                    Some(temp_path.to_string()),
                    Some(format!("Interrupted transfer temporary-file cleanup failed: {error}")),
                    true,
                )
                .await?;
            Ok(())
        }
    }
}

fn is_owned_temp_path(transfer: &FileTransferStorageRecord, temp_path: &Path) -> bool {
    if transfer.direction != "download" {
        return false;
    }
    let target = Path::new(&transfer.local_path);
    if temp_path.parent() != target.parent() {
        return false;
    }
    let Some(name) = temp_path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    name.starts_with(&format!(".dbx-download-{}-", transfer.id)) && name.ends_with(".part")
}

fn emit_transfer<R: Runtime>(app: &AppHandle<R>, transfer: &FileTransferStorageRecord) {
    let _ = app.emit(TRANSFER_EVENT, transfer);
}

fn cancelled_failure() -> TransferFailure {
    TransferFailure { status: "cancelled", message: "Download cancelled".to_string(), invalidate_operator: false }
}

fn cancelled_active_failure() -> TransferFailure {
    TransferFailure { status: "cancelled", message: "Download cancelled".to_string(), invalidate_operator: true }
}

fn remote_failure(message: impl ToString) -> TransferFailure {
    TransferFailure { status: "failed", message: message.to_string(), invalidate_operator: true }
}

fn local_failure(message: impl ToString) -> TransferFailure {
    TransferFailure { status: "failed", message: message.to_string(), invalidate_operator: false }
}

fn sanitize_error(message: &str) -> String {
    let mut sanitized = message.replace('\0', "");
    if sanitized.len() > 2_000 {
        sanitized.truncate(2_000);
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbx_core::storage::Storage;
    use std::pin::Pin;
    use std::process::Command;
    use std::task::{Context, Poll};

    struct FailedReader;

    impl FuturesAsyncRead for FailedReader {
        fn poll_read(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &mut [u8]) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "injected disconnect")))
        }
    }

    struct StalledReader;

    impl FuturesAsyncRead for StalledReader {
        fn poll_read(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &mut [u8]) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }

    struct DiskFullWriter;

    impl AsyncWrite for DiskFullWriter {
        fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &[u8]) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::from_raw_os_error(28)))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct DiskFullAfterFirstWrite {
        writes: usize,
    }

    impl AsyncWrite for DiskFullAfterFirstWrite {
        fn poll_write(mut self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
            self.writes += 1;
            if self.writes == 1 {
                Poll::Ready(Ok(buffer.len()))
            } else {
                Poll::Ready(Err(io::Error::from_raw_os_error(28)))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn local_destination_must_be_absolute_new_and_not_symlinked() {
        assert!(validate_local_destination(Path::new("relative/file.bin")).await.is_err());
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().canonicalize().unwrap().join("download.bin");
        assert_eq!(validate_local_destination(&target).await.unwrap(), target);
        let ambiguous = target.parent().unwrap().join("nested").join("..").join("escape.bin");
        assert!(validate_local_destination(&ambiguous).await.unwrap_err().contains("path segments"));
        tokio::fs::write(&target, b"existing").await.unwrap();
        assert!(validate_local_destination(&target).await.unwrap_err().contains("already exists"));
    }

    #[test]
    fn local_destination_requires_dialog_scope_authorization_even_before_it_exists() {
        let app = tauri::test::mock_builder()
            .plugin(tauri_plugin_fs::init())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().canonicalize().unwrap().join("new-download.bin");
        let scope = app.fs_scope();
        assert!(validate_local_authorization(&scope, &target).unwrap_err().contains("not authorized"));
        scope.allow_file(&target).unwrap();
        assert!(validate_local_authorization(&scope, &target).is_ok());
        assert!(scope.is_allowed(&target), "nonexistent authorized target must remain matchable");
        drop(app);
    }

    #[tokio::test]
    async fn publish_is_no_clobber_and_removes_the_operation_temp() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("download.bin");
        let temp =
            TempFileBuilder::new().prefix(".dbx-download-test-").suffix(".part").tempfile_in(directory.path()).unwrap();
        std::fs::write(temp.path(), b"payload").unwrap();
        let (_, temp_path) = temp.into_parts();
        let original_temp = temp_path.to_path_buf();
        publish_temp_file(temp_path, target.clone()).await.unwrap();
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"payload");
        assert!(!original_temp.exists());

        let second =
            TempFileBuilder::new().prefix(".dbx-download-test-").suffix(".part").tempfile_in(directory.path()).unwrap();
        std::fs::write(second.path(), b"replacement").unwrap();
        let (_, second_path) = second.into_parts();
        assert!(publish_temp_file(second_path, target.clone()).await.is_err());
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"payload");
    }

    #[test]
    fn recovery_only_accepts_the_operation_owned_sibling_temp() {
        let target = std::env::temp_dir().join("report.csv");
        let transfer = FileTransferStorageRecord {
            id: "transfer-1".to_string(),
            connection_id: "connection-1".to_string(),
            direction: "download".to_string(),
            remote_path: "report.csv".to_string(),
            local_path: target.to_string_lossy().into_owned(),
            temp_path: None,
            status: "running".to_string(),
            bytes_transferred: 10,
            total_bytes: Some(20),
            error: None,
            created_at: String::new(),
            updated_at: String::new(),
            completed_at: None,
        };
        let owned = target.parent().unwrap().join(".dbx-download-transfer-1-random.part");
        assert!(is_owned_temp_path(&transfer, &owned));
        assert!(!is_owned_temp_path(&transfer, &target.parent().unwrap().join(".dbx-download-other-random.part")));
        assert!(!is_owned_temp_path(&transfer, &target.parent().unwrap().join("../outside.part")));
    }

    #[tokio::test]
    async fn crash_recovery_removes_only_the_owned_residual_file() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        let state = AppState::new(storage);
        let target = directory.path().join("report.csv");
        let owned = directory.path().join(".dbx-download-transfer-1-random.part");
        tokio::fs::write(&owned, b"partial").await.unwrap();
        state
            .storage
            .create_file_transfer(
                "transfer-1".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                "transfer-1",
                "running".into(),
                7,
                Some(20),
                Some(owned.to_string_lossy().into_owned()),
                None,
                false,
            )
            .await
            .unwrap();

        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        assert_eq!(interrupted.len(), 1);
        recover_owned_temp_file(&state, &interrupted[0]).await.unwrap();
        assert!(!owned.exists());
        let recovered = state.storage.get_file_transfer("transfer-1").await.unwrap().unwrap();
        assert_eq!(recovered.status, "failed");
        assert!(recovered.completed_at.is_some());
    }

    #[tokio::test]
    async fn persisted_queued_cancel_intent_wins_before_worker_start() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        storage
            .create_file_transfer(
                "queued-cancel".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                directory.path().join("report.csv").to_string_lossy().into_owned(),
            )
            .await
            .unwrap();
        storage.request_file_transfer_cancel("queued-cancel").await.unwrap();
        let state = Arc::new(AppState::new(storage));
        let app = tauri::test::mock_builder()
            .manage(state.clone())
            .manage(FileManagerRuntime::default())
            .manage(FileTransferRuntime::default())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let cancellation = CancellationToken::new();
        app.state::<FileTransferRuntime>().register(
            "queued-cancel".into(),
            "connection-1".into(),
            cancellation.clone(),
        );

        run_download_worker(app.handle().clone(), "queued-cancel".into(), "connection-1".into(), cancellation).await;

        let terminal = state.storage.get_file_transfer("queued-cancel").await.unwrap().unwrap();
        assert_eq!(terminal.status, "cancelled");
        assert_eq!(terminal.bytes_transferred, 0);
        assert!(terminal.completed_at.is_some());
    }

    #[tokio::test]
    async fn limiter_defaults_are_eight_global_and_four_per_connection() {
        let runtime = FileTransferRuntime::default();
        assert_eq!(runtime.global_limit.available_permits(), 8);
        assert_eq!(runtime.connection_limit("ftp-1").available_permits(), 4);
        assert!(!cancelled_failure().invalidate_operator);
        assert!(cancelled_active_failure().invalidate_operator);
    }

    #[tokio::test]
    async fn bounded_chunk_copy_surfaces_disconnect_disk_full_and_stall() {
        assert_eq!(DOWNLOAD_BUFFER_SIZE, 4 * 1024 * 1024);
        let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_SIZE];
        let mut sink = tokio::io::sink();

        let disconnected =
            transfer_one_chunk(&mut FailedReader, &mut sink, &mut buffer, Duration::from_millis(50)).await.unwrap_err();
        assert!(disconnected.invalidate_operator);
        assert!(disconnected.message.contains("injected disconnect"));

        let stalled = transfer_one_chunk(&mut StalledReader, &mut sink, &mut buffer, Duration::from_millis(10))
            .await
            .unwrap_err();
        assert!(stalled.invalidate_operator);
        assert!(stalled.message.contains("watchdog"));

        let mut input = futures::io::Cursor::new(vec![7_u8; 1024]);
        let disk_full = transfer_one_chunk(&mut input, &mut DiskFullWriter, &mut buffer, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(!disk_full.invalidate_operator);
        assert!(
            disk_full.message.contains("No space left") || disk_full.message.contains("space"),
            "{}",
            disk_full.message
        );
    }

    #[tokio::test]
    async fn disk_full_terminal_snapshot_keeps_all_prior_successful_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        storage
            .create_file_transfer(
                "disk-full".into(),
                "connection-1".into(),
                "download".into(),
                "remote.bin".into(),
                directory.path().join("local.bin").to_string_lossy().into_owned(),
            )
            .await
            .unwrap();
        let progress = TransferProgressSnapshot::new();
        progress.record_total(Some(2_048));
        let mut writer = DiskFullAfterFirstWrite { writes: 0 };
        let mut buffer = vec![0_u8; 1_024];

        let mut first = futures::io::Cursor::new(vec![1_u8; 1_024]);
        let first_count =
            transfer_one_chunk(&mut first, &mut writer, &mut buffer, Duration::from_millis(50)).await.unwrap();
        progress.record_bytes(i64::try_from(first_count).unwrap());
        let mut second = futures::io::Cursor::new(vec![2_u8; 1_024]);
        let failure =
            transfer_one_chunk(&mut second, &mut writer, &mut buffer, Duration::from_millis(50)).await.unwrap_err();
        assert!(failure.message.contains("space") || failure.message.contains("No space left"));

        storage
            .update_file_transfer(
                "disk-full",
                "failed".into(),
                progress.bytes(),
                progress.total(),
                None,
                Some(failure.message),
                true,
            )
            .await
            .unwrap();
        let terminal = storage.get_file_transfer("disk-full").await.unwrap().unwrap();
        assert_eq!(terminal.bytes_transferred, 1_024);
        assert_eq!(terminal.total_bytes, Some(2_048));
        assert_eq!(terminal.status, "failed");
    }

    #[tokio::test]
    #[ignore = "run through tests/ftp-contract.sh with a pinned FTP image"]
    async fn fixed_ftp_download_contract() {
        use super::super::file_manager::{build_operator, FileConnectionConfig, FtpConnectionConfig};

        let endpoint = std::env::var("DBX_TEST_FTP_ENDPOINT").expect("DBX_TEST_FTP_ENDPOINT is required");
        let username = std::env::var("DBX_TEST_FTP_USERNAME").unwrap_or_else(|_| "dbx".to_string());
        let password = std::env::var("DBX_TEST_FTP_PASSWORD").unwrap_or_else(|_| "dbx-password".to_string());
        let config =
            FileConnectionConfig::Ftp(FtpConnectionConfig { endpoint, root: "/ftp/dbx".to_string(), username });
        let operator = build_operator(&config, Some(&password)).unwrap();
        let reader_future = operator.reader_with("ftp/dbx/fixture.txt").concurrent(1).chunk(DOWNLOAD_BUFFER_SIZE);
        let mut reader = reader_future.await.unwrap().into_futures_async_read(..).await.unwrap();

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("fixture.txt");
        let temp = TempFileBuilder::new()
            .prefix(".dbx-download-contract-")
            .suffix(".part")
            .tempfile_in(directory.path())
            .unwrap();
        let (std_file, temp_path) = temp.into_parts();
        let mut output = tokio::fs::File::from_std(std_file);
        let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_SIZE];
        let mut bytes = 0;
        loop {
            let count = transfer_one_chunk(&mut reader, &mut output, &mut buffer, IO_PROGRESS_WATCHDOG).await.unwrap();
            if count == 0 {
                break;
            }
            bytes += count;
        }
        output.flush().await.unwrap();
        output.sync_all().await.unwrap();
        drop(output);
        publish_temp_file(temp_path, target.clone()).await.unwrap();

        assert_eq!(bytes, b"dbx ftp fixture\n".len());
        assert_eq!(tokio::fs::read(target).await.unwrap(), b"dbx ftp fixture\n");
    }

    async fn wait_for_transfer_status(
        storage: &Storage,
        transfer_id: &str,
        expected: &[&str],
    ) -> FileTransferStorageRecord {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let record = storage.get_file_transfer(transfer_id).await.unwrap().unwrap();
                if expected.contains(&record.status.as_str()) {
                    return record;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("transfer {transfer_id} did not reach {expected:?}"))
    }

    async fn wait_for_owned_temp_bytes(directory: &Path, transfer_id: &str) -> u64 {
        let prefix = format!(".dbx-download-{transfer_id}-");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(length) = std::fs::read_dir(directory)
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".part"))
                    })
                    .filter_map(|entry| entry.metadata().ok())
                    .map(|metadata| metadata.len())
                    .find(|length| *length > 0)
                {
                    return length;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("transfer {transfer_id} did not write its temporary file"))
    }

    async fn create_worker_transfer<R: Runtime>(
        app: &tauri::App<R>,
        transfer_id: &str,
        remote_path: &str,
        local_path: &Path,
    ) -> (CancellationToken, tokio::task::JoinHandle<()>)
    where
        R: Runtime,
    {
        let state = app.state::<Arc<AppState>>();
        state
            .storage
            .create_file_transfer(
                transfer_id.to_string(),
                "ftp-contract".into(),
                "download".into(),
                remote_path.to_string(),
                local_path.to_string_lossy().into_owned(),
            )
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        app.state::<FileTransferRuntime>().register(
            transfer_id.to_string(),
            "ftp-contract".into(),
            cancellation.clone(),
        );
        let worker = tokio::spawn(run_download_worker(
            app.handle().clone(),
            transfer_id.to_string(),
            "ftp-contract".into(),
            cancellation.clone(),
        ));
        (cancellation, worker)
    }

    fn assert_no_owned_temp(directory: &Path, transfer_id: &str) {
        let prefix = format!(".dbx-download-{transfer_id}-");
        let residuals = std::fs::read_dir(directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.starts_with(&prefix) && name.ends_with(".part"))
            .collect::<Vec<_>>();
        assert!(residuals.is_empty(), "residual temporary files: {residuals:?}");
    }

    #[tokio::test]
    #[ignore = "run through tests/ftp-contract.sh with a pinned FTP image"]
    async fn fixed_ftp_worker_success_cancel_and_disconnect_contract() {
        use super::super::file_manager::{FileConnectionConfig, FtpConnectionConfig};

        let endpoint = std::env::var("DBX_TEST_FTP_ENDPOINT").expect("DBX_TEST_FTP_ENDPOINT is required");
        let username = std::env::var("DBX_TEST_FTP_USERNAME").unwrap_or_else(|_| "dbx".to_string());
        let password = std::env::var("DBX_TEST_FTP_PASSWORD").unwrap_or_else(|_| "dbx-password".to_string());
        let container = std::env::var("DBX_TEST_FTP_CONTAINER").expect("DBX_TEST_FTP_CONTAINER is required");
        let directory = tempfile::tempdir().unwrap();
        let download_directory = directory.path().canonicalize().unwrap();
        let storage = Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        let config =
            FileConnectionConfig::Ftp(FtpConnectionConfig { endpoint, root: "/ftp/dbx".to_string(), username });
        storage
            .save_file_connection(
                "ftp-contract".into(),
                "FTP contract".into(),
                "ftp".into(),
                serde_json::to_string(&config).unwrap(),
                Some(password),
                "ftp\n127.0.0.1\n2121\ndbx".into(),
                true,
                None,
            )
            .await
            .unwrap();
        let state = Arc::new(AppState::new(storage));
        let app = tauri::test::mock_builder()
            .manage(state.clone())
            .manage(FileManagerRuntime::default())
            .manage(FileTransferRuntime::default())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();

        let success_target = download_directory.join("success.txt");
        let (_, success_worker) = create_worker_transfer(&app, "ftp-success", "fixture.txt", &success_target).await;
        success_worker.await.unwrap();
        let success = state.storage.get_file_transfer("ftp-success").await.unwrap().unwrap();
        assert_eq!(success.status, "completed", "{success:?}");
        assert_eq!(tokio::fs::read(&success_target).await.unwrap(), b"dbx ftp fixture\n");
        assert_no_owned_temp(&download_directory, "ftp-success");

        let cancel_target = download_directory.join("cancel.bin");
        let (cancel_token, cancel_worker) =
            create_worker_transfer(&app, "ftp-cancel", "large.bin", &cancel_target).await;
        wait_for_transfer_status(&state.storage, "ftp-cancel", &["running"]).await;
        let cancel_written = wait_for_owned_temp_bytes(&download_directory, "ftp-cancel").await;
        state.storage.request_file_transfer_cancel("ftp-cancel").await.unwrap();
        cancel_token.cancel();
        cancel_worker.await.unwrap();
        let cancelled = state.storage.get_file_transfer("ftp-cancel").await.unwrap().unwrap();
        assert_eq!(cancelled.status, "cancelled");
        assert!(cancelled.bytes_transferred >= i64::try_from(cancel_written).unwrap());
        assert!(cancelled.bytes_transferred > 0);
        assert!(!cancel_target.exists());
        assert_no_owned_temp(&download_directory, "ftp-cancel");

        let disconnect_target = download_directory.join("disconnect.bin");
        let (_, disconnect_worker) =
            create_worker_transfer(&app, "ftp-disconnect", "large.bin", &disconnect_target).await;
        wait_for_transfer_status(&state.storage, "ftp-disconnect", &["running"]).await;
        let disconnect_written = wait_for_owned_temp_bytes(&download_directory, "ftp-disconnect").await;
        let kill = tokio::task::spawn_blocking(move || Command::new("docker").args(["kill", &container]).status())
            .await
            .unwrap()
            .unwrap();
        assert!(kill.success());
        tokio::time::timeout(Duration::from_secs(10), disconnect_worker)
            .await
            .expect("disconnect must terminate active FTP I/O")
            .unwrap();
        let disconnected = state.storage.get_file_transfer("ftp-disconnect").await.unwrap().unwrap();
        assert_eq!(disconnected.status, "failed");
        assert!(disconnected.bytes_transferred >= i64::try_from(disconnect_written).unwrap());
        assert!(disconnected.bytes_transferred > 0);
        assert!(disconnected.error.as_deref().is_some_and(|error| !error.is_empty()));
        assert!(!disconnect_target.exists());
        assert_no_owned_temp(&download_directory, "ftp-disconnect");
    }
}
