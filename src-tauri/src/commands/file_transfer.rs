use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cap_fs_ext::{
    ambient_authority, DirExt, FollowSymlinks, MetadataExt as CapabilityMetadataExt, OpenOptionsFollowExt,
};
use cap_std::fs::{Dir, OpenOptions};
use dbx_core::connection::AppState;
use dbx_core::storage::FileTransferStorageRecord;
use futures::io::AsyncRead as FuturesAsyncRead;
use futures::io::AsyncReadExt as FuturesAsyncReadExt;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager, Runtime, State, WebviewWindow};
use tauri_plugin_fs::FsExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::file_manager::{
    validate_remote_relative_path, CancellationSignal, FileManagerRuntime, PreparedFileOperation,
};

const DOWNLOAD_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const GLOBAL_TRANSFER_LIMIT: usize = 8;
const CONNECTION_TRANSFER_LIMIT: usize = 4;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);
const GLOBAL_PROGRESS_INTERVAL: Duration = Duration::from_millis(50);
const IO_PROGRESS_WATCHDOG: Duration = Duration::from_secs(30);
const CREATE_TEMP_TIMEOUT: Duration = Duration::from_secs(30);
const DOWNLOAD_OPERATION_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const TRANSFER_EVENT: &str = "file-transfer-progress";

#[cfg(test)]
#[derive(Clone)]
struct TestRemoteReaderBarrier {
    opened: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static TEST_REMOTE_READER_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
#[derive(Clone)]
struct TestBlockingBarrier {
    entry_name: OsString,
    opened: Arc<tokio::sync::Notify>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
}

#[cfg(test)]
static TEST_CREATE_TEMP_BARRIER: std::sync::OnceLock<Mutex<Option<TestBlockingBarrier>>> = std::sync::OnceLock::new();

#[cfg(test)]
static TEST_LEAF_MUTATION_BARRIER: std::sync::OnceLock<Mutex<Option<TestBlockingBarrier>>> = std::sync::OnceLock::new();

#[cfg(test)]
static TEST_UNSUPPORTED_ATOMIC_RENAME: std::sync::OnceLock<Mutex<Option<OsString>>> = std::sync::OnceLock::new();

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

#[derive(Debug, Serialize)]
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

struct CreateTempCompletion {
    file: std::fs::File,
    identity: String,
    timed_out: bool,
}

#[derive(Debug)]
struct ValidatedLocalDestination {
    path: PathBuf,
    directory_identity: String,
}

struct AnchoredDestination {
    directory: Arc<Dir>,
    parent_path: PathBuf,
    target_name: OsString,
    temp_name: OsString,
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
                    recover_interrupted_transfer(state, &transfer).await?;
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
    start_download_inner(app, window, state.inner(), runtime.inner(), input).await
}

async fn start_download_inner<R: Runtime>(
    app: AppHandle<R>,
    window: WebviewWindow<R>,
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    input: StartDownloadInput,
) -> Result<StartTransferResult, String> {
    runtime.ensure_recovered(state).await?;
    let remote_path = validate_remote_relative_path(&input.remote_path)?;
    let local = validate_local_destination(Path::new(&input.local_path)).await?;
    let fs_scope = window
        .try_fs_scope()
        .ok_or_else(|| "File-system authorization is unavailable; choose the destination again".to_string())?;
    validate_local_authorization(&fs_scope, &local.path)?;
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
            local.path.to_string_lossy().into_owned(),
            local.directory_identity,
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
    get_file_transfer_inner(state.inner(), runtime.inner(), &transfer_id).await
}

async fn get_file_transfer_inner(
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    transfer_id: &str,
) -> Result<FileTransferStorageRecord, String> {
    runtime.ensure_recovered(state).await?;
    state.storage.get_file_transfer(transfer_id).await?.ok_or_else(|| "File transfer not found".to_string())
}

#[tauri::command]
pub async fn list_file_transfers(
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    connection_id: Option<String>,
) -> Result<Vec<FileTransferStorageRecord>, String> {
    list_file_transfers_inner(state.inner(), runtime.inner(), connection_id.as_deref()).await
}

async fn list_file_transfers_inner(
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    connection_id: Option<&str>,
) -> Result<Vec<FileTransferStorageRecord>, String> {
    runtime.ensure_recovered(state).await?;
    state.storage.list_file_transfers(connection_id, 100).await
}

#[tauri::command]
pub async fn cancel_file_transfer(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    transfer_id: String,
) -> Result<FileTransferStorageRecord, String> {
    cancel_file_transfer_inner(&app, state.inner(), runtime.inner(), &transfer_id).await
}

async fn cancel_file_transfer_inner<R: Runtime>(
    app: &AppHandle<R>,
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    transfer_id: &str,
) -> Result<FileTransferStorageRecord, String> {
    runtime.ensure_recovered(state).await?;
    let record = state.storage.request_file_transfer_cancel(transfer_id).await?;
    if record.status == "cancelling" {
        emit_transfer(app, &record);
        if let Some(cancellation) = runtime.cancellation(transfer_id) {
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
        let mutation_in_flight = Arc::new(AtomicBool::new(false));
        let operation = execute_download(
            &app,
            &state,
            &runtime,
            &transfer_id,
            &prepared,
            &cancellation,
            &connection_cancellation,
            mutation_in_flight.clone(),
            progress.clone(),
        );
        tokio::pin!(operation);
        let operation_deadline = tokio::time::sleep(DOWNLOAD_OPERATION_TIMEOUT);
        tokio::pin!(operation_deadline);
        let operation_result = tokio::select! {
            result = &mut operation => result,
            _ = &mut operation_deadline => {
                if mutation_in_flight.load(Ordering::Acquire) {
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
                if mutation_in_flight.load(Ordering::Acquire) {
                    operation.await
                } else {
                    Err(cancelled_active_failure())
                }
            },
            _ = connection_cancellation.cancelled() => {
                if mutation_in_flight.load(Ordering::Acquire) {
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
        operation_result.inspect_err(|failure| {
            if failure.invalidate_operator {
                file_manager.evict_revision(&connection_id, prepared.revision);
            }
        })
    }
    .await;

    finalize_download_result(&app, &state, &transfer_id, result, &progress).await;
    runtime.unregister(&transfer_id);
}

async fn finalize_download_result<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    transfer_id: &str,
    result: Result<DownloadOutcome, TransferFailure>,
    progress: &TransferProgressSnapshot,
) {
    let latest = state.storage.get_file_transfer(transfer_id).await.ok().flatten();
    if result.is_err() && latest.as_ref().is_some_and(|record| record.status == "publishing") {
        if let Some(record) = latest.as_ref() {
            if let Err(error) = recover_interrupted_transfer(state, record).await {
                log::error!("Failed to reconcile publishing file transfer: {error}");
            }
        }
        if let Ok(Some(record)) = state.storage.get_file_transfer(transfer_id).await {
            emit_transfer(app, &record);
        }
        return;
    }
    let cleanup_error = if result.is_err() {
        match latest.as_ref() {
            Some(record) => cleanup_active_temp(record).await.err(),
            None => None,
        }
    } else {
        None
    };
    let (status, bytes_transferred, total_bytes, error) = match result {
        Ok(outcome) => ("completed", outcome.bytes_transferred, outcome.total_bytes, None),
        Err(failure) => (
            failure.status,
            progress.bytes().max(latest.as_ref().map_or(0, |record| record.bytes_transferred)),
            progress.total().or_else(|| latest.as_ref().and_then(|record| record.total_bytes)),
            Some(sanitize_error(&match cleanup_error {
                Some(cleanup) => format!("{}; temporary-file cleanup failed safely: {cleanup}", failure.message),
                None => failure.message,
            })),
        ),
    };
    match state
        .storage
        .update_file_transfer(transfer_id, status.to_string(), bytes_transferred, total_bytes, None, None, error, true)
        .await
    {
        Ok(record) => emit_transfer(app, &record),
        Err(error) => log::error!("Failed to persist terminal file transfer state: {error}"),
    }
}

async fn cleanup_active_temp(record: &FileTransferStorageRecord) -> Result<(), String> {
    let Some(temp_path) = record.temp_path.as_deref() else {
        return Ok(());
    };
    if !is_owned_temp_path(record, Path::new(temp_path)) {
        return Err("temporary-file identity is invalid".to_string());
    }
    let local_path = PathBuf::from(&record.local_path);
    let temp_path = PathBuf::from(temp_path);
    let directory_identity = record.local_directory_identity.clone();
    let expected_temp_identity = record
        .temp_identity
        .clone()
        .ok_or_else(|| "temporary-file identity is unavailable; no file was removed".to_string())?;
    tokio::task::spawn_blocking(move || {
        let anchored = AnchoredDestination::reopen(&local_path, &temp_path, &directory_identity)?;
        anchored.remove_owned_temp(&expected_temp_identity).map(|_| ()).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())?
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
    cancellation: &CancellationToken,
    connection_cancellation: &CancellationSignal,
    mutation_in_flight: Arc<AtomicBool>,
    progress_snapshot: Arc<TransferProgressSnapshot>,
) -> Result<DownloadOutcome, TransferFailure> {
    let record = state
        .storage
        .get_file_transfer(transfer_id)
        .await
        .map_err(local_failure)?
        .ok_or_else(|| local_failure("File transfer not found"))?;
    let local_path = PathBuf::from(&record.local_path);

    let metadata =
        watched_remote(prepared.operator.stat(&prepared.remote_path), "Remote file metadata timed out").await?;
    if !metadata.mode().is_file() {
        return Err(remote_failure("The remote path is not a file"));
    }
    let total_bytes = i64::try_from(metadata.content_length()).ok();
    progress_snapshot.record_total(total_bytes);

    let parent = local_path.parent().ok_or_else(|| local_failure("Local destination parent is required"))?;
    let temp_name = format!(".dbx-download-{transfer_id}-{}.part", Uuid::new_v4());
    let temp_path = parent.join(&temp_name);
    let persisted_temp_path = temp_path.to_string_lossy().into_owned();
    let preparing = state
        .storage
        .update_file_transfer(
            transfer_id,
            "running".to_string(),
            0,
            total_bytes,
            Some(persisted_temp_path.clone()),
            None,
            None,
            false,
        )
        .await
        .map_err(local_failure)?;
    emit_transfer(app, &preparing);

    let expected_directory_identity = record.local_directory_identity.clone();
    let anchored = tokio::task::spawn_blocking(move || {
        AnchoredDestination::open(&local_path, &temp_path, &expected_directory_identity)
    })
    .await
    .map_err(|error| local_failure(error.to_string()))?
    .map_err(local_failure)?;
    let anchored = Arc::new(anchored);

    mutation_in_flight.store(true, Ordering::Release);
    let creation = await_create_temp(anchored.clone(), CREATE_TEMP_TIMEOUT).await;
    let (std_file, temp_identity, create_timed_out) = match creation {
        Ok(creation) => (creation.file, creation.identity, creation.timed_out),
        Err(error) => {
            mutation_in_flight.store(false, Ordering::Release);
            return Err(error);
        }
    };
    let running_result = state
        .storage
        .update_file_transfer(
            transfer_id,
            "running".to_string(),
            0,
            total_bytes,
            Some(persisted_temp_path.clone()),
            Some(temp_identity.clone()),
            None,
            false,
        )
        .await;
    let running = match running_result {
        Ok(running) => running,
        Err(error) => {
            drop(std_file);
            let cleanup_target = anchored.clone();
            let cleanup_identity = temp_identity.clone();
            let cleanup = tokio::task::spawn_blocking(move || cleanup_target.remove_owned_temp(&cleanup_identity))
                .await
                .map_err(|cleanup| cleanup.to_string())
                .and_then(|cleanup| cleanup.map_err(|cleanup| cleanup.to_string()));
            mutation_in_flight.store(false, Ordering::Release);
            return Err(local_failure(match cleanup {
                Ok(_) => error,
                Err(cleanup) => format!(
                    "{error}; temporary-file cleanup after identity persistence failure failed safely: {cleanup}"
                ),
            }));
        }
    };
    mutation_in_flight.store(false, Ordering::Release);
    emit_transfer(app, &running);
    if create_timed_out {
        drop(std_file);
        return Err(local_failure("Creating the download temporary file timed out"));
    }
    if cancellation.is_cancelled() {
        drop(std_file);
        return Err(cancelled_active_failure());
    }
    if connection_cancellation.is_cancelled() {
        drop(std_file);
        return Err(TransferFailure {
            status: "cancelled",
            message: "The file connection was removed while the download was running".to_string(),
            invalidate_operator: true,
        });
    }

    let mut output = tokio::fs::File::from_std(std_file);
    let reader_future = prepared.operator.reader_with(&prepared.remote_path).concurrent(1).chunk(DOWNLOAD_BUFFER_SIZE);
    let reader = watched_remote(async { reader_future.await }, "Opening the remote file timed out").await?;
    let mut reader =
        watched_remote(reader.into_futures_async_read(..), "Preparing the remote stream timed out").await?;
    wait_at_test_remote_reader_barrier().await;
    let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_SIZE];
    let mut bytes_transferred = 0_i64;
    let mut last_progress = Instant::now();

    loop {
        let count = transfer_one_chunk(
            &mut reader,
            &mut output,
            &mut buffer,
            IO_PROGRESS_WATCHDOG,
            &mut bytes_transferred,
            &progress_snapshot,
        )
        .await?;
        if count == 0 {
            break;
        }

        if last_progress.elapsed() >= PROGRESS_INTERVAL {
            let progress = state
                .storage
                .update_file_transfer(
                    transfer_id,
                    "running".to_string(),
                    bytes_transferred,
                    total_bytes,
                    Some(persisted_temp_path.clone()),
                    Some(temp_identity.clone()),
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

    let publishing = state
        .storage
        .update_file_transfer(
            transfer_id,
            "publishing".to_string(),
            bytes_transferred,
            total_bytes,
            Some(persisted_temp_path),
            Some(temp_identity.clone()),
            None,
            false,
        )
        .await
        .map_err(local_failure)?;
    emit_transfer(app, &publishing);

    // Once publishing is durable, cancellation waits for reconciliation:
    // the no-clobber rename may already have installed the destination.
    mutation_in_flight.store(true, Ordering::Release);
    let publish_target = anchored.clone();
    tokio::task::spawn_blocking(move || publish_target.publish(&temp_identity))
        .await
        .map_err(|error| local_failure(error.to_string()))?
        .map_err(local_failure)?;
    Ok(DownloadOutcome { bytes_transferred, total_bytes })
}

async fn await_create_temp(
    anchored: Arc<AnchoredDestination>,
    timeout: Duration,
) -> Result<CreateTempCompletion, TransferFailure> {
    let mut task = tokio::task::spawn_blocking(move || anchored.create_temp());
    let (result, timed_out) = match tokio::time::timeout(timeout, &mut task).await {
        Ok(result) => (result, false),
        Err(_) => (task.await, true),
    };
    let (file, identity) = result
        .map_err(|error| local_failure(error.to_string()))?
        .map_err(|error| local_failure(format!("Failed to create download temporary file: {error}")))?;
    Ok(CreateTempCompletion { file, identity, timed_out })
}

#[cfg(test)]
fn install_test_remote_reader_barrier() -> TestRemoteReaderBarrier {
    let barrier = TestRemoteReaderBarrier {
        opened: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    *TEST_REMOTE_READER_BARRIER.get_or_init(|| Mutex::new(None)).lock().unwrap_or_else(|error| error.into_inner()) =
        Some(barrier.clone());
    barrier
}

#[cfg(test)]
async fn wait_at_test_remote_reader_barrier() {
    let barrier = TEST_REMOTE_READER_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take();
    if let Some(barrier) = barrier {
        barrier.opened.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(not(test))]
async fn wait_at_test_remote_reader_barrier() {}

#[cfg(test)]
fn install_test_blocking_barrier(
    slot: &std::sync::OnceLock<Mutex<Option<TestBlockingBarrier>>>,
    entry_name: &OsStr,
) -> TestBlockingBarrier {
    let barrier = TestBlockingBarrier {
        entry_name: entry_name.to_os_string(),
        opened: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new((Mutex::new(false), std::sync::Condvar::new())),
    };
    *slot.get_or_init(|| Mutex::new(None)).lock().unwrap_or_else(|error| error.into_inner()) = Some(barrier.clone());
    barrier
}

#[cfg(test)]
fn wait_at_test_blocking_barrier(slot: &std::sync::OnceLock<Mutex<Option<TestBlockingBarrier>>>, entry_name: &OsStr) {
    let barrier = {
        let mut guard = slot.get_or_init(|| Mutex::new(None)).lock().unwrap_or_else(|error| error.into_inner());
        if guard
            .as_ref()
            .is_some_and(|barrier| barrier.entry_name == OsStr::new("*") || barrier.entry_name == entry_name)
        {
            guard.take()
        } else {
            None
        }
    };
    if let Some(barrier) = barrier {
        barrier.opened.notify_one();
        let (released, condition) = &*barrier.release;
        let mut released = released.lock().unwrap_or_else(|error| error.into_inner());
        while !*released {
            released = condition.wait(released).unwrap_or_else(|error| error.into_inner());
        }
    }
}

#[cfg(test)]
fn wait_at_test_create_temp_barrier(entry_name: &OsStr) {
    wait_at_test_blocking_barrier(&TEST_CREATE_TEMP_BARRIER, entry_name);
}

#[cfg(not(test))]
fn wait_at_test_create_temp_barrier(_entry_name: &OsStr) {}

#[cfg(test)]
fn wait_at_test_leaf_mutation_barrier(entry_name: &OsStr) {
    wait_at_test_blocking_barrier(&TEST_LEAF_MUTATION_BARRIER, entry_name);
}

#[cfg(not(test))]
fn wait_at_test_leaf_mutation_barrier(_entry_name: &OsStr) {}

#[cfg(test)]
fn release_test_blocking_barrier(barrier: &TestBlockingBarrier) {
    let (released, condition) = &*barrier.release;
    *released.lock().unwrap_or_else(|error| error.into_inner()) = true;
    condition.notify_all();
}

async fn transfer_one_chunk<R, W>(
    reader: &mut R,
    output: &mut W,
    buffer: &mut [u8],
    watchdog: Duration,
    bytes_transferred: &mut i64,
    progress_snapshot: &TransferProgressSnapshot,
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
    let mut offset = 0;
    while offset < count {
        let written = tokio::time::timeout(watchdog, output.write(&buffer[offset..count]))
            .await
            .map_err(|_| local_failure("Local write made no progress before the I/O watchdog expired"))?
            .map_err(|error| local_failure(format!("Failed to write the download: {error}")))?;
        if written == 0 {
            return Err(local_failure("Failed to write the download: write returned zero bytes"));
        }
        offset += written;
        *bytes_transferred = (*bytes_transferred).saturating_add(i64::try_from(written).unwrap_or(i64::MAX));
        progress_snapshot.record_bytes(*bytes_transferred);
    }
    Ok(count)
}

impl AnchoredDestination {
    fn open(local_path: &Path, temp_path: &Path, expected_directory_identity: &str) -> Result<Self, String> {
        Self::open_internal(local_path, temp_path, expected_directory_identity, true)
    }

    fn reopen(local_path: &Path, temp_path: &Path, expected_directory_identity: &str) -> Result<Self, String> {
        Self::open_internal(local_path, temp_path, expected_directory_identity, false)
    }

    fn open_internal(
        local_path: &Path,
        temp_path: &Path,
        expected_directory_identity: &str,
        require_target_absent: bool,
    ) -> Result<Self, String> {
        let parent = local_path.parent().ok_or_else(|| "Local destination parent is required".to_string())?;
        if temp_path.parent() != Some(parent) {
            return Err("Download temporary file is not a sibling of the destination".to_string());
        }
        let target_name = single_file_name(local_path, "Local destination")?;
        let temp_name = single_file_name(temp_path, "Download temporary file")?;
        let directory = open_absolute_directory_nofollow(parent)
            .map_err(|error| format!("Failed to open the destination directory safely: {error}"))?;
        let actual_identity = directory_identity(&directory)
            .map_err(|error| format!("Failed to identify destination directory: {error}"))?;
        if actual_identity != expected_directory_identity {
            return Err("Local destination directory changed after it was authorized".to_string());
        }
        if require_target_absent {
            match directory.symlink_metadata(&target_name) {
                Ok(_) => return Err("Local download destination already exists".to_string()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("Failed to inspect local download destination: {error}")),
            }
        }
        Ok(Self { directory: Arc::new(directory), parent_path: parent.to_path_buf(), target_name, temp_name })
    }

    fn create_temp(&self) -> io::Result<(std::fs::File, String)> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true).follow(FollowSymlinks::No);
        wait_at_test_create_temp_barrier(&self.temp_name);
        let file = self.directory.open_with(&self.temp_name, &options)?;
        let identity = metadata_identity(&file.metadata()?);
        Ok((file.into_std(), identity))
    }

    fn entry_identity(&self, name: &OsStr) -> io::Result<Option<String>> {
        match self.directory.symlink_metadata(name) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                Err(io::Error::other("operation-owned path was replaced by a symbolic link"))
            }
            Ok(metadata) if !metadata.is_file() => {
                Err(io::Error::other("operation-owned path was replaced by a non-file entry"))
            }
            Ok(_) => {
                let mut options = OpenOptions::new();
                options.read(true).follow(FollowSymlinks::No);
                let file = self.directory.open_with(name, &options)?;
                Ok(Some(metadata_identity(&file.metadata()?)))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn remove_owned_temp(&self, expected_identity: &str) -> io::Result<bool> {
        let Some(actual_identity) = self.entry_identity(&self.temp_name)? else {
            return Ok(false);
        };
        if expected_identity != actual_identity {
            return Err(io::Error::other("operation-owned temporary file identity changed"));
        }

        wait_at_test_leaf_mutation_barrier(&self.temp_name);
        let (quarantine, quarantine_name, quarantine_path) = self.quarantine_entry(&self.temp_name, ".dbx-cleanup-")?;
        let payload_name = OsStr::new("payload.part");
        let moved_identity = entry_identity_in(&quarantine, payload_name)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "quarantined temporary file is missing"))?;
        if moved_identity != expected_identity {
            return Err(io::Error::other(format!(
                "operation-owned temporary file was replaced; replacement preserved in {}",
                quarantine_path.display()
            )));
        }
        quarantine.remove_file(payload_name)?;
        quarantine.try_clone()?.into_std_file().sync_all()?;
        self.directory.remove_dir(&quarantine_name)?;
        self.sync_directory()?;
        Ok(true)
    }

    fn quarantine_entry(&self, source_name: &OsStr, directory_prefix: &str) -> io::Result<(Dir, OsString, PathBuf)> {
        let quarantine_name = OsString::from(format!("{directory_prefix}{}", Uuid::new_v4()));
        #[cfg(unix)]
        let builder = {
            use cap_std::fs::DirBuilderExt;
            let mut builder = cap_std::fs::DirBuilder::new();
            builder.mode(0o700);
            builder
        };
        #[cfg(not(unix))]
        let builder = cap_std::fs::DirBuilder::new();
        self.directory.create_dir_with(&quarantine_name, &builder)?;
        let quarantine = self.directory.open_dir_nofollow(&quarantine_name)?;
        let quarantine_path = self.parent_path.join(&quarantine_name);
        let payload_name = OsStr::new("payload.part");
        if let Err(error) = atomic_rename_noreplace(
            &self.directory,
            &self.parent_path,
            source_name,
            &quarantine,
            &quarantine_path,
            payload_name,
        ) {
            let _ = self.directory.remove_dir(&quarantine_name);
            return Err(error);
        }
        Ok((quarantine, quarantine_name, quarantine_path))
    }

    fn rename_temp_to_target(&self, expected_identity: &str) -> io::Result<()> {
        let actual_identity = self
            .entry_identity(&self.temp_name)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "download temporary file is missing"))?;
        if actual_identity != expected_identity {
            return Err(io::Error::other("download temporary file identity changed"));
        }
        wait_at_test_leaf_mutation_barrier(&self.temp_name);
        atomic_rename_noreplace(
            &self.directory,
            &self.parent_path,
            &self.temp_name,
            &self.directory,
            &self.parent_path,
            &self.target_name,
        )?;
        let target_identity = self
            .entry_identity(&self.target_name)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "published destination is missing"))?;
        if target_identity != expected_identity {
            let (_, _, quarantine_path) = self.quarantine_entry(&self.target_name, ".dbx-rejected-publish-")?;
            self.sync_directory()?;
            return Err(io::Error::other(format!(
                "published destination identity does not match the download; replacement preserved in {}",
                quarantine_path.display()
            )));
        }
        self.sync_directory()
    }

    fn publish(&self, expected_identity: &str) -> Result<(), String> {
        self.rename_temp_to_target(expected_identity).map_err(|error| {
            format!("Failed to atomically publish the download without replacing the destination: {error}")
        })?;
        Ok(())
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.directory.try_clone()?.into_std_file().sync_all()
    }
}

fn single_file_name(path: &Path, label: &str) -> Result<OsString, String> {
    let name = path.file_name().ok_or_else(|| format!("{label} must name a file"))?;
    if matches!(name.to_str(), Some("" | "." | "..")) {
        return Err(format!("{label} has an invalid file name"));
    }
    Ok(name.to_os_string())
}

fn open_absolute_directory_nofollow(path: &Path) -> io::Result<Dir> {
    if !path.is_absolute() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "directory path must be absolute"));
    }
    let mut root = PathBuf::new();
    let mut segments = Vec::<OsString>::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => root.push(prefix.as_os_str()),
            Component::RootDir => root.push(component.as_os_str()),
            Component::Normal(segment) => segments.push(segment.to_os_string()),
            Component::CurDir | Component::ParentDir => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "unsafe directory path component"));
            }
        }
    }
    if root.as_os_str().is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "directory root is missing"));
    }
    let mut directory = Dir::open_ambient_dir(root, ambient_authority())?;
    for segment in segments {
        directory = directory.open_dir_nofollow(segment)?;
    }
    Ok(directory)
}

fn directory_identity(directory: &Dir) -> io::Result<String> {
    Ok(metadata_identity(&directory.dir_metadata()?))
}

fn metadata_identity(metadata: &cap_std::fs::Metadata) -> String {
    format!("cap:{}:{}", CapabilityMetadataExt::dev(metadata), CapabilityMetadataExt::ino(metadata))
}

fn entry_identity_in(directory: &Dir, name: &OsStr) -> io::Result<Option<String>> {
    match directory.symlink_metadata(name) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(io::Error::other("operation-owned path was replaced by a symbolic link"))
        }
        Ok(metadata) if !metadata.is_file() => {
            Err(io::Error::other("operation-owned path was replaced by a non-file entry"))
        }
        Ok(_) => {
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            let file = directory.open_with(name, &options)?;
            Ok(Some(metadata_identity(&file.metadata()?)))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
fn test_atomic_rename_error(source_name: &OsStr) -> Option<io::Error> {
    let mut guard = TEST_UNSUPPORTED_ATOMIC_RENAME
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if guard.as_deref() == Some(source_name) {
        guard.take();
        Some(io::Error::new(io::ErrorKind::Unsupported, "injected filesystem without atomic no-replace rename"))
    } else {
        None
    }
}

#[cfg(not(test))]
fn test_atomic_rename_error(_source_name: &OsStr) -> Option<io::Error> {
    None
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn atomic_rename_noreplace(
    source_directory: &Dir,
    _source_path: &Path,
    source_name: &OsStr,
    destination_directory: &Dir,
    _destination_path: &Path,
    destination_name: &OsStr,
) -> io::Result<()> {
    if let Some(error) = test_atomic_rename_error(source_name) {
        return Err(error);
    }
    rustix::fs::renameat_with(
        source_directory,
        source_name,
        destination_directory,
        destination_name,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(|error| {
        if matches!(error, rustix::io::Errno::NOSYS | rustix::io::Errno::OPNOTSUPP | rustix::io::Errno::INVAL) {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!("filesystem does not support atomic no-replace rename: {error}"),
            )
        } else {
            io::Error::from(error)
        }
    })
}

#[cfg(windows)]
fn atomic_rename_noreplace(
    source_directory: &Dir,
    _source_path: &Path,
    source_name: &OsStr,
    destination_directory: &Dir,
    _destination_path: &Path,
    destination_name: &OsStr,
) -> io::Result<()> {
    use cap_std::fs::OpenOptionsExt;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FileRenameInfoEx, SetFileInformationByHandle, DELETE, FILE_READ_ATTRIBUTES, FILE_RENAME_INFO,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    if let Some(error) = test_atomic_rename_error(source_name) {
        return Err(error);
    }

    let destination = destination_name.encode_wide().collect::<Vec<_>>();
    if destination.is_empty() || destination.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "destination file name is invalid"));
    }
    let file_name_bytes = destination
        .len()
        .checked_mul(std::mem::size_of::<u16>())
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination file name is too long"))?;
    let buffer_size = std::mem::size_of::<FILE_RENAME_INFO>()
        .checked_add((destination.len() - 1) * std::mem::size_of::<u16>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination file name is too long"))?;
    let buffer_size_u32 = u32::try_from(buffer_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "destination file name is too long"))?;
    let mut buffer = vec![0usize; buffer_size.div_ceil(std::mem::size_of::<usize>())];
    let rename_info = buffer.as_mut_ptr().cast::<FILE_RENAME_INFO>();

    let mut source_options = OpenOptions::new();
    source_options
        .access_mode(DELETE | FILE_READ_ATTRIBUTES)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .follow(FollowSymlinks::No);
    let source = source_directory.open_with(source_name, &source_options)?;

    unsafe {
        (*rename_info).Anonymous.Flags = 0;
        (*rename_info).RootDirectory = destination_directory.as_raw_handle();
        (*rename_info).FileNameLength = file_name_bytes;
        std::ptr::copy_nonoverlapping(
            destination.as_ptr(),
            std::ptr::addr_of_mut!((*rename_info).FileName).cast::<u16>(),
            destination.len(),
        );
    }
    let renamed = unsafe {
        SetFileInformationByHandle(source.as_raw_handle(), FileRenameInfoEx, rename_info.cast(), buffer_size_u32)
    };
    if renamed == 0 {
        let error = io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(1 | 17 | 50 | 87 | 120)) {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("filesystem does not support same-volume atomic no-replace rename: {error}"),
            ))
        } else {
            Err(error)
        }
    } else {
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn atomic_rename_noreplace(
    _source_directory: &Dir,
    _source_path: &Path,
    source_name: &OsStr,
    _destination_directory: &Dir,
    _destination_path: &Path,
    _destination_name: &OsStr,
) -> io::Result<()> {
    if let Some(error) = test_atomic_rename_error(source_name) {
        return Err(error);
    }
    Err(io::Error::new(io::ErrorKind::Unsupported, "atomic no-replace rename is unavailable on this platform"))
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

async fn validate_local_destination(path: &Path) -> Result<ValidatedLocalDestination, String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || validate_local_destination_sync(&path))
        .await
        .map_err(|error| error.to_string())?
}

fn validate_local_destination_sync(path: &Path) -> Result<ValidatedLocalDestination, String> {
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
    let directory = open_absolute_directory_nofollow(parent)
        .map_err(|error| format!("Local download destination parent is unavailable or unsafe: {error}"))?;
    let target_name = single_file_name(path, "Local download destination")?;
    match directory.symlink_metadata(target_name) {
        Ok(_) => return Err("Local download destination already exists".to_string()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("Failed to inspect local download destination: {error}")),
    }
    let directory_identity =
        directory_identity(&directory).map_err(|error| format!("Failed to identify local destination: {error}"))?;
    Ok(ValidatedLocalDestination { path: path.to_path_buf(), directory_identity })
}

fn validate_local_authorization(scope: &tauri::fs::Scope, path: &Path) -> Result<(), String> {
    if scope.is_allowed(path) {
        Ok(())
    } else {
        Err("Local download destination is not authorized; choose it with the save dialog".to_string())
    }
}

async fn recover_interrupted_transfer(state: &AppState, transfer: &FileTransferStorageRecord) -> Result<(), String> {
    let Some(temp_path) = transfer.temp_path.as_deref() else {
        if transfer.status == "publishing" {
            state
                .storage
                .update_file_transfer(
                    &transfer.id,
                    "failed".to_string(),
                    transfer.bytes_transferred,
                    transfer.total_bytes,
                    None,
                    None,
                    Some("Publishing transfer has no durable temporary-file identity".to_string()),
                    true,
                )
                .await?;
        }
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
                transfer.temp_identity.clone(),
                Some("Interrupted transfer has an invalid temporary-file path; no file was removed".to_string()),
                true,
            )
            .await?;
        return Ok(());
    }

    let local_path = PathBuf::from(&transfer.local_path);
    let temp_path = PathBuf::from(temp_path);
    let persisted_temp_path = temp_path.to_string_lossy().into_owned();
    let directory_identity = transfer.local_directory_identity.clone();
    let expected_temp_identity = transfer.temp_identity.clone();
    let publishing = transfer.status == "publishing";
    let reconciliation = tokio::task::spawn_blocking(move || {
        let anchored = AnchoredDestination::reopen(&local_path, &temp_path, &directory_identity)?;
        if publishing {
            let expected = expected_temp_identity
                .as_deref()
                .ok_or_else(|| "Publishing transfer has no durable file identity".to_string())?;
            let target_identity = anchored.entry_identity(&anchored.target_name).map_err(|error| error.to_string())?;
            let temp_identity = anchored.entry_identity(&anchored.temp_name).map_err(|error| error.to_string())?;
            if target_identity.as_deref() == Some(expected) {
                if temp_identity.as_deref() == Some(expected) {
                    anchored.remove_owned_temp(expected).map_err(|error| error.to_string())?;
                }
                Ok(true)
            } else if target_identity.is_none() && temp_identity.as_deref() == Some(expected) {
                anchored.publish(expected)?;
                Ok(true)
            } else if target_identity.is_some() && temp_identity.as_deref() == Some(expected) {
                anchored.remove_owned_temp(expected).map_err(|error| error.to_string())?;
                Ok(false)
            } else if target_identity.is_some() && temp_identity.is_none() {
                Err("Published destination identity does not match the download; existing file was left in place"
                    .to_string())
            } else {
                Ok(false)
            }
        } else {
            let expected = expected_temp_identity.as_deref().ok_or_else(|| {
                "Interrupted transfer has no durable temporary-file identity; no file was removed".to_string()
            })?;
            anchored.remove_owned_temp(expected).map(|_| false).map_err(|error| error.to_string())
        }
    })
    .await
    .map_err(|error| error.to_string())?;

    match reconciliation {
        Ok(true) => {
            state
                .storage
                .update_file_transfer(
                    &transfer.id,
                    "completed".to_string(),
                    transfer.bytes_transferred,
                    transfer.total_bytes,
                    None,
                    None,
                    None,
                    true,
                )
                .await?;
        }
        Ok(false) if publishing => {
            state
                .storage
                .update_file_transfer(
                    &transfer.id,
                    "failed".to_string(),
                    transfer.bytes_transferred,
                    transfer.total_bytes,
                    None,
                    None,
                    Some("The application exited before publishing the download".to_string()),
                    true,
                )
                .await?;
        }
        Ok(false) => {}
        Err(error) => {
            state
                .storage
                .update_file_transfer(
                    &transfer.id,
                    "failed".to_string(),
                    transfer.bytes_transferred,
                    transfer.total_bytes,
                    Some(persisted_temp_path),
                    transfer.temp_identity.clone(),
                    Some(format!("Interrupted transfer reconciliation failed safely: {error}")),
                    true,
                )
                .await?;
        }
    }
    Ok(())
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

    struct PartialThenDiskFull {
        first_write: usize,
        wrote: bool,
    }

    impl AsyncWrite for PartialThenDiskFull {
        fn poll_write(mut self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
            if self.wrote {
                Poll::Ready(Err(io::Error::from_raw_os_error(28)))
            } else {
                self.wrote = true;
                Poll::Ready(Ok(self.first_write.min(buffer.len())))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct PartialThenStall {
        first_write: usize,
        wrote: bool,
    }

    impl AsyncWrite for PartialThenStall {
        fn poll_write(mut self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
            if self.wrote {
                Poll::Pending
            } else {
                self.wrote = true;
                Poll::Ready(Ok(self.first_write.min(buffer.len())))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn canonical_directory_identity(path: &Path) -> String {
        let directory = open_absolute_directory_nofollow(path).unwrap();
        directory_identity(&directory).unwrap()
    }

    #[tokio::test]
    async fn local_destination_must_be_absolute_new_and_not_symlinked() {
        assert!(validate_local_destination(Path::new("relative/file.bin")).await.is_err());
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().canonicalize().unwrap().join("download.bin");
        let validated = validate_local_destination(&target).await.unwrap();
        assert_eq!(validated.path, target);
        assert_eq!(validated.directory_identity, canonical_directory_identity(target.parent().unwrap()));
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
    async fn command_contract_authorizes_starts_cancels_and_queries_without_streaming_bytes_over_ipc() {
        use super::super::file_manager::{password_scope, FileConnectionConfig, FtpConnectionConfig};

        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let storage = Storage::open(&parent.join("dbx.sqlite")).await.unwrap();
        let config = FileConnectionConfig::Ftp(FtpConnectionConfig {
            endpoint: "ftp://127.0.0.1:9".to_string(),
            root: "/".to_string(),
            username: "dbx".to_string(),
        });
        let scope = password_scope(&config).unwrap();
        storage
            .save_file_connection(
                "ftp-command".into(),
                "FTP command".into(),
                "ftp".into(),
                serde_json::to_string(&config).unwrap(),
                Some("password".into()),
                scope,
                true,
                None,
            )
            .await
            .unwrap();
        let state = Arc::new(AppState::new(storage));
        let app = tauri::test::mock_builder()
            .plugin(tauri_plugin_fs::init())
            .manage(state.clone())
            .manage(FileManagerRuntime::default())
            .manage(FileTransferRuntime::default())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let window = tauri::WebviewWindowBuilder::new(&app, "main", Default::default()).build().unwrap();
        let runtime = app.state::<FileTransferRuntime>();
        let _global_barrier = runtime.global_limit.clone().acquire_many_owned(8).await.unwrap();
        let target = parent.join("raw-path.bin");
        let input = || StartDownloadInput {
            connection_id: "ftp-command".to_string(),
            remote_path: "a%2Fb".to_string(),
            local_path: target.to_string_lossy().into_owned(),
        };

        let unauthorized =
            start_download_inner(app.handle().clone(), window.clone(), &state, runtime.inner(), input()).await;
        assert!(unauthorized.unwrap_err().contains("not authorized"));

        window.fs_scope().allow_file(&target).unwrap();
        let started =
            start_download_inner(app.handle().clone(), window.clone(), &state, runtime.inner(), input()).await.unwrap();
        let serialized = serde_json::to_value(&started).unwrap();
        assert_eq!(serialized.as_object().unwrap().len(), 1);
        assert_eq!(serialized["transferId"], started.transfer_id);
        assert!(serialized.get("bytes").is_none());
        assert!(serialized.get("bytesTransferred").is_none());

        let persisted = get_file_transfer_inner(&state, runtime.inner(), &started.transfer_id).await.unwrap();
        assert_eq!(persisted.remote_path, "a%2Fb");
        assert_eq!(persisted.status, "queued");
        let serialized = serde_json::to_value(&persisted).unwrap();
        assert!(serialized.get("localDirectoryIdentity").is_none());
        assert!(serialized.get("tempPath").is_none());
        assert!(serialized.get("tempIdentity").is_none());

        let cancelling =
            cancel_file_transfer_inner(app.handle(), &state, runtime.inner(), &started.transfer_id).await.unwrap();
        assert_eq!(cancelling.status, "cancelling");
        let terminal = wait_for_transfer_status(&state.storage, &started.transfer_id, &["cancelled"]).await;
        assert_eq!(terminal.bytes_transferred, 0);
        let queried = get_file_transfer_inner(&state, runtime.inner(), &started.transfer_id).await.unwrap();
        assert_eq!(queried.status, "cancelled");
        assert!(list_file_transfers_inner(&state, runtime.inner(), Some("ftp-command"))
            .await
            .unwrap()
            .iter()
            .any(|record| record.id == started.transfer_id && record.status == "cancelled"));

        let existing = parent.join("existing.bin");
        std::fs::write(&existing, b"existing").unwrap();
        window.fs_scope().allow_file(&existing).unwrap();
        let existing_error = start_download_inner(
            app.handle().clone(),
            window,
            &state,
            runtime.inner(),
            StartDownloadInput {
                connection_id: "ftp-command".to_string(),
                remote_path: "fixture.bin".to_string(),
                local_path: existing.to_string_lossy().into_owned(),
            },
        )
        .await
        .unwrap_err();
        assert!(existing_error.contains("already exists"));
    }

    #[tokio::test]
    async fn publish_is_no_clobber_and_removes_the_operation_temp() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("download.bin");
        let temp_path = parent.join(".dbx-download-test-first.part");
        let identity = canonical_directory_identity(&parent);
        let anchored = AnchoredDestination::open(&target, &temp_path, &identity).unwrap();
        let (mut temp, temp_identity) = anchored.create_temp().unwrap();
        use std::io::Write;
        temp.write_all(b"payload").unwrap();
        temp.sync_all().unwrap();
        drop(temp);
        anchored.publish(&temp_identity).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"payload");
        assert!(!temp_path.exists());

        let second_path = parent.join(".dbx-download-test-second.part");
        let second = AnchoredDestination::reopen(&target, &second_path, &identity).unwrap();
        let (mut second_temp, second_identity) = second.create_temp().unwrap();
        second_temp.write_all(b"replacement").unwrap();
        second_temp.sync_all().unwrap();
        drop(second_temp);
        assert!(second.publish(&second_identity).is_err());
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"payload");
        second.remove_owned_temp(&second_identity).unwrap();
    }

    #[tokio::test]
    async fn create_timeout_awaits_blocking_task_before_cleanup_can_start() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("download.bin");
        let temp_path = parent.join(".dbx-download-create-timeout-random.part");
        let anchored =
            Arc::new(AnchoredDestination::open(&target, &temp_path, &canonical_directory_identity(&parent)).unwrap());
        let barrier = install_test_blocking_barrier(&TEST_CREATE_TEMP_BARRIER, temp_path.file_name().unwrap());
        let create_target = anchored.clone();
        let creation = tokio::spawn(async move { await_create_temp(create_target, Duration::from_millis(10)).await });

        tokio::time::timeout(Duration::from_secs(2), barrier.opened.notified()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!creation.is_finished(), "timed-out create must still be structurally awaited");
        assert!(!temp_path.exists());

        release_test_blocking_barrier(&barrier);
        let completion = creation.await.unwrap().unwrap();
        assert!(completion.timed_out);
        drop(completion.file);
        assert!(temp_path.exists());
        anchored.remove_owned_temp(&completion.identity).unwrap();
        assert!(!temp_path.exists());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!temp_path.exists(), "no detached create may recreate the file after cleanup");
    }

    #[tokio::test]
    async fn unsupported_atomic_rename_is_rejected_without_a_hard_link_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("download.bin");
        let temp_path = parent.join(".dbx-download-unsupported-rename-random.part");
        let anchored = AnchoredDestination::open(&target, &temp_path, &canonical_directory_identity(&parent)).unwrap();
        let (mut temp, identity) = anchored.create_temp().unwrap();
        use std::io::Write;
        temp.write_all(b"payload").unwrap();
        temp.sync_all().unwrap();
        drop(temp);
        *TEST_UNSUPPORTED_ATOMIC_RENAME
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(temp_path.file_name().unwrap().to_os_string());

        let error = anchored.publish(&identity).unwrap_err();
        assert!(error.contains("without atomic no-replace rename"), "{error}");
        assert!(!target.exists());
        assert_eq!(std::fs::read(&temp_path).unwrap(), b"payload");
        anchored.remove_owned_temp(&identity).unwrap();
    }

    #[tokio::test]
    async fn publish_leaf_swap_is_detected_without_deleting_the_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("download.bin");
        let temp_path = parent.join(".dbx-download-publish-swap-random.part");
        let displaced = parent.join("displaced-original.part");
        let anchored =
            Arc::new(AnchoredDestination::open(&target, &temp_path, &canonical_directory_identity(&parent)).unwrap());
        let (mut temp, identity) = anchored.create_temp().unwrap();
        use std::io::Write;
        temp.write_all(b"owned payload").unwrap();
        temp.sync_all().unwrap();
        drop(temp);

        let barrier = install_test_blocking_barrier(&TEST_LEAF_MUTATION_BARRIER, temp_path.file_name().unwrap());
        let publish_target = anchored.clone();
        let publish_identity = identity.clone();
        let publishing = tokio::task::spawn_blocking(move || publish_target.publish(&publish_identity));
        tokio::time::timeout(Duration::from_secs(2), barrier.opened.notified()).await.unwrap();
        std::fs::rename(&temp_path, &displaced).unwrap();
        std::fs::write(&temp_path, b"replacement payload").unwrap();
        release_test_blocking_barrier(&barrier);

        let error = publishing.await.unwrap().unwrap_err();
        assert!(error.contains("identity does not match"), "{error}");
        assert_eq!(std::fs::read(&displaced).unwrap(), b"owned payload");
        assert!(!target.exists());
        let quarantined = std::fs::read_dir(&parent)
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with(".dbx-rejected-publish-"))
            .expect("rejected replacement must remain quarantined");
        assert_eq!(std::fs::read(quarantined.path().join("payload.part")).unwrap(), b"replacement payload");
    }

    #[tokio::test]
    async fn cleanup_leaf_swap_quarantines_and_preserves_the_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("download.bin");
        let temp_path = parent.join(".dbx-download-cleanup-swap-random.part");
        let displaced = parent.join("displaced-original.part");
        let anchored =
            Arc::new(AnchoredDestination::open(&target, &temp_path, &canonical_directory_identity(&parent)).unwrap());
        let (mut temp, identity) = anchored.create_temp().unwrap();
        use std::io::Write;
        temp.write_all(b"owned payload").unwrap();
        temp.sync_all().unwrap();
        drop(temp);

        let barrier = install_test_blocking_barrier(&TEST_LEAF_MUTATION_BARRIER, temp_path.file_name().unwrap());
        let cleanup_target = anchored.clone();
        let cleanup_identity = identity.clone();
        let cleanup = tokio::task::spawn_blocking(move || cleanup_target.remove_owned_temp(&cleanup_identity));
        tokio::time::timeout(Duration::from_secs(2), barrier.opened.notified()).await.unwrap();
        std::fs::rename(&temp_path, &displaced).unwrap();
        std::fs::write(&temp_path, b"replacement payload").unwrap();
        release_test_blocking_barrier(&barrier);

        let error = cleanup.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("replacement preserved"));
        assert_eq!(std::fs::read(&displaced).unwrap(), b"owned payload");
        let quarantined = std::fs::read_dir(&parent)
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with(".dbx-cleanup-"))
            .expect("replacement quarantine must remain");
        assert_eq!(std::fs::read(quarantined.path().join("payload.part")).unwrap(), b"replacement payload");
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
            local_directory_identity: "identity".to_string(),
            temp_path: None,
            temp_identity: None,
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
    async fn crash_before_temp_create_does_not_remove_an_unrelated_file() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let storage = Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        let state = AppState::new(storage);
        let target = parent.join("report.csv");
        let owned = parent.join(".dbx-download-transfer-1-random.part");
        let unrelated = parent.join("unrelated.part");
        tokio::fs::write(&unrelated, b"unrelated").await.unwrap();
        state
            .storage
            .create_file_transfer(
                "transfer-1".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
                canonical_directory_identity(&parent),
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
                None,
                false,
            )
            .await
            .unwrap();

        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        assert_eq!(interrupted.len(), 1);
        recover_interrupted_transfer(&state, &interrupted[0]).await.unwrap();
        assert!(!owned.exists());
        assert_eq!(tokio::fs::read(&unrelated).await.unwrap(), b"unrelated");
        let recovered = state.storage.get_file_transfer("transfer-1").await.unwrap().unwrap();
        assert_eq!(recovered.status, "failed");
        assert!(recovered.completed_at.is_some());
    }

    #[tokio::test]
    async fn crash_after_temp_create_before_identity_persist_preserves_unproven_file() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let storage = Storage::open(&parent.join("dbx.sqlite")).await.unwrap();
        let state = AppState::new(storage);
        let target = parent.join("report.csv");
        let owned = parent.join(".dbx-download-transfer-create-window-random.part");
        tokio::fs::write(&owned, b"partial").await.unwrap();
        state
            .storage
            .create_file_transfer(
                "transfer-create-window".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
                canonical_directory_identity(&parent),
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                "transfer-create-window",
                "running".into(),
                0,
                Some(20),
                Some(owned.to_string_lossy().into_owned()),
                None,
                None,
                false,
            )
            .await
            .unwrap();

        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        recover_interrupted_transfer(&state, &interrupted[0]).await.unwrap();
        assert_eq!(tokio::fs::read(&owned).await.unwrap(), b"partial");
        let recovered = state.storage.get_file_transfer("transfer-create-window").await.unwrap().unwrap();
        assert_eq!(recovered.status, "failed");
        assert!(recovered.error.unwrap().contains("no durable temporary-file identity"));
    }

    #[tokio::test]
    async fn publishing_crash_after_rename_is_reconciled_to_completed() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("report.csv");
        let temp_path = parent.join(".dbx-download-transfer-publishing-random.part");
        let identity = canonical_directory_identity(&parent);
        let anchored = AnchoredDestination::open(&target, &temp_path, &identity).unwrap();
        let (mut temp, temp_identity) = anchored.create_temp().unwrap();
        use std::io::Write;
        temp.write_all(b"complete payload").unwrap();
        temp.sync_all().unwrap();
        drop(temp);

        let storage = Storage::open(&parent.join("dbx.sqlite")).await.unwrap();
        let state = AppState::new(storage);
        state
            .storage
            .create_file_transfer(
                "transfer-publishing".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
                identity,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                "transfer-publishing",
                "publishing".into(),
                16,
                Some(16),
                Some(temp_path.to_string_lossy().into_owned()),
                Some(temp_identity.clone()),
                None,
                false,
            )
            .await
            .unwrap();
        anchored.rename_temp_to_target(&temp_identity).unwrap();

        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        assert_eq!(interrupted[0].status, "publishing");
        recover_interrupted_transfer(&state, &interrupted[0]).await.unwrap();
        let recovered = state.storage.get_file_transfer("transfer-publishing").await.unwrap().unwrap();
        assert_eq!(recovered.status, "completed");
        assert_eq!(std::fs::read(target).unwrap(), b"complete payload");
        assert!(!temp_path.exists());
    }

    #[tokio::test]
    async fn publishing_recovery_leaves_an_unproven_target_in_place() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("report.csv");
        let temp_path = parent.join(".dbx-download-transfer-publishing-mismatch-random.part");
        let directory_identity = canonical_directory_identity(&parent);
        let anchored = AnchoredDestination::open(&target, &temp_path, &directory_identity).unwrap();
        let (mut temp, temp_identity) = anchored.create_temp().unwrap();
        use std::io::Write;
        temp.write_all(b"download payload").unwrap();
        temp.sync_all().unwrap();
        drop(temp);

        let storage = Storage::open(&parent.join("dbx.sqlite")).await.unwrap();
        let state = AppState::new(storage);
        state
            .storage
            .create_file_transfer(
                "transfer-publishing-mismatch".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
                directory_identity,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                "transfer-publishing-mismatch",
                "publishing".into(),
                16,
                Some(16),
                Some(temp_path.to_string_lossy().into_owned()),
                Some(temp_identity),
                None,
                false,
            )
            .await
            .unwrap();

        std::fs::remove_file(&temp_path).unwrap();
        std::fs::write(&target, b"user replacement").unwrap();
        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        recover_interrupted_transfer(&state, &interrupted[0]).await.unwrap();

        let recovered = state.storage.get_file_transfer("transfer-publishing-mismatch").await.unwrap().unwrap();
        assert_eq!(recovered.status, "failed");
        assert!(recovered.error.unwrap().contains("existing file was left in place"));
        assert_eq!(std::fs::read(&target).unwrap(), b"user replacement");
        assert!(!parent
            .read_dir()
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().starts_with(".dbx-rejected-publish-")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replaced_destination_directory_is_rejected_without_touching_attacker_entries() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let authorized = root.path().join("authorized");
        let moved = root.path().join("authorized-moved");
        let attacker = root.path().join("attacker");
        std::fs::create_dir(&authorized).unwrap();
        std::fs::create_dir(&attacker).unwrap();
        let authorized = authorized.canonicalize().unwrap();
        let target = authorized.join("report.csv");
        let temp_path = authorized.join(".dbx-download-transfer-swap-random.part");
        let expected_identity = canonical_directory_identity(&authorized);
        std::fs::rename(&authorized, &moved).unwrap();
        symlink(&attacker, &authorized).unwrap();
        let attacker_temp = attacker.join(".dbx-download-transfer-swap-random.part");
        std::fs::write(&attacker_temp, b"do not delete").unwrap();

        let error = AnchoredDestination::open(&target, &temp_path, &expected_identity).err().unwrap();
        assert!(error.contains("safely") || error.contains("changed"), "{error}");

        let storage = Storage::open(&root.path().join("dbx.sqlite")).await.unwrap();
        let state = AppState::new(storage);
        state
            .storage
            .create_file_transfer(
                "transfer-swap".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
                expected_identity,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                "transfer-swap",
                "running".into(),
                0,
                None,
                Some(temp_path.to_string_lossy().into_owned()),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        recover_interrupted_transfer(&state, &interrupted[0]).await.unwrap();
        assert_eq!(std::fs::read(attacker_temp).unwrap(), b"do not delete");
        assert_eq!(state.storage.get_file_transfer("transfer-swap").await.unwrap().unwrap().status, "failed");
    }

    #[tokio::test]
    async fn persisted_queued_cancel_intent_wins_before_worker_start() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let storage = Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        storage
            .create_file_transfer(
                "queued-cancel".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                parent.join("report.csv").to_string_lossy().into_owned(),
                canonical_directory_identity(&parent),
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
        let progress = TransferProgressSnapshot::new();
        let mut bytes = 0;

        let disconnected = transfer_one_chunk(
            &mut FailedReader,
            &mut sink,
            &mut buffer,
            Duration::from_millis(50),
            &mut bytes,
            &progress,
        )
        .await
        .unwrap_err();
        assert!(disconnected.invalidate_operator);
        assert!(disconnected.message.contains("injected disconnect"));

        let stalled = transfer_one_chunk(
            &mut StalledReader,
            &mut sink,
            &mut buffer,
            Duration::from_millis(10),
            &mut bytes,
            &progress,
        )
        .await
        .unwrap_err();
        assert!(stalled.invalidate_operator);
        assert!(stalled.message.contains("watchdog"));

        let mut input = futures::io::Cursor::new(vec![7_u8; 1024]);
        let disk_full = transfer_one_chunk(
            &mut input,
            &mut DiskFullWriter,
            &mut buffer,
            Duration::from_millis(50),
            &mut bytes,
            &progress,
        )
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
    async fn partial_write_failure_and_cancel_account_exact_successful_bytes() {
        let mut buffer = vec![0_u8; 1_024];
        let mut input = futures::io::Cursor::new(vec![1_u8; 1_024]);
        let mut disk_full = PartialThenDiskFull { first_write: 137, wrote: false };
        let progress = TransferProgressSnapshot::new();
        let mut bytes = 0;
        let failure = transfer_one_chunk(
            &mut input,
            &mut disk_full,
            &mut buffer,
            Duration::from_millis(50),
            &mut bytes,
            &progress,
        )
        .await
        .unwrap_err();
        assert!(failure.message.contains("space") || failure.message.contains("No space left"));
        assert_eq!(bytes, 137);
        assert_eq!(progress.bytes(), 137);

        let mut input = futures::io::Cursor::new(vec![2_u8; 1_024]);
        let mut stalled = PartialThenStall { first_write: 211, wrote: false };
        let progress = TransferProgressSnapshot::new();
        let mut bytes = 0;
        let cancelled = tokio::time::timeout(
            Duration::from_millis(20),
            transfer_one_chunk(&mut input, &mut stalled, &mut buffer, Duration::from_secs(30), &mut bytes, &progress),
        )
        .await;
        assert!(cancelled.is_err());
        assert_eq!(bytes, 211);
        assert_eq!(progress.bytes(), 211);
    }

    #[tokio::test]
    async fn disk_full_terminal_snapshot_keeps_all_prior_successful_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("local.bin");
        let temp_path = parent.join(".dbx-download-disk-full-random.part");
        let directory_identity = canonical_directory_identity(&parent);
        let anchored = AnchoredDestination::open(&target, &temp_path, &directory_identity).unwrap();
        let (mut temp, temp_identity) = anchored.create_temp().unwrap();
        use std::io::Write;
        temp.write_all(&vec![1_u8; 1_024]).unwrap();
        temp.sync_all().unwrap();
        drop(temp);
        let storage = Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        storage
            .create_file_transfer(
                "disk-full".into(),
                "connection-1".into(),
                "download".into(),
                "remote.bin".into(),
                target.to_string_lossy().into_owned(),
                directory_identity,
            )
            .await
            .unwrap();
        storage
            .update_file_transfer(
                "disk-full",
                "running".into(),
                0,
                Some(2_048),
                Some(temp_path.to_string_lossy().into_owned()),
                Some(temp_identity),
                None,
                false,
            )
            .await
            .unwrap();
        let progress = TransferProgressSnapshot::new();
        progress.record_total(Some(2_048));
        let mut writer = DiskFullAfterFirstWrite { writes: 0 };
        let mut buffer = vec![0_u8; 1_024];

        let mut first = futures::io::Cursor::new(vec![1_u8; 1_024]);
        let mut bytes = 0;
        transfer_one_chunk(&mut first, &mut writer, &mut buffer, Duration::from_millis(50), &mut bytes, &progress)
            .await
            .unwrap();
        let mut second = futures::io::Cursor::new(vec![2_u8; 1_024]);
        let failure =
            transfer_one_chunk(&mut second, &mut writer, &mut buffer, Duration::from_millis(50), &mut bytes, &progress)
                .await
                .unwrap_err();
        assert!(failure.message.contains("space") || failure.message.contains("No space left"));

        let state = Arc::new(AppState::new(storage));
        let app = tauri::test::mock_builder()
            .manage(state.clone())
            .manage(FileTransferRuntime::default())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        finalize_download_result(app.handle(), &state, "disk-full", Err(failure), &progress).await;

        let runtime = app.state::<FileTransferRuntime>();
        let terminal = get_file_transfer_inner(&state, runtime.inner(), "disk-full").await.unwrap();
        assert_eq!(terminal.bytes_transferred, 1_024);
        assert_eq!(terminal.total_bytes, Some(2_048));
        assert_eq!(terminal.status, "failed");
        assert!(!temp_path.exists());
        assert!(!target.exists());
        assert!(list_file_transfers_inner(&state, runtime.inner(), Some("connection-1"))
            .await
            .unwrap()
            .iter()
            .any(|record| record.id == "disk-full" && record.status == "failed" && record.bytes_transferred == 1_024));
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
        let parent = directory.path().canonicalize().unwrap();
        let target = parent.join("fixture.txt");
        let temp_path = parent.join(".dbx-download-contract-random.part");
        let anchored = AnchoredDestination::open(&target, &temp_path, &canonical_directory_identity(&parent)).unwrap();
        let (std_file, temp_identity) = anchored.create_temp().unwrap();
        let mut output = tokio::fs::File::from_std(std_file);
        let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_SIZE];
        let progress = TransferProgressSnapshot::new();
        let mut bytes = 0_i64;
        loop {
            let count =
                transfer_one_chunk(&mut reader, &mut output, &mut buffer, IO_PROGRESS_WATCHDOG, &mut bytes, &progress)
                    .await
                    .unwrap();
            if count == 0 {
                break;
            }
        }
        output.flush().await.unwrap();
        output.sync_all().await.unwrap();
        drop(output);
        anchored.publish(&temp_identity).unwrap();

        assert_eq!(bytes, i64::try_from(b"dbx ftp fixture\n".len()).unwrap());
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

    async fn create_worker_transfer<R>(
        app: &tauri::App<R>,
        transfer_id: &str,
        remote_path: &str,
        local_path: &Path,
    ) -> (CancellationToken, tokio::task::JoinHandle<()>)
    where
        R: Runtime,
    {
        let state = app.state::<Arc<AppState>>();
        let parent = local_path.parent().unwrap();
        state
            .storage
            .create_file_transfer(
                transfer_id.to_string(),
                "ftp-contract".into(),
                "download".into(),
                remote_path.to_string(),
                local_path.to_string_lossy().into_owned(),
                canonical_directory_identity(parent),
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
        use super::super::file_manager::{password_scope, FileConnectionConfig, FtpConnectionConfig};

        let endpoint = std::env::var("DBX_TEST_FTP_ENDPOINT").expect("DBX_TEST_FTP_ENDPOINT is required");
        let username = std::env::var("DBX_TEST_FTP_USERNAME").unwrap_or_else(|_| "dbx".to_string());
        let password = std::env::var("DBX_TEST_FTP_PASSWORD").unwrap_or_else(|_| "dbx-password".to_string());
        let container = std::env::var("DBX_TEST_FTP_CONTAINER").expect("DBX_TEST_FTP_CONTAINER is required");
        let directory = tempfile::tempdir().unwrap();
        let download_directory = directory.path().canonicalize().unwrap();
        let storage = Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        let config =
            FileConnectionConfig::Ftp(FtpConnectionConfig { endpoint, root: "/ftp/dbx".to_string(), username });
        let scope = password_scope(&config).unwrap();
        storage
            .save_file_connection(
                "ftp-contract".into(),
                "FTP contract".into(),
                "ftp".into(),
                serde_json::to_string(&config).unwrap(),
                Some(password),
                scope,
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

        let create_cancel_target = download_directory.join("create-cancel.bin");
        let create_barrier = install_test_blocking_barrier(&TEST_CREATE_TEMP_BARRIER, OsStr::new("*"));
        let (create_cancel_token, create_cancel_worker) =
            create_worker_transfer(&app, "ftp-create-cancel", "fixture.txt", &create_cancel_target).await;
        tokio::time::timeout(Duration::from_secs(10), create_barrier.opened.notified())
            .await
            .expect("temporary-file create must reach its explicit barrier");
        state.storage.request_file_transfer_cancel("ftp-create-cancel").await.unwrap();
        create_cancel_token.cancel();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!create_cancel_worker.is_finished(), "worker cleanup must wait for the blocking create task");
        release_test_blocking_barrier(&create_barrier);
        create_cancel_worker.await.unwrap();
        let create_cancelled = state.storage.get_file_transfer("ftp-create-cancel").await.unwrap().unwrap();
        assert_eq!(create_cancelled.status, "cancelled");
        assert_eq!(create_cancelled.bytes_transferred, 0);
        assert!(!create_cancel_target.exists());
        assert_no_owned_temp(&download_directory, "ftp-create-cancel");

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
        let disconnect_barrier = install_test_remote_reader_barrier();
        let (_, disconnect_worker) =
            create_worker_transfer(&app, "ftp-disconnect", "large.bin", &disconnect_target).await;
        tokio::time::timeout(Duration::from_secs(10), disconnect_barrier.opened.notified())
            .await
            .expect("FTP reader must reach the explicit disconnect barrier");
        let at_barrier = state.storage.get_file_transfer("ftp-disconnect").await.unwrap().unwrap();
        assert_eq!(at_barrier.status, "running");
        assert_eq!(at_barrier.bytes_transferred, 0);
        let kill = tokio::task::spawn_blocking(move || Command::new("docker").args(["kill", &container]).status())
            .await
            .unwrap()
            .unwrap();
        assert!(kill.success());
        disconnect_barrier.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), disconnect_worker)
            .await
            .expect("disconnect must terminate active FTP I/O")
            .unwrap();
        let disconnected = state.storage.get_file_transfer("ftp-disconnect").await.unwrap().unwrap();
        assert_eq!(disconnected.status, "failed");
        assert_eq!(disconnected.bytes_transferred, 0);
        assert!(disconnected.error.as_deref().is_some_and(|error| !error.is_empty()));
        assert!(!disconnect_target.exists());
        assert_no_owned_temp(&download_directory, "ftp-disconnect");
    }
}
