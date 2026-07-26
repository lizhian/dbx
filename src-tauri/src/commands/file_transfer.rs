use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::io;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cap_fs_ext::{
    ambient_authority, DirExt, FollowSymlinks, MetadataExt as CapabilityMetadataExt, OpenOptionsFollowExt,
};
use cap_std::fs::{Dir, OpenOptions};
use dbx_core::connection::AppState;
use dbx_core::file_secrets::{FileSecretRedactor, RedactedFileText};
use dbx_core::storage::FileTransferStorageRecord;
use futures::io::AsyncRead as FuturesAsyncRead;
use futures::io::AsyncReadExt as FuturesAsyncReadExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Emitter, Manager, Runtime, State, WebviewWindow};
use tauri_plugin_fs::FsExt;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, OnceCell, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::file_manager::{
    parse_storage_config, validate_remote_relative_path, CancellationSignal, FileConnectionConfig, FileManagerRuntime,
    HdfsConnectionConfig, NativeRenameError, PreparedFileMutation, PreparedFileOperation, RemoteFileFingerprint,
    UploadPolicy, UploadPublishResolution, UploadPublishState,
};
use super::file_manager_webdav::WebdavMutationErrorKind;

const DOWNLOAD_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const UPLOAD_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const S3_UPLOAD_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const REMOTE_COPY_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const GLOBAL_TRANSFER_LIMIT: usize = 8;
const CONNECTION_TRANSFER_LIMIT: usize = 4;
const GLOBAL_UPLOAD_HANDLE_LIMIT: usize = 32;
const CONNECTION_UPLOAD_HANDLE_LIMIT: usize = 8;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);
const GLOBAL_PROGRESS_INTERVAL: Duration = Duration::from_millis(50);
const IO_PROGRESS_WATCHDOG: Duration = Duration::from_secs(30);
const CREATE_TEMP_TIMEOUT: Duration = Duration::from_secs(30);
const DOWNLOAD_OPERATION_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const TRANSFER_EVENT: &str = "file-transfer-progress";
const WEBHDFS_REPLACE_UNSUPPORTED: &str = "WebHDFS copy and rename do not support Replace policy in v1";

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
static TEST_DOWNLOAD_AFTER_CHUNK_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_UPLOAD_AFTER_CHUNK_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_UPLOAD_AFTER_CLOSE_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_REMOTE_COPY_AFTER_CLOSE_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_REMOTE_COPY_AFTER_CHUNK_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_REMOTE_RENAME_AFTER_PUBLISH_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_TRANSFER_BEFORE_INSERT_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn install_test_transfer_before_insert_barrier() -> TestRemoteReaderBarrier {
    let barrier = TestRemoteReaderBarrier {
        opened: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let mut slot = TEST_TRANSFER_BEFORE_INSERT_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert!(slot.is_none(), "only one transfer insert barrier may be installed");
    *slot = Some(barrier.clone());
    barrier
}

#[cfg(test)]
fn clear_test_transfer_before_insert_barrier() {
    *TEST_TRANSFER_BEFORE_INSERT_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = None;
}

#[cfg(test)]
async fn wait_test_transfer_before_insert_barrier() {
    let barrier = TEST_TRANSFER_BEFORE_INSERT_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    if let Some(barrier) = barrier {
        barrier.opened.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(test)]
static TEST_SFTP_RENAME_BEFORE_DISPATCH_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_HDFS_NATIVE_RENAME_BEFORE_DISPATCH_BARRIER: std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_REMOTE_COPY_MAX_READ_CHUNK: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static TEST_REMOTE_COPY_MAX_WRITE_CHUNK: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static TEST_REMOTE_COPY_MAX_RELAY_PAYLOAD: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static TEST_S3_COPY_AFTER_COMMIT_RESPONSE_LOSS: std::sync::OnceLock<Mutex<Option<String>>> = std::sync::OnceLock::new();

#[cfg(test)]
static TEST_S3_COPY_CHUNK: std::sync::OnceLock<Mutex<Option<(String, usize)>>> = std::sync::OnceLock::new();

#[cfg(test)]
static TEST_REMOTE_COPY_WRITER_OPEN_SIDE_EFFECT_FAILURE: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
static TEST_REMOTE_COPY_PERSISTENCE_FAILURE_AFTER_VERIFY: AtomicBool = AtomicBool::new(false);

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

type RemotePathLockKey = (String, String);
type RemotePathLockRegistry = Arc<Mutex<HashMap<RemotePathLockKey, Arc<AsyncMutex<()>>>>>;

pub struct FileTransferRuntime {
    global_limit: Arc<Semaphore>,
    connection_limits: Mutex<HashMap<String, Arc<Semaphore>>>,
    active: Mutex<HashMap<String, ActiveTransfer>>,
    recovery: OnceCell<()>,
    last_progress_event: Mutex<Option<Instant>>,
    path_locks: RemotePathLockRegistry,
}

struct RemotePathLockGuards {
    registry: RemotePathLockRegistry,
    keys: Vec<RemotePathLockKey>,
    locks: Vec<Arc<AsyncMutex<()>>>,
    guards: Vec<OwnedMutexGuard<()>>,
}

impl Drop for RemotePathLockGuards {
    fn drop(&mut self) {
        self.guards.clear();
        self.locks.clear();
        let mut registry = self.registry.lock().unwrap_or_else(|error| error.into_inner());
        for key in &self.keys {
            if registry.get(key).is_some_and(|lock| Arc::strong_count(lock) == 1) {
                registry.remove(key);
            }
        }
    }
}

struct ActiveTransfer {
    connection_id: String,
    cancellation: CancellationToken,
    upload: bool,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartDownloadInput {
    pub connection_id: String,
    pub remote_path: String,
    pub local_path: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartUploadInput {
    pub connection_id: String,
    pub local_path: String,
    pub remote_path: String,
    pub policy: UploadPolicy,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoteMutationPolicy {
    BestEffortNoClobber {
        #[serde(rename = "atomicNoClobber")]
        atomic_no_clobber: bool,
        #[serde(rename = "externalToctouRisk")]
        external_toctou_risk: bool,
    },
    Replace {
        confirmed: bool,
    },
}

impl RemoteMutationPolicy {
    fn validate(self) -> Result<(), String> {
        match self {
            Self::BestEffortNoClobber { atomic_no_clobber: false, external_toctou_risk: true } => Ok(()),
            Self::BestEffortNoClobber { .. } => {
                Err("FTP copy/rename requires atomicNoClobber=false and externalToctouRisk=true".to_string())
            }
            Self::Replace { confirmed: true } => Ok(()),
            Self::Replace { confirmed: false } => {
                Err("Replace requires explicit confirmation before the operation starts".to_string())
            }
        }
    }

    fn replace(self) -> bool {
        matches!(self, Self::Replace { confirmed: true })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartRemoteTransferInput {
    pub connection_id: String,
    pub source_path: String,
    pub destination_path: String,
    pub policy: RemoteMutationPolicy,
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

struct UploadOutcome {
    bytes_transferred: i64,
    total_bytes: Option<i64>,
    publish_outcome: Option<String>,
}

struct RemoteTransferOutcome {
    bytes_transferred: i64,
    total_bytes: i64,
    operation_outcome: &'static str,
    operation_phase: &'static str,
    source_fingerprint: String,
    destination_fingerprint: String,
}

struct VerifiedRemoteContent {
    fingerprint: RemoteFileFingerprint,
    sha256: String,
}

impl VerifiedRemoteContent {
    fn durable_fingerprint(&self) -> String {
        format!("{};relay_sha256:{}", self.fingerprint.encode(), self.sha256)
    }
}

struct RemoteTransferFailure {
    failure: TransferFailure,
    operation_outcome: &'static str,
    operation_phase: &'static str,
    partial_destination: Option<String>,
    source_fingerprint: Option<String>,
    destination_fingerprint: Option<String>,
}

enum NativeRenameDispatchOutcome {
    Finished(Result<(), NativeRenameError>),
    TransferCancelled,
    ConnectionCancelled,
    TimedOut,
}

struct UploadFailure {
    failure: TransferFailure,
    partial_destination: Option<String>,
    abort_outcome: Option<String>,
    publish_outcome: Option<String>,
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

#[derive(Debug)]
struct ValidatedLocalSource {
    path: PathBuf,
    directory_identity: String,
    identity: String,
    fingerprint: String,
    total_bytes: i64,
    file: std::fs::File,
    verification_file: Arc<std::fs::File>,
}

#[derive(Clone, Debug)]
struct UploadSourceProof {
    path: PathBuf,
    directory_identity: String,
    identity: String,
    fingerprint: String,
    total_bytes: i64,
    verification_file: Arc<std::fs::File>,
}

impl ValidatedLocalSource {
    fn proof(&self) -> UploadSourceProof {
        UploadSourceProof {
            path: self.path.clone(),
            directory_identity: self.directory_identity.clone(),
            identity: self.identity.clone(),
            fingerprint: self.fingerprint.clone(),
            total_bytes: self.total_bytes,
            verification_file: self.verification_file.clone(),
        }
    }
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
            path_locks: Arc::new(Mutex::new(HashMap::new())),
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

    async fn lock_remote_paths(
        &self,
        connection_id: &str,
        source_path: &str,
        destination_path: &str,
    ) -> RemotePathLockGuards {
        let mut keys = vec![
            (connection_id.to_string(), source_path.to_string()),
            (connection_id.to_string(), destination_path.to_string()),
        ];
        keys.sort();
        keys.dedup();
        let locks = {
            let mut path_locks = self.path_locks.lock().unwrap_or_else(|error| error.into_inner());
            keys.iter()
                .map(|key| path_locks.entry(key.clone()).or_insert_with(|| Arc::new(AsyncMutex::new(()))).clone())
                .collect()
        };
        let mut locked = RemotePathLockGuards { registry: self.path_locks.clone(), keys, locks, guards: Vec::new() };
        for lock in &locked.locks {
            locked.guards.push(lock.clone().lock_owned().await);
        }
        locked
    }

    fn register(&self, transfer_id: String, connection_id: String, cancellation: CancellationToken) {
        self.active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(transfer_id, ActiveTransfer { connection_id, cancellation, upload: false });
    }

    fn register_upload(
        &self,
        transfer_id: String,
        connection_id: String,
        cancellation: CancellationToken,
    ) -> Result<(), String> {
        let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
        let global_uploads = active.values().filter(|transfer| transfer.upload).count();
        let connection_uploads =
            active.values().filter(|transfer| transfer.upload && transfer.connection_id == connection_id).count();
        if global_uploads >= GLOBAL_UPLOAD_HANDLE_LIMIT {
            return Err("Too many active or queued uploads; wait for an upload to finish".to_string());
        }
        if connection_uploads >= CONNECTION_UPLOAD_HANDLE_LIMIT {
            return Err("Too many active or queued uploads for this connection".to_string());
        }
        active.insert(transfer_id, ActiveTransfer { connection_id, cancellation, upload: true });
        Ok(())
    }

    fn cancellation(&self, transfer_id: &str) -> Option<CancellationToken> {
        self.active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(transfer_id)
            .map(|transfer| transfer.cancellation.clone())
    }

    pub(super) fn cancel_connection(&self, connection_id: &str) -> usize {
        let cancellations = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|transfer| transfer.connection_id == connection_id)
            .map(|transfer| transfer.cancellation.clone())
            .collect::<Vec<_>>();
        for cancellation in &cancellations {
            cancellation.cancel();
        }
        cancellations.len()
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

    async fn ensure_recovered(&self, state: &AppState, file_manager: &FileManagerRuntime) -> Result<(), String> {
        self.recovery
            .get_or_try_init(|| async {
                let interrupted = state.storage.recover_interrupted_file_transfers().await?;
                for transfer in interrupted {
                    recover_interrupted_transfer(state, file_manager, &transfer).await?;
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
    let file_manager = app.state::<FileManagerRuntime>();
    if let Err(error) = runtime.ensure_recovered(&state, file_manager.inner()).await {
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
    let file_manager = app.state::<FileManagerRuntime>();
    runtime.ensure_recovered(state, file_manager.inner()).await?;
    let remote_path = validate_remote_relative_path(&input.remote_path)?;
    let local = validate_local_destination(Path::new(&input.local_path)).await?;
    let fs_scope = window
        .try_fs_scope()
        .ok_or_else(|| "File-system authorization is unavailable; choose the destination again".to_string())?;
    validate_local_authorization(&fs_scope, &local.path)?;
    let _admission = file_manager.begin_operation(&input.connection_id)?;
    let connection = state
        .storage
        .load_file_connection(&input.connection_id)
        .await?
        .ok_or_else(|| "File connection not found".to_string())?;

    let transfer_id = Uuid::new_v4().to_string();
    let cancellation = CancellationToken::new();
    runtime.register(transfer_id.clone(), input.connection_id.clone(), cancellation.clone());
    let record = state
        .storage
        .create_file_transfer(
            transfer_id.clone(),
            input.connection_id.clone(),
            "download".to_string(),
            remote_path,
            local.path.to_string_lossy().into_owned(),
            local.directory_identity,
            connection.revision,
        )
        .await;
    let record = match record {
        Ok(record) => record,
        Err(error) => {
            runtime.unregister(&transfer_id);
            return Err(error);
        }
    };
    emit_transfer(&app, &record);
    let worker_id = transfer_id.clone();
    let connection_id = input.connection_id;
    tokio::spawn(async move {
        run_download_worker(app, worker_id, connection_id, cancellation).await;
    });

    Ok(StartTransferResult { transfer_id })
}

#[tauri::command]
pub async fn start_upload(
    app: AppHandle,
    window: WebviewWindow,
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    input: StartUploadInput,
) -> Result<StartTransferResult, String> {
    start_upload_inner(app, window, state.inner(), runtime.inner(), input).await
}

async fn start_upload_inner<R: Runtime>(
    app: AppHandle<R>,
    window: WebviewWindow<R>,
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    input: StartUploadInput,
) -> Result<StartTransferResult, String> {
    let file_manager = app.state::<FileManagerRuntime>();
    runtime.ensure_recovered(state, file_manager.inner()).await?;
    input.policy.validate()?;
    let remote_path = validate_remote_relative_path(&input.remote_path)?;
    let local_path = validate_local_source_path(Path::new(&input.local_path))?;
    let fs_scope = window
        .try_fs_scope()
        .ok_or_else(|| "File-system authorization is unavailable; choose the source file again".to_string())?;
    validate_local_upload_authorization(&fs_scope, &local_path)?;
    let _admission = file_manager.begin_operation(&input.connection_id)?;
    let connection = state
        .storage
        .load_file_connection(&input.connection_id)
        .await?
        .ok_or_else(|| "File connection not found".to_string())?;
    let local = validate_local_source(&local_path).await?;

    let transfer_id = Uuid::new_v4().to_string();
    let cancellation = CancellationToken::new();
    runtime.register_upload(transfer_id.clone(), input.connection_id.clone(), cancellation.clone())?;
    let queued = match state
        .storage
        .create_file_upload_transfer(
            transfer_id.clone(),
            input.connection_id.clone(),
            remote_path,
            local.path.to_string_lossy().into_owned(),
            local.directory_identity.clone(),
            local.fingerprint.clone(),
            local.total_bytes,
            connection.revision,
        )
        .await
    {
        Ok(record) => record,
        Err(error) => {
            runtime.unregister(&transfer_id);
            return Err(error);
        }
    };
    emit_transfer(&app, &queued);
    let worker_id = transfer_id.clone();
    let connection_id = input.connection_id;
    let policy = input.policy;
    tokio::spawn(async move {
        run_upload_worker(app, worker_id, connection_id, cancellation, local, policy).await;
    });

    Ok(StartTransferResult { transfer_id })
}

#[tauri::command]
pub async fn start_file_copy(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    input: StartRemoteTransferInput,
) -> Result<StartTransferResult, String> {
    start_remote_transfer_inner(app, state.inner(), runtime.inner(), input, "copy").await
}

#[tauri::command]
pub async fn start_file_rename(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    input: StartRemoteTransferInput,
) -> Result<StartTransferResult, String> {
    start_remote_transfer_inner(app, state.inner(), runtime.inner(), input, "rename").await
}

async fn start_remote_transfer_inner<R: Runtime>(
    app: AppHandle<R>,
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    input: StartRemoteTransferInput,
    operation: &'static str,
) -> Result<StartTransferResult, String> {
    let file_manager = app.state::<FileManagerRuntime>();
    runtime.ensure_recovered(state, file_manager.inner()).await?;
    input.policy.validate()?;
    let source_path = validate_remote_relative_path(&input.source_path)?;
    let destination_path = validate_remote_relative_path(&input.destination_path)?;
    if source_path == destination_path {
        return Err("Source and destination paths must be different".to_string());
    }
    let _admission = file_manager.begin_operation(&input.connection_id)?;
    let connection = state
        .storage
        .load_file_connection(&input.connection_id)
        .await?
        .ok_or_else(|| "File connection not found".to_string())?;
    if input.policy.replace()
        && matches!(parse_storage_config(&connection)?, FileConnectionConfig::Hdfs(HdfsConnectionConfig::Webhdfs(_)))
    {
        return Err(WEBHDFS_REPLACE_UNSUPPORTED.to_string());
    }
    let transfer_id = Uuid::new_v4().to_string();
    let cancellation = CancellationToken::new();
    runtime.register(transfer_id.clone(), input.connection_id.clone(), cancellation.clone());
    #[cfg(test)]
    wait_test_transfer_before_insert_barrier().await;
    let queued = match state
        .storage
        .create_file_remote_transfer(
            transfer_id.clone(),
            input.connection_id.clone(),
            operation.to_string(),
            source_path,
            destination_path,
            connection.revision,
        )
        .await
    {
        Ok(record) => record,
        Err(error) => {
            runtime.unregister(&transfer_id);
            return Err(error);
        }
    };
    emit_transfer(&app, &queued);
    let worker_id = transfer_id.clone();
    let connection_id = input.connection_id;
    let policy = input.policy;
    tokio::spawn(async move {
        run_remote_transfer_worker(app, worker_id, connection_id, cancellation, operation, policy).await;
    });
    Ok(StartTransferResult { transfer_id })
}

#[tauri::command]
pub async fn get_file_transfer(
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    file_manager: State<'_, FileManagerRuntime>,
    transfer_id: String,
) -> Result<FileTransferStorageRecord, String> {
    get_file_transfer_inner(state.inner(), runtime.inner(), file_manager.inner(), &transfer_id).await
}

async fn get_file_transfer_inner(
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    file_manager: &FileManagerRuntime,
    transfer_id: &str,
) -> Result<FileTransferStorageRecord, String> {
    runtime.ensure_recovered(state, file_manager).await?;
    state.storage.get_file_transfer(transfer_id).await?.ok_or_else(|| "File transfer not found".to_string())
}

#[tauri::command]
pub async fn list_file_transfers(
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    file_manager: State<'_, FileManagerRuntime>,
    connection_id: Option<String>,
) -> Result<Vec<FileTransferStorageRecord>, String> {
    list_file_transfers_inner(state.inner(), runtime.inner(), file_manager.inner(), connection_id.as_deref()).await
}

async fn list_file_transfers_inner(
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    file_manager: &FileManagerRuntime,
    connection_id: Option<&str>,
) -> Result<Vec<FileTransferStorageRecord>, String> {
    runtime.ensure_recovered(state, file_manager).await?;
    state.storage.list_file_transfers(connection_id, 100).await
}

#[tauri::command]
pub async fn cancel_file_transfer(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    file_manager: State<'_, FileManagerRuntime>,
    transfer_id: String,
) -> Result<FileTransferStorageRecord, String> {
    cancel_file_transfer_inner(&app, state.inner(), runtime.inner(), file_manager.inner(), &transfer_id).await
}

#[tauri::command]
pub async fn retry_file_rename_source_delete(
    app: AppHandle,
    state: State<'_, Arc<AppState>>,
    runtime: State<'_, FileTransferRuntime>,
    file_manager: State<'_, FileManagerRuntime>,
    transfer_id: String,
) -> Result<FileTransferStorageRecord, String> {
    retry_file_rename_source_delete_inner(&app, state.inner(), runtime.inner(), file_manager.inner(), &transfer_id)
        .await
}

async fn retry_file_rename_source_delete_inner<R: Runtime>(
    app: &AppHandle<R>,
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    file_manager: &FileManagerRuntime,
    transfer_id: &str,
) -> Result<FileTransferStorageRecord, String> {
    runtime.ensure_recovered(state, file_manager).await?;
    let record =
        state.storage.get_file_transfer(transfer_id).await?.ok_or_else(|| "File transfer not found".to_string())?;
    if record.direction != "rename"
        || record.status != "partial"
        || record.operation_outcome.as_deref() != Some("copied_source_delete_failed")
        || record.operation_phase.as_deref() != Some("delete_uncertain")
    {
        return Err("Rename is not eligible for source-delete recovery".to_string());
    }
    let cancellation = CancellationToken::new();
    let (_connection_permit, _global_permit) = acquire_transfer_permits(runtime, &record.connection_id, &cancellation)
        .await
        .map_err(|failure| failure.message)?;
    let _path_locks = runtime.lock_remote_paths(&record.connection_id, &record.remote_path, &record.local_path).await;
    let record =
        state.storage.get_file_transfer(transfer_id).await?.ok_or_else(|| "File transfer not found".to_string())?;
    if record.direction != "rename"
        || record.status != "partial"
        || record.operation_outcome.as_deref() != Some("copied_source_delete_failed")
        || record.operation_phase.as_deref() != Some("delete_uncertain")
    {
        return Err("Rename is not eligible for source-delete recovery".to_string());
    }
    let expected_revision =
        record.connection_revision.ok_or_else(|| "Rename recovery has no durable connection revision".to_string())?;
    let prepared = file_manager
        .prepare_file_mutation_operation(state, &record.connection_id, &record.remote_path, expected_revision)
        .await?;
    if prepared.uses_server_side_copy() {
        let destination = prepared.fingerprint_remote_file(&record.local_path).await?;
        if record.destination_fingerprint.as_deref() != Some(destination.encode().as_str()) {
            return Err("Destination fingerprint changed; source was not deleted".to_string());
        }
        let source = match prepared.fingerprint_remote_file(&record.remote_path).await {
            Ok(source) => source,
            Err(error) if error.contains("no longer exists") => {
                let completed = state.storage.complete_file_rename_retry(transfer_id).await?;
                emit_transfer(app, &completed);
                return Ok(completed);
            }
            Err(error) => return Err(error),
        };
        if record.source_fingerprint.as_deref() != Some(source.encode().as_str()) {
            return Err("Source fingerprint changed; source was not deleted".to_string());
        }
        prepared
            .delete_source_if_fingerprints_match(state, &record.remote_path, &record.local_path, &source, &destination)
            .await?;
        let completed = state.storage.complete_file_rename_retry(transfer_id).await?;
        emit_transfer(app, &completed);
        return Ok(completed);
    }
    let destination = match verify_remote_content(&prepared, &record.local_path, &cancellation).await {
        Ok(verified) => verified,
        Err(failure) => {
            if failure.invalidate_operator {
                file_manager.evict_revision(&record.connection_id, prepared.revision);
            }
            return Err(failure.message);
        }
    };
    let destination_matches = record
        .destination_fingerprint
        .as_deref()
        .is_some_and(|expected| persisted_verified_remote_content_matches(expected, &destination));
    if !destination_matches {
        return Err("Destination content or fingerprint changed; source was not deleted".to_string());
    }
    if persisted_relay_hash(record.source_fingerprint.as_deref())
        != persisted_relay_hash(record.destination_fingerprint.as_deref())
    {
        return Err("Persisted source and destination content hashes differ; source was not deleted".to_string());
    }
    match prepared.fingerprint_remote_file(&record.remote_path).await {
        Ok(_) => {}
        Err(error) if error.contains("no longer exists") => {
            let completed = state.storage.complete_file_rename_retry(transfer_id).await?;
            emit_transfer(app, &completed);
            return Ok(completed);
        }
        Err(error) => {
            file_manager.evict_revision(&record.connection_id, prepared.revision);
            return Err(error);
        }
    }
    let source = match verify_remote_content(&prepared, &record.remote_path, &cancellation).await {
        Ok(verified) => verified,
        Err(failure) => {
            if failure.invalidate_operator {
                file_manager.evict_revision(&record.connection_id, prepared.revision);
            }
            return Err(failure.message);
        }
    };
    let source_matches = record
        .source_fingerprint
        .as_deref()
        .is_some_and(|expected| persisted_verified_remote_content_matches(expected, &source));
    if !source_matches || source.sha256 != destination.sha256 {
        return Err("Source content or fingerprint changed; source was not deleted".to_string());
    }
    prepared
        .delete_source_if_fingerprints_match(
            state,
            &record.remote_path,
            &record.local_path,
            &source.fingerprint,
            &destination.fingerprint,
        )
        .await?;
    let completed = state.storage.complete_file_rename_retry(transfer_id).await?;
    emit_transfer(app, &completed);
    Ok(completed)
}

async fn cancel_file_transfer_inner<R: Runtime>(
    app: &AppHandle<R>,
    state: &Arc<AppState>,
    runtime: &FileTransferRuntime,
    file_manager: &FileManagerRuntime,
    transfer_id: &str,
) -> Result<FileTransferStorageRecord, String> {
    runtime.ensure_recovered(state, file_manager).await?;
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
            prepared = file_manager.prepare_file_operation_at_revision(
                &state,
                &connection_id,
                &remote_path,
                initial.connection_revision.ok_or_else(|| {
                    local_failure("Download has no durable file connection revision")
                })?
            ) => {
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

async fn run_upload_worker<R: Runtime>(
    app: AppHandle<R>,
    transfer_id: String,
    connection_id: String,
    cancellation: CancellationToken,
    local: ValidatedLocalSource,
    policy: UploadPolicy,
) {
    let state = app.state::<Arc<AppState>>().inner().clone();
    let runtime = app.state::<FileTransferRuntime>();
    let file_manager = app.state::<FileManagerRuntime>();
    let progress = Arc::new(TransferProgressSnapshot::new());
    progress.record_total(Some(local.total_bytes));

    let result = async {
        let initial = transfer_record_for_worker(&state, &transfer_id).await.map_err(UploadFailure::from)?;
        let (_connection_permit, _global_permit) =
            acquire_transfer_permits(&runtime, &connection_id, &cancellation).await.map_err(UploadFailure::from)?;
        let current = transfer_record_for_worker(&state, &transfer_id).await.map_err(UploadFailure::from)?;
        if current.remote_path != initial.remote_path || current.local_path != initial.local_path {
            return Err(UploadFailure::from(local_failure("File transfer paths changed while queued")));
        }
        let expected_revision = initial
            .connection_revision
            .ok_or_else(|| UploadFailure::from(local_failure("Queued upload has no durable connection revision")))?;
        if current.connection_revision != Some(expected_revision) {
            return Err(UploadFailure::from(local_failure("File transfer connection revision changed while queued")));
        }
        let partial_relative = upload_partial_path(&current.remote_path, &transfer_id);
        let running = state
            .storage
            .start_file_upload_transfer(
                &transfer_id,
                partial_relative.clone(),
                local.fingerprint.clone(),
                local.total_bytes,
                expected_revision,
            )
            .await
            .map_err(local_failure)
            .map_err(UploadFailure::from)?;
        emit_transfer(&app, &running);
        if running.status == "cancelling" || cancellation.is_cancelled() {
            return Err(UploadFailure::from(upload_cancelled_failure()));
        }
        let prepared = tokio::select! {
            _ = cancellation.cancelled() => return Err(UploadFailure::from(upload_cancelled_failure())),
            prepared = file_manager.prepare_file_mutation_operation(
                &state,
                &connection_id,
                &current.remote_path,
                expected_revision,
            ) => {
                prepared.map_err(remote_failure).map_err(UploadFailure::from)?
            }
        };
        let partial_configured = sibling_remote_path(&prepared.remote_path, &partial_relative);
        let result = execute_upload(
            UploadExecutionContext {
                app: &app,
                state: &state,
                runtime: &runtime,
                transfer_id: &transfer_id,
                prepared: &prepared,
                target_relative: &current.remote_path,
                partial_relative: &partial_relative,
                partial_configured: &partial_configured,
                cancellation: &cancellation,
                progress_snapshot: progress.clone(),
                policy,
            },
            local,
        )
        .await;
        result.inspect_err(|failure| {
            if failure.failure.invalidate_operator {
                file_manager.evict_revision(&connection_id, prepared.revision);
            }
        })
    }
    .await;

    finalize_upload_result(&app, &state, &transfer_id, result, &progress).await;
    runtime.unregister(&transfer_id);
}

async fn run_remote_transfer_worker<R: Runtime>(
    app: AppHandle<R>,
    transfer_id: String,
    connection_id: String,
    cancellation: CancellationToken,
    operation: &'static str,
    policy: RemoteMutationPolicy,
) {
    let state = app.state::<Arc<AppState>>().inner().clone();
    let runtime = app.state::<FileTransferRuntime>();
    let file_manager = app.state::<FileManagerRuntime>();
    let progress = Arc::new(TransferProgressSnapshot::new());
    let result = async {
        let initial = transfer_record_for_worker(&state, &transfer_id).await.map_err(remote_transfer_before_copy)?;
        let source_path = initial.remote_path.clone();
        let destination_path = initial.local_path.clone();
        let (_connection_permit, _global_permit) = acquire_transfer_permits(&runtime, &connection_id, &cancellation)
            .await
            .map_err(remote_transfer_before_copy)?;
        let _path_locks = tokio::select! {
            _ = cancellation.cancelled() => return Err(remote_transfer_before_copy(cancelled_failure())),
            locks = runtime.lock_remote_paths(&connection_id, &source_path, &destination_path) => locks,
        };
        let current = transfer_record_for_worker(&state, &transfer_id).await.map_err(remote_transfer_before_copy)?;
        if current.remote_path != source_path
            || current.local_path != destination_path
            || current.direction != operation
        {
            return Err(remote_transfer_before_copy(local_failure(
                "Remote transfer paths or operation changed while queued",
            )));
        }
        let expected_revision = current.connection_revision.ok_or_else(|| {
            remote_transfer_before_copy(local_failure("Queued remote transfer has no connection revision"))
        })?;
        let prepared = tokio::select! {
            _ = cancellation.cancelled() => return Err(remote_transfer_before_copy(cancelled_failure())),
            prepared = file_manager.prepare_file_mutation_operation(
                &state,
                &connection_id,
                &source_path,
                expected_revision,
            ) => prepared.map_err(remote_failure).map_err(remote_transfer_before_copy)?,
        };
        let result = execute_remote_transfer(
            &app,
            &state,
            &runtime,
            &transfer_id,
            operation,
            policy,
            &source_path,
            &destination_path,
            &prepared,
            &cancellation,
            progress.clone(),
        )
        .await;
        result.inspect_err(|failure| {
            if failure.failure.invalidate_operator {
                file_manager.evict_revision(&connection_id, prepared.revision);
            }
        })
    }
    .await;
    finalize_remote_transfer_result(&app, &state, &transfer_id, result, &progress).await;
    runtime.unregister(&transfer_id);
}

async fn verify_remote_content(
    prepared: &PreparedFileMutation<'_>,
    relative_path: &str,
    cancellation: &CancellationToken,
) -> Result<VerifiedRemoteContent, TransferFailure> {
    let before = prepared.fingerprint_remote_file(relative_path).await.map_err(remote_failure)?;
    let relative_path = validate_remote_relative_path(relative_path).map_err(local_failure)?;
    let (bytes_read, sha256) = if prepared.uses_exact_ftp_relay() {
        let mut ftp = tokio::time::timeout(IO_PROGRESS_WATCHDOG, prepared.open_exact_ftp_read_session())
            .await
            .map_err(|_| remote_failure("Opening the remote verification session timed out"))?
            .map_err(remote_failure)?;
        let mut reader = tokio::time::timeout(IO_PROGRESS_WATCHDOG, ftp.retr_as_stream(&relative_path))
            .await
            .map_err(|_| remote_failure("Opening the remote verification reader timed out"))?
            .map_err(|error| remote_failure(prepared.redact_exact_ftp_error(error)))?;
        let result = hash_remote_reader(prepared, &mut reader, cancellation).await?;
        let finalize = tokio::time::timeout(IO_PROGRESS_WATCHDOG, ftp.finalize_retr_stream(reader))
            .await
            .map_err(|_| remote_failure("Finalizing the remote verification reader timed out"))?
            .map_err(|error| remote_failure(prepared.redact_exact_ftp_error(error)));
        let _ = ftp.quit().await;
        finalize?;
        result
    } else {
        let configured = prepared.configured_path(&relative_path).map_err(local_failure)?;
        let mut reader_builder = prepared.operator.reader_with(&configured).concurrent(1);
        if !prepared.uses_streaming_webhdfs_upload() {
            reader_builder = reader_builder.chunk(REMOTE_COPY_BUFFER_SIZE);
        }
        let reader = tokio::time::timeout(IO_PROGRESS_WATCHDOG, reader_builder)
            .await
            .map_err(|_| remote_failure("Opening the remote verification reader timed out"))?
            .map_err(|error| remote_failure(prepared.redact_operator_error(error)))?;
        let mut reader = reader
            .into_futures_async_read(..)
            .await
            .map_err(|error| remote_failure(prepared.redact_operator_error(error)))?;
        hash_futures_remote_reader(prepared, &mut reader, cancellation).await?
    };
    let after = prepared.fingerprint_remote_file(&relative_path).await.map_err(remote_failure)?;
    if before != after || bytes_read != before.size {
        return Err(remote_failure(
            "Remote file changed while its content hash was being verified; source deletion was not attempted",
        ));
    }
    Ok(VerifiedRemoteContent { fingerprint: after, sha256 })
}

async fn hash_remote_reader<R>(
    prepared: &PreparedFileMutation<'_>,
    reader: &mut R,
    cancellation: &CancellationToken,
) -> Result<(u64, String), TransferFailure>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; REMOTE_COPY_BUFFER_SIZE];
    let mut bytes_read = 0_u64;
    let mut hasher = Sha256::new();
    loop {
        let count = tokio::select! {
            _ = cancellation.cancelled() => return Err(cancelled_active_failure()),
            _ = prepared.cancellation.cancelled() => {
                return Err(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed during remote content verification".to_string(),
                    invalidate_operator: true,
                })
            },
            result = tokio::time::timeout(IO_PROGRESS_WATCHDOG, reader.read(&mut buffer)) => {
                result
                    .map_err(|_| remote_failure("Remote content verification made no progress before the I/O watchdog expired"))?
                    .map_err(|error| remote_failure(prepared.redact_remote_error(error.to_string())))?
            }
        };
        if count == 0 {
            break;
        }
        bytes_read = bytes_read.saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        hasher.update(&buffer[..count]);
    }
    Ok((bytes_read, format!("{:x}", hasher.finalize())))
}

async fn hash_futures_remote_reader<R>(
    prepared: &PreparedFileMutation<'_>,
    reader: &mut R,
    cancellation: &CancellationToken,
) -> Result<(u64, String), TransferFailure>
where
    R: FuturesAsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; REMOTE_COPY_BUFFER_SIZE];
    let mut bytes_read = 0_u64;
    let mut hasher = Sha256::new();
    loop {
        let count = tokio::select! {
            _ = cancellation.cancelled() => return Err(cancelled_active_failure()),
            _ = prepared.cancellation.cancelled() => {
                return Err(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed during remote content verification".to_string(),
                    invalidate_operator: true,
                })
            },
            result = tokio::time::timeout(
                IO_PROGRESS_WATCHDOG,
                FuturesAsyncReadExt::read(reader, &mut buffer),
            ) => {
                result
                    .map_err(|_| remote_failure("Remote content verification made no progress before the I/O watchdog expired"))?
                    .map_err(|error| remote_failure(prepared.redact_remote_error(error.to_string())))?
            }
        };
        if count == 0 {
            break;
        }
        bytes_read = bytes_read.saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        hasher.update(&buffer[..count]);
    }
    Ok((bytes_read, format!("{:x}", hasher.finalize())))
}

enum StreamingDestinationWriter {
    OpenDal(opendal::Writer),
    Webhdfs(super::file_manager_webhdfs::WebhdfsStreamingWriter),
}

impl StreamingDestinationWriter {
    async fn write(&mut self, bytes: Bytes, prepared: &PreparedFileMutation<'_>) -> Result<(), String> {
        match self {
            Self::OpenDal(writer) => writer.write(bytes).await.map_err(|error| prepared.redact_operator_error(error)),
            Self::Webhdfs(writer) => writer.write(bytes).await.map_err(|error| prepared.redact_remote_error(error)),
        }
    }

    async fn close(&mut self, prepared: &PreparedFileMutation<'_>) -> Result<(), String> {
        match self {
            Self::OpenDal(writer) => {
                writer.close().await.map(|_| ()).map_err(|error| prepared.redact_operator_error(error))
            }
            Self::Webhdfs(writer) => writer.close().await.map_err(|error| prepared.redact_remote_error(error)),
        }
    }
}

impl AbortableUpload for StreamingDestinationWriter {
    fn abort(&mut self) -> Pin<Box<dyn Future<Output = Result<(), UploadAbortError>> + Send + '_>> {
        Box::pin(async move {
            match self {
                Self::OpenDal(writer) => opendal::Writer::abort(writer).await.map_err(|error| {
                    if error.kind() == opendal::ErrorKind::Unsupported {
                        UploadAbortError::Unsupported
                    } else {
                        UploadAbortError::Failed(error.to_string())
                    }
                }),
                Self::Webhdfs(writer) => {
                    writer.abort_and_wait().await.map_err(UploadAbortError::Failed)?;
                    Err(UploadAbortError::Unsupported)
                }
            }
        })
    }
}

async fn open_remote_copy_writer(
    prepared: &PreparedFileMutation<'_>,
    partial_relative: &str,
    partial_configured: &str,
    expected_size: u64,
) -> Result<StreamingDestinationWriter, TransferFailure> {
    let idle_timeout = prepared.transfer_idle_timeout(IO_PROGRESS_WATCHDOG);
    if prepared.uses_streaming_webhdfs_upload() {
        return tokio::time::timeout(
            idle_timeout,
            prepared.open_webhdfs_streaming_writer(partial_relative, expected_size, Arc::new(AtomicBool::new(false))),
        )
        .await
        .map_err(|_| remote_failure("Opening the WebHDFS remote copy destination timed out"))?
        .map(StreamingDestinationWriter::Webhdfs)
        .map_err(remote_failure);
    }
    let writer = tokio::time::timeout(
        idle_timeout,
        prepared
            .operator
            .writer_with(partial_configured)
            .append(prepared.requires_append_streaming_write())
            .chunk(REMOTE_COPY_BUFFER_SIZE)
            .concurrent(1),
    )
    .await
    .map_err(|_| remote_failure("Opening the remote copy destination timed out"))?
    .map_err(|error| remote_failure(prepared.redact_operator_error(error)))?;
    #[cfg(test)]
    if TEST_REMOTE_COPY_WRITER_OPEN_SIDE_EFFECT_FAILURE.swap(false, Ordering::SeqCst) {
        let mut writer = writer;
        writer
            .write(Bytes::from_static(b"injected writer-open side effect"))
            .await
            .map_err(|error| remote_failure(prepared.redact_operator_error(error)))?;
        writer.close().await.map_err(|error| remote_failure(prepared.redact_operator_error(error)))?;
        return Err(remote_failure(
            "Injected remote copy writer-open failure after creating its operation-owned partial",
        ));
    }
    Ok(StreamingDestinationWriter::OpenDal(writer))
}

async fn open_upload_writer(
    prepared: &PreparedFileMutation<'_>,
    partial_relative: &str,
    partial_configured: &str,
    expected_size: u64,
    chunk_size: usize,
) -> Result<StreamingDestinationWriter, String> {
    if prepared.uses_streaming_webhdfs_upload() {
        return prepared
            .open_webhdfs_streaming_writer(partial_relative, expected_size, Arc::new(AtomicBool::new(false)))
            .await
            .map(StreamingDestinationWriter::Webhdfs);
    }
    prepared
        .operator
        .writer_with(partial_configured)
        .append(prepared.requires_append_streaming_write())
        .chunk(chunk_size)
        .concurrent(1)
        .await
        .map(StreamingDestinationWriter::OpenDal)
        .map_err(|error| prepared.redact_operator_error(error))
}

fn injected_remote_copy_persistence_error() -> Option<String> {
    #[cfg(test)]
    if TEST_REMOTE_COPY_PERSISTENCE_FAILURE_AFTER_VERIFY.swap(false, Ordering::SeqCst) {
        return Some("Injected persistence failure after the remote copy partial was verified".to_string());
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn execute_remote_transfer<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    runtime: &FileTransferRuntime,
    transfer_id: &str,
    operation: &'static str,
    policy: RemoteMutationPolicy,
    source_path: &str,
    destination_path: &str,
    prepared: &PreparedFileMutation<'_>,
    cancellation: &CancellationToken,
    progress: Arc<TransferProgressSnapshot>,
) -> Result<RemoteTransferOutcome, RemoteTransferFailure> {
    if policy.replace() && prepared.uses_streaming_webhdfs_upload() {
        return Err(remote_transfer_before_copy(remote_failure(WEBHDFS_REPLACE_UNSUPPORTED)));
    }
    if prepared.uses_server_side_copy()
        || (operation == "rename"
            && (prepared.uses_native_sftp_rename()
                || prepared.uses_direct_hdfs_native_rename()
                || prepared.uses_direct_webhdfs_rename()))
    {
        return execute_server_side_remote_transfer(
            app,
            state,
            transfer_id,
            operation,
            policy,
            source_path,
            destination_path,
            prepared,
            cancellation,
            progress,
        )
        .await;
    }
    let source_before = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(remote_transfer_before_copy(TransferFailure {
                status: "cancelled",
                message: "Remote transfer was cancelled during source preflight".to_string(),
                invalidate_operator: false,
            }))
        }
        _ = prepared.cancellation.cancelled() => {
            return Err(remote_transfer_before_copy(TransferFailure {
                status: "cancelled",
                message: "The file connection was removed during remote source preflight".to_string(),
                invalidate_operator: true,
            }))
        }
        result = prepared.stat_remote_file(source_path) => {
            result.map_err(remote_failure).map_err(remote_transfer_before_copy)?
        }
    };
    let total_bytes = i64::try_from(source_before.size)
        .map_err(|_| local_failure("Remote source size is not representable"))
        .map_err(remote_transfer_before_copy)?;
    progress.record_total(Some(total_bytes));
    if prepared
        .remote_entry_exists(destination_path)
        .await
        .map_err(remote_failure)
        .map_err(remote_transfer_before_copy)?
    {
        if !policy.replace() {
            return Err(remote_transfer_before_copy(remote_failure(
                "Remote destination already exists; best_effort_no_clobber does not replace it",
            )));
        }
        prepared
            .stat_remote_file(destination_path)
            .await
            .map_err(remote_failure)
            .map_err(remote_transfer_before_copy)?;
    }
    let partial_relative = remote_copy_partial_path(destination_path, transfer_id);
    if prepared
        .remote_entry_exists(&partial_relative)
        .await
        .map_err(remote_failure)
        .map_err(remote_transfer_before_copy)?
    {
        return Err(remote_transfer_before_copy(remote_failure(
            "Operation-owned remote copy partial unexpectedly already exists",
        )));
    }
    let source_configured =
        prepared.configured_path(source_path).map_err(local_failure).map_err(remote_transfer_before_copy)?;
    let partial_configured =
        prepared.configured_path(&partial_relative).map_err(local_failure).map_err(remote_transfer_before_copy)?;
    let copying = state
        .storage
        .update_file_remote_transfer_phase(
            transfer_id,
            "running".to_string(),
            "copying".to_string(),
            0,
            Some(total_bytes),
            Some(partial_relative.clone()),
            Some(source_before.encode()),
            None,
        )
        .await
        .map_err(local_failure)
        .map_err(remote_transfer_before_copy)?;
    emit_transfer(app, &copying);
    if copying.status == "cancelling" || cancellation.is_cancelled() {
        return Err(remote_transfer_before_copy(cancelled_failure()));
    }

    let relay_buffer_size = prepared.transfer_chunk_size(REMOTE_COPY_BUFFER_SIZE);
    let idle_timeout = prepared.transfer_idle_timeout(IO_PROGRESS_WATCHDOG);
    let mut reader_builder = prepared.operator.reader_with(&source_configured).concurrent(1);
    if !prepared.uses_streaming_webhdfs_upload() {
        reader_builder = reader_builder.chunk(relay_buffer_size);
    }
    let reader = tokio::time::timeout(idle_timeout, reader_builder)
        .await
        .map_err(|_| remote_failure("Opening the remote copy source timed out"))
        .and_then(|result| result.map_err(|error| remote_failure(prepared.redact_operator_error(error))))
        .map_err(remote_transfer_before_copy)?;
    let mut reader = reader
        .into_futures_async_read(..)
        .await
        .map_err(|error| remote_transfer_before_copy(remote_failure(prepared.redact_operator_error(error))))?;
    let mut writer =
        match open_remote_copy_writer(prepared, &partial_relative, &partial_configured, source_before.size).await {
            Ok(writer) => writer,
            Err(failure) => {
                return Err(cleanup_remote_copy_partial(
                    prepared,
                    &partial_relative,
                    None,
                    failure,
                    Some(source_before.encode()),
                )
                .await)
            }
        };
    let mut buffer = vec![0_u8; relay_buffer_size];
    let mut bytes_transferred = 0_i64;
    let mut hasher = Sha256::new();
    let mut last_progress = Instant::now();
    let body = async {
        loop {
            let count = tokio::select! {
                _ = cancellation.cancelled() => return Err(cancelled_active_failure()),
                _ = prepared.cancellation.cancelled() => {
                    return Err(TransferFailure {
                        status: "cancelled",
                        message: "The file connection was removed while the remote copy was running".to_string(),
                        invalidate_operator: true,
                    })
                },
                result = tokio::time::timeout(idle_timeout, reader.read(&mut buffer)) => {
                    result
                        .map_err(|_| remote_failure("Remote copy read made no progress before the I/O watchdog expired"))?
                        .map_err(|error| remote_failure(prepared.redact_remote_error(error.to_string())))?
                }
            };
            if count == 0 {
                break;
            }
            record_test_remote_copy_read(count);
            hasher.update(&buffer[..count]);
            record_test_remote_copy_write(count);
            tokio::select! {
                _ = cancellation.cancelled() => return Err(cancelled_active_failure()),
                _ = prepared.cancellation.cancelled() => {
                    return Err(TransferFailure {
                        status: "cancelled",
                        message: "The file connection was removed while the remote copy was running".to_string(),
                        invalidate_operator: true,
                    })
                },
                result = tokio::time::timeout(
                    idle_timeout,
                    writer.write(Bytes::copy_from_slice(&buffer[..count]), prepared),
                ) => {
                    result
                        .map_err(|_| remote_failure("Remote copy write made no progress before the I/O watchdog expired"))?
                        .map_err(remote_failure)?;
                }
            }
            bytes_transferred =
                bytes_transferred.saturating_add(i64::try_from(count).unwrap_or(i64::MAX));
            progress.record_bytes(bytes_transferred);
            wait_at_test_remote_copy_after_chunk_barrier().await;
            if last_progress.elapsed() >= PROGRESS_INTERVAL {
                let update = state
                    .storage
                    .update_file_remote_transfer_phase(
                        transfer_id,
                        "running".to_string(),
                        "copying".to_string(),
                        bytes_transferred,
                        Some(total_bytes),
                        Some(partial_relative.clone()),
                        Some(source_before.encode()),
                        None,
                    )
                    .await
                    .map_err(local_failure)?;
                if update.status == "running" && runtime.should_emit_progress() {
                    emit_transfer(app, &update);
                }
                last_progress = Instant::now();
            }
        }
        if bytes_transferred != total_bytes {
            return Err(remote_failure(format!(
                "Remote source size changed during copy: expected {total_bytes}, copied {bytes_transferred}"
            )));
        }
        tokio::time::timeout(idle_timeout, writer.close(prepared))
            .await
            .map_err(|_| remote_failure("Closing the remote copy destination timed out"))?
            .map_err(remote_failure)?;
        Ok(())
    }
    .await;
    if let Err(mut failure) = body {
        match tokio::time::timeout(idle_timeout, writer.abort()).await {
            Ok(Ok(())) | Ok(Err(UploadAbortError::Unsupported)) => {}
            Ok(Err(UploadAbortError::Failed(error))) => {
                failure.message.push_str(&format!(
                    "; remote copy writer abort failed: {}",
                    prepared.redact_remote_error(sanitize_error(&error))
                ));
                failure.invalidate_operator = true;
            }
            Err(_) => {
                failure.message.push_str("; remote copy writer abort timed out");
                failure.invalidate_operator = true;
                return Err(remote_partial_failure(
                    failure.message,
                    partial_relative.clone(),
                    Some(source_before.encode()),
                    None,
                ));
            }
        }
        drop(writer);
        return Err(cleanup_remote_copy_partial(prepared, &partial_relative, None, failure, None).await);
    }
    wait_at_test_remote_copy_after_close_barrier().await;

    let source_after = match prepared.fingerprint_remote_file(source_path).await {
        Ok(fingerprint) => fingerprint,
        Err(error) => {
            return Err(
                cleanup_remote_copy_partial(prepared, &partial_relative, None, remote_failure(error), None).await
            )
        }
    };
    let relay_hash = format!("{:x}", hasher.finalize());
    let partial_verified = match verify_remote_content(prepared, &partial_relative, cancellation).await {
        Ok(verified) => verified,
        Err(failure) => {
            return Err(cleanup_remote_copy_partial(
                prepared,
                &partial_relative,
                None,
                failure,
                Some(source_after.encode()),
            )
            .await)
        }
    };
    let partial_fingerprint = &partial_verified.fingerprint;
    if source_after != source_before {
        return Err(cleanup_remote_copy_partial(
            prepared,
            &partial_relative,
            Some(partial_fingerprint),
            remote_failure("Remote source changed while it was being copied; the partial was not published"),
            Some(source_before.encode()),
        )
        .await);
    }
    if partial_fingerprint.size != source_before.size || partial_verified.sha256 != relay_hash {
        return Err(remote_partial_failure(
            format!(
                "Operation-owned partial content mismatch: expected {} bytes and relay SHA-256 {}, actual {} bytes and SHA-256 {}; partial was preserved",
                source_before.size, relay_hash, partial_fingerprint.size, partial_verified.sha256
            ),
            partial_relative,
            Some(source_before.encode()),
            Some(partial_verified.durable_fingerprint()),
        ));
    }
    let source_durable = format!("{};relay_sha256:{relay_hash}", source_after.encode());
    let partial_durable = partial_verified.durable_fingerprint();
    let publishing_result = if let Some(error) = injected_remote_copy_persistence_error() {
        Err(error)
    } else {
        state
            .storage
            .update_file_remote_transfer_phase(
                transfer_id,
                "publishing".to_string(),
                "copying".to_string(),
                bytes_transferred,
                Some(total_bytes),
                Some(partial_relative.clone()),
                Some(source_durable.clone()),
                Some(partial_durable),
            )
            .await
    };
    let publishing = match publishing_result {
        Ok(publishing) => publishing,
        Err(error) => {
            return Err(cleanup_remote_copy_partial(
                prepared,
                &partial_relative,
                Some(partial_fingerprint),
                local_failure(error),
                Some(source_durable),
            )
            .await)
        }
    };
    emit_transfer(app, &publishing);
    if publishing.status == "cancelling" || cancellation.is_cancelled() {
        return Err(cleanup_remote_copy_partial(
            prepared,
            &partial_relative,
            Some(partial_fingerprint),
            cancelled_active_failure(),
            Some(source_durable),
        )
        .await);
    }
    match prepared
        .publish_owned_remote_partial(
            state,
            &partial_relative,
            destination_path,
            total_bytes,
            policy.replace(),
            cancellation,
        )
        .await
    {
        Ok(UploadPublishResolution { state: UploadPublishState::Completed, .. }) => {}
        Ok(UploadPublishResolution { state: UploadPublishState::PartialSource, detail }) => {
            return Err(cleanup_remote_copy_partial(
                prepared,
                &partial_relative,
                Some(partial_fingerprint),
                remote_failure(detail),
                Some(source_durable),
            )
            .await)
        }
        Ok(UploadPublishResolution { state: UploadPublishState::PartialTarget, detail }) => {
            return Err(remote_partial_failure(detail, destination_path.to_string(), Some(source_durable), None))
        }
        Ok(UploadPublishResolution { state: UploadPublishState::Unknown, detail }) => {
            return Err(RemoteTransferFailure {
                failure: partial_failure(detail),
                operation_outcome: "failed_with_partial_destination",
                operation_phase: "copying",
                partial_destination: None,
                source_fingerprint: Some(source_durable),
                destination_fingerprint: None,
            })
        }
        Err(error) => {
            return Err(cleanup_remote_copy_partial(
                prepared,
                &partial_relative,
                Some(partial_fingerprint),
                remote_failure(error),
                Some(source_durable),
            )
            .await)
        }
    }
    let destination_verified =
        verify_remote_content(prepared, destination_path, cancellation).await.map_err(|failure| {
            RemoteTransferFailure {
                failure: TransferFailure { status: "partial", ..failure },
                operation_outcome: "failed_with_partial_destination",
                operation_phase: "copying",
                partial_destination: Some(destination_path.to_string()),
                source_fingerprint: Some(source_durable.clone()),
                destination_fingerprint: None,
            }
        })?;
    let destination_durable = destination_verified.durable_fingerprint();
    if destination_verified.fingerprint.size != source_before.size || destination_verified.sha256 != relay_hash {
        return Err(remote_partial_failure(
            "Published destination content does not match the relayed source; source deletion was not attempted"
                .to_string(),
            destination_path.to_string(),
            Some(source_durable),
            Some(destination_durable),
        ));
    }
    if operation == "copy" {
        return Ok(RemoteTransferOutcome {
            bytes_transferred,
            total_bytes,
            operation_outcome: "completed",
            operation_phase: "completed",
            source_fingerprint: source_durable,
            destination_fingerprint: destination_durable,
        });
    }

    let published = state
        .storage
        .update_file_remote_transfer_phase(
            transfer_id,
            "publishing".to_string(),
            "published_before_delete".to_string(),
            bytes_transferred,
            Some(total_bytes),
            None,
            Some(source_durable.clone()),
            Some(destination_durable.clone()),
        )
        .await
        .map_err(local_failure)
        .map_err(|failure| RemoteTransferFailure {
            failure,
            operation_outcome: "copied_source_delete_failed",
            operation_phase: "published_before_delete",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_durable.clone()),
            destination_fingerprint: Some(destination_durable.clone()),
        })?;
    emit_transfer(app, &published);
    wait_at_test_remote_rename_after_publish_barrier().await;
    let source_verified =
        verify_remote_content(prepared, source_path, cancellation).await.map_err(|failure| RemoteTransferFailure {
            failure: TransferFailure { status: "partial", ..failure },
            operation_outcome: "copied_source_delete_failed",
            operation_phase: "published_before_delete",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_durable.clone()),
            destination_fingerprint: Some(destination_durable.clone()),
        })?;
    let destination_reverified =
        verify_remote_content(prepared, destination_path, cancellation).await.map_err(|failure| {
            RemoteTransferFailure {
                failure: TransferFailure { status: "partial", ..failure },
                operation_outcome: "copied_source_delete_failed",
                operation_phase: "published_before_delete",
                partial_destination: Some(destination_path.to_string()),
                source_fingerprint: Some(source_verified.durable_fingerprint()),
                destination_fingerprint: Some(destination_durable.clone()),
            }
        })?;
    let source_verified_durable = source_verified.durable_fingerprint();
    let destination_verified_durable = destination_reverified.durable_fingerprint();
    if source_verified.fingerprint != source_after
        || destination_reverified.fingerprint != destination_verified.fingerprint
        || source_verified.sha256 != relay_hash
        || destination_reverified.sha256 != relay_hash
        || source_verified.sha256 != destination_reverified.sha256
    {
        return Err(RemoteTransferFailure {
            failure: partial_failure(
                "Current source or destination content no longer matches the relayed copy; source deletion was not attempted",
            ),
            operation_outcome: "copied_source_delete_failed",
            operation_phase: "published_before_delete",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_verified_durable),
            destination_fingerprint: Some(destination_verified_durable),
        });
    }
    state
        .storage
        .update_file_remote_transfer_phase(
            transfer_id,
            "publishing".to_string(),
            "delete_uncertain".to_string(),
            bytes_transferred,
            Some(total_bytes),
            None,
            Some(source_verified_durable.clone()),
            Some(destination_verified_durable.clone()),
        )
        .await
        .map_err(local_failure)
        .map_err(|failure| RemoteTransferFailure {
            failure,
            operation_outcome: "copied_source_delete_failed",
            operation_phase: "published_before_delete",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_verified_durable.clone()),
            destination_fingerprint: Some(destination_verified_durable.clone()),
        })?;
    if let Err(error) = prepared
        .delete_source_if_fingerprints_match(
            state,
            source_path,
            destination_path,
            &source_verified.fingerprint,
            &destination_reverified.fingerprint,
        )
        .await
    {
        return Err(RemoteTransferFailure {
            failure: partial_failure(error),
            operation_outcome: "copied_source_delete_failed",
            operation_phase: "delete_uncertain",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_verified_durable),
            destination_fingerprint: Some(destination_verified_durable),
        });
    }
    Ok(RemoteTransferOutcome {
        bytes_transferred,
        total_bytes,
        operation_outcome: "completed",
        operation_phase: "completed",
        source_fingerprint: source_verified_durable,
        destination_fingerprint: destination_verified_durable,
    })
}

async fn reconcile_uncertain_server_side_copy(
    prepared: &PreparedFileMutation<'_>,
    source: &RemoteFileFingerprint,
    destination_path: &str,
    detail: String,
    cancelled: bool,
) -> RemoteTransferFailure {
    match prepared.fingerprint_remote_file(destination_path).await {
        Err(error) if error.contains("no longer exists") => remote_transfer_before_copy(TransferFailure {
            status: if cancelled { "cancelled" } else { "failed" },
            message: format!("{detail}; destination is absent after reconciliation"),
            invalidate_operator: false,
        }),
        Ok(destination)
            if destination.size == source.size
                && source.etag.is_some()
                && destination.etag == source.etag =>
        {
            RemoteTransferFailure {
                failure: partial_failure(format!(
                    "{detail}; destination is committed and matches the durable source fingerprint, but the copy response was lost"
                )),
                operation_outcome: "copy_committed_response_unknown",
                operation_phase: "copying",
                partial_destination: Some(destination_path.to_string()),
                source_fingerprint: Some(source.encode()),
                destination_fingerprint: Some(destination.encode()),
            }
        }
        Ok(destination) => RemoteTransferFailure {
            failure: partial_failure(format!(
                "{detail}; destination exists but could not be proven to match the source"
            )),
            operation_outcome: "destination_present_unproven",
            operation_phase: "copying",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source.encode()),
            destination_fingerprint: Some(destination.encode()),
        },
        Err(error) => RemoteTransferFailure {
            failure: partial_failure(format!("{detail}; destination reconciliation failed: {error}")),
            operation_outcome: "destination_state_unknown",
            operation_phase: "copying",
            partial_destination: None,
            source_fingerprint: Some(source.encode()),
            destination_fingerprint: None,
        },
    }
}

async fn reconcile_uncertain_native_move(
    prepared: &PreparedFileMutation<'_>,
    source_before: &RemoteFileFingerprint,
    source_path: &str,
    destination_path: &str,
    detail: String,
    cancelled: bool,
) -> RemoteTransferFailure {
    let (source, destination) = if prepared.uses_direct_hdfs_native_rename() || prepared.uses_direct_webhdfs_rename() {
        // The direct RPC future has been cancelled or returned an uncertain
        // transport result. Remove its cache entry before any observation so
        // no later operation can reuse that client, then reconcile through a
        // separately constructed and bounded OpenDAL client.
        prepared.evict_uncertain_direct_rename();
        match prepared.observe_uncertain_direct_rename_fresh(source_path, destination_path, IO_PROGRESS_WATCHDOG).await
        {
            Ok(observation) => observation,
            Err(error) => (Err(error.clone()), Err(error)),
        }
    } else {
        (prepared.fingerprint_remote_file(source_path).await, prepared.fingerprint_remote_file(destination_path).await)
    };
    match (source, destination) {
        (Err(source_error), Ok(destination))
            if source_error.contains("no longer exists")
                && prepared.native_rename_destination_matches(source_before, &destination) =>
        {
            RemoteTransferFailure {
                failure: partial_failure(format!(
                    "{detail}; source is absent and destination is present, but the MOVE response was not observed"
                )),
                operation_outcome: "move_committed_response_unknown",
                operation_phase: "copying",
                partial_destination: Some(destination_path.to_string()),
                source_fingerprint: Some(source_before.encode()),
                destination_fingerprint: Some(destination.encode()),
            }
        }
        (Ok(source), Err(destination_error)) if destination_error.contains("no longer exists") => {
            RemoteTransferFailure {
                failure: partial_failure(format!(
                    "{detail}; source remains and destination is currently absent, but a dispatched MOVE may still commit{}",
                    if cancelled { " after cancellation" } else { "" }
                )),
                operation_outcome: "destination_state_unknown",
                operation_phase: "copying",
                partial_destination: None,
                source_fingerprint: Some(source.encode()),
                destination_fingerprint: None,
            }
        }
        (Ok(source), Ok(destination)) => RemoteTransferFailure {
            failure: partial_failure(format!(
                "{detail}; both source and destination are present, so the server outcome is unproven"
            )),
            operation_outcome: "destination_present_unproven",
            operation_phase: "copying",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source.encode()),
            destination_fingerprint: Some(destination.encode()),
        },
        (source, destination) => RemoteTransferFailure {
            failure: partial_failure(format!(
                "{detail}; native rename reconciliation was inconclusive (source={}, destination={})",
                source.as_ref().map(|_| "present").unwrap_or("unknown"),
                destination.as_ref().map(|_| "present").unwrap_or("unknown")
            )),
            operation_outcome: "destination_state_unknown",
            operation_phase: "copying",
            partial_destination: None,
            source_fingerprint: Some(source_before.encode()),
            destination_fingerprint: None,
        },
    }
}

fn invalidate_hdfs_native_after_uncertain_move(
    prepared: &PreparedFileMutation<'_>,
    mut failure: RemoteTransferFailure,
) -> RemoteTransferFailure {
    if prepared.uses_direct_hdfs_native_rename() || prepared.uses_direct_webhdfs_rename() {
        failure.failure.invalidate_operator = true;
    }
    failure
}

async fn reconcile_uncertain_webdav_copy(
    prepared: &PreparedFileMutation<'_>,
    source: &RemoteFileFingerprint,
    destination_path: &str,
    detail: String,
) -> RemoteTransferFailure {
    match prepared.fingerprint_remote_file(destination_path).await {
        Ok(destination)
            if destination.size == source.size && source.etag.is_some() && destination.etag == source.etag =>
        {
            RemoteTransferFailure {
                failure: partial_failure(format!(
                    "{detail}; destination matches the durable source fingerprint, but the COPY response was lost"
                )),
                operation_outcome: "copy_committed_response_unknown",
                operation_phase: "copying",
                partial_destination: Some(destination_path.to_string()),
                source_fingerprint: Some(source.encode()),
                destination_fingerprint: Some(destination.encode()),
            }
        }
        Ok(destination) => RemoteTransferFailure {
            failure: partial_failure(format!(
                "{detail}; destination exists but could not be proven to match the source"
            )),
            operation_outcome: "destination_present_unproven",
            operation_phase: "copying",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source.encode()),
            destination_fingerprint: Some(destination.encode()),
        },
        Err(error) if error.contains("no longer exists") => RemoteTransferFailure {
            failure: partial_failure(format!(
                "{detail}; destination is currently absent, but a dispatched COPY may still commit"
            )),
            operation_outcome: "destination_state_unknown",
            operation_phase: "copying",
            partial_destination: None,
            source_fingerprint: Some(source.encode()),
            destination_fingerprint: None,
        },
        Err(error) => RemoteTransferFailure {
            failure: partial_failure(format!("{detail}; destination reconciliation failed: {error}")),
            operation_outcome: "destination_state_unknown",
            operation_phase: "copying",
            partial_destination: None,
            source_fingerprint: Some(source.encode()),
            destination_fingerprint: None,
        },
    }
}

#[cfg(test)]
fn install_test_s3_copy_after_commit_response_loss(destination_path: &str) {
    *TEST_S3_COPY_AFTER_COMMIT_RESPONSE_LOSS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(destination_path.to_string());
}

#[cfg(test)]
fn take_test_s3_copy_after_commit_response_loss(destination_path: &str) -> bool {
    let mut target = TEST_S3_COPY_AFTER_COMMIT_RESPONSE_LOSS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if target.as_deref() == Some(destination_path) {
        target.take();
        true
    } else {
        false
    }
}

#[cfg(test)]
fn install_test_s3_copy_chunk(destination_path: &str, chunk_size: usize) {
    *TEST_S3_COPY_CHUNK.get_or_init(|| Mutex::new(None)).lock().unwrap_or_else(|error| error.into_inner()) =
        Some((destination_path.to_string(), chunk_size));
}

#[cfg(test)]
fn take_test_s3_copy_chunk(destination_path: &str) -> Option<usize> {
    let mut target =
        TEST_S3_COPY_CHUNK.get_or_init(|| Mutex::new(None)).lock().unwrap_or_else(|error| error.into_inner());
    if target.as_ref().is_some_and(|(path, _)| path == destination_path) {
        target.take().map(|(_, chunk_size)| chunk_size)
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_server_side_remote_transfer<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    transfer_id: &str,
    operation: &'static str,
    policy: RemoteMutationPolicy,
    source_path: &str,
    destination_path: &str,
    prepared: &PreparedFileMutation<'_>,
    cancellation: &CancellationToken,
    progress: Arc<TransferProgressSnapshot>,
) -> Result<RemoteTransferOutcome, RemoteTransferFailure> {
    let source_before =
        prepared.stat_remote_file(source_path).await.map_err(remote_failure).map_err(remote_transfer_before_copy)?;
    let total_bytes = i64::try_from(source_before.size)
        .map_err(|_| local_failure("Remote source size is not representable"))
        .map_err(remote_transfer_before_copy)?;
    progress.record_total(Some(total_bytes));
    let destination_exists = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(remote_transfer_before_copy(TransferFailure {
                status: "cancelled",
                message: "Remote transfer was cancelled during destination preflight".to_string(),
                invalidate_operator: false,
            }))
        }
        _ = prepared.cancellation.cancelled() => {
            return Err(remote_transfer_before_copy(TransferFailure {
                status: "cancelled",
                message: "The file connection was removed during destination preflight".to_string(),
                invalidate_operator: true,
            }))
        }
        result = prepared.remote_entry_exists(destination_path) => {
            result.map_err(remote_failure).map_err(remote_transfer_before_copy)?
        }
    };
    if destination_exists && !policy.replace() {
        return Err(remote_transfer_before_copy(remote_failure(
            "Remote destination already exists; best_effort_no_clobber does not replace it",
        )));
    }
    let source_configured =
        prepared.configured_path(source_path).map_err(local_failure).map_err(remote_transfer_before_copy)?;
    let destination_configured =
        prepared.configured_path(destination_path).map_err(local_failure).map_err(remote_transfer_before_copy)?;
    let copying = state
        .storage
        .update_file_remote_transfer_phase(
            transfer_id,
            "running".to_string(),
            "copying".to_string(),
            0,
            Some(total_bytes),
            None,
            Some(source_before.encode()),
            None,
        )
        .await
        .map_err(local_failure)
        .map_err(remote_transfer_before_copy)?;
    emit_transfer(app, &copying);
    if copying.status == "cancelling" || cancellation.is_cancelled() {
        return Err(remote_transfer_before_copy(cancelled_failure()));
    }

    if operation == "rename" && prepared.uses_native_rename() {
        let _mutation_guard = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "Native rename was cancelled while waiting for the mutation lock".to_string(),
                    invalidate_operator: false,
                }))
            }
            _ = prepared.cancellation.cancelled() => {
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed while native rename was waiting for the mutation lock".to_string(),
                    invalidate_operator: true,
                }))
            }
            guard = prepared.acquire_mutation_guard() => guard,
        };
        let preflight = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "Native rename was cancelled before dispatch".to_string(),
                    invalidate_operator: false,
                }))
            }
            _ = prepared.cancellation.cancelled() => {
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed before native rename dispatch".to_string(),
                    invalidate_operator: true,
                }))
            }
            result = prepared.preflight_native_rename(source_path, destination_path, policy.replace()) => result,
        };
        preflight.map_err(remote_failure).map_err(remote_transfer_before_copy)?;
        if cancellation.is_cancelled() {
            return Err(remote_transfer_before_copy(TransferFailure {
                status: "cancelled",
                message: "Native rename was cancelled before dispatch".to_string(),
                invalidate_operator: false,
            }));
        }
        if prepared.cancellation.is_cancelled() {
            return Err(remote_transfer_before_copy(TransferFailure {
                status: "cancelled",
                message: "The file connection was removed before native rename dispatch".to_string(),
                invalidate_operator: true,
            }));
        }
        let dispatch_started = Arc::new(AtomicBool::new(false));
        let dispatch_for_request = dispatch_started.clone();
        let mutation = {
            let dispatch =
                prepared.dispatch_native_rename(source_path, destination_path, policy.replace(), dispatch_for_request);
            tokio::select! {
                biased;
                result = tokio::time::timeout(IO_PROGRESS_WATCHDOG, dispatch) => {
                    match result {
                        Ok(result) => NativeRenameDispatchOutcome::Finished(result),
                        Err(_) => NativeRenameDispatchOutcome::TimedOut,
                    }
                },
                _ = cancellation.cancelled() => NativeRenameDispatchOutcome::TransferCancelled,
                _ = prepared.cancellation.cancelled() => NativeRenameDispatchOutcome::ConnectionCancelled,
            }
        };
        match mutation {
            NativeRenameDispatchOutcome::TransferCancelled => {
                if dispatch_started.load(Ordering::Acquire) {
                    let failure = reconcile_uncertain_native_move(
                        prepared,
                        &source_before,
                        source_path,
                        destination_path,
                        "Native rename was cancelled after dispatch and its outcome is uncertain".to_string(),
                        true,
                    )
                    .await;
                    return Err(invalidate_hdfs_native_after_uncertain_move(prepared, failure));
                }
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "Native rename was cancelled before dispatch".to_string(),
                    invalidate_operator: false,
                }));
            }
            NativeRenameDispatchOutcome::ConnectionCancelled => {
                if dispatch_started.load(Ordering::Acquire) {
                    let failure = reconcile_uncertain_native_move(
                        prepared,
                        &source_before,
                        source_path,
                        destination_path,
                        "The file connection was removed after native rename dispatch".to_string(),
                        true,
                    )
                    .await;
                    return Err(invalidate_hdfs_native_after_uncertain_move(prepared, failure));
                }
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed before native rename dispatch".to_string(),
                    invalidate_operator: true,
                }));
            }
            NativeRenameDispatchOutcome::Finished(Ok(())) => {
                let destination = prepared.fingerprint_remote_file(destination_path).await.map_err(|error| {
                    RemoteTransferFailure {
                        failure: partial_failure(error),
                        operation_outcome: "move_committed_verification_failed",
                        operation_phase: "copying",
                        partial_destination: Some(destination_path.to_string()),
                        source_fingerprint: Some(source_before.encode()),
                        destination_fingerprint: None,
                    }
                })?;
                if !prepared.native_rename_destination_matches(&source_before, &destination) {
                    return Err(RemoteTransferFailure {
                        failure: partial_failure("Native rename destination size did not match the source"),
                        operation_outcome: "move_committed_verification_failed",
                        operation_phase: "copying",
                        partial_destination: Some(destination_path.to_string()),
                        source_fingerprint: Some(source_before.encode()),
                        destination_fingerprint: Some(destination.encode()),
                    });
                }
                match prepared.fingerprint_remote_file(source_path).await {
                    Err(error) if error.contains("no longer exists") => {}
                    Ok(source) => {
                        return Err(RemoteTransferFailure {
                            failure: partial_failure("Native rename returned success but the source is still present"),
                            operation_outcome: "move_committed_verification_failed",
                            operation_phase: "copying",
                            partial_destination: Some(destination_path.to_string()),
                            source_fingerprint: Some(source.encode()),
                            destination_fingerprint: Some(destination.encode()),
                        });
                    }
                    Err(error) => {
                        return Err(RemoteTransferFailure {
                            failure: partial_failure(error),
                            operation_outcome: "move_committed_verification_failed",
                            operation_phase: "copying",
                            partial_destination: Some(destination_path.to_string()),
                            source_fingerprint: Some(source_before.encode()),
                            destination_fingerprint: Some(destination.encode()),
                        });
                    }
                }
                progress.record_bytes(total_bytes);
                return Ok(RemoteTransferOutcome {
                    bytes_transferred: total_bytes,
                    total_bytes,
                    operation_outcome: "completed",
                    operation_phase: "completed",
                    source_fingerprint: source_before.encode(),
                    destination_fingerprint: destination.encode(),
                });
            }
            NativeRenameDispatchOutcome::Finished(Err(error)) => {
                if !error.is_outcome_unknown() {
                    return Err(remote_transfer_before_copy(remote_failure(error.message)));
                }
                let failure = reconcile_uncertain_native_move(
                    prepared,
                    &source_before,
                    source_path,
                    destination_path,
                    error.message,
                    false,
                )
                .await;
                return Err(invalidate_hdfs_native_after_uncertain_move(prepared, failure));
            }
            NativeRenameDispatchOutcome::TimedOut => {
                let failure = reconcile_uncertain_native_move(
                    prepared,
                    &source_before,
                    source_path,
                    destination_path,
                    "Native rename timed out".to_string(),
                    false,
                )
                .await;
                return Err(invalidate_hdfs_native_after_uncertain_move(prepared, failure));
            }
        }
    }

    if operation == "copy" && prepared.uses_native_webdav_copy() {
        let _mutation_guard = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "Remote COPY was cancelled while waiting for the mutation lock".to_string(),
                    invalidate_operator: false,
                }))
            }
            _ = prepared.cancellation.cancelled() => {
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed while WebDAV COPY was waiting for the mutation lock".to_string(),
                    invalidate_operator: true,
                }))
            }
            guard = prepared.acquire_mutation_guard() => guard,
        };
        let preflight = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "Remote COPY was cancelled before dispatch".to_string(),
                    invalidate_operator: false,
                }))
            }
            _ = prepared.cancellation.cancelled() => {
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed before WebDAV COPY dispatch".to_string(),
                    invalidate_operator: true,
                }))
            }
            result = prepared.preflight_native_webdav_mutation(source_path, destination_path, policy.replace()) => result,
        };
        preflight.map_err(remote_failure).map_err(remote_transfer_before_copy)?;
        if cancellation.is_cancelled() {
            return Err(remote_transfer_before_copy(TransferFailure {
                status: "cancelled",
                message: "Remote COPY was cancelled before dispatch".to_string(),
                invalidate_operator: false,
            }));
        }
        if prepared.cancellation.is_cancelled() {
            return Err(remote_transfer_before_copy(TransferFailure {
                status: "cancelled",
                message: "The file connection was removed before WebDAV COPY dispatch".to_string(),
                invalidate_operator: true,
            }));
        }
        let dispatch_started = Arc::new(AtomicBool::new(false));
        let dispatch_for_request = dispatch_started.clone();
        let mutation = tokio::select! {
            biased;
            result = tokio::time::timeout(
                IO_PROGRESS_WATCHDOG,
                prepared.dispatch_native_webdav_copy(source_path, destination_path, dispatch_for_request),
            ) => result,
            _ = cancellation.cancelled() => {
                if dispatch_started.load(Ordering::Acquire) {
                    return Err(reconcile_uncertain_webdav_copy(
                        prepared,
                        &source_before,
                        destination_path,
                        "WebDAV COPY was cancelled after dispatch".to_string(),
                    ).await);
                }
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "Remote COPY was cancelled before dispatch".to_string(),
                    invalidate_operator: false,
                }));
            }
            _ = prepared.cancellation.cancelled() => {
                if dispatch_started.load(Ordering::Acquire) {
                    return Err(reconcile_uncertain_webdav_copy(
                        prepared,
                        &source_before,
                        destination_path,
                        "The file connection was removed after WebDAV COPY dispatch".to_string(),
                    ).await);
                }
                return Err(remote_transfer_before_copy(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed before WebDAV COPY dispatch".to_string(),
                    invalidate_operator: true,
                }));
            }
        };
        match mutation {
            Ok(Ok(())) => {
                let source_after = prepared
                    .fingerprint_remote_file(source_path)
                    .await
                    .map_err(remote_failure)
                    .map_err(remote_transfer_before_copy)?;
                let destination = prepared.fingerprint_remote_file(destination_path).await.map_err(|error| {
                    RemoteTransferFailure {
                        failure: partial_failure(error),
                        operation_outcome: "failed_with_partial_destination",
                        operation_phase: "copying",
                        partial_destination: Some(destination_path.to_string()),
                        source_fingerprint: Some(source_after.encode()),
                        destination_fingerprint: None,
                    }
                })?;
                if source_after != source_before || destination.size != source_before.size {
                    return Err(RemoteTransferFailure {
                        failure: partial_failure("WebDAV source changed during COPY or destination size did not match"),
                        operation_outcome: "failed_with_partial_destination",
                        operation_phase: "copying",
                        partial_destination: Some(destination_path.to_string()),
                        source_fingerprint: Some(source_after.encode()),
                        destination_fingerprint: Some(destination.encode()),
                    });
                }
                progress.record_bytes(total_bytes);
                return Ok(RemoteTransferOutcome {
                    bytes_transferred: total_bytes,
                    total_bytes,
                    operation_outcome: "completed",
                    operation_phase: "completed",
                    source_fingerprint: source_after.encode(),
                    destination_fingerprint: destination.encode(),
                });
            }
            Ok(Err(error)) => {
                if !error.is_outcome_unknown() {
                    return Err(remote_transfer_before_copy(remote_failure(error.message)));
                }
                return Err(
                    reconcile_uncertain_webdav_copy(prepared, &source_before, destination_path, error.message).await
                );
            }
            Err(_) => {
                return Err(reconcile_uncertain_webdav_copy(
                    prepared,
                    &source_before,
                    destination_path,
                    "WebDAV COPY timed out".to_string(),
                )
                .await);
            }
        }
    }

    let copier = prepared
        .operator
        .copier_with(&source_configured, &destination_configured)
        .if_not_exists(!policy.replace())
        .source_content_length_hint(source_before.size);
    #[cfg(test)]
    let copier = if let Some(chunk_size) = take_test_s3_copy_chunk(destination_path) {
        copier.chunk(chunk_size)
    } else {
        copier
    };
    let mut copier = copier
        .concurrent(1)
        .await
        .map_err(|error| remote_transfer_before_copy(remote_failure(prepared.redact_operator_error(error))))?;
    let mut server_side_copied_bytes = 0_i64;
    let mut last_progress = Instant::now();
    loop {
        let step = tokio::select! {
            _ = cancellation.cancelled() => {
                let abort = copier.abort().await;
                let detail = abort.err().map(|error| format!("; abort failed: {error}")).unwrap_or_default();
                return Err(reconcile_uncertain_server_side_copy(
                    prepared, &source_before, destination_path,
                    format!("Server-side COPY cancellation response was uncertain{detail}"),
                    true,
                ).await);
            }
            _ = prepared.cancellation.cancelled() => {
                let abort = copier.abort().await;
                let detail = abort.err().map(|error| format!("; abort failed: {error}")).unwrap_or_default();
                return Err(reconcile_uncertain_server_side_copy(
                    prepared, &source_before, destination_path,
                    format!("The file connection was removed during server-side COPY{detail}"),
                    true,
                ).await);
            }
            result = tokio::time::timeout(IO_PROGRESS_WATCHDOG, copier.next()) => result,
        };
        match step {
            Ok(Ok(Some(bytes))) => {
                server_side_copied_bytes =
                    server_side_copied_bytes.saturating_add(i64::try_from(bytes).unwrap_or(i64::MAX)).min(total_bytes);
                progress.record_bytes(server_side_copied_bytes);
                if last_progress.elapsed() >= PROGRESS_INTERVAL {
                    let update = state
                        .storage
                        .update_file_remote_transfer_phase(
                            transfer_id,
                            "running".to_string(),
                            "copying".to_string(),
                            server_side_copied_bytes,
                            Some(total_bytes),
                            None,
                            Some(source_before.encode()),
                            None,
                        )
                        .await
                        .map_err(local_failure)
                        .map_err(remote_transfer_before_copy)?;
                    emit_transfer(app, &update);
                    last_progress = Instant::now();
                }
                continue;
            }
            Ok(Ok(None)) => break,
            Ok(Err(error)) => {
                let abort = copier.abort().await;
                let detail = abort.err().map(|abort| format!("; abort failed: {abort}")).unwrap_or_default();
                return Err(reconcile_uncertain_server_side_copy(
                    prepared,
                    &source_before,
                    destination_path,
                    prepared.redact_remote_error(format!("{error}{detail}")),
                    false,
                )
                .await);
            }
            Err(_) => {
                let abort = copier.abort().await;
                let detail = abort.err().map(|error| format!("; abort failed: {error}")).unwrap_or_default();
                return Err(reconcile_uncertain_server_side_copy(
                    prepared,
                    &source_before,
                    destination_path,
                    format!("Server-side COPY timed out{detail}"),
                    false,
                )
                .await);
            }
        }
    }

    #[cfg(test)]
    if take_test_s3_copy_after_commit_response_loss(destination_path) {
        return Err(reconcile_uncertain_server_side_copy(
            prepared,
            &source_before,
            destination_path,
            "Injected after-commit S3 copy response loss".to_string(),
            false,
        )
        .await);
    }

    let source_after = prepared.fingerprint_remote_file(source_path).await.map_err(|error| RemoteTransferFailure {
        failure: partial_failure(error),
        operation_outcome: "failed_with_partial_destination",
        operation_phase: "copying",
        partial_destination: Some(destination_path.to_string()),
        source_fingerprint: Some(source_before.encode()),
        destination_fingerprint: None,
    })?;
    let destination =
        prepared.fingerprint_remote_file(destination_path).await.map_err(|error| RemoteTransferFailure {
            failure: partial_failure(error),
            operation_outcome: "failed_with_partial_destination",
            operation_phase: "copying",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_after.encode()),
            destination_fingerprint: None,
        })?;
    if source_after != source_before || destination.size != source_before.size {
        return Err(RemoteTransferFailure {
            failure: partial_failure(
                "Source changed during server-side copy or destination size did not match; source deletion was not attempted",
            ),
            operation_outcome: "failed_with_partial_destination",
            operation_phase: "copying",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_after.encode()),
            destination_fingerprint: Some(destination.encode()),
        });
    }
    progress.record_bytes(total_bytes);
    let source_durable = source_after.encode();
    let destination_durable = destination.encode();
    if operation == "copy" {
        return Ok(RemoteTransferOutcome {
            bytes_transferred: total_bytes,
            total_bytes,
            operation_outcome: "completed",
            operation_phase: "completed",
            source_fingerprint: source_durable,
            destination_fingerprint: destination_durable,
        });
    }

    let published = state
        .storage
        .update_file_remote_transfer_phase(
            transfer_id,
            "publishing".to_string(),
            "published_before_delete".to_string(),
            total_bytes,
            Some(total_bytes),
            None,
            Some(source_durable.clone()),
            Some(destination_durable.clone()),
        )
        .await
        .map_err(local_failure)
        .map_err(|failure| RemoteTransferFailure {
            failure,
            operation_outcome: "copied_source_delete_failed",
            operation_phase: "published_before_delete",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_durable.clone()),
            destination_fingerprint: Some(destination_durable.clone()),
        })?;
    emit_transfer(app, &published);
    wait_at_test_remote_rename_after_publish_barrier().await;
    state
        .storage
        .update_file_remote_transfer_phase(
            transfer_id,
            "publishing".to_string(),
            "delete_uncertain".to_string(),
            total_bytes,
            Some(total_bytes),
            None,
            Some(source_durable.clone()),
            Some(destination_durable.clone()),
        )
        .await
        .map_err(local_failure)
        .map_err(|failure| RemoteTransferFailure {
            failure,
            operation_outcome: "copied_source_delete_failed",
            operation_phase: "published_before_delete",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_durable.clone()),
            destination_fingerprint: Some(destination_durable.clone()),
        })?;
    if let Err(error) = prepared
        .delete_source_if_fingerprints_match(state, source_path, destination_path, &source_after, &destination)
        .await
    {
        return Err(RemoteTransferFailure {
            failure: partial_failure(error),
            operation_outcome: "copied_source_delete_failed",
            operation_phase: "delete_uncertain",
            partial_destination: Some(destination_path.to_string()),
            source_fingerprint: Some(source_durable),
            destination_fingerprint: Some(destination_durable),
        });
    }
    Ok(RemoteTransferOutcome {
        bytes_transferred: total_bytes,
        total_bytes,
        operation_outcome: "completed",
        operation_phase: "completed",
        source_fingerprint: source_durable,
        destination_fingerprint: destination_durable,
    })
}

struct UploadExecutionContext<'a, 'runtime, R: Runtime> {
    app: &'a AppHandle<R>,
    state: &'a AppState,
    runtime: &'a FileTransferRuntime,
    transfer_id: &'a str,
    prepared: &'a PreparedFileMutation<'runtime>,
    target_relative: &'a str,
    partial_relative: &'a str,
    partial_configured: &'a str,
    cancellation: &'a CancellationToken,
    progress_snapshot: Arc<TransferProgressSnapshot>,
    policy: UploadPolicy,
}

async fn execute_upload<R: Runtime>(
    context: UploadExecutionContext<'_, '_, R>,
    local: ValidatedLocalSource,
) -> Result<UploadOutcome, UploadFailure> {
    let UploadExecutionContext {
        app,
        state,
        runtime,
        transfer_id,
        prepared,
        target_relative,
        partial_relative,
        partial_configured,
        cancellation,
        progress_snapshot,
        policy,
    } = context;
    prepared.guard_destination_path(target_relative).await.map_err(remote_failure).map_err(UploadFailure::from)?;
    prepared.guard_destination_path(partial_relative).await.map_err(remote_failure).map_err(UploadFailure::from)?;
    ensure_remote_target_absent(prepared, &prepared.remote_path).await.map_err(UploadFailure::from)?;
    if local.total_bytes == 0 {
        return execute_empty_upload(
            app,
            state,
            transfer_id,
            prepared,
            target_relative,
            partial_relative,
            local,
            cancellation,
            policy,
        )
        .await;
    }
    if prepared.uses_streaming_webdav_upload() {
        return execute_webdav_streaming_upload(
            app,
            state,
            runtime,
            transfer_id,
            prepared,
            target_relative,
            partial_relative,
            local,
            cancellation,
            progress_snapshot,
            policy,
        )
        .await;
    }
    let upload_buffer_size = prepared.transfer_chunk_size(if prepared.uses_server_side_copy() {
        S3_UPLOAD_BUFFER_SIZE
    } else {
        UPLOAD_BUFFER_SIZE
    });
    let idle_timeout = prepared.transfer_idle_timeout(IO_PROGRESS_WATCHDOG);
    let mut writer = tokio::select! {
        _ = cancellation.cancelled() => return Err(UploadFailure::from(upload_cancelled_failure())),
        _ = prepared.cancellation.cancelled() => {
            return Err(UploadFailure::from(TransferFailure {
                status: "cancelled",
                message: "The file connection was removed while the upload was queued".to_string(),
                invalidate_operator: true,
            }))
        },
        result = tokio::time::timeout(
            idle_timeout,
            open_upload_writer(
                prepared,
                partial_relative,
                partial_configured,
                u64::try_from(local.total_bytes).unwrap_or(u64::MAX),
                upload_buffer_size,
            ),
        ) => {
            result
                .map_err(|_| remote_failure("Opening the remote upload timed out"))
                .and_then(|result| {
                    result.map_err(remote_failure)
                })
                .map_err(UploadFailure::from)?
        }
    };

    let proof = local.proof();
    let file = local.file;
    let mut source = tokio::fs::File::from_std(file);
    let mut buffer = vec![0_u8; upload_buffer_size];
    let mut bytes_transferred = 0_i64;
    let mut last_progress = Instant::now();
    let deadline = tokio::time::Instant::now() + DOWNLOAD_OPERATION_TIMEOUT;

    let body_result = async {
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(remote_failure("Upload operation timed out"));
            }
            let count = tokio::select! {
                _ = cancellation.cancelled() => return Err(upload_cancelled_active_failure()),
                _ = prepared.cancellation.cancelled() => {
                    return Err(TransferFailure {
                        status: "cancelled",
                        message: "The file connection was removed while the upload was running".to_string(),
                        invalidate_operator: true,
                    })
                },
                result = tokio::time::timeout(IO_PROGRESS_WATCHDOG, source.read(&mut buffer)) => {
                    result
                        .map_err(|_| local_failure("Local upload read made no progress before the I/O watchdog expired"))?
                        .map_err(|error| local_failure(format!("Failed to read the upload source: {error}")))?
                }
            };
            if count == 0 {
                break;
            }
            let chunk = Bytes::copy_from_slice(&buffer[..count]);
            tokio::select! {
                _ = cancellation.cancelled() => return Err(upload_cancelled_active_failure()),
                _ = prepared.cancellation.cancelled() => {
                    return Err(TransferFailure {
                        status: "cancelled",
                        message: "The file connection was removed while the upload was running".to_string(),
                        invalidate_operator: true,
                    })
                },
                result = tokio::time::timeout(idle_timeout, writer.write(chunk, prepared)) => {
                    result
                        .map_err(|_| remote_failure("Remote upload write made no progress before the I/O watchdog expired"))?
                        .map_err(remote_failure)?;
                }
            }
            bytes_transferred =
                bytes_transferred.saturating_add(i64::try_from(count).unwrap_or(i64::MAX));
            progress_snapshot.record_bytes(bytes_transferred);
            wait_at_test_upload_after_chunk_barrier().await;
            if last_progress.elapsed() >= PROGRESS_INTERVAL {
                let progress = state
                    .storage
                    .update_file_transfer(
                        transfer_id,
                        "running".to_string(),
                        bytes_transferred,
                        Some(proof.total_bytes),
                        Some(partial_relative.to_string()),
                        Some(proof.fingerprint.clone()),
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

        verify_upload_source_unchanged(&proof, bytes_transferred).map_err(local_failure)?;
        ensure_remote_target_absent(prepared, &prepared.remote_path).await?;
        Ok(())
    }
    .await;

    if let Err(failure) = body_result {
        return Err(abort_upload(writer, prepared, partial_relative, failure).await);
    }

    match tokio::time::timeout(idle_timeout, writer.close(prepared)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            return Err(abort_upload(writer, prepared, partial_relative, remote_failure(error)).await);
        }
        Err(_) => {
            return Err(abort_upload(
                writer,
                prepared,
                partial_relative,
                remote_failure("Closing the remote upload timed out"),
            )
            .await);
        }
    }
    wait_at_test_upload_after_close_barrier().await;

    publish_closed_upload(
        app,
        state,
        ClosedUploadContext {
            transfer_id,
            prepared,
            target_relative,
            partial_relative,
            proof: &proof,
            bytes_transferred,
            cancellation,
            policy,
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_webdav_streaming_upload<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    runtime: &FileTransferRuntime,
    transfer_id: &str,
    prepared: &PreparedFileMutation<'_>,
    target_relative: &str,
    partial_relative: &str,
    local: ValidatedLocalSource,
    cancellation: &CancellationToken,
    progress_snapshot: Arc<TransferProgressSnapshot>,
    policy: UploadPolicy,
) -> Result<UploadOutcome, UploadFailure> {
    let proof = local.proof();
    let size = u64::try_from(local.total_bytes)
        .map_err(|_| UploadFailure::from(local_failure("Upload size is not representable")))?;
    let callback_progress = progress_snapshot.clone();
    let progress: Arc<dyn Fn(u64) + Send + Sync> = Arc::new(move |bytes| {
        callback_progress.record_bytes(i64::try_from(bytes).unwrap_or(i64::MAX));
    });
    if cancellation.is_cancelled() {
        return Err(cleanup_closed_upload_partial(prepared, partial_relative, upload_cancelled_failure()).await);
    }
    if prepared.cancellation.is_cancelled() {
        return Err(cleanup_closed_upload_partial(
            prepared,
            partial_relative,
            TransferFailure {
                status: "cancelled",
                message: "The file connection was removed before the WebDAV upload was dispatched".to_string(),
                invalidate_operator: true,
            },
        )
        .await);
    }
    let file = tokio::fs::File::from_std(local.file);
    let dispatch_started = Arc::new(AtomicBool::new(false));
    let put_request =
        prepared.put_webdav_upload_partial(partial_relative, file, size, progress, dispatch_started.clone());
    tokio::pin!(put_request);
    let mut tick = tokio::time::interval(PROGRESS_INTERVAL);
    let started = Instant::now();
    let mut last_observed_bytes = 0_i64;
    let mut last_progress_at = Instant::now();
    let put = loop {
        tokio::select! {
            biased;
            result = &mut put_request => {
                match result {
                    Ok(()) => break Ok(()),
                    Err(error) if error.kind == WebdavMutationErrorKind::FailedBeforeMutation => {
                        return Err(UploadFailure::from(remote_failure(error.message)));
                    }
                    Err(error) if !error.is_outcome_unknown() => {
                        return Err(
                            cleanup_closed_upload_partial(
                                prepared,
                                partial_relative,
                                remote_failure(error.message),
                            )
                            .await,
                        );
                    }
                    Err(error) => break Err(remote_failure(error.message)),
                }
            }
            _ = cancellation.cancelled() => {
                if !dispatch_started.load(Ordering::Acquire) {
                    return Err(
                        cleanup_closed_upload_partial(
                            prepared,
                            partial_relative,
                            upload_cancelled_failure(),
                        )
                        .await,
                    );
                }
                break Err(upload_cancelled_active_failure());
            }
            _ = prepared.cancellation.cancelled() => {
                if !dispatch_started.load(Ordering::Acquire) {
                    return Err(
                        cleanup_closed_upload_partial(
                            prepared,
                            partial_relative,
                            TransferFailure {
                                status: "cancelled",
                                message: "The file connection was removed before the WebDAV upload was dispatched".to_string(),
                                invalidate_operator: true,
                            },
                        )
                        .await,
                    );
                }
                break Err(TransferFailure {
                    status: "cancelled",
                    message: "The file connection was removed while the WebDAV upload was running".to_string(),
                    invalidate_operator: true,
                });
            }
            _ = tick.tick() => {
                let bytes = progress_snapshot.bytes();
                if bytes != last_observed_bytes {
                    last_observed_bytes = bytes;
                    last_progress_at = Instant::now();
                } else if last_progress_at.elapsed() >= IO_PROGRESS_WATCHDOG {
                    break Err(remote_failure("WebDAV streaming PUT made no progress before the I/O watchdog expired"));
                }
                if started.elapsed() >= DOWNLOAD_OPERATION_TIMEOUT {
                    break Err(remote_failure("WebDAV streaming PUT timed out"));
                }
                let update = match state
                    .storage
                    .update_file_transfer(
                        transfer_id,
                        "running".to_string(),
                        bytes,
                        Some(proof.total_bytes),
                        Some(partial_relative.to_string()),
                        Some(proof.fingerprint.clone()),
                        None,
                        false,
                    )
                    .await
                {
                    Ok(update) => update,
                    Err(error) => break Err(local_failure(format!("Failed to persist WebDAV upload progress: {error}"))),
                };
                if update.status == "running" && runtime.should_emit_progress() {
                    emit_transfer(app, &update);
                }
            }
        }
    };
    if let Err(failure) = put {
        return Err(UploadFailure {
            failure: TransferFailure {
                status: "partial",
                message: format!(
                    "{}; the operation-owned WebDAV partial was retained because the dispatched PUT outcome is uncertain",
                    failure.message
                ),
                invalidate_operator: true,
            },
            partial_destination: Some(partial_relative.to_string()),
            abort_outcome: Some("not_attempted_put_outcome_uncertain".to_string()),
            publish_outcome: None,
        });
    }
    wait_at_test_upload_after_close_barrier().await;
    if let Err(error) = verify_upload_source_unchanged(&proof, proof.total_bytes) {
        return Err(cleanup_closed_upload_partial(prepared, partial_relative, local_failure(error)).await);
    }
    if let Err(failure) = ensure_remote_target_absent(prepared, &prepared.remote_path).await {
        return Err(cleanup_closed_upload_partial(prepared, partial_relative, failure).await);
    }
    publish_closed_upload(
        app,
        state,
        ClosedUploadContext {
            transfer_id,
            prepared,
            target_relative,
            partial_relative,
            proof: &proof,
            bytes_transferred: proof.total_bytes,
            cancellation,
            policy,
        },
    )
    .await
}

async fn execute_empty_upload<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    transfer_id: &str,
    prepared: &PreparedFileMutation<'_>,
    target_relative: &str,
    partial_relative: &str,
    local: ValidatedLocalSource,
    cancellation: &CancellationToken,
    policy: UploadPolicy,
) -> Result<UploadOutcome, UploadFailure> {
    let proof = local.proof();
    if cancellation.is_cancelled() {
        return Err(UploadFailure::from(upload_cancelled_failure()));
    }
    if prepared.cancellation.is_cancelled() {
        return Err(UploadFailure::from(TransferFailure {
            status: "cancelled",
            message: "The file connection was removed before the empty upload started".to_string(),
            invalidate_operator: true,
        }));
    }
    verify_upload_source_unchanged(&proof, 0).map_err(local_failure).map_err(UploadFailure::from)?;
    wait_at_test_upload_after_chunk_barrier().await;
    if let Err(error) =
        tokio::time::timeout(IO_PROGRESS_WATCHDOG, prepared.create_empty_owned_upload_partial(partial_relative))
            .await
            .map_err(|_| "Creating the empty operation-owned upload partial timed out".to_string())
            .and_then(|result| result)
    {
        return Err(cleanup_closed_upload_partial(
            prepared,
            partial_relative,
            remote_failure(format!("Failed to create the empty upload partial: {error}")),
        )
        .await);
    }
    publish_closed_upload(
        app,
        state,
        ClosedUploadContext {
            transfer_id,
            prepared,
            target_relative,
            partial_relative,
            proof: &proof,
            bytes_transferred: 0,
            cancellation,
            policy,
        },
    )
    .await
}

struct ClosedUploadContext<'a> {
    transfer_id: &'a str,
    prepared: &'a PreparedFileMutation<'a>,
    target_relative: &'a str,
    partial_relative: &'a str,
    proof: &'a UploadSourceProof,
    bytes_transferred: i64,
    cancellation: &'a CancellationToken,
    policy: UploadPolicy,
}

async fn publish_closed_upload<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    context: ClosedUploadContext<'_>,
) -> Result<UploadOutcome, UploadFailure> {
    let ClosedUploadContext {
        transfer_id,
        prepared,
        target_relative,
        partial_relative,
        proof,
        bytes_transferred,
        cancellation,
        policy,
    } = context;
    if let Err(error) = verify_upload_source_unchanged(proof, bytes_transferred) {
        return Err(cleanup_closed_upload_partial(prepared, partial_relative, local_failure(error)).await);
    }
    if cancellation.is_cancelled() {
        return Err(cleanup_closed_upload_partial(prepared, partial_relative, upload_cancelled_active_failure()).await);
    }
    if prepared.cancellation.is_cancelled() {
        return Err(cleanup_closed_upload_partial(
            prepared,
            partial_relative,
            TransferFailure {
                status: "cancelled",
                message: "The file connection was removed after the upload closed and before publish".to_string(),
                invalidate_operator: true,
            },
        )
        .await);
    }
    let publishing = match state
        .storage
        .update_file_transfer(
            transfer_id,
            "publishing".to_string(),
            bytes_transferred,
            Some(proof.total_bytes),
            Some(partial_relative.to_string()),
            Some(proof.fingerprint.clone()),
            None,
            false,
        )
        .await
    {
        Ok(record) => record,
        Err(error) => {
            return Err(cleanup_closed_upload_partial(
                prepared,
                partial_relative,
                local_failure(format!("Failed to persist upload publishing state: {error}")),
            )
            .await)
        }
    };
    emit_transfer(app, &publishing);
    if publishing.status == "cancelling" || cancellation.is_cancelled() {
        return Err(cleanup_closed_upload_partial(prepared, partial_relative, upload_cancelled_active_failure()).await);
    }
    if prepared.cancellation.is_cancelled() {
        return Err(cleanup_closed_upload_partial(
            prepared,
            partial_relative,
            TransferFailure {
                status: "cancelled",
                message: "The file connection was removed before upload publish".to_string(),
                invalidate_operator: true,
            },
        )
        .await);
    }
    match prepared
        .publish_owned_upload_partial(state, partial_relative, target_relative, proof.total_bytes, policy, cancellation)
        .await
    {
        Ok(UploadPublishResolution { state: UploadPublishState::Completed, .. }) => Ok(UploadOutcome {
            bytes_transferred,
            total_bytes: Some(proof.total_bytes),
            publish_outcome: Some("completed".to_string()),
        }),
        Ok(UploadPublishResolution { state: UploadPublishState::PartialSource, detail }) => Err(UploadFailure {
            failure: partial_failure(format!("Upload publish was not completed: {detail}")),
            partial_destination: Some(partial_relative.to_string()),
            abort_outcome: Some("not_applicable_after_close".to_string()),
            publish_outcome: Some("partial_source".to_string()),
        }),
        Ok(UploadPublishResolution { state: UploadPublishState::PartialTarget, detail }) => Err(UploadFailure {
            failure: partial_failure(format!("Upload publish target exists but ownership is unproven: {detail}")),
            partial_destination: Some(target_relative.to_string()),
            abort_outcome: Some("not_applicable_after_close".to_string()),
            publish_outcome: Some("partial_target_unproven".to_string()),
        }),
        Ok(UploadPublishResolution { state: UploadPublishState::Unknown, detail }) => Err(UploadFailure {
            failure: partial_failure(format!("Upload publish outcome is unknown: {detail}")),
            partial_destination: None,
            abort_outcome: Some("not_applicable_after_close".to_string()),
            publish_outcome: Some("unknown".to_string()),
        }),
        Err(error) => {
            let status = if cancellation.is_cancelled() || prepared.cancellation.is_cancelled() {
                upload_cancelled_active_failure()
            } else {
                remote_failure(format!("Failed to publish the uploaded file: {error}"))
            };
            Err(cleanup_closed_upload_partial(prepared, partial_relative, status).await)
        }
    }
}

async fn ensure_remote_target_absent(prepared: &PreparedFileMutation<'_>, path: &str) -> Result<(), TransferFailure> {
    match tokio::time::timeout(IO_PROGRESS_WATCHDOG, prepared.operator.stat(path)).await {
        Ok(Ok(_)) => Err(remote_failure("Remote upload destination already exists")),
        Ok(Err(error)) if error.kind() == opendal::ErrorKind::NotFound => Ok(()),
        Ok(Err(error)) => Err(remote_failure(prepared.redact_operator_error(error))),
        Err(_) => Err(remote_failure("Checking the remote upload destination timed out")),
    }
}

async fn abort_upload(
    writer: StreamingDestinationWriter,
    prepared: &PreparedFileMutation<'_>,
    partial_relative: &str,
    failure: TransferFailure,
) -> UploadFailure {
    let watchdog = prepared.transfer_idle_timeout(IO_PROGRESS_WATCHDOG);
    let mut outcome = abort_upload_control_flow(writer, partial_relative, failure, watchdog, || {
        prepared.delete_owned_upload_partial(partial_relative)
    })
    .await;
    outcome.failure.message = prepared.redact_remote_error(outcome.failure.message);
    outcome.abort_outcome = outcome.abort_outcome.map(|value| prepared.redact_remote_error(value));
    outcome.publish_outcome = outcome.publish_outcome.map(|value| prepared.redact_remote_error(value));
    outcome
}

#[derive(Debug)]
enum UploadAbortError {
    Unsupported,
    Failed(String),
}

trait AbortableUpload {
    fn abort(&mut self) -> Pin<Box<dyn Future<Output = Result<(), UploadAbortError>> + Send + '_>>;
}

impl AbortableUpload for opendal::Writer {
    fn abort(&mut self) -> Pin<Box<dyn Future<Output = Result<(), UploadAbortError>> + Send + '_>> {
        Box::pin(async move {
            opendal::Writer::abort(self).await.map_err(|error| {
                if error.kind() == opendal::ErrorKind::Unsupported {
                    UploadAbortError::Unsupported
                } else {
                    UploadAbortError::Failed(error.to_string())
                }
            })
        })
    }
}

async fn abort_upload_control_flow<A, Cleanup, CleanupFuture>(
    mut writer: A,
    partial_relative: &str,
    mut failure: TransferFailure,
    watchdog: Duration,
    cleanup: Cleanup,
) -> UploadFailure
where
    A: AbortableUpload,
    Cleanup: FnOnce() -> CleanupFuture,
    CleanupFuture: Future<Output = Result<(), String>>,
{
    let abort = tokio::time::timeout(watchdog, writer.abort()).await;
    let abort_outcome = match abort {
        Ok(Ok(())) => return resolve_successful_upload_abort(failure),
        Ok(Err(UploadAbortError::Unsupported)) => "unsupported".to_string(),
        Ok(Err(UploadAbortError::Failed(error))) => format!("failed: {}", sanitize_error(&error)),
        Err(_) => {
            failure.invalidate_operator = true;
            failure.status = "partial";
            failure.message.push_str("; writer abort timed out; operation-owned partial was preserved");
            return UploadFailure {
                failure,
                partial_destination: Some(partial_relative.to_string()),
                abort_outcome: Some("timed_out".to_string()),
                publish_outcome: None,
            };
        }
    };
    drop(writer);

    let cleanup = tokio::time::timeout(watchdog, cleanup())
        .await
        .map_err(|_| "operation-owned partial cleanup timed out".to_string())
        .and_then(|result| result);
    resolve_upload_cleanup(failure, abort_outcome, cleanup, partial_relative)
}

fn resolve_successful_upload_abort(failure: TransferFailure) -> UploadFailure {
    UploadFailure {
        failure,
        partial_destination: None,
        abort_outcome: Some("succeeded".to_string()),
        publish_outcome: None,
    }
}

async fn cleanup_closed_upload_partial(
    prepared: &PreparedFileMutation<'_>,
    partial_relative: &str,
    failure: TransferFailure,
) -> UploadFailure {
    let cleanup = tokio::time::timeout(IO_PROGRESS_WATCHDOG, prepared.delete_owned_upload_partial(partial_relative))
        .await
        .map_err(|_| "operation-owned partial cleanup timed out".to_string())
        .and_then(|result| result);
    resolve_upload_cleanup(failure, "not_applicable_after_close".to_string(), cleanup, partial_relative)
}

fn resolve_upload_cleanup(
    mut failure: TransferFailure,
    abort_outcome: String,
    cleanup: Result<(), String>,
    partial_relative: &str,
) -> UploadFailure {
    match cleanup {
        Ok(()) => UploadFailure {
            failure,
            partial_destination: None,
            abort_outcome: Some(format!("{abort_outcome}; operation_owned_partial_cleaned")),
            publish_outcome: None,
        },
        Err(error) => {
            failure.status = "partial";
            failure.invalidate_operator = true;
            failure.message = format!(
                "{}; operation-owned partial cleanup failed safely: {}",
                failure.message,
                sanitize_error(&error)
            );
            UploadFailure {
                failure,
                partial_destination: Some(partial_relative.to_string()),
                abort_outcome: Some(abort_outcome),
                publish_outcome: None,
            }
        }
    }
}

async fn finalize_upload_result<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    transfer_id: &str,
    result: Result<UploadOutcome, UploadFailure>,
    progress: &TransferProgressSnapshot,
) {
    let latest = state.storage.get_file_transfer(transfer_id).await.ok().flatten();
    let (status, bytes_transferred, total_bytes, error, partial_destination, abort_outcome, publish_outcome) =
        match result {
            Ok(outcome) => {
                ("completed", outcome.bytes_transferred, outcome.total_bytes, None, None, None, outcome.publish_outcome)
            }
            Err(upload) => (
                upload.failure.status,
                progress.bytes().max(latest.as_ref().map_or(0, |record| record.bytes_transferred)),
                progress.total().or_else(|| latest.as_ref().and_then(|record| record.total_bytes)),
                Some(sanitize_error(&upload.failure.message)),
                upload.partial_destination,
                upload.abort_outcome,
                upload.publish_outcome,
            ),
        };
    match state
        .storage
        .finish_file_upload_transfer(
            transfer_id,
            status.to_string(),
            bytes_transferred,
            total_bytes,
            optional_persisted_file_error(error),
            partial_destination,
            abort_outcome,
            publish_outcome,
        )
        .await
    {
        Ok(record) => emit_transfer(app, &record),
        Err(error) => log::error!("Failed to persist terminal file transfer state: {error}"),
    }
}

async fn finalize_remote_transfer_result<R: Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    transfer_id: &str,
    result: Result<RemoteTransferOutcome, RemoteTransferFailure>,
    progress: &TransferProgressSnapshot,
) {
    let latest = state.storage.get_file_transfer(transfer_id).await.ok().flatten();
    let (
        status,
        bytes_transferred,
        total_bytes,
        error,
        partial_destination,
        operation_outcome,
        operation_phase,
        source_fingerprint,
        destination_fingerprint,
    ) = match result {
        Ok(outcome) => (
            "completed",
            outcome.bytes_transferred,
            Some(outcome.total_bytes),
            None,
            None,
            outcome.operation_outcome,
            outcome.operation_phase,
            Some(outcome.source_fingerprint),
            Some(outcome.destination_fingerprint),
        ),
        Err(remote) => (
            remote.failure.status,
            progress.bytes().max(latest.as_ref().map_or(0, |record| record.bytes_transferred)),
            progress.total().or_else(|| latest.as_ref().and_then(|record| record.total_bytes)),
            Some(sanitize_error(&remote.failure.message)),
            remote.partial_destination,
            remote.operation_outcome,
            remote.operation_phase,
            remote.source_fingerprint.or_else(|| latest.as_ref().and_then(|record| record.source_fingerprint.clone())),
            remote
                .destination_fingerprint
                .or_else(|| latest.as_ref().and_then(|record| record.destination_fingerprint.clone())),
        ),
    };
    match state
        .storage
        .finish_file_remote_transfer(
            transfer_id,
            status.to_string(),
            bytes_transferred,
            total_bytes,
            optional_persisted_file_error(error),
            partial_destination,
            operation_outcome.to_string(),
            operation_phase.to_string(),
            source_fingerprint,
            destination_fingerprint,
        )
        .await
    {
        Ok(record) => emit_transfer(app, &record),
        Err(error) => log::error!("Failed to persist terminal remote transfer state: {error}"),
    }
}

fn remote_transfer_before_copy(failure: TransferFailure) -> RemoteTransferFailure {
    RemoteTransferFailure {
        failure,
        operation_outcome: "failed_before_copy",
        operation_phase: "queued",
        partial_destination: None,
        source_fingerprint: None,
        destination_fingerprint: None,
    }
}

fn remote_partial_failure(
    message: impl ToString,
    partial_destination: String,
    source_fingerprint: Option<String>,
    destination_fingerprint: Option<String>,
) -> RemoteTransferFailure {
    RemoteTransferFailure {
        failure: partial_failure(message),
        operation_outcome: "failed_with_partial_destination",
        operation_phase: "copying",
        partial_destination: Some(partial_destination),
        source_fingerprint,
        destination_fingerprint,
    }
}

async fn cleanup_remote_copy_partial(
    prepared: &PreparedFileMutation<'_>,
    partial_path: &str,
    expected_fingerprint: Option<&RemoteFileFingerprint>,
    failure: TransferFailure,
    source_fingerprint: Option<String>,
) -> RemoteTransferFailure {
    let current = prepared.fingerprint_remote_file(partial_path).await;
    match current {
        Ok(current) if expected_fingerprint.is_some_and(|expected| expected != &current) => remote_partial_failure(
            format!("{}; operation-owned partial fingerprint changed and was preserved", failure.message),
            partial_path.to_string(),
            source_fingerprint,
            Some(current.encode()),
        ),
        Ok(_) => match prepared.delete_owned_remote_partial(partial_path).await {
            Ok(()) => RemoteTransferFailure {
                failure,
                operation_outcome: "failed_before_copy",
                operation_phase: "copying",
                partial_destination: None,
                source_fingerprint,
                destination_fingerprint: None,
            },
            Err(error) => remote_partial_failure(
                format!("{}; operation-owned partial cleanup failed safely: {error}", failure.message),
                partial_path.to_string(),
                source_fingerprint,
                expected_fingerprint.map(RemoteFileFingerprint::encode),
            ),
        },
        Err(error) if error.contains("no longer exists") => RemoteTransferFailure {
            failure,
            operation_outcome: "failed_before_copy",
            operation_phase: "copying",
            partial_destination: None,
            source_fingerprint,
            destination_fingerprint: None,
        },
        Err(error) => remote_partial_failure(
            format!("{}; partial ownership verification failed safely: {error}", failure.message),
            partial_path.to_string(),
            source_fingerprint,
            expected_fingerprint.map(RemoteFileFingerprint::encode),
        ),
    }
}

impl From<TransferFailure> for UploadFailure {
    fn from(failure: TransferFailure) -> Self {
        Self { failure, partial_destination: None, abort_outcome: None, publish_outcome: None }
    }
}

fn upload_partial_path(remote_path: &str, transfer_id: &str) -> String {
    let name = format!(".dbx-upload-{transfer_id}-{}.part", Uuid::new_v4());
    remote_path.rsplit_once('/').map_or(name.clone(), |(parent, _)| format!("{parent}/{name}"))
}

fn remote_copy_partial_path(destination_path: &str, transfer_id: &str) -> String {
    let name = format!(".dbx-copy-{transfer_id}-{}.part", Uuid::new_v4());
    destination_path.rsplit_once('/').map_or(name.clone(), |(parent, _)| format!("{parent}/{name}"))
}

fn sibling_remote_path(final_path: &str, sibling_relative: &str) -> String {
    let sibling_name = sibling_relative.rsplit('/').next().unwrap_or(sibling_relative);
    final_path
        .rsplit_once('/')
        .map_or_else(|| sibling_name.to_string(), |(parent, _)| format!("{parent}/{sibling_name}"))
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
            let file_manager = app.state::<FileManagerRuntime>();
            if let Err(error) = recover_interrupted_transfer(state, file_manager.inner(), record).await {
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
        .update_file_transfer(
            transfer_id,
            status.to_string(),
            bytes_transferred,
            total_bytes,
            None,
            None,
            optional_persisted_file_error(error),
            true,
        )
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
        watched_remote(prepared, prepared.operator.stat(&prepared.remote_path), "Remote file metadata timed out")
            .await?;
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
    let mut reader_future = prepared.operator.reader_with(&prepared.remote_path).concurrent(1);
    if !prepared.uses_streaming_webhdfs_read() {
        reader_future = reader_future.chunk(DOWNLOAD_BUFFER_SIZE);
    }
    let reader = watched_remote(prepared, async { reader_future.await }, "Opening the remote file timed out").await?;
    let mut reader =
        watched_remote(prepared, reader.into_futures_async_read(..), "Preparing the remote stream timed out").await?;
    wait_at_test_remote_reader_barrier().await;
    let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_SIZE];
    let mut bytes_transferred = 0_i64;
    let mut last_progress = Instant::now();

    loop {
        let count = transfer_one_chunk(
            Some(prepared),
            &mut reader,
            &mut output,
            &mut buffer,
            IO_PROGRESS_WATCHDOG,
            &mut bytes_transferred,
            &progress_snapshot,
        )
        .await
        .map_err(|mut failure| {
            failure.message = prepared.redact_remote_error(failure.message);
            failure
        })?;
        if count == 0 {
            break;
        }
        wait_at_test_download_after_chunk_barrier().await;

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
fn install_test_download_after_chunk_barrier() -> TestRemoteReaderBarrier {
    install_test_async_barrier(&TEST_DOWNLOAD_AFTER_CHUNK_BARRIER)
}

#[cfg(test)]
async fn wait_at_test_download_after_chunk_barrier() {
    wait_at_test_async_barrier(&TEST_DOWNLOAD_AFTER_CHUNK_BARRIER).await;
}

#[cfg(not(test))]
async fn wait_at_test_download_after_chunk_barrier() {}

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
fn install_test_upload_after_chunk_barrier() -> TestRemoteReaderBarrier {
    let barrier = TestRemoteReaderBarrier {
        opened: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    *TEST_UPLOAD_AFTER_CHUNK_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(barrier.clone());
    barrier
}

#[cfg(test)]
async fn wait_at_test_upload_after_chunk_barrier() {
    let barrier = TEST_UPLOAD_AFTER_CHUNK_BARRIER
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
async fn wait_at_test_upload_after_chunk_barrier() {}

#[cfg(test)]
fn install_test_upload_after_close_barrier() -> TestRemoteReaderBarrier {
    let barrier = TestRemoteReaderBarrier {
        opened: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    *TEST_UPLOAD_AFTER_CLOSE_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(barrier.clone());
    barrier
}

#[cfg(test)]
async fn wait_at_test_upload_after_close_barrier() {
    let barrier = TEST_UPLOAD_AFTER_CLOSE_BARRIER
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
async fn wait_at_test_upload_after_close_barrier() {}

#[cfg(test)]
fn install_test_remote_copy_after_close_barrier() -> TestRemoteReaderBarrier {
    install_test_async_barrier(&TEST_REMOTE_COPY_AFTER_CLOSE_BARRIER)
}

#[cfg(test)]
fn install_test_remote_copy_after_chunk_barrier() -> TestRemoteReaderBarrier {
    install_test_async_barrier(&TEST_REMOTE_COPY_AFTER_CHUNK_BARRIER)
}

#[cfg(test)]
async fn wait_at_test_remote_copy_after_chunk_barrier() {
    wait_at_test_async_barrier(&TEST_REMOTE_COPY_AFTER_CHUNK_BARRIER).await;
}

#[cfg(not(test))]
async fn wait_at_test_remote_copy_after_chunk_barrier() {}

#[cfg(test)]
async fn wait_at_test_remote_copy_after_close_barrier() {
    wait_at_test_async_barrier(&TEST_REMOTE_COPY_AFTER_CLOSE_BARRIER).await;
}

#[cfg(not(test))]
async fn wait_at_test_remote_copy_after_close_barrier() {}

#[cfg(test)]
fn install_test_remote_rename_after_publish_barrier() -> TestRemoteReaderBarrier {
    install_test_async_barrier(&TEST_REMOTE_RENAME_AFTER_PUBLISH_BARRIER)
}

#[cfg(test)]
async fn wait_at_test_remote_rename_after_publish_barrier() {
    wait_at_test_async_barrier(&TEST_REMOTE_RENAME_AFTER_PUBLISH_BARRIER).await;
}

#[cfg(not(test))]
async fn wait_at_test_remote_rename_after_publish_barrier() {}

#[cfg(test)]
fn install_test_sftp_rename_before_dispatch_barrier() -> TestRemoteReaderBarrier {
    install_test_async_barrier(&TEST_SFTP_RENAME_BEFORE_DISPATCH_BARRIER)
}

#[cfg(test)]
fn install_test_hdfs_native_rename_before_dispatch_barrier() -> TestRemoteReaderBarrier {
    install_test_async_barrier(&TEST_HDFS_NATIVE_RENAME_BEFORE_DISPATCH_BARRIER)
}

#[cfg(test)]
pub(super) async fn wait_at_test_sftp_rename_before_dispatch_barrier() {
    wait_at_test_async_barrier(&TEST_SFTP_RENAME_BEFORE_DISPATCH_BARRIER).await;
}

#[cfg(test)]
pub(super) async fn wait_at_test_hdfs_native_rename_before_dispatch_barrier() {
    wait_at_test_async_barrier(&TEST_HDFS_NATIVE_RENAME_BEFORE_DISPATCH_BARRIER).await;
}

#[cfg(test)]
fn reset_test_remote_copy_high_water() {
    TEST_REMOTE_COPY_MAX_READ_CHUNK.store(0, Ordering::SeqCst);
    TEST_REMOTE_COPY_MAX_WRITE_CHUNK.store(0, Ordering::SeqCst);
    TEST_REMOTE_COPY_MAX_RELAY_PAYLOAD.store(0, Ordering::SeqCst);
}

#[cfg(test)]
fn record_test_remote_copy_read(bytes: usize) {
    TEST_REMOTE_COPY_MAX_READ_CHUNK.fetch_max(bytes, Ordering::SeqCst);
    TEST_REMOTE_COPY_MAX_RELAY_PAYLOAD.fetch_max(bytes, Ordering::SeqCst);
}

#[cfg(not(test))]
fn record_test_remote_copy_read(_bytes: usize) {}

#[cfg(test)]
fn record_test_remote_copy_write(bytes: usize) {
    TEST_REMOTE_COPY_MAX_WRITE_CHUNK.fetch_max(bytes, Ordering::SeqCst);
    TEST_REMOTE_COPY_MAX_RELAY_PAYLOAD.fetch_max(bytes, Ordering::SeqCst);
}

#[cfg(not(test))]
fn record_test_remote_copy_write(_bytes: usize) {}

#[cfg(test)]
fn test_remote_copy_high_water() -> (usize, usize, usize) {
    (
        TEST_REMOTE_COPY_MAX_READ_CHUNK.load(Ordering::SeqCst),
        TEST_REMOTE_COPY_MAX_WRITE_CHUNK.load(Ordering::SeqCst),
        TEST_REMOTE_COPY_MAX_RELAY_PAYLOAD.load(Ordering::SeqCst),
    )
}

#[cfg(test)]
fn install_test_async_barrier(
    slot: &std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>>,
) -> TestRemoteReaderBarrier {
    let barrier = TestRemoteReaderBarrier {
        opened: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    *slot.get_or_init(|| Mutex::new(None)).lock().unwrap_or_else(|error| error.into_inner()) = Some(barrier.clone());
    barrier
}

#[cfg(test)]
async fn wait_at_test_async_barrier(slot: &std::sync::OnceLock<Mutex<Option<TestRemoteReaderBarrier>>>) {
    let barrier = slot.get_or_init(|| Mutex::new(None)).lock().unwrap_or_else(|error| error.into_inner()).take();
    if let Some(barrier) = barrier {
        barrier.opened.notify_one();
        barrier.release.notified().await;
    }
}

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
    prepared: Option<&PreparedFileOperation>,
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
        .map_err(|error| {
            let message = error.to_string();
            remote_failure(prepared.map_or(message.clone(), |prepared| prepared.redact_remote_error(message)))
        })?;
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
    prepared: &PreparedFileOperation,
    future: impl std::future::Future<Output = Result<T, opendal::Error>>,
    timeout_message: &'static str,
) -> Result<T, TransferFailure> {
    tokio::time::timeout(IO_PROGRESS_WATCHDOG, future)
        .await
        .map_err(|_| remote_failure(timeout_message))?
        .map_err(|error| remote_failure(prepared.redact_operator_error(error)))
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

async fn validate_local_source(path: &Path) -> Result<ValidatedLocalSource, String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || validate_local_source_sync(&path)).await.map_err(|error| error.to_string())?
}

fn validate_local_source_path(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("Local upload source must be an absolute path".to_string());
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::CurDir | std::path::Component::ParentDir))
    {
        return Err("Local upload source cannot contain '.' or '..' path segments".to_string());
    }
    if path.file_name().is_none() {
        return Err("Local upload source must name a file".to_string());
    }
    Ok(path.to_path_buf())
}

fn validate_local_source_sync(path: &Path) -> Result<ValidatedLocalSource, String> {
    let path = validate_local_source_path(path)?;
    let parent = path.parent().ok_or_else(|| "Local upload source parent is required".to_string())?;
    let name = single_file_name(&path, "Local upload source")?;
    let directory = open_absolute_directory_nofollow(parent)
        .map_err(|error| format!("Local upload source parent is unavailable or unsafe: {error}"))?;
    let path_metadata =
        directory.symlink_metadata(&name).map_err(|error| format!("Local upload source is unavailable: {error}"))?;
    if path_metadata.file_type().is_symlink() {
        return Err("Local upload source cannot be a symbolic link".to_string());
    }
    if !path_metadata.is_file() {
        return Err("Local upload source must be a regular file".to_string());
    }
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = directory
        .open_with(&name, &options)
        .map_err(|error| format!("Failed to open upload source safely: {error}"))?;
    let opened_metadata =
        file.metadata().map_err(|error| format!("Failed to inspect the opened upload source: {error}"))?;
    if metadata_identity(&path_metadata) != metadata_identity(&opened_metadata) {
        return Err("Local upload source changed while it was being opened".to_string());
    }
    let total_bytes =
        i64::try_from(opened_metadata.len()).map_err(|_| "Local upload source is too large".to_string())?;
    let directory_identity =
        directory_identity(&directory).map_err(|error| format!("Failed to identify upload source parent: {error}"))?;
    let file = file.into_std();
    let verification_file = Arc::new(
        file.try_clone().map_err(|error| format!("Failed to retain upload source verification handle: {error}"))?,
    );
    let fingerprint = source_fingerprint_for_open_file(&file)?;
    Ok(ValidatedLocalSource {
        path,
        directory_identity,
        identity: metadata_identity(&opened_metadata),
        fingerprint,
        total_bytes,
        file,
        verification_file,
    })
}

fn verify_upload_source_unchanged(proof: &UploadSourceProof, bytes_transferred: i64) -> Result<(), String> {
    let parent = proof.path.parent().ok_or_else(|| "Local upload source parent is required".to_string())?;
    let name = single_file_name(&proof.path, "Local upload source")?;
    let directory = open_absolute_directory_nofollow(parent)
        .map_err(|error| format!("Local upload source parent changed or became unsafe: {error}"))?;
    if directory_identity(&directory).map_err(|error| format!("Failed to verify upload source parent: {error}"))?
        != proof.directory_identity
    {
        return Err("Upload source parent changed while the transfer was running".to_string());
    }
    let path_metadata = directory.symlink_metadata(&name).map_err(|error| {
        format!("Local upload source changed or disappeared while the transfer was running: {error}")
    })?;
    let final_fingerprint = source_fingerprint_for_open_file(&proof.verification_file)?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || metadata_identity(&path_metadata) != proof.identity
        || bytes_transferred != proof.total_bytes
        || final_fingerprint != proof.fingerprint
    {
        return Err("Upload source changed while the transfer was running; the remote partial will not be published"
            .to_string());
    }
    Ok(())
}

fn source_fingerprint_for_open_file(file: &std::fs::File) -> Result<String, String> {
    let metadata =
        file.metadata().map_err(|error| format!("Failed to inspect the opened upload source handle: {error}"))?;
    let change_token = source_change_token_for_open_file(file, &metadata)?;
    Ok(source_fingerprint(metadata.len(), metadata.modified().ok(), change_token))
}

fn source_fingerprint(length: u64, modified: Option<std::time::SystemTime>, change_token: String) -> String {
    let modified = modified
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|value| format!("{}:{}", value.as_secs(), value.subsec_nanos()))
        .unwrap_or_else(|| "unknown".to_string());
    format!("{length}:{modified}:{change_token}")
}

#[cfg(unix)]
fn source_change_token_for_open_file(_file: &std::fs::File, metadata: &std::fs::Metadata) -> Result<String, String> {
    use std::os::unix::fs::MetadataExt;

    Ok(format!("ctime:{}:{}", metadata.ctime(), metadata.ctime_nsec()))
}

#[cfg(windows)]
fn source_change_token_for_open_file(file: &std::fs::File, _metadata: &std::fs::Metadata) -> Result<String, String> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{FileBasicInfo, GetFileInformationByHandleEx, FILE_BASIC_INFO};

    let mut basic_info = FILE_BASIC_INFO::default();
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileBasicInfo,
            (&mut basic_info as *mut FILE_BASIC_INFO).cast(),
            u32::try_from(std::mem::size_of::<FILE_BASIC_INFO>())
                .map_err(|_| "Windows FILE_BASIC_INFO size is not representable".to_string())?,
        )
    };
    if result == 0 {
        return Err(format!(
            "Failed to read the upload source Windows change token: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(format!("change_time:{}", basic_info.ChangeTime))
}

#[cfg(not(any(unix, windows)))]
fn source_change_token_for_open_file(_file: &std::fs::File, _metadata: &std::fs::Metadata) -> Result<String, String> {
    Err("Upload source change-token verification is unsupported on this platform".to_string())
}

fn validate_local_upload_authorization(scope: &tauri::fs::Scope, path: &Path) -> Result<(), String> {
    if scope.is_allowed(path) {
        Ok(())
    } else {
        Err("Local upload source is not authorized; choose it with the open dialog".to_string())
    }
}

async fn recover_interrupted_transfer(
    state: &AppState,
    file_manager: &FileManagerRuntime,
    transfer: &FileTransferStorageRecord,
) -> Result<(), String> {
    if transfer.direction == "upload" {
        let owned_partial =
            transfer.temp_path.as_deref().filter(|path| is_owned_upload_partial(transfer, path)).map(str::to_string);
        let (status, partial_destination, abort_outcome, publish_outcome, error) = if transfer.status == "publishing"
            && owned_partial.is_some()
        {
            match file_manager.reconcile_interrupted_upload(state, transfer).await {
                Ok(UploadPublishResolution { state: UploadPublishState::PartialSource, detail }) => (
                    "partial",
                    owned_partial,
                    Some("not_applicable_after_close".to_string()),
                    Some("partial_source".to_string()),
                    format!("Interrupted upload publish left its operation-owned source partial: {detail}"),
                ),
                Ok(UploadPublishResolution { state: UploadPublishState::PartialTarget, detail }) => (
                    "partial",
                    Some(transfer.remote_path.clone()),
                    Some("not_applicable_after_close".to_string()),
                    Some("partial_target_unproven".to_string()),
                    format!("Interrupted upload publish left an unproven target: {detail}"),
                ),
                Ok(UploadPublishResolution { state: UploadPublishState::Unknown, detail }) => (
                    "partial",
                    None,
                    Some("not_applicable_after_close".to_string()),
                    Some("unknown".to_string()),
                    format!("Interrupted upload publish outcome is unknown: {detail}"),
                ),
                Ok(UploadPublishResolution { state: UploadPublishState::Completed, .. }) => {
                    unreachable!("read-only interrupted publish reconciliation cannot prove ownership")
                }
                Err(error) => (
                    "partial",
                    owned_partial,
                    Some("not_applicable_after_close".to_string()),
                    Some("unknown".to_string()),
                    format!("Interrupted upload publish could not be reconciled safely: {error}"),
                ),
            }
        } else if let Some(path) = owned_partial {
            (
                    "partial",
                    Some(path.clone()),
                    Some("not_attempted_after_application_exit".to_string()),
                    None,
                    format!(
                        "The application exited before the upload completed; operation-owned remote partial may remain at {path}"
                    ),
                )
        } else {
            (
                "failed",
                None,
                Some("not_attempted_after_application_exit".to_string()),
                None,
                "The application exited before the upload created an owned remote partial".to_string(),
            )
        };
        state
            .storage
            .finish_file_upload_transfer(
                &transfer.id,
                status.to_string(),
                transfer.bytes_transferred,
                transfer.total_bytes,
                Some(persisted_file_error(error)),
                partial_destination,
                abort_outcome,
                publish_outcome,
            )
            .await?;
        return Ok(());
    }
    if matches!(transfer.direction.as_str(), "copy" | "rename") {
        let phase = transfer.operation_phase.as_deref().unwrap_or("queued");
        if transfer.direction == "rename" && matches!(phase, "published_before_delete" | "delete_uncertain") {
            let expected_revision = transfer
                .connection_revision
                .ok_or_else(|| "Interrupted rename has no durable connection revision".to_string())?;
            let prepared = file_manager
                .prepare_file_mutation_operation(
                    state,
                    &transfer.connection_id,
                    &transfer.remote_path,
                    expected_revision,
                )
                .await?;
            let server_side_copy = prepared.uses_server_side_copy();
            let source = prepared.fingerprint_remote_file(&transfer.remote_path).await;
            let destination = prepared.fingerprint_remote_file(&transfer.local_path).await;
            let source_relay_hash = persisted_relay_hash(transfer.source_fingerprint.as_deref());
            let destination_relay_hash = persisted_relay_hash(transfer.destination_fingerprint.as_deref());
            let relay_hashes_match =
                server_side_copy || source_relay_hash.is_some_and(|source| Some(source) == destination_relay_hash);
            let destination_matches = relay_hashes_match
                && destination.as_ref().is_ok_and(|fingerprint| {
                    transfer
                        .destination_fingerprint
                        .as_deref()
                        .is_some_and(|expected| persisted_remote_fingerprint_matches(expected, fingerprint))
                });
            let source_matches = relay_hashes_match
                && source.as_ref().is_ok_and(|fingerprint| {
                    transfer
                        .source_fingerprint
                        .as_deref()
                        .is_some_and(|expected| persisted_remote_fingerprint_matches(expected, fingerprint))
                });
            let source_missing = source.as_ref().is_err_and(|error| error.contains("no longer exists"));
            let mut destination_verification_error = None;
            let destination_content_matches = if source_missing && destination_matches && server_side_copy {
                true
            } else if source_missing && destination_matches {
                let cancellation = CancellationToken::new();
                match verify_remote_content(&prepared, &transfer.local_path, &cancellation).await {
                    Ok(verified) => transfer
                        .destination_fingerprint
                        .as_deref()
                        .is_some_and(|expected| persisted_verified_remote_content_matches(expected, &verified)),
                    Err(failure) => {
                        if failure.invalidate_operator {
                            file_manager.evict_revision(&transfer.connection_id, prepared.revision);
                        }
                        destination_verification_error = Some(failure.message);
                        false
                    }
                }
            } else {
                false
            };
            let (status, outcome, terminal_phase, error) = if source_missing && destination_content_matches {
                ("completed", "completed", "completed", None)
            } else {
                let detail = if let Some(error) = destination_verification_error {
                    format!(
                        "Interrupted rename destination content could not be verified; completion was refused: {error}"
                    )
                } else if source_missing && destination_matches {
                    "Interrupted rename destination content no longer matches the durable copy; completion was refused"
                        .to_string()
                } else if source_matches && destination_matches {
                    "Rename copy was published before exit; source deletion requires explicit fingerprint-checked retry"
                        .to_string()
                } else {
                    "Interrupted rename fingerprints no longer match; source was not deleted".to_string()
                };
                ("partial", "copied_source_delete_failed", "delete_uncertain", Some(detail))
            };
            state
                .storage
                .finish_file_remote_transfer(
                    &transfer.id,
                    status.to_string(),
                    transfer.bytes_transferred,
                    transfer.total_bytes,
                    optional_persisted_file_error(error),
                    (status == "partial").then(|| transfer.local_path.clone()),
                    outcome.to_string(),
                    terminal_phase.to_string(),
                    transfer.source_fingerprint.clone(),
                    transfer.destination_fingerprint.clone(),
                )
                .await?;
            return Ok(());
        }

        let owned_partial = transfer
            .temp_path
            .as_deref()
            .filter(|path| is_owned_remote_copy_partial(transfer, path))
            .map(str::to_string);
        // S3 writes directly to the final key during the copying phase. Its
        // durable fingerprint intentionally has no FTP relay hash.
        let direct_destination_may_exist = phase == "copying"
            && transfer
                .source_fingerprint
                .as_deref()
                .is_some_and(|fingerprint| persisted_relay_hash(Some(fingerprint)).is_none());
        let partial_destination = owned_partial.or_else(|| {
            (transfer.status == "publishing" || direct_destination_may_exist).then(|| transfer.local_path.clone())
        });
        let (status, outcome, error) = if let Some(path) = partial_destination.as_ref() {
            (
                "partial",
                "failed_with_partial_destination",
                format!(
                    "The application exited during remote {}; the operation-owned or published destination was preserved at {path}",
                    transfer.direction
                ),
            )
        } else {
            (
                "failed",
                "failed_before_copy",
                format!("The application exited before remote {} published a destination", transfer.direction),
            )
        };
        state
            .storage
            .finish_file_remote_transfer(
                &transfer.id,
                status.to_string(),
                transfer.bytes_transferred,
                transfer.total_bytes,
                Some(persisted_file_error(error)),
                partial_destination,
                outcome.to_string(),
                phase.to_string(),
                transfer.source_fingerprint.clone(),
                transfer.destination_fingerprint.clone(),
            )
            .await?;
        return Ok(());
    }
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
                    Some(RedactedFileText::from_static("Publishing transfer has no durable temporary-file identity")),
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
                Some(RedactedFileText::from_static(
                    "Interrupted transfer has an invalid temporary-file path; no file was removed",
                )),
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
                    Some(RedactedFileText::from_static("The application exited before publishing the download")),
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
                    Some(persisted_file_error(format!("Interrupted transfer reconciliation failed safely: {error}"))),
                    true,
                )
                .await?;
        }
    }
    Ok(())
}

fn is_owned_upload_partial(transfer: &FileTransferStorageRecord, partial_path: &str) -> bool {
    if transfer.direction != "upload" {
        return false;
    }
    let target_parent = transfer.remote_path.rsplit_once('/').map(|(parent, _)| parent);
    let partial_parent = partial_path.rsplit_once('/').map(|(parent, _)| parent);
    if target_parent != partial_parent {
        return false;
    }
    let name = partial_path.rsplit('/').next().unwrap_or(partial_path);
    name.starts_with(&format!(".dbx-upload-{}-", transfer.id)) && name.ends_with(".part")
}

fn is_owned_remote_copy_partial(transfer: &FileTransferStorageRecord, partial_path: &str) -> bool {
    if !matches!(transfer.direction.as_str(), "copy" | "rename") {
        return false;
    }
    let target_parent = transfer.local_path.rsplit_once('/').map(|(parent, _)| parent);
    let partial_parent = partial_path.rsplit_once('/').map(|(parent, _)| parent);
    if target_parent != partial_parent {
        return false;
    }
    let name = partial_path.rsplit('/').next().unwrap_or(partial_path);
    name.starts_with(&format!(".dbx-copy-{}-", transfer.id)) && name.ends_with(".part")
}

fn persisted_remote_fingerprint_matches(persisted: &str, observed: &RemoteFileFingerprint) -> bool {
    match persisted.rsplit_once(";relay_sha256:") {
        Some((remote, relay_hash)) => {
            remote == observed.encode()
                && relay_hash.len() == 64
                && relay_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        }
        None => persisted == observed.encode(),
    }
}

fn persisted_verified_remote_content_matches(persisted: &str, observed: &VerifiedRemoteContent) -> bool {
    persisted_remote_fingerprint_matches(persisted, &observed.fingerprint)
        && persisted_relay_hash(Some(persisted)) == Some(observed.sha256.as_str())
}

fn persisted_relay_hash(persisted: Option<&str>) -> Option<&str> {
    persisted
        .and_then(|fingerprint| fingerprint.rsplit_once(";relay_sha256:"))
        .map(|(_, relay_hash)| relay_hash)
        .filter(|relay_hash| relay_hash.len() == 64 && relay_hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
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

fn upload_cancelled_failure() -> TransferFailure {
    TransferFailure { status: "cancelled", message: "Upload cancelled".to_string(), invalidate_operator: false }
}

fn upload_cancelled_active_failure() -> TransferFailure {
    TransferFailure { status: "cancelled", message: "Upload cancelled".to_string(), invalidate_operator: true }
}

fn remote_failure(message: impl ToString) -> TransferFailure {
    TransferFailure { status: "failed", message: message.to_string(), invalidate_operator: true }
}

fn partial_failure(message: impl ToString) -> TransferFailure {
    TransferFailure { status: "partial", message: message.to_string(), invalidate_operator: true }
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

fn persisted_file_error(message: String) -> RedactedFileText {
    FileSecretRedactor::default().redact(message)
}

fn optional_persisted_file_error(message: Option<String>) -> Option<RedactedFileText> {
    message.map(persisted_file_error)
}

#[cfg(test)]
mod tests {
    use super::super::file_manager::{delete_file_connection, FileSecretStorageTestExt, TEST_FILE_SECRET_KEY};
    use super::*;
    use dbx_core::storage::Storage;
    use std::collections::BTreeSet;
    use std::pin::Pin;
    use std::process::{Command, Stdio};
    use std::task::{Context, Poll};

    async fn ensure_test_connection(storage: &Storage, id: &str) -> i64 {
        if let Some(record) = storage.load_file_connection(id).await.unwrap() {
            return record.revision;
        }
        use super::super::file_manager::{password_scope, FtpConnectionConfig};
        let config = FileConnectionConfig::Ftp(FtpConnectionConfig {
            endpoint: "ftp://127.0.0.1:21".to_string(),
            root: "/".to_string(),
            username: "dbx".to_string(),
        });
        let scope = password_scope(&config).unwrap();
        storage
            .save_file_connection(
                id.to_string(),
                "Transfer test".to_string(),
                "ftp".to_string(),
                serde_json::to_string(&config).unwrap(),
                None,
                scope,
                false,
                None,
            )
            .await
            .unwrap()
            .revision
    }

    struct FailedReader;

    impl FuturesAsyncRead for FailedReader {
        fn poll_read(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &mut [u8]) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "injected disconnect")))
        }
    }

    #[test]
    fn cancel_connection_cancels_queued_and_running_transfers_only_for_target_connection() {
        let runtime = FileTransferRuntime::default();
        let queued = CancellationToken::new();
        let running = CancellationToken::new();
        let other = CancellationToken::new();
        runtime.register("queued".into(), "ftp-1".into(), queued.clone());
        runtime.register_upload("running".into(), "ftp-1".into(), running.clone()).unwrap();
        runtime.register("other".into(), "ftp-2".into(), other.clone());

        assert_eq!(runtime.cancel_connection("ftp-1"), 2);
        assert!(queued.is_cancelled());
        assert!(running.is_cancelled());
        assert!(!other.is_cancelled());
        assert_eq!(runtime.cancel_connection("missing"), 0);
        assert!(!other.is_cancelled());
    }

    #[tokio::test]
    async fn transfer_start_and_delete_are_linearized_on_both_sides_of_conditional_insert() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        ensure_test_connection(&storage, "delete-before-insert").await;
        ensure_test_connection(&storage, "insert-before-delete").await;
        let state = Arc::new(AppState::new(storage));
        let app = tauri::test::mock_builder()
            .manage(state.clone())
            .manage(FileManagerRuntime::default())
            .manage(FileTransferRuntime::default())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();

        let before_insert = install_test_transfer_before_insert_barrier();
        let start_handle = app.handle().clone();
        let start_task = tokio::spawn(async move {
            let state = start_handle.state::<Arc<AppState>>();
            let runtime = start_handle.state::<FileTransferRuntime>();
            start_remote_transfer_inner(
                start_handle.clone(),
                state.inner(),
                runtime.inner(),
                StartRemoteTransferInput {
                    connection_id: "delete-before-insert".to_string(),
                    source_path: "source.bin".to_string(),
                    destination_path: "destination.bin".to_string(),
                    policy: RemoteMutationPolicy::Replace { confirmed: true },
                },
                "copy",
            )
            .await
        });
        before_insert.opened.notified().await;
        delete_file_connection(
            app.state::<Arc<AppState>>(),
            app.state::<FileManagerRuntime>(),
            app.state::<FileTransferRuntime>(),
            "delete-before-insert".to_string(),
        )
        .await
        .unwrap();
        before_insert.release.notify_one();
        let error = start_task.await.unwrap().unwrap_err();
        assert!(error.contains("changed or was deleted"), "{error}");
        clear_test_transfer_before_insert_barrier();
        assert!(state.storage.list_file_transfers(Some("delete-before-insert"), 100).await.unwrap().is_empty());

        let inserted = start_remote_transfer_inner(
            app.handle().clone(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            StartRemoteTransferInput {
                connection_id: "insert-before-delete".to_string(),
                source_path: "source.bin".to_string(),
                destination_path: "destination.bin".to_string(),
                policy: RemoteMutationPolicy::Replace { confirmed: true },
            },
            "rename",
        )
        .await
        .unwrap();
        assert!(state.storage.get_file_transfer(&inserted.transfer_id).await.unwrap().is_some());
        delete_file_connection(
            app.state::<Arc<AppState>>(),
            app.state::<FileManagerRuntime>(),
            app.state::<FileTransferRuntime>(),
            "insert-before-delete".to_string(),
        )
        .await
        .unwrap();
        let terminal = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let record = state.storage.get_file_transfer(&inserted.transfer_id).await.unwrap().unwrap();
                if record.completed_at.is_some() {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("inserted transfer must reach a durable terminal state after delete cancellation");
        assert!(matches!(terminal.status.as_str(), "cancelled" | "failed" | "partial"));
        assert_eq!(terminal.connection_revision, Some(1));
        assert!(app.state::<FileTransferRuntime>().cancellation(&inserted.transfer_id).is_none());
    }

    struct StalledReader;

    impl FuturesAsyncRead for StalledReader {
        fn poll_read(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &mut [u8]) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }

    #[derive(Clone, Copy)]
    enum FakeAbortBehavior {
        Success,
        Unsupported,
        Failed,
        Stalled,
    }

    struct FakeAbortableUpload {
        behavior: FakeAbortBehavior,
    }

    impl AbortableUpload for FakeAbortableUpload {
        fn abort(&mut self) -> Pin<Box<dyn Future<Output = Result<(), UploadAbortError>> + Send + '_>> {
            Box::pin(async move {
                match self.behavior {
                    FakeAbortBehavior::Success => Ok(()),
                    FakeAbortBehavior::Unsupported => Err(UploadAbortError::Unsupported),
                    FakeAbortBehavior::Failed => Err(UploadAbortError::Failed("injected abort failure".to_string())),
                    FakeAbortBehavior::Stalled => std::future::pending().await,
                }
            })
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

    #[test]
    fn upload_policy_is_required_and_rejects_replace_or_false_safety_claims() {
        let valid = serde_json::json!({
            "connectionId": "ftp-1",
            "localPath": "/tmp/source.bin",
            "remotePath": "source.bin",
            "policy": {
                "mode": "best_effort_no_clobber",
                "atomicNoClobber": false,
                "externalToctouRisk": true
            }
        });
        let parsed: StartUploadInput = serde_json::from_value(valid.clone()).unwrap();
        assert_eq!(parsed.policy, UploadPolicy::best_effort_no_clobber());
        parsed.policy.validate().unwrap();

        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove("policy");
        assert!(serde_json::from_value::<StartUploadInput>(missing).is_err());

        let mut replace = valid.clone();
        replace["policy"]["mode"] = serde_json::json!("replace");
        assert!(serde_json::from_value::<StartUploadInput>(replace).is_err());

        let mut unknown = valid.clone();
        unknown["policy"]["overwrite"] = serde_json::json!(true);
        assert!(serde_json::from_value::<StartUploadInput>(unknown).is_err());

        let mut false_atomic_claim = valid;
        false_atomic_claim["policy"]["atomicNoClobber"] = serde_json::json!(true);
        let parsed: StartUploadInput = serde_json::from_value(false_atomic_claim).unwrap();
        assert!(parsed.policy.validate().unwrap_err().contains("atomicNoClobber=false"));
    }

    #[test]
    fn remote_mutation_input_rejects_cross_connection_fields_and_unconfirmed_replace() {
        let valid = serde_json::json!({
            "connectionId": "ftp-1",
            "sourcePath": "source.bin",
            "destinationPath": "destination.bin",
            "policy": {
                "mode": "best_effort_no_clobber",
                "atomicNoClobber": false,
                "externalToctouRisk": true
            }
        });
        let parsed: StartRemoteTransferInput = serde_json::from_value(valid.clone()).unwrap();
        parsed.policy.validate().unwrap();

        let mut cross_connection = valid.clone();
        cross_connection["destinationConnectionId"] = serde_json::json!("ftp-2");
        assert!(serde_json::from_value::<StartRemoteTransferInput>(cross_connection).is_err());

        let mut unknown_policy = valid.clone();
        unknown_policy["policy"]["overwrite"] = serde_json::json!(true);
        assert!(serde_json::from_value::<StartRemoteTransferInput>(unknown_policy).is_err());

        let mut missing_policy = valid.clone();
        missing_policy.as_object_mut().unwrap().remove("policy");
        assert!(serde_json::from_value::<StartRemoteTransferInput>(missing_policy).is_err());

        let mut replace = valid;
        replace["policy"] = serde_json::json!({"mode": "replace", "confirmed": false});
        let parsed: StartRemoteTransferInput = serde_json::from_value(replace.clone()).unwrap();
        assert!(parsed.policy.validate().unwrap_err().contains("explicit confirmation"));
        replace["policy"]["confirmed"] = serde_json::json!(true);
        serde_json::from_value::<StartRemoteTransferInput>(replace).unwrap().policy.validate().unwrap();
    }

    #[tokio::test]
    async fn webhdfs_replace_is_rejected_before_creating_or_starting_a_transfer() {
        use super::super::file_manager::{
            save_file_connection, FileConnectionInput, FileConnectionSecrets, HdfsConnectionConfig,
        };
        use super::super::file_manager_webhdfs::{WebhdfsAuthentication, WebhdfsConnectionConfig, WebhdfsWriteOptions};

        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        let state = Arc::new(AppState::new(storage));
        let app = tauri::test::mock_builder()
            .manage(state.clone())
            .manage(FileManagerRuntime::default())
            .manage(FileTransferRuntime::default())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let connection = save_file_connection(
            app.state::<Arc<AppState>>(),
            app.state::<FileManagerRuntime>(),
            FileConnectionInput {
                id: Some("webhdfs-replace-guard".to_string()),
                expected_revision: None,
                name: "WebHDFS replace guard".to_string(),
                config: FileConnectionConfig::Hdfs(HdfsConnectionConfig::Webhdfs(WebhdfsConnectionConfig {
                    endpoint: "http://127.0.0.1:9870".to_string(),
                    root: "/".to_string(),
                    authentication: WebhdfsAuthentication::Simple,
                    user_name: "dbx".to_string(),
                    disable_list_batch: false,
                    allowed_data_node_origins: vec!["http://localhost:9864".to_string()],
                    data_node_hostname_mapping: Default::default(),
                    tls_ca_certificate_path: None,
                    proxy_url: None,
                    proxy_bypass: None,
                    allow_tls_downgrade: false,
                    connect_timeout_seconds: 10,
                    control_timeout_seconds: 30,
                    idle_timeout_seconds: 30,
                    chunk_size_mib: 4,
                    write_options: WebhdfsWriteOptions::default(),
                })),
                secrets: Some(FileConnectionSecrets {
                    clear_webhdfs_credentials: Some(true),
                    ..FileConnectionSecrets::default()
                }),
            },
        )
        .await
        .unwrap();

        let result = start_remote_transfer_inner(
            app.handle().clone(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            StartRemoteTransferInput {
                connection_id: connection.id.clone(),
                source_path: "source.bin".to_string(),
                destination_path: "destination.bin".to_string(),
                policy: RemoteMutationPolicy::Replace { confirmed: true },
            },
            "copy",
        )
        .await;
        let Err(error) = result else {
            panic!("WebHDFS Replace must fail before the worker starts");
        };
        assert_eq!(error, WEBHDFS_REPLACE_UNSUPPORTED);
        assert!(state.storage.list_file_transfers(Some(&connection.id), 100).await.unwrap().is_empty());
    }

    #[test]
    fn persisted_remote_fingerprint_accepts_s3_fields_and_validates_optional_ftp_relay_hash() {
        let observed =
            RemoteFileFingerprint { size: 16, modified: "2026-07-25T00:00:00Z".to_string(), etag: None, version: None };
        let hash = "a".repeat(64);
        let persisted = format!("{};relay_sha256:{hash}", observed.encode());
        assert!(persisted_remote_fingerprint_matches(&persisted, &observed));
        assert_eq!(persisted_relay_hash(Some(&persisted)), Some(hash.as_str()));
        assert!(!persisted_remote_fingerprint_matches(
            &format!("{}0;relay_sha256:{hash}", observed.encode()),
            &observed
        ));
        assert!(persisted_remote_fingerprint_matches(&observed.encode(), &observed));
        assert!(!persisted_remote_fingerprint_matches(&format!("{}0", observed.encode()), &observed));
        assert!(!persisted_remote_fingerprint_matches(&format!("{};relay_sha256:short", observed.encode()), &observed));
    }

    #[tokio::test]
    async fn remote_path_locks_serialize_reversed_pairs_and_prune_registry() {
        let runtime = Arc::new(FileTransferRuntime::default());
        let first = runtime.lock_remote_paths("ftp-1", "source.bin", "destination.bin").await;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let second_runtime = runtime.clone();
        let second = tokio::spawn(async move {
            entered_tx.send(()).unwrap();
            second_runtime.lock_remote_paths("ftp-1", "destination.bin", "source.bin").await
        });
        entered_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!second.is_finished(), "reversed source/destination paths must use the same locks");

        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), second).await.unwrap().unwrap();
        drop(second);
        assert!(runtime.path_locks.lock().unwrap().is_empty());

        let same_path = runtime.lock_remote_paths("ftp-1", "same.bin", "same.bin").await;
        drop(same_path);
        assert!(runtime.path_locks.lock().unwrap().is_empty());
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

    async fn recover_test_transfer(state: &AppState, transfer: &FileTransferStorageRecord) {
        recover_interrupted_transfer(state, &FileManagerRuntime::default(), transfer).await.unwrap();
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
    async fn local_upload_source_requires_absolute_regular_nofollow_file() {
        assert!(validate_local_source(Path::new("relative/file.bin")).await.unwrap_err().contains("absolute"));
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        assert!(validate_local_source(&parent).await.unwrap_err().contains("regular file"));
        let source = parent.join("source.bin");
        tokio::fs::write(&source, b"payload").await.unwrap();
        let validated = validate_local_source(&source).await.unwrap();
        assert_eq!(validated.path, source);
        assert_eq!(validated.total_bytes, 7);
        assert_eq!(validated.directory_identity, canonical_directory_identity(&parent));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let link = parent.join("source-link.bin");
            symlink(&source, &link).unwrap();
            assert!(validate_local_source(&link).await.unwrap_err().contains("symbolic link"));
        }
    }

    #[test]
    fn local_upload_source_requires_dialog_scope_authorization() {
        let app = tauri::test::mock_builder()
            .plugin(tauri_plugin_fs::init())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().canonicalize().unwrap().join("source.bin");
        std::fs::write(&source, b"payload").unwrap();
        let scope = app.fs_scope();
        assert!(validate_local_upload_authorization(&scope, &source).unwrap_err().contains("not authorized"));
        scope.allow_file(&source).unwrap();
        assert!(validate_local_upload_authorization(&scope, &source).is_ok());
        drop(app);
    }

    #[tokio::test]
    async fn opened_upload_handle_remains_bound_and_path_replacement_is_rejected() {
        use std::io::Read;

        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let source = parent.join("source.bin");
        let moved = parent.join("opened-source.bin");
        tokio::fs::write(&source, b"original").await.unwrap();
        let validated = validate_local_source(&source).await.unwrap();

        tokio::fs::rename(&source, &moved).await.unwrap();
        tokio::fs::write(&source, b"attacker").await.unwrap();
        let mut opened = &validated.file;
        let mut bytes = Vec::new();
        opened.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"original");
        assert!(verify_upload_source_unchanged(&validated.proof(), 8).unwrap_err().contains("changed"));
    }

    #[tokio::test]
    async fn upload_source_truncation_growth_and_modification_are_rejected() {
        async fn assert_changed(initial: &[u8], replacement: &[u8], transferred: i64) {
            let directory = tempfile::tempdir().unwrap();
            let source = directory.path().canonicalize().unwrap().join("source.bin");
            tokio::fs::write(&source, initial).await.unwrap();
            let validated = validate_local_source(&source).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
            tokio::fs::write(&source, replacement).await.unwrap();
            assert!(verify_upload_source_unchanged(&validated.proof(), transferred).unwrap_err().contains("changed"));
        }

        assert_changed(b"original", b"short", 5).await;
        assert_changed(b"original", b"original-longer", 15).await;
        assert_changed(b"original", b"modified", 8).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_source_same_size_rewrite_is_rejected_after_mtime_is_restored() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().canonicalize().unwrap().join("source.bin");
        tokio::fs::write(&source, b"original").await.unwrap();
        let validated = validate_local_source(&source).await.unwrap();
        let original_modified = validated.file.metadata().unwrap().modified().unwrap();

        tokio::time::sleep(Duration::from_millis(2)).await;
        tokio::fs::write(&source, b"modified").await.unwrap();
        std::fs::File::options()
            .write(true)
            .open(&source)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(original_modified))
            .unwrap();

        assert_eq!(validated.file.metadata().unwrap().modified().unwrap(), original_modified);
        assert!(verify_upload_source_unchanged(&validated.proof(), 8).unwrap_err().contains("changed"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_upload_source_change_token_comes_from_the_open_file_handle() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        std::fs::write(&source, b"original").unwrap();
        let file = std::fs::File::open(&source).unwrap();
        let metadata = file.metadata().unwrap();
        let initial = source_change_token_for_open_file(&file, &metadata).unwrap();

        std::thread::sleep(Duration::from_millis(2));
        std::fs::write(&source, b"modified").unwrap();
        let changed = source_change_token_for_open_file(&file, &file.metadata().unwrap()).unwrap();

        assert_ne!(initial, changed);
        assert!(initial.starts_with("change_time:"));
    }

    #[tokio::test]
    async fn upload_abort_control_flow_covers_success_failure_unsupported_and_timeout() {
        let cleanup_called = Arc::new(AtomicBool::new(false));
        let cleanup_probe = cleanup_called.clone();
        let succeeded = abort_upload_control_flow(
            FakeAbortableUpload { behavior: FakeAbortBehavior::Success },
            "dir/.dbx-upload-transfer-random.part",
            upload_cancelled_active_failure(),
            Duration::from_millis(10),
            move || async move {
                cleanup_probe.store(true, Ordering::Release);
                Ok(())
            },
        )
        .await;
        assert_eq!(succeeded.failure.status, "cancelled");
        assert_eq!(succeeded.partial_destination, None);
        assert_eq!(succeeded.abort_outcome.as_deref(), Some("succeeded"));
        assert!(!cleanup_called.load(Ordering::Acquire));

        let cleanup_called = Arc::new(AtomicBool::new(false));
        let cleanup_probe = cleanup_called.clone();
        let unsupported_cleaned = abort_upload_control_flow(
            FakeAbortableUpload { behavior: FakeAbortBehavior::Unsupported },
            "dir/.dbx-upload-transfer-random.part",
            upload_cancelled_active_failure(),
            Duration::from_millis(10),
            move || async move {
                cleanup_probe.store(true, Ordering::Release);
                Ok(())
            },
        )
        .await;
        assert_eq!(unsupported_cleaned.failure.status, "cancelled");
        assert_eq!(unsupported_cleaned.partial_destination, None);
        assert_eq!(unsupported_cleaned.abort_outcome.as_deref(), Some("unsupported; operation_owned_partial_cleaned"));
        assert!(cleanup_called.load(Ordering::Acquire));

        let cleanup_called = Arc::new(AtomicBool::new(false));
        let cleanup_probe = cleanup_called.clone();
        let failed_uncleaned = abort_upload_control_flow(
            FakeAbortableUpload { behavior: FakeAbortBehavior::Failed },
            "dir/.dbx-upload-transfer-random.part",
            remote_failure("write failed"),
            Duration::from_millis(10),
            move || async move {
                cleanup_probe.store(true, Ordering::Release);
                Err("injected cleanup failure".to_string())
            },
        )
        .await;
        assert_eq!(failed_uncleaned.failure.status, "partial");
        assert_eq!(failed_uncleaned.partial_destination.as_deref(), Some("dir/.dbx-upload-transfer-random.part"));
        assert_eq!(failed_uncleaned.abort_outcome.as_deref(), Some("failed: injected abort failure"));
        assert!(failed_uncleaned.failure.invalidate_operator);
        assert!(failed_uncleaned.failure.message.contains("cleanup failed safely"));
        assert!(cleanup_called.load(Ordering::Acquire));

        let cleanup_called = Arc::new(AtomicBool::new(false));
        let cleanup_probe = cleanup_called.clone();
        let timed_out = abort_upload_control_flow(
            FakeAbortableUpload { behavior: FakeAbortBehavior::Stalled },
            "dir/.dbx-upload-transfer-random.part",
            remote_failure("write timed out"),
            Duration::from_millis(1),
            move || async move {
                cleanup_probe.store(true, Ordering::Release);
                Ok(())
            },
        )
        .await;
        assert_eq!(timed_out.failure.status, "partial");
        assert_eq!(timed_out.partial_destination.as_deref(), Some("dir/.dbx-upload-transfer-random.part"));
        assert_eq!(timed_out.abort_outcome.as_deref(), Some("timed_out"));
        assert!(timed_out.failure.invalidate_operator);
        assert!(timed_out.failure.message.contains("partial was preserved"));
        assert!(!cleanup_called.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn command_contract_authorizes_starts_cancels_and_queries_without_streaming_bytes_over_ipc() {
        use super::super::file_manager::{password_scope, FileConnectionConfig, FtpConnectionConfig};

        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let storage =
            Storage::open_with_file_secret_key(&parent.join("dbx.sqlite"), TEST_FILE_SECRET_KEY).await.unwrap();
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
        let authorized_secret = parent.join("authorized-secret.bin");
        std::fs::write(&authorized_secret, b"secret").unwrap();
        let unauthorized_existing = start_upload_inner(
            app.handle().clone(),
            window.clone(),
            &state,
            runtime.inner(),
            StartUploadInput {
                connection_id: "ftp-command".to_string(),
                local_path: authorized_secret.to_string_lossy().into_owned(),
                remote_path: "secret.bin".to_string(),
                policy: UploadPolicy::best_effort_no_clobber(),
            },
        )
        .await
        .unwrap_err();
        let unauthorized_missing = start_upload_inner(
            app.handle().clone(),
            window.clone(),
            &state,
            runtime.inner(),
            StartUploadInput {
                connection_id: "ftp-command".to_string(),
                local_path: parent.join("missing-secret.bin").to_string_lossy().into_owned(),
                remote_path: "secret.bin".to_string(),
                policy: UploadPolicy::best_effort_no_clobber(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(unauthorized_existing, unauthorized_missing);
        assert!(unauthorized_existing.contains("not authorized"));

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

        let file_manager = app.state::<FileManagerRuntime>();
        let persisted =
            get_file_transfer_inner(&state, runtime.inner(), file_manager.inner(), &started.transfer_id).await.unwrap();
        assert_eq!(persisted.remote_path, "a%2Fb");
        assert_eq!(persisted.status, "queued");
        let serialized = serde_json::to_value(&persisted).unwrap();
        assert!(serialized.get("localDirectoryIdentity").is_none());
        assert!(serialized.get("tempPath").is_none());
        assert!(serialized.get("tempIdentity").is_none());

        let cancelling = cancel_file_transfer_inner(
            app.handle(),
            &state,
            runtime.inner(),
            file_manager.inner(),
            &started.transfer_id,
        )
        .await
        .unwrap();
        assert_eq!(cancelling.status, "cancelling");
        let terminal = wait_for_transfer_status(&state.storage, &started.transfer_id, &["cancelled"]).await;
        assert_eq!(terminal.bytes_transferred, 0);
        let queried =
            get_file_transfer_inner(&state, runtime.inner(), file_manager.inner(), &started.transfer_id).await.unwrap();
        assert_eq!(queried.status, "cancelled");
        assert!(list_file_transfers_inner(&state, runtime.inner(), file_manager.inner(), Some("ftp-command"))
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
            connection_revision: None,
            partial_destination: None,
            abort_outcome: None,
            publish_outcome: None,
            operation_outcome: None,
            operation_phase: None,
            source_fingerprint: None,
            destination_fingerprint: None,
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

        let remote_copy = FileTransferStorageRecord {
            direction: "copy".to_string(),
            remote_path: "reports/source.csv".to_string(),
            local_path: "reports/final.csv".to_string(),
            ..transfer
        };
        assert!(is_owned_remote_copy_partial(&remote_copy, "reports/.dbx-copy-transfer-1-random.part"));
        assert!(!is_owned_remote_copy_partial(&remote_copy, "other/.dbx-copy-transfer-1-random.part"));
        assert!(!is_owned_remote_copy_partial(&remote_copy, "reports/.dbx-copy-someone-else-random.part"));
        assert!(!is_owned_remote_copy_partial(
            &FileTransferStorageRecord { direction: "download".to_string(), ..remote_copy.clone() },
            "reports/.dbx-copy-transfer-1-random.part"
        ));
    }

    #[tokio::test]
    async fn upload_crash_recovery_reports_only_an_owned_remote_partial() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let storage =
            Storage::open_with_file_secret_key(&parent.join("dbx.sqlite"), TEST_FILE_SECRET_KEY).await.unwrap();
        let state = AppState::new(storage);
        let connection_revision = ensure_test_connection(&state.storage, "connection-1").await;
        state
            .storage
            .create_file_transfer(
                "upload-crash".into(),
                "connection-1".into(),
                "upload".into(),
                "reports/final.csv".into(),
                parent.join("source.csv").to_string_lossy().into_owned(),
                canonical_directory_identity(&parent),
                connection_revision,
            )
            .await
            .unwrap();
        let owned_partial = "reports/.dbx-upload-upload-crash-random.part";
        state
            .storage
            .update_file_transfer(
                "upload-crash",
                "running".into(),
                11,
                Some(20),
                Some(owned_partial.into()),
                Some("source-fingerprint".into()),
                None,
                false,
            )
            .await
            .unwrap();

        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        assert_eq!(interrupted.len(), 1);
        recover_test_transfer(&state, &interrupted[0]).await;
        let recovered = state.storage.get_file_transfer("upload-crash").await.unwrap().unwrap();
        assert_eq!(recovered.status, "partial");
        assert_eq!(recovered.partial_destination.as_deref(), Some(owned_partial));
        assert_eq!(recovered.abort_outcome.as_deref(), Some("not_attempted_after_application_exit"));
        assert!(recovered.error.as_deref().unwrap().contains(owned_partial));

        let non_owned = FileTransferStorageRecord {
            temp_path: Some("reports/.dbx-upload-someone-else-random.part".into()),
            ..interrupted[0].clone()
        };
        assert!(!is_owned_upload_partial(&non_owned, non_owned.temp_path.as_deref().unwrap()));
    }

    #[tokio::test]
    async fn crash_before_temp_create_does_not_remove_an_unrelated_file() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        let state = AppState::new(storage);
        let connection_revision = ensure_test_connection(&state.storage, "connection-1").await;
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
                connection_revision,
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
        recover_test_transfer(&state, &interrupted[0]).await;
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
        let storage =
            Storage::open_with_file_secret_key(&parent.join("dbx.sqlite"), TEST_FILE_SECRET_KEY).await.unwrap();
        let state = AppState::new(storage);
        let connection_revision = ensure_test_connection(&state.storage, "connection-1").await;
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
                connection_revision,
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
        recover_test_transfer(&state, &interrupted[0]).await;
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

        let storage =
            Storage::open_with_file_secret_key(&parent.join("dbx.sqlite"), TEST_FILE_SECRET_KEY).await.unwrap();
        let state = AppState::new(storage);
        let connection_revision = ensure_test_connection(&state.storage, "connection-1").await;
        state
            .storage
            .create_file_transfer(
                "transfer-publishing".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
                identity,
                connection_revision,
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
        recover_test_transfer(&state, &interrupted[0]).await;
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

        let storage =
            Storage::open_with_file_secret_key(&parent.join("dbx.sqlite"), TEST_FILE_SECRET_KEY).await.unwrap();
        let state = AppState::new(storage);
        let connection_revision = ensure_test_connection(&state.storage, "connection-1").await;
        state
            .storage
            .create_file_transfer(
                "transfer-publishing-mismatch".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
                directory_identity,
                connection_revision,
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
        recover_test_transfer(&state, &interrupted[0]).await;

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

        let storage =
            Storage::open_with_file_secret_key(&root.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY).await.unwrap();
        let state = AppState::new(storage);
        let connection_revision = ensure_test_connection(&state.storage, "connection-1").await;
        state
            .storage
            .create_file_transfer(
                "transfer-swap".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                target.to_string_lossy().into_owned(),
                expected_identity,
                connection_revision,
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
        recover_test_transfer(&state, &interrupted[0]).await;
        assert_eq!(std::fs::read(attacker_temp).unwrap(), b"do not delete");
        assert_eq!(state.storage.get_file_transfer("transfer-swap").await.unwrap().unwrap().status, "failed");
    }

    #[tokio::test]
    async fn persisted_queued_cancel_intent_wins_before_worker_start() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().canonicalize().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        let connection_revision = ensure_test_connection(&storage, "connection-1").await;
        storage
            .create_file_transfer(
                "queued-cancel".into(),
                "connection-1".into(),
                "download".into(),
                "report.csv".into(),
                parent.join("report.csv").to_string_lossy().into_owned(),
                canonical_directory_identity(&parent),
                connection_revision,
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

    #[test]
    fn upload_handle_admission_is_bounded_globally_and_per_connection() {
        let runtime = FileTransferRuntime::default();
        for index in 0..CONNECTION_UPLOAD_HANDLE_LIMIT {
            runtime.register_upload(format!("same-{index}"), "ftp-1".to_string(), CancellationToken::new()).unwrap();
        }
        assert!(runtime
            .register_upload("same-overflow".to_string(), "ftp-1".to_string(), CancellationToken::new())
            .unwrap_err()
            .contains("this connection"));
        for index in CONNECTION_UPLOAD_HANDLE_LIMIT..GLOBAL_UPLOAD_HANDLE_LIMIT {
            runtime
                .register_upload(format!("global-{index}"), format!("ftp-{index}"), CancellationToken::new())
                .unwrap();
        }
        assert!(runtime
            .register_upload("global-overflow".to_string(), "another".to_string(), CancellationToken::new())
            .unwrap_err()
            .contains("Too many active"));
    }

    #[tokio::test]
    async fn prepared_upload_does_not_hold_the_connection_mutation_lock_during_data_flow() {
        use super::super::file_manager::{password_scope, FileConnectionConfig, FtpConnectionConfig};

        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        let config = FileConnectionConfig::Ftp(FtpConnectionConfig {
            endpoint: "ftp://127.0.0.1:9".to_string(),
            root: "/".to_string(),
            username: "dbx".to_string(),
        });
        storage
            .save_file_connection(
                "ftp-lock".into(),
                "FTP lock".into(),
                "ftp".into(),
                serde_json::to_string(&config).unwrap(),
                Some("password".into()),
                password_scope(&config).unwrap(),
                true,
                None,
            )
            .await
            .unwrap();
        let state = AppState::new(storage);
        let runtime = FileManagerRuntime::default();
        let prepared = runtime.prepare_file_mutation_operation(&state, "ftp-lock", "target.bin", 1).await.unwrap();
        assert!(prepared.mutation_lock_is_available());
    }

    #[tokio::test]
    async fn bounded_chunk_copy_surfaces_disconnect_disk_full_and_stall() {
        assert_eq!(DOWNLOAD_BUFFER_SIZE, 4 * 1024 * 1024);
        let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_SIZE];
        let mut sink = tokio::io::sink();
        let progress = TransferProgressSnapshot::new();
        let mut bytes = 0;

        let disconnected = transfer_one_chunk(
            None,
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
            None,
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
            None,
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
            None,
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
            transfer_one_chunk(
                None,
                &mut input,
                &mut stalled,
                &mut buffer,
                Duration::from_secs(30),
                &mut bytes,
                &progress,
            ),
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
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        let connection_revision = ensure_test_connection(&storage, "connection-1").await;
        storage
            .create_file_transfer(
                "disk-full".into(),
                "connection-1".into(),
                "download".into(),
                "remote.bin".into(),
                target.to_string_lossy().into_owned(),
                directory_identity,
                connection_revision,
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
        transfer_one_chunk(
            None,
            &mut first,
            &mut writer,
            &mut buffer,
            Duration::from_millis(50),
            &mut bytes,
            &progress,
        )
        .await
        .unwrap();
        let mut second = futures::io::Cursor::new(vec![2_u8; 1_024]);
        let failure = transfer_one_chunk(
            None,
            &mut second,
            &mut writer,
            &mut buffer,
            Duration::from_millis(50),
            &mut bytes,
            &progress,
        )
        .await
        .unwrap_err();
        assert!(failure.message.contains("space") || failure.message.contains("No space left"));

        let state = Arc::new(AppState::new(storage));
        let app = tauri::test::mock_builder()
            .manage(state.clone())
            .manage(FileManagerRuntime::default())
            .manage(FileTransferRuntime::default())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        finalize_download_result(app.handle(), &state, "disk-full", Err(failure), &progress).await;

        let runtime = app.state::<FileTransferRuntime>();
        let file_manager = app.state::<FileManagerRuntime>();
        let terminal =
            get_file_transfer_inner(&state, runtime.inner(), file_manager.inner(), "disk-full").await.unwrap();
        assert_eq!(terminal.bytes_transferred, 1_024);
        assert_eq!(terminal.total_bytes, Some(2_048));
        assert_eq!(terminal.status, "failed");
        assert!(!temp_path.exists());
        assert!(!target.exists());
        assert!(list_file_transfers_inner(&state, runtime.inner(), file_manager.inner(), Some("connection-1"))
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
            let count = transfer_one_chunk(
                None,
                &mut reader,
                &mut output,
                &mut buffer,
                IO_PROGRESS_WATCHDOG,
                &mut bytes,
                &progress,
            )
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
        create_worker_transfer_for_connection(app, "ftp-contract", transfer_id, remote_path, local_path).await
    }

    async fn create_worker_transfer_for_connection<R>(
        app: &tauri::App<R>,
        connection_id: &str,
        transfer_id: &str,
        remote_path: &str,
        local_path: &Path,
    ) -> (CancellationToken, tokio::task::JoinHandle<()>)
    where
        R: Runtime,
    {
        let state = app.state::<Arc<AppState>>();
        let parent = local_path.parent().unwrap();
        let connection_revision = ensure_test_connection(&state.storage, connection_id).await;
        state
            .storage
            .create_file_transfer(
                transfer_id.to_string(),
                connection_id.to_string(),
                "download".into(),
                remote_path.to_string(),
                local_path.to_string_lossy().into_owned(),
                canonical_directory_identity(parent),
                connection_revision,
            )
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        app.state::<FileTransferRuntime>().register(
            transfer_id.to_string(),
            connection_id.to_string(),
            cancellation.clone(),
        );
        let worker = tokio::spawn(run_download_worker(
            app.handle().clone(),
            transfer_id.to_string(),
            connection_id.to_string(),
            cancellation.clone(),
        ));
        (cancellation, worker)
    }

    async fn create_upload_worker_transfer<R>(
        app: &tauri::App<R>,
        transfer_id: &str,
        remote_path: &str,
        local_path: &Path,
    ) -> (CancellationToken, tokio::task::JoinHandle<()>)
    where
        R: Runtime,
    {
        create_upload_worker_transfer_for_connection(app, "ftp-contract", transfer_id, remote_path, local_path).await
    }

    async fn create_upload_worker_transfer_for_connection<R>(
        app: &tauri::App<R>,
        connection_id: &str,
        transfer_id: &str,
        remote_path: &str,
        local_path: &Path,
    ) -> (CancellationToken, tokio::task::JoinHandle<()>)
    where
        R: Runtime,
    {
        let local = validate_local_source(local_path).await.unwrap();
        let state = app.state::<Arc<AppState>>();
        let connection = state.storage.load_file_connection(connection_id).await.unwrap().unwrap();
        state
            .storage
            .create_file_upload_transfer(
                transfer_id.to_string(),
                connection_id.to_string(),
                remote_path.to_string(),
                local.path.to_string_lossy().into_owned(),
                local.directory_identity.clone(),
                local.fingerprint.clone(),
                local.total_bytes,
                connection.revision,
            )
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        app.state::<FileTransferRuntime>()
            .register_upload(transfer_id.to_string(), connection_id.to_string(), cancellation.clone())
            .unwrap();
        let worker = tokio::spawn(run_upload_worker(
            app.handle().clone(),
            transfer_id.to_string(),
            connection_id.to_string(),
            cancellation.clone(),
            local,
            UploadPolicy::best_effort_no_clobber(),
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

    fn assert_no_remote_upload_partial(container: &str, transfer_id: &str) {
        let prefix = format!(".dbx-upload-{transfer_id}-");
        let output = Command::new("docker")
            .args(["exec", container, "find", "/ftp/dbx", "-type", "f", "-name", &format!("{prefix}*.part"), "-print"])
            .output()
            .unwrap();
        assert!(output.status.success(), "docker find failed: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.stdout.is_empty(), "residual remote partial: {}", String::from_utf8_lossy(&output.stdout));
    }

    fn assert_no_remote_copy_partial(container: &str, transfer_id: &str) {
        let prefix = format!(".dbx-copy-{transfer_id}-");
        let output = Command::new("docker")
            .args(["exec", container, "find", "/ftp/dbx", "-type", "f", "-name", &format!("{prefix}*.part"), "-print"])
            .output()
            .unwrap();
        assert!(output.status.success(), "docker find failed: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.stdout.is_empty(), "residual remote partial: {}", String::from_utf8_lossy(&output.stdout));
    }

    async fn build_ftp_contract_app(
    ) -> (tauri::App<tauri::test::MockRuntime>, Arc<AppState>, opendal::Operator, tempfile::TempDir, String) {
        use super::super::file_manager::{build_operator, password_scope, FileConnectionConfig, FtpConnectionConfig};

        let endpoint = std::env::var("DBX_TEST_FTP_ENDPOINT").expect("DBX_TEST_FTP_ENDPOINT is required");
        let username = std::env::var("DBX_TEST_FTP_USERNAME").unwrap_or_else(|_| "dbx".to_string());
        let password = std::env::var("DBX_TEST_FTP_PASSWORD").unwrap_or_else(|_| "dbx-password".to_string());
        let container = std::env::var("DBX_TEST_FTP_CONTAINER").expect("DBX_TEST_FTP_CONTAINER is required");
        let config =
            FileConnectionConfig::Ftp(FtpConnectionConfig { endpoint, root: "/ftp/dbx".to_string(), username });
        let operator = build_operator(&config, Some(&password)).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
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
        (app, state, operator, directory, container)
    }

    async fn build_s3_contract_app(
    ) -> (tauri::App<tauri::test::MockRuntime>, Arc<AppState>, opendal::Operator, opendal::Operator, tempfile::TempDir)
    {
        use super::super::file_manager::{password_scope, FileConnectionConfig, S3ConnectionConfig};
        use opendal::services::S3;

        let endpoint = std::env::var("DBX_TEST_S3_ENDPOINT").expect("DBX_TEST_S3_ENDPOINT is required");
        let direct_endpoint = std::env::var("DBX_TEST_S3_DIRECT_ENDPOINT").unwrap_or_else(|_| endpoint.clone());
        let bucket = std::env::var("DBX_TEST_S3_BUCKET").expect("DBX_TEST_S3_BUCKET is required");
        let root = std::env::var("DBX_TEST_S3_ROOT").expect("DBX_TEST_S3_ROOT is required");
        let region = std::env::var("DBX_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let access_key_id = std::env::var("DBX_TEST_S3_ACCESS_KEY_ID").expect("DBX_TEST_S3_ACCESS_KEY_ID is required");
        let secret_access_key =
            std::env::var("DBX_TEST_S3_SECRET_ACCESS_KEY").expect("DBX_TEST_S3_SECRET_ACCESS_KEY is required");
        let session_token = std::env::var("DBX_TEST_S3_SESSION_TOKEN").ok();
        let config = FileConnectionConfig::S3(S3ConnectionConfig {
            endpoint: endpoint.clone(),
            region: region.clone(),
            bucket: bucket.clone(),
            root: root.clone(),
            virtual_host_style: false,
            anonymous: false,
        });
        let build_operator = |endpoint: &str, root: &str| {
            let mut builder = S3::default()
                .endpoint(endpoint)
                .region(&region)
                .bucket(&bucket)
                .root(root)
                .access_key_id(&access_key_id)
                .secret_access_key(&secret_access_key)
                .disable_config_load()
                .disable_ec2_metadata();
            if let Some(session_token) = session_token.as_deref() {
                builder = builder.session_token(session_token);
            }
            opendal::Operator::new(builder).unwrap().finish()
        };
        let operator = build_operator(&endpoint, &root);
        let bucket_operator = build_operator(&direct_endpoint, "/");
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        let mut persisted_secrets =
            vec![("access_key_id".to_string(), access_key_id), ("secret_access_key".to_string(), secret_access_key)];
        if let Some(session_token) = session_token {
            persisted_secrets.push(("session_token".to_string(), session_token));
        }
        storage
            .save_file_connection_with_secret_bundle(
                "s3-contract".into(),
                "S3 contract".into(),
                "s3".into(),
                serde_json::to_string(&config).unwrap(),
                persisted_secrets,
                vec![
                    "password".to_string(),
                    "password_scope".to_string(),
                    "access_key_id".to_string(),
                    "secret_access_key".to_string(),
                    "session_token".to_string(),
                ],
                "s3_scope".to_string(),
                password_scope(&config).unwrap(),
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
        (app, state, operator, bucket_operator, directory)
    }

    fn webhdfs_data_node_hostname_mapping() -> std::collections::BTreeMap<String, String> {
        let Some(raw) =
            std::env::var("DBX_TEST_WEBHDFS_DATANODE_MAPPING").ok().filter(|value| !value.trim().is_empty())
        else {
            return std::collections::BTreeMap::new();
        };
        raw.split([',', '\n'])
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let (host, socket) = entry
                    .split_once('=')
                    .unwrap_or_else(|| panic!("DBX_TEST_WEBHDFS_DATANODE_MAPPING entry must be host=socket: {entry}"));
                let host = host.trim();
                let socket = socket.trim();
                assert!(!host.is_empty(), "DBX_TEST_WEBHDFS_DATANODE_MAPPING host must not be empty");
                assert!(!socket.is_empty(), "DBX_TEST_WEBHDFS_DATANODE_MAPPING socket must not be empty");
                (host.to_string(), socket.to_string())
            })
            .collect()
    }

    async fn build_webhdfs_contract_app_for(
        connection_id: &str,
        root: String,
    ) -> (tauri::App<tauri::test::MockRuntime>, Arc<AppState>, tempfile::TempDir) {
        use super::super::file_manager::{
            save_file_connection, FileConnectionConfig, FileConnectionInput, FileConnectionSecrets,
            HdfsConnectionConfig,
        };
        use super::super::file_manager_webhdfs::{WebhdfsAuthentication, WebhdfsConnectionConfig, WebhdfsWriteOptions};

        let config = WebhdfsConnectionConfig {
            endpoint: std::env::var("DBX_TEST_WEBHDFS_ENDPOINT").expect("DBX_TEST_WEBHDFS_ENDPOINT is required"),
            root,
            authentication: WebhdfsAuthentication::Simple,
            user_name: std::env::var("DBX_TEST_WEBHDFS_USER").unwrap_or_else(|_| "hadoop".to_string()),
            disable_list_batch: false,
            allowed_data_node_origins: vec![std::env::var("DBX_TEST_WEBHDFS_DATANODE_ORIGIN")
                .expect("DBX_TEST_WEBHDFS_DATANODE_ORIGIN is required")],
            data_node_hostname_mapping: webhdfs_data_node_hostname_mapping(),
            tls_ca_certificate_path: None,
            proxy_url: None,
            proxy_bypass: None,
            allow_tls_downgrade: false,
            connect_timeout_seconds: 10,
            control_timeout_seconds: 30,
            idle_timeout_seconds: 30,
            chunk_size_mib: 4,
            write_options: WebhdfsWriteOptions::default(),
        };
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
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
        let saved = save_file_connection(
            app.state::<Arc<AppState>>(),
            app.state::<FileManagerRuntime>(),
            FileConnectionInput {
                id: Some(connection_id.to_string()),
                expected_revision: None,
                name: format!("WebHDFS contract {connection_id}"),
                config: FileConnectionConfig::Hdfs(HdfsConnectionConfig::Webhdfs(config)),
                secrets: Some(FileConnectionSecrets {
                    clear_webhdfs_credentials: Some(true),
                    ..FileConnectionSecrets::default()
                }),
            },
        )
        .await
        .unwrap();
        assert_eq!(saved.id, connection_id);
        (app, state, directory)
    }

    fn webhdfs_rss_size_bytes() -> u64 {
        let size = std::env::var("DBX_TEST_WEBHDFS_RSS_SIZE_BYTES")
            .expect("DBX_TEST_WEBHDFS_RSS_SIZE_BYTES is required")
            .parse::<u64>()
            .expect("DBX_TEST_WEBHDFS_RSS_SIZE_BYTES must be an integer");
        assert!(size >= 3, "DBX_TEST_WEBHDFS_RSS_SIZE_BYTES must be at least 3");
        assert!(i64::try_from(size).is_ok(), "DBX_TEST_WEBHDFS_RSS_SIZE_BYTES exceeds the transfer counter range");
        size
    }

    fn create_webhdfs_rss_sparse_source(size: u64) -> PathBuf {
        use std::io::{Seek, SeekFrom, Write};

        let local_dir = PathBuf::from(
            std::env::var("DBX_TEST_WEBHDFS_RSS_LOCAL_DIR").expect("DBX_TEST_WEBHDFS_RSS_LOCAL_DIR is required"),
        )
        .canonicalize()
        .expect("DBX_TEST_WEBHDFS_RSS_LOCAL_DIR must exist");
        let source = local_dir.join(format!("dbx-webhdfs-rss-{}-{}.bin", std::process::id(), Uuid::new_v4()));
        let mut file = std::fs::OpenOptions::new().create_new(true).read(true).write(true).open(&source).unwrap();
        file.set_len(size).unwrap();
        for (offset, byte) in [(0, 0x11_u8), (size / 2, 0x22_u8), (size - 1, 0x33_u8)] {
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&[byte]).unwrap();
        }
        file.sync_all().unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            let allocated = file.metadata().unwrap().blocks().saturating_mul(512);
            assert!(
                allocated <= 16 * 1024 * 1024,
                "RSS source must remain sparse: allocated {allocated} bytes for logical size {size}"
            );
        }
        drop(file);
        source
    }

    async fn wait_for_webhdfs_rss_terminal(storage: &Storage, transfer_id: &str) -> FileTransferStorageRecord {
        tokio::time::timeout(Duration::from_secs(26 * 60 * 60), async {
            loop {
                let record = storage.get_file_transfer(transfer_id).await.unwrap().unwrap();
                if matches!(record.status.as_str(), "completed" | "failed" | "partial" | "cancelled") {
                    return record;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("WebHDFS RSS transfer {transfer_id} did not terminate within 26 hours"))
    }

    async fn build_webhdfs_rss_contract_app() -> (tauri::App<tauri::test::MockRuntime>, Arc<AppState>, tempfile::TempDir)
    {
        build_webhdfs_contract_app_for(
            "webhdfs-rss-contract",
            std::env::var("DBX_TEST_WEBHDFS_ROOT").expect("DBX_TEST_WEBHDFS_ROOT is required"),
        )
        .await
    }

    async fn start_webhdfs_rss_upload(
        app: &tauri::App<tauri::test::MockRuntime>,
        state: &Arc<AppState>,
        source: &Path,
        destination: &str,
    ) -> StartTransferResult {
        let window = tauri::WebviewWindowBuilder::new(app, "main", Default::default()).build().unwrap();
        window.fs_scope().allow_file(source).unwrap();
        start_upload_inner(
            app.handle().clone(),
            window,
            state,
            app.state::<FileTransferRuntime>().inner(),
            StartUploadInput {
                connection_id: "webhdfs-rss-contract".to_string(),
                local_path: source.to_string_lossy().into_owned(),
                remote_path: destination.to_string(),
                policy: UploadPolicy::best_effort_no_clobber(),
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "run through tests/webhdfs-contract.sh with DBX_TEST_WEBHDFS_RSS_SIZES_GIB"]
    async fn fixed_webhdfs_production_rss_seed_contract() {
        use super::super::file_manager_webhdfs::{reset_test_open_request_count, test_open_request_count};

        let size = webhdfs_rss_size_bytes();
        let (app, state, _directory) = build_webhdfs_rss_contract_app().await;
        let source = create_webhdfs_rss_sparse_source(size);
        reset_test_open_request_count();
        let started = start_webhdfs_rss_upload(&app, &state, &source, "rss-source.bin").await;
        let transfer = wait_for_webhdfs_rss_terminal(&state.storage, &started.transfer_id).await;
        assert_eq!(transfer.status, "completed", "{transfer:?}");
        assert_eq!(transfer.bytes_transferred, i64::try_from(size).unwrap(), "{transfer:?}");
        assert_eq!(test_open_request_count(), 0, "WebHDFS upload must not issue OPEN requests");
        std::fs::remove_file(source).unwrap();
    }

    #[tokio::test]
    #[ignore = "run through tests/webhdfs-contract.sh with DBX_TEST_WEBHDFS_RSS_SIZES_GIB"]
    async fn fixed_webhdfs_production_worker_rss_contract() {
        use super::super::file_manager_webhdfs::{reset_test_open_request_count, test_open_request_count};

        let operation =
            std::env::var("DBX_TEST_WEBHDFS_RSS_OPERATION").expect("DBX_TEST_WEBHDFS_RSS_OPERATION is required");
        assert!(matches!(operation.as_str(), "upload" | "copy"), "RSS operation must be upload or copy");
        let size = webhdfs_rss_size_bytes();
        let (app, state, _directory) = build_webhdfs_rss_contract_app().await;
        reset_test_open_request_count();
        let (started, local_source) = if operation == "upload" {
            let source = create_webhdfs_rss_sparse_source(size);
            let started = start_webhdfs_rss_upload(&app, &state, &source, "rss-upload.bin").await;
            (started, Some(source))
        } else {
            let started = start_remote_transfer_inner(
                app.handle().clone(),
                &state,
                app.state::<FileTransferRuntime>().inner(),
                StartRemoteTransferInput {
                    connection_id: "webhdfs-rss-contract".to_string(),
                    source_path: "rss-source.bin".to_string(),
                    destination_path: "rss-copy.bin".to_string(),
                    policy: RemoteMutationPolicy::BestEffortNoClobber {
                        atomic_no_clobber: false,
                        external_toctou_risk: true,
                    },
                },
                "copy",
            )
            .await
            .unwrap();
            (started, None)
        };
        let transfer = wait_for_webhdfs_rss_terminal(&state.storage, &started.transfer_id).await;
        assert_eq!(transfer.status, "completed", "{transfer:?}");
        assert_eq!(transfer.bytes_transferred, i64::try_from(size).unwrap(), "{transfer:?}");
        assert!(transfer.partial_destination.is_none(), "{transfer:?}");
        let open_requests = test_open_request_count();
        let open_limit = if operation == "upload" { 1 } else { 4 };
        assert!(
            open_requests <= open_limit,
            "WebHDFS {operation} issued {open_requests} OPEN requests, expected at most {open_limit}"
        );
        if let Some(source) = local_source {
            std::fs::remove_file(source).unwrap();
        }
        println!(
            "DBX_WEBHDFS_RSS operation={operation} size_bytes={size} bytes_transferred={} namenode_open_requests={open_requests}",
            transfer.bytes_transferred
        );
    }

    async fn build_webhdfs_contract_app() -> (tauri::App<tauri::test::MockRuntime>, Arc<AppState>, tempfile::TempDir) {
        build_webhdfs_contract_app_for(
            "webhdfs-contract",
            std::env::var("DBX_TEST_WEBHDFS_ROOT").expect("DBX_TEST_WEBHDFS_ROOT is required"),
        )
        .await
    }

    async fn assert_webhdfs_upload_artifacts_absent(
        app: &tauri::App<tauri::test::MockRuntime>,
        connection_id: &str,
        parent: &str,
        transfer_id: &str,
        destination: &str,
    ) {
        let upload_prefix = format!(".dbx-upload-{transfer_id}-");
        for _ in 0..8 {
            let page = super::super::file_manager::list_file_entries(
                app.state::<Arc<AppState>>(),
                app.state::<FileManagerRuntime>(),
                connection_id.to_string(),
                parent.to_string(),
                None,
            )
            .await
            .unwrap();
            let residuals = page
                .entries
                .iter()
                .filter(|entry| {
                    let name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
                    entry.path == destination || (name.starts_with(&upload_prefix) && name.ends_with(".part"))
                })
                .map(|entry| entry.path.clone())
                .collect::<Vec<_>>();
            assert!(
                residuals.is_empty(),
                "WebHDFS transfer left destination or operation-owned partials: {residuals:?}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn webhdfs_fault_control(control: &str, method: &str, route: &str) -> serde_json::Value {
        let output = Command::new("curl")
            .args(["--silent", "--show-error", "--fail", "--request", method])
            .arg(format!("{control}{route}"))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "WebHDFS fault control request failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).expect("WebHDFS fault control returned invalid JSON")
    }

    async fn wait_for_webhdfs_fault_trigger(trace: &Path, label: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let trace = tokio::fs::read_to_string(trace).await.unwrap_or_default();
                let trigger = trace
                    .lines()
                    .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                    .find(|event| {
                        event.get("event").and_then(serde_json::Value::as_str) == Some("trigger")
                            && event.get("label").and_then(serde_json::Value::as_str) == Some(label)
                    });
                if let Some(trigger) = trigger {
                    assert_eq!(trigger.get("action").and_then(serde_json::Value::as_str), Some("reset"));
                    assert_eq!(trigger.get("direction").and_then(serde_json::Value::as_str), Some("upstream"));
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("WebHDFS DataNode fault did not trigger");
    }

    async fn assert_webhdfs_fault_proxy_idle(control: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let health = webhdfs_fault_control(control, "GET", "/health");
                if health.get("activeConnections").and_then(serde_json::Value::as_u64) == Some(0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("WebHDFS DataNode fault proxy retained active connections");
        for _ in 0..8 {
            let health = webhdfs_fault_control(control, "GET", "/health");
            assert_eq!(
                health.get("activeConnections").and_then(serde_json::Value::as_u64),
                Some(0),
                "WebHDFS DataNode fault proxy reopened a connection after worker termination"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn build_webdav_contract_app(
    ) -> (tauri::App<tauri::test::MockRuntime>, Arc<AppState>, opendal::Operator, tempfile::TempDir) {
        use super::super::file_manager::{
            build_operator, password_scope, FileConnectionConfig, WebdavAuthentication, WebdavConnectionConfig,
        };

        let endpoint = std::env::var("DBX_TEST_WEBDAV_ENDPOINT").expect("DBX_TEST_WEBDAV_ENDPOINT is required");
        let username = std::env::var("DBX_TEST_WEBDAV_USERNAME").unwrap_or_else(|_| "dbx".to_string());
        let password = std::env::var("DBX_TEST_WEBDAV_PASSWORD").unwrap_or_else(|_| "dbx-password".to_string());
        let config = FileConnectionConfig::Webdav(WebdavConnectionConfig {
            endpoint,
            root: "/tenant/root/".to_string(),
            authentication: WebdavAuthentication::Basic,
            username,
        });
        let operator = build_operator(&config, Some(&password)).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        storage
            .save_file_connection_with_secret_bundle(
                "webdav-contract".into(),
                "WebDAV contract".into(),
                "webdav".into(),
                serde_json::to_string(&config).unwrap(),
                vec![("password".to_string(), password)],
                vec!["password".to_string(), "webdav_token".to_string()],
                "webdav_scope".to_string(),
                password_scope(&config).unwrap(),
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
        (app, state, operator, directory)
    }

    async fn build_sftp_contract_app() -> (tauri::App<tauri::test::MockRuntime>, Arc<AppState>, tempfile::TempDir) {
        use super::super::file_manager::{
            password_scope, FileConnectionConfig, SftpAuthentication, SftpConnectionConfig,
        };

        let endpoint = std::env::var("DBX_TEST_SFTP_ENDPOINT").expect("DBX_TEST_SFTP_ENDPOINT is required");
        let username = std::env::var("DBX_TEST_SFTP_USERNAME").expect("DBX_TEST_SFTP_USERNAME is required");
        let root = std::env::var("DBX_TEST_SFTP_ROOT").expect("DBX_TEST_SFTP_ROOT is required");
        let private_key_file =
            std::env::var("DBX_TEST_SFTP_PRIVATE_KEY_FILE").expect("DBX_TEST_SFTP_PRIVATE_KEY_FILE is required");
        let private_key = std::fs::read_to_string(private_key_file).unwrap();
        let passphrase = std::env::var("DBX_TEST_SFTP_PRIVATE_KEY_PASSPHRASE")
            .expect("DBX_TEST_SFTP_PRIVATE_KEY_PASSPHRASE is required");
        let config = FileConnectionConfig::Sftp(SftpConnectionConfig {
            endpoint,
            root,
            username,
            authentication: SftpAuthentication::PrivateKey,
        });
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        storage
            .save_file_connection_with_secret_bundle(
                "sftp-contract".into(),
                "SFTP contract".into(),
                "sftp".into(),
                serde_json::to_string(&config).unwrap(),
                vec![
                    ("sftp_private_key".to_string(), private_key),
                    ("sftp_private_key_passphrase".to_string(), passphrase),
                ],
                vec!["sftp_private_key".to_string(), "sftp_private_key_passphrase".to_string()],
                "sftp_scope".to_string(),
                password_scope(&config).unwrap(),
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
        (app, state, directory)
    }

    async fn build_hdfs_native_contract_app() -> (tauri::App<tauri::test::MockRuntime>, Arc<AppState>, tempfile::TempDir)
    {
        use super::super::file_manager::{password_scope, FileConnectionConfig, HdfsConnectionConfig};
        use super::super::file_manager_hdfs_native::{HdfsNativeAuthenticationEnvironment, HdfsNativeConnectionConfig};

        let config = FileConnectionConfig::Hdfs(HdfsConnectionConfig::Native(HdfsNativeConnectionConfig {
            name_node_uri: std::env::var("DBX_TEST_HDFS_NATIVE_NAMENODE")
                .expect("DBX_TEST_HDFS_NATIVE_NAMENODE is required"),
            root: std::env::var("DBX_TEST_HDFS_NATIVE_ROOT").expect("DBX_TEST_HDFS_NATIVE_ROOT is required"),
            options: std::collections::BTreeMap::from([(
                "dfs.client.use.datanode.hostname".to_string(),
                "true".to_string(),
            )]),
            hadoop_config_directory: Some(
                std::env::var("DBX_TEST_HDFS_NATIVE_HADOOP_CONFIG_DIR")
                    .expect("DBX_TEST_HDFS_NATIVE_HADOOP_CONFIG_DIR is required"),
            ),
            authentication_environment: Some(HdfsNativeAuthenticationEnvironment {
                user_name: "HADOOP_USER_NAME".to_string(),
            }),
        }));
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
        storage
            .save_file_connection_with_secret_bundle(
                "hdfs-native-contract".into(),
                "HDFS Native contract".into(),
                "hdfs".into(),
                serde_json::to_string(&config).unwrap(),
                Vec::new(),
                vec![
                    "password".to_string(),
                    "password_scope".to_string(),
                    "access_key_id".to_string(),
                    "secret_access_key".to_string(),
                    "session_token".to_string(),
                    "s3_scope".to_string(),
                    "webdav_token".to_string(),
                    "webdav_scope".to_string(),
                    "sftp_private_key".to_string(),
                    "sftp_private_key_passphrase".to_string(),
                    "sftp_scope".to_string(),
                ],
                "password_scope".to_string(),
                password_scope(&config).unwrap(),
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
        (app, state, directory)
    }

    #[tokio::test]
    #[ignore = "run through tests/hdfs-native-contract.sh with the fixed Hadoop service and fault proxies"]
    async fn fixed_hdfs_native_transfer_contract() {
        fn post_fault_control(control: &str, route: &str) {
            let output = Command::new("curl")
                .args(["--silent", "--show-error", "--fail", "--request", "POST"])
                .arg(format!("{control}{route}"))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "HDFS Native fault control request failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        async fn wait_for_fault_trigger(trace: &Path, label: &str) -> u64 {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let trace = tokio::fs::read_to_string(trace).await.unwrap_or_default();
                    let events = trace
                        .lines()
                        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                        .collect::<Vec<_>>();
                    let binding = events.iter().find(|event| {
                        event.get("event").and_then(serde_json::Value::as_str) == Some("bind")
                            && event.get("label").and_then(serde_json::Value::as_str) == Some(label)
                    });
                    let trigger = events.iter().find(|event| {
                        event.get("event").and_then(serde_json::Value::as_str) == Some("trigger")
                            && event.get("label").and_then(serde_json::Value::as_str) == Some(label)
                    });
                    if let (Some(binding), Some(trigger)) = (binding, trigger) {
                        let bound_pair = binding.get("pairId").and_then(serde_json::Value::as_u64).unwrap();
                        assert_eq!(
                            trigger.get("pairId").and_then(serde_json::Value::as_u64),
                            Some(bound_pair),
                            "{label} triggered on a different proxy pair"
                        );
                        assert_eq!(
                            trigger.get("boundPairId").and_then(serde_json::Value::as_u64),
                            Some(bound_pair),
                            "{label} lost its next-pair binding"
                        );
                        assert_eq!(trigger.get("scope").and_then(serde_json::Value::as_str), Some("next"));
                        break bound_pair;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("HDFS Native fault '{label}' did not trigger"))
        }

        async fn wait_for_proxy_client_release(trace: &Path, pair_id: u64, label: &str, expect_suppressed_end: bool) {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let trace = tokio::fs::read_to_string(trace).await.unwrap_or_default();
                    let released = trace
                        .lines()
                        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                        .any(|event| {
                            if event.get("pairId").and_then(serde_json::Value::as_u64) != Some(pair_id)
                                || event.get("side").and_then(serde_json::Value::as_str) != Some("client")
                            {
                                return false;
                            }
                            let event_name = event.get("event").and_then(serde_json::Value::as_str);
                            if expect_suppressed_end {
                                event_name == Some("end-suppressed")
                                    && event.get("direction").and_then(serde_json::Value::as_str) == Some("upstream")
                            } else {
                                event_name == Some("close")
                                    || (event_name == Some("end")
                                        && event.get("direction").and_then(serde_json::Value::as_str)
                                            == Some("upstream"))
                            }
                        });
                    if released {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("HDFS Native fault '{label}' did not release its client socket"));
        }

        fn proxy_health(control: &str) -> serde_json::Value {
            let output = Command::new("curl")
                .args(["--silent", "--show-error", "--fail"])
                .arg(format!("{control}/health"))
                .output()
                .unwrap();
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
        }

        fn proxy_active_pairs(control: &str) -> BTreeSet<u64> {
            proxy_health(control)
                .get("activePairs")
                .and_then(serde_json::Value::as_array)
                .unwrap()
                .iter()
                .map(|pair| pair.get("pairId").and_then(serde_json::Value::as_u64).unwrap())
                .collect()
        }

        fn proxy_traffic_totals(control: &str) -> (u64, u64, u64, u64) {
            let health = proxy_health(control);
            let totals = health.get("totals").unwrap();
            (
                totals.get("upstreamBytes").and_then(serde_json::Value::as_u64).unwrap(),
                totals.get("downstreamBytes").and_then(serde_json::Value::as_u64).unwrap(),
                totals.get("upstreamChunks").and_then(serde_json::Value::as_u64).unwrap(),
                totals.get("downstreamChunks").and_then(serde_json::Value::as_u64).unwrap(),
            )
        }

        fn proxy_open_count(trace: &Path) -> usize {
            std::fs::read_to_string(trace)
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .filter(|event| event.get("event").and_then(serde_json::Value::as_str) == Some("open"))
                .count()
        }

        async fn wait_for_proxy_baseline(control: &str, baseline: &BTreeSet<u64>) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if proxy_active_pairs(control) == *baseline {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("HDFS Native proxy connections did not return to baseline");
        }

        async fn wait_for_proxy_subset(control: &str, baseline: &BTreeSet<u64>) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if proxy_active_pairs(control).is_subset(baseline) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("HDFS Native proxy retained a connection outside the allowed baseline");
        }

        async fn wait_for_proxy_subset_including(control: &str, allowed: &BTreeSet<u64>, required_pair: u64) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let active = proxy_active_pairs(control);
                    if active.contains(&required_pair) && active.is_subset(allowed) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("HDFS Native proxy did not retain exactly the required fault pair within its allowed baseline");
        }

        async fn assert_proxy_traffic_quiet(control: &str) {
            let before = proxy_traffic_totals(control);
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_eq!(
                proxy_traffic_totals(control),
                before,
                "cancelled HDFS Native transfer continued emitting proxy traffic"
            );
        }

        async fn assert_no_hdfs_owned_partial(operator: &opendal::Operator, transfer_id: &str, destination: &str) {
            let parent =
                destination.rsplit_once('/').map_or_else(|| "/".to_string(), |(parent, _)| format!("{parent}/"));
            let upload_prefix = format!(".dbx-upload-{transfer_id}-");
            let copy_prefix = format!(".dbx-copy-{transfer_id}-");
            let residuals = operator
                .list_with(&parent)
                .recursive(false)
                .await
                .unwrap()
                .into_iter()
                .map(|entry| entry.path().to_string())
                .filter(|path| {
                    let name = path.trim_end_matches('/').rsplit('/').next().unwrap_or(path);
                    (name.starts_with(&upload_prefix) || name.starts_with(&copy_prefix)) && name.ends_with(".part")
                })
                .collect::<Vec<_>>();
            assert!(residuals.is_empty(), "unexpected HDFS Native operation-owned partials: {residuals:?}");
        }

        async fn warm_hdfs_read_cache(file_manager: &FileManagerRuntime, state: &AppState) {
            let warm = file_manager.prepare_file_operation(state, "hdfs-native-contract", "fixture.txt").await.unwrap();
            let metadata = tokio::time::timeout(Duration::from_secs(10), warm.operator.stat(&warm.remote_path))
                .await
                .expect("HDFS Native read-cache warmup timed out")
                .unwrap();
            assert!(metadata.mode().is_file());
            drop(warm);
            assert_eq!(file_manager.operator_count(), 1, "HDFS Native read cache was not retained");
        }

        let (app, state, directory) = build_hdfs_native_contract_app().await;
        app.state::<FileTransferRuntime>()
            .ensure_recovered(&state, app.state::<FileManagerRuntime>().inner())
            .await
            .unwrap();
        let connection = state.storage.load_file_connection("hdfs-native-contract").await.unwrap().unwrap();
        let file_manager = app.state::<FileManagerRuntime>();
        let prepared = file_manager
            .prepare_file_mutation_operation(&state, "hdfs-native-contract", "fixture.txt", connection.revision)
            .await
            .unwrap();
        let operator = &prepared.operator;
        let local_root = directory.path().canonicalize().unwrap();
        let datanode_control = std::env::var("DBX_TEST_HDFS_NATIVE_DATANODE_FAULT_CONTROL")
            .expect("DBX_TEST_HDFS_NATIVE_DATANODE_FAULT_CONTROL is required");
        let datanode_trace = PathBuf::from(
            std::env::var("DBX_TEST_HDFS_NATIVE_DATANODE_PROXY_TRACE")
                .expect("DBX_TEST_HDFS_NATIVE_DATANODE_PROXY_TRACE is required"),
        );
        let namenode_control = std::env::var("DBX_TEST_HDFS_NATIVE_NAMENODE_FAULT_CONTROL")
            .expect("DBX_TEST_HDFS_NATIVE_NAMENODE_FAULT_CONTROL is required");
        let namenode_trace = PathBuf::from(
            std::env::var("DBX_TEST_HDFS_NATIVE_NAMENODE_PROXY_TRACE")
                .expect("DBX_TEST_HDFS_NATIVE_NAMENODE_PROXY_TRACE is required"),
        );
        let source = format!("product-transfer-{}-source.bin", Uuid::new_v4());
        let configured_source = prepared.configured_path(&source).unwrap();
        let chunk = vec![0x4d_u8; REMOTE_COPY_BUFFER_SIZE];
        let mut source_writer = operator.writer(&configured_source).await.unwrap();
        for _ in 0..8 {
            source_writer.write(chunk.clone()).await.unwrap();
        }
        source_writer.close().await.unwrap();
        drop(source_writer);
        assert_eq!(operator.stat(&configured_source).await.unwrap().content_length(), 32 * 1024 * 1024);

        let no_datanode_pairs = BTreeSet::new();
        wait_for_proxy_baseline(&datanode_control, &no_datanode_pairs).await;
        let normal_download_uncached_namenode_baseline = proxy_active_pairs(&namenode_control);
        let normal_download_uncached_datanode_baseline = proxy_active_pairs(&datanode_control);
        warm_hdfs_read_cache(file_manager.inner(), &state).await;
        wait_for_proxy_subset(&datanode_control, &normal_download_uncached_datanode_baseline).await;
        let normal_download_datanode_baseline = proxy_active_pairs(&datanode_control);
        let download_target = local_root.join("hdfs-native-download.bin");
        let (_, download_worker) = create_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-download-success",
            &source,
            &download_target,
        )
        .await;
        download_worker.await.unwrap();
        let download = state.storage.get_file_transfer("hdfs-native-download-success").await.unwrap().unwrap();
        assert_eq!(download.status, "completed", "{download:?}");
        assert_eq!(tokio::fs::metadata(&download_target).await.unwrap().len(), 32 * 1024 * 1024);
        assert_no_owned_temp(&local_root, "hdfs-native-download-success");
        wait_for_proxy_subset(&datanode_control, &normal_download_datanode_baseline).await;
        assert_eq!(file_manager.operator_count(), 1, "download must retain its warmed read-cache operator");
        file_manager.evict_revision("hdfs-native-contract", connection.revision);
        assert_eq!(file_manager.operator_count(), 0, "download read-cache eviction must remove the cached operator");
        wait_for_proxy_subset(&namenode_control, &normal_download_uncached_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &normal_download_uncached_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;

        let upload_source = local_root.join("hdfs-native-upload-source.bin");
        let upload_payload = vec![0x5a_u8; 32 * 1024 * 1024 + 137];
        tokio::fs::write(&upload_source, &upload_payload).await.unwrap();
        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let upload_namenode_baseline = proxy_active_pairs(&namenode_control);
        let upload_datanode_baseline = proxy_active_pairs(&datanode_control);
        let (_, upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-upload-success",
            "hdfs-native-upload-target.bin",
            &upload_source,
        )
        .await;
        upload_worker.await.unwrap();
        let upload = state.storage.get_file_transfer("hdfs-native-upload-success").await.unwrap().unwrap();
        assert_eq!(upload.status, "completed", "{upload:?}");
        assert_eq!(upload.publish_outcome.as_deref(), Some("completed"), "{upload:?}");
        wait_for_proxy_subset(&namenode_control, &upload_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &upload_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        assert_eq!(
            operator.read(&prepared.configured_path("hdfs-native-upload-target.bin").unwrap()).await.unwrap().to_vec(),
            upload_payload
        );

        reset_test_remote_copy_high_water();
        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let copy_namenode_baseline = proxy_active_pairs(&namenode_control);
        let copy_datanode_baseline = proxy_active_pairs(&datanode_control);
        create_remote_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-copy-success",
            "copy",
            &source,
            "hdfs-native-copy-target.bin",
        )
        .await
        .await
        .unwrap();
        let copy = state.storage.get_file_transfer("hdfs-native-copy-success").await.unwrap().unwrap();
        assert_eq!(copy.status, "completed", "{copy:?}");
        assert_eq!(copy.bytes_transferred, 32 * 1024 * 1024, "{copy:?}");
        assert_eq!(copy.operation_outcome.as_deref(), Some("completed"), "{copy:?}");
        let (max_read_chunk, max_write_chunk, max_relay_payload) = test_remote_copy_high_water();
        assert!((1..=REMOTE_COPY_BUFFER_SIZE).contains(&max_read_chunk), "{max_read_chunk}");
        assert!((1..=REMOTE_COPY_BUFFER_SIZE).contains(&max_write_chunk), "{max_write_chunk}");
        assert!((1..=REMOTE_COPY_BUFFER_SIZE).contains(&max_relay_payload), "{max_relay_payload}");
        wait_for_proxy_subset(&namenode_control, &copy_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &copy_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        let copy_path = prepared.configured_path("hdfs-native-copy-target.bin").unwrap();
        assert_eq!(operator.stat(&copy_path).await.unwrap().content_length(), 32 * 1024 * 1024);
        assert_eq!(operator.read(&copy_path).await.unwrap().to_vec(), vec![0x4d_u8; 32 * 1024 * 1024]);

        let datanode_opens_before_rename = proxy_open_count(&datanode_trace);
        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let rename_namenode_baseline = proxy_active_pairs(&namenode_control);
        let rename_datanode_baseline = proxy_active_pairs(&datanode_control);
        create_remote_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-rename-success",
            "rename",
            "hdfs-native-copy-target.bin",
            "hdfs-native-rename-target.bin",
        )
        .await
        .await
        .unwrap();
        let rename = state.storage.get_file_transfer("hdfs-native-rename-success").await.unwrap().unwrap();
        assert_eq!(rename.status, "completed", "{rename:?}");
        wait_for_proxy_subset(&namenode_control, &rename_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &rename_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        assert_eq!(operator.stat(&copy_path).await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        let renamed_path = prepared.configured_path("hdfs-native-rename-target.bin").unwrap();
        assert_eq!(operator.stat(&renamed_path).await.unwrap().content_length(), 32 * 1024 * 1024);
        assert_eq!(
            proxy_open_count(&datanode_trace),
            datanode_opens_before_rename,
            "HDFS Native rename must not open a DataNode relay"
        );

        let no_clobber_source = prepared.configured_path("hdfs-native-no-clobber-source.bin").unwrap();
        let no_clobber_target = prepared.configured_path("hdfs-native-no-clobber-target.bin").unwrap();
        operator.write(&no_clobber_source, b"source".to_vec()).await.unwrap();
        operator.write(&no_clobber_target, b"keep".to_vec()).await.unwrap();
        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let no_clobber_namenode_baseline = proxy_active_pairs(&namenode_control);
        let no_clobber_datanode_baseline = proxy_active_pairs(&datanode_control);
        create_remote_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-rename-no-clobber",
            "rename",
            "hdfs-native-no-clobber-source.bin",
            "hdfs-native-no-clobber-target.bin",
        )
        .await
        .await
        .unwrap();
        let no_clobber = state.storage.get_file_transfer("hdfs-native-rename-no-clobber").await.unwrap().unwrap();
        assert_eq!(no_clobber.status, "failed", "{no_clobber:?}");
        wait_for_proxy_subset(&namenode_control, &no_clobber_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &no_clobber_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        assert_eq!(operator.read(&no_clobber_source).await.unwrap().to_vec(), b"source");
        assert_eq!(operator.read(&no_clobber_target).await.unwrap().to_vec(), b"keep");

        let datanode_opens_before_replace = proxy_open_count(&datanode_trace);
        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let replace_namenode_baseline = proxy_active_pairs(&namenode_control);
        let replace_datanode_baseline = proxy_active_pairs(&datanode_control);
        create_remote_worker_transfer_with_policy(
            &app,
            "hdfs-native-contract",
            "hdfs-native-rename-replace",
            "rename",
            "hdfs-native-no-clobber-source.bin",
            "hdfs-native-no-clobber-target.bin",
            RemoteMutationPolicy::Replace { confirmed: true },
        )
        .await
        .await
        .unwrap();
        let replace = state.storage.get_file_transfer("hdfs-native-rename-replace").await.unwrap().unwrap();
        assert_eq!(replace.status, "completed", "{replace:?}");
        wait_for_proxy_subset(&namenode_control, &replace_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &replace_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        assert_eq!(operator.stat(&no_clobber_source).await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        assert_eq!(
            proxy_open_count(&datanode_trace),
            datanode_opens_before_replace,
            "HDFS Native replace rename must not open a DataNode relay"
        );
        assert_eq!(operator.read(&no_clobber_target).await.unwrap().to_vec(), b"source");

        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let cancel_download_uncached_namenode_baseline = proxy_active_pairs(&namenode_control);
        let cancel_download_uncached_datanode_baseline = proxy_active_pairs(&datanode_control);
        warm_hdfs_read_cache(file_manager.inner(), &state).await;
        wait_for_proxy_subset(&datanode_control, &cancel_download_uncached_datanode_baseline).await;
        let cancel_download_datanode_baseline = proxy_active_pairs(&datanode_control);
        let download_cancel_barrier = install_test_download_after_chunk_barrier();
        let cancelled_download_target = local_root.join("hdfs-native-download-cancelled.bin");
        let (_, cancelled_download_worker) = create_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-download-cancelled",
            &source,
            &cancelled_download_target,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), download_cancel_barrier.opened.notified())
            .await
            .expect("HDFS Native download did not complete its first DataNode chunk");
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            file_manager.inner(),
            "hdfs-native-download-cancelled",
        )
        .await
        .unwrap();
        download_cancel_barrier.release.notify_one();
        cancelled_download_worker.await.unwrap();
        let cancelled_download =
            state.storage.get_file_transfer("hdfs-native-download-cancelled").await.unwrap().unwrap();
        assert_eq!(cancelled_download.status, "cancelled", "{cancelled_download:?}");
        assert!(cancelled_download.bytes_transferred > 0, "{cancelled_download:?}");
        assert!(!cancelled_download_target.exists());
        assert_no_owned_temp(&local_root, "hdfs-native-download-cancelled");
        wait_for_proxy_subset(&datanode_control, &cancel_download_datanode_baseline).await;
        assert_eq!(file_manager.operator_count(), 0, "cancelled download must evict its read-cache operator");
        wait_for_proxy_subset(&namenode_control, &cancel_download_uncached_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &cancel_download_uncached_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;

        let cancelled_upload_source = local_root.join("hdfs-native-upload-cancelled-source.bin");
        tokio::fs::write(&cancelled_upload_source, vec![0x37_u8; 32 * 1024 * 1024 + 137]).await.unwrap();
        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let cancel_upload_namenode_baseline = proxy_active_pairs(&namenode_control);
        let cancel_upload_datanode_baseline = proxy_active_pairs(&datanode_control);
        let upload_cancel_barrier = install_test_upload_after_chunk_barrier();
        let (_, cancelled_upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-upload-cancelled",
            "hdfs-native-upload-cancelled.bin",
            &cancelled_upload_source,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(15), upload_cancel_barrier.opened.notified())
            .await
            .expect("HDFS Native upload did not complete its first DataNode chunk");
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            file_manager.inner(),
            "hdfs-native-upload-cancelled",
        )
        .await
        .unwrap();
        upload_cancel_barrier.release.notify_one();
        cancelled_upload_worker.await.unwrap();
        let cancelled_upload = state.storage.get_file_transfer("hdfs-native-upload-cancelled").await.unwrap().unwrap();
        assert_eq!(cancelled_upload.status, "cancelled", "{cancelled_upload:?}");
        assert!(cancelled_upload.bytes_transferred > 0, "{cancelled_upload:?}");
        assert_eq!(cancelled_upload.partial_destination, None, "{cancelled_upload:?}");
        wait_for_proxy_subset(&namenode_control, &cancel_upload_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &cancel_upload_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        assert!(!operator
            .exists(&prepared.configured_path("hdfs-native-upload-cancelled.bin").unwrap())
            .await
            .unwrap());
        assert_no_hdfs_owned_partial(operator, "hdfs-native-upload-cancelled", "hdfs-native-upload-cancelled.bin")
            .await;

        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let cancel_copy_namenode_baseline = proxy_active_pairs(&namenode_control);
        let cancel_copy_datanode_baseline = proxy_active_pairs(&datanode_control);
        let copy_cancel_barrier = install_test_remote_copy_after_chunk_barrier();
        let cancelled_copy_worker = create_remote_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-copy-cancelled",
            "copy",
            &source,
            "hdfs-native-copy-cancelled.bin",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(20), copy_cancel_barrier.opened.notified())
            .await
            .expect("HDFS Native copy did not complete its first relay chunk");
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            file_manager.inner(),
            "hdfs-native-copy-cancelled",
        )
        .await
        .unwrap();
        copy_cancel_barrier.release.notify_one();
        cancelled_copy_worker.await.unwrap();
        let cancelled_copy = state.storage.get_file_transfer("hdfs-native-copy-cancelled").await.unwrap().unwrap();
        assert_eq!(cancelled_copy.status, "cancelled", "{cancelled_copy:?}");
        assert!(cancelled_copy.bytes_transferred > 0, "{cancelled_copy:?}");
        wait_for_proxy_subset(&namenode_control, &cancel_copy_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &cancel_copy_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        assert_no_hdfs_owned_partial(operator, "hdfs-native-copy-cancelled", "hdfs-native-copy-cancelled.bin").await;

        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let reset_recovery_uncached_namenode_baseline = proxy_active_pairs(&namenode_control);
        let reset_recovery_uncached_datanode_baseline = proxy_active_pairs(&datanode_control);
        warm_hdfs_read_cache(file_manager.inner(), &state).await;
        wait_for_proxy_subset(&datanode_control, &reset_recovery_uncached_datanode_baseline).await;
        let reset_recovery_datanode_baseline = proxy_active_pairs(&datanode_control);
        let reset_recovery_label = "hdfs-download-reset-recovery";
        post_fault_control(
            &datanode_control,
            &format!(
                "/arm?action=reset&direction=downstream&bytes={}&label={reset_recovery_label}&scope=next",
                128 * 1024
            ),
        );
        let reset_recovery_target = local_root.join("hdfs-native-download-reset-recovery.bin");
        let (_, reset_recovery_worker) = create_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-download-reset-recovery",
            &source,
            &reset_recovery_target,
        )
        .await;
        let _reset_recovery_pair = wait_for_fault_trigger(&datanode_trace, reset_recovery_label).await;
        tokio::time::timeout(Duration::from_secs(15), reset_recovery_worker)
            .await
            .expect("HDFS Native download did not recover from the transient DataNode reset")
            .unwrap();
        let reset_recovery =
            state.storage.get_file_transfer("hdfs-native-download-reset-recovery").await.unwrap().unwrap();
        assert_eq!(reset_recovery.status, "completed", "{reset_recovery:?}");
        assert_eq!(reset_recovery.bytes_transferred, 32 * 1024 * 1024, "{reset_recovery:?}");
        assert_eq!(reset_recovery.error, None, "{reset_recovery:?}");
        assert_eq!(tokio::fs::read(&reset_recovery_target).await.unwrap(), vec![0x4d_u8; 32 * 1024 * 1024]);
        assert_no_owned_temp(&local_root, "hdfs-native-download-reset-recovery");
        wait_for_proxy_subset(&datanode_control, &reset_recovery_datanode_baseline).await;
        assert_eq!(file_manager.operator_count(), 1, "recovered download must retain its warmed read-cache operator");
        file_manager.evict_revision("hdfs-native-contract", connection.revision);
        assert_eq!(file_manager.operator_count(), 0, "download read-cache eviction must remove the cached operator");
        wait_for_proxy_subset(&namenode_control, &reset_recovery_uncached_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &reset_recovery_uncached_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;

        let timeout_label = "hdfs-upload-timeout";
        let timeout_source = local_root.join("hdfs-native-upload-timeout-source.bin");
        tokio::fs::write(&timeout_source, vec![0x6b_u8; 32 * 1024 * 1024 + 137]).await.unwrap();
        post_fault_control(
            &datanode_control,
            &format!("/arm?action=blackhole&direction=upstream&bytes={}&label={timeout_label}&scope=next", 128 * 1024),
        );
        assert_eq!(file_manager.operator_count(), 0, "mutation paths must not retain a read-cache operator");
        let timeout_upload_namenode_baseline = proxy_active_pairs(&namenode_control);
        let timeout_upload_datanode_baseline = proxy_active_pairs(&datanode_control);
        let (_, timeout_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "hdfs-native-contract",
            "hdfs-native-upload-timeout",
            "hdfs-native-upload-timeout.bin",
            &timeout_source,
        )
        .await;
        let timeout_pair = wait_for_fault_trigger(&datanode_trace, timeout_label).await;
        tokio::time::timeout(IO_PROGRESS_WATCHDOG + Duration::from_secs(15), timeout_worker)
            .await
            .expect("timed-out HDFS Native upload did not terminate")
            .unwrap();
        wait_for_proxy_client_release(&datanode_trace, timeout_pair, timeout_label, true).await;
        let mut timeout_upload_datanode_fault_baseline = timeout_upload_datanode_baseline.clone();
        timeout_upload_datanode_fault_baseline.insert(timeout_pair);
        wait_for_proxy_subset_including(&datanode_control, &timeout_upload_datanode_fault_baseline, timeout_pair).await;
        wait_for_proxy_subset(&namenode_control, &timeout_upload_namenode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        post_fault_control(&datanode_control, "/drop");
        let timed_out = state.storage.get_file_transfer("hdfs-native-upload-timeout").await.unwrap().unwrap();
        assert!(matches!(timed_out.status.as_str(), "failed" | "partial"), "{timed_out:?}");
        assert!(
            timed_out.error.as_deref().is_some_and(|error| {
                error.contains("HdfsNativeTimeout:")
                    || error.contains("timed out")
                    || error.contains("watchdog expired")
            }),
            "{timed_out:?}"
        );
        wait_for_proxy_subset(&namenode_control, &timeout_upload_namenode_baseline).await;
        wait_for_proxy_subset(&datanode_control, &timeout_upload_datanode_baseline).await;
        assert_proxy_traffic_quiet(&namenode_control).await;
        assert_proxy_traffic_quiet(&datanode_control).await;
        assert_no_hdfs_owned_partial(operator, "hdfs-native-upload-timeout", "hdfs-native-upload-timeout.bin").await;

        let recovery_id = "hdfs-native-upload-recovery";
        let recovery_partial = format!(".dbx-upload-{recovery_id}-fixed.part");
        operator.write(&recovery_partial, b"recovery".to_vec()).await.unwrap();
        state
            .storage
            .create_file_upload_transfer(
                recovery_id.into(),
                "hdfs-native-contract".into(),
                "hdfs-native-upload-recovery-target.bin".into(),
                local_root.join("missing-recovery-source.bin").to_string_lossy().into_owned(),
                canonical_directory_identity(&local_root),
                "recovery-source-fingerprint".into(),
                8,
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .start_file_upload_transfer(
                recovery_id,
                recovery_partial.clone(),
                "recovery-source-fingerprint".into(),
                8,
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                recovery_id,
                "publishing".into(),
                8,
                Some(8),
                Some(recovery_partial.clone()),
                Some("recovery-source-fingerprint".into()),
                None,
                false,
            )
            .await
            .unwrap();
        let interrupted = state
            .storage
            .recover_interrupted_file_transfers()
            .await
            .unwrap()
            .into_iter()
            .find(|item| item.id == recovery_id)
            .unwrap();
        recover_interrupted_transfer(&state, file_manager.inner(), &interrupted).await.unwrap();
        let recovered = state.storage.get_file_transfer(recovery_id).await.unwrap().unwrap();
        assert_eq!(recovered.status, "partial", "{recovered:?}");
        assert_eq!(recovered.publish_outcome.as_deref(), Some("partial_source"), "{recovered:?}");
        prepared.delete_owned_upload_partial(&recovery_partial).await.unwrap();
        assert_no_hdfs_owned_partial(operator, recovery_id, "hdfs-native-upload-recovery-target.bin").await;

        let namenode_fault_cases = [
            (
                "reset",
                "upstream",
                "hdfs-rename-namenode-reset-recovery",
                "hdfs-native-nn-reset-recovery-source.bin",
                "hdfs-native-nn-reset-recovery-target.bin",
                b"namenode reset recovery payload".as_slice(),
            ),
            (
                "blackhole",
                "downstream",
                "hdfs-rename-response-loss",
                "hdfs-native-nn-blackhole-source.bin",
                "hdfs-native-nn-blackhole-target.bin",
                b"namenode blackhole payload".as_slice(),
            ),
        ];
        for (_, _, _, source_path, _, payload) in namenode_fault_cases {
            operator.write(&prepared.configured_path(source_path).unwrap(), payload.to_vec()).await.unwrap();
        }
        drop(prepared);
        file_manager.evict_revision("hdfs-native-contract", connection.revision);
        let no_proxy_pairs = BTreeSet::new();
        wait_for_proxy_baseline(&namenode_control, &no_proxy_pairs).await;
        wait_for_proxy_baseline(&datanode_control, &no_proxy_pairs).await;

        for (action, direction, label, source_path, destination_path, payload) in namenode_fault_cases {
            let datanode_opens_before_rename = proxy_open_count(&datanode_trace);
            let dispatch_barrier = install_test_hdfs_native_rename_before_dispatch_barrier();
            let worker = create_remote_worker_transfer_for_connection(
                &app,
                "hdfs-native-contract",
                label,
                "rename",
                source_path,
                destination_path,
            )
            .await;
            tokio::time::timeout(Duration::from_secs(10), dispatch_barrier.opened.notified())
                .await
                .expect("HDFS Native rename did not finish preflight before NameNode fault arming");
            post_fault_control(
                &namenode_control,
                &format!("/arm?action={action}&direction={direction}&bytes=1&label={label}&scope=next"),
            );
            dispatch_barrier.release.notify_one();
            let fault_pair = wait_for_fault_trigger(&namenode_trace, label).await;
            let worker_deadline = if action == "blackhole" {
                IO_PROGRESS_WATCHDOG + Duration::from_secs(15)
            } else {
                Duration::from_secs(20)
            };
            tokio::time::timeout(worker_deadline, worker)
                .await
                .unwrap_or_else(|_| panic!("{label} rename worker did not reach a controlled terminal state"))
                .unwrap();
            if action == "blackhole" {
                wait_for_proxy_client_release(&namenode_trace, fault_pair, label, false).await;
            }

            let result = state.storage.get_file_transfer(label).await.unwrap().unwrap();
            if action == "reset" {
                assert_eq!(result.status, "completed", "{result:?}");
                assert_eq!(result.operation_outcome.as_deref(), Some("completed"), "{result:?}");
                assert_eq!(result.error.as_deref(), None, "{result:?}");
            } else {
                assert!(matches!(result.status.as_str(), "failed" | "partial"), "{result:?}");
                assert!(
                    matches!(
                        result.operation_outcome.as_deref(),
                        Some("destination_state_unknown")
                            | Some("move_committed_response_unknown")
                            | Some("destination_present_unproven")
                    ),
                    "{result:?}"
                );
                let error = result.error.as_deref().expect("faulted rename must persist a classified error");
                let lower = error.to_ascii_lowercase();
                assert!(
                    error.contains("HdfsNativeTimeout:") || lower.contains("timed out") || lower.contains("watchdog"),
                    "NameNode blackhole was not classified as a timeout: {error}"
                );
                for secret in [
                    std::env::var("DBX_TEST_HDFS_NATIVE_CONTRACT_USER").unwrap(),
                    std::env::var("DBX_TEST_HDFS_NATIVE_ROOT").unwrap(),
                    "token".to_string(),
                ] {
                    assert!(!error.to_ascii_lowercase().contains(&secret.to_ascii_lowercase()), "{error}");
                }
            }
            assert_eq!(file_manager.operator_count(), 0, "HDFS Native rename must not retain a cached operator");
            let retained_fault_pairs = BTreeSet::from([fault_pair]);
            if action == "blackhole" {
                wait_for_proxy_baseline(&namenode_control, &retained_fault_pairs).await;
            } else {
                wait_for_proxy_baseline(&namenode_control, &no_proxy_pairs).await;
            }
            wait_for_proxy_baseline(&datanode_control, &no_proxy_pairs).await;
            if action == "blackhole" {
                assert_proxy_traffic_quiet(&namenode_control).await;
                assert_proxy_traffic_quiet(&datanode_control).await;
            }
            assert_eq!(
                proxy_open_count(&datanode_trace),
                datanode_opens_before_rename,
                "HDFS Native rename must not open a DataNode relay"
            );

            let warmed = file_manager
                .prepare_file_mutation_operation(&state, "hdfs-native-contract", source_path, connection.revision)
                .await
                .unwrap();
            let source = warmed.configured_path(source_path).unwrap();
            let destination = warmed.configured_path(destination_path).unwrap();
            let source_exists = warmed.operator.exists(&source).await.unwrap();
            let destination_exists = warmed.operator.exists(&destination).await.unwrap();
            if action == "reset" {
                assert!(!source_exists, "{result:?}");
                assert!(destination_exists, "{result:?}");
            } else {
                assert_ne!(source_exists, destination_exists, "{result:?}");
            }
            let survivor = if source_exists { &source } else { &destination };
            assert_eq!(warmed.operator.read(survivor).await.unwrap().to_vec(), payload);
            assert_eq!(file_manager.operator_count(), 0);
            drop(warmed);
            assert_eq!(file_manager.operator_count(), 0);
            if action == "blackhole" {
                wait_for_proxy_baseline(&namenode_control, &retained_fault_pairs).await;
                wait_for_proxy_baseline(&datanode_control, &no_proxy_pairs).await;
                assert_proxy_traffic_quiet(&namenode_control).await;
                assert_proxy_traffic_quiet(&datanode_control).await;
                post_fault_control(&namenode_control, "/drop");
            }
            wait_for_proxy_baseline(&namenode_control, &no_proxy_pairs).await;
            wait_for_proxy_baseline(&datanode_control, &no_proxy_pairs).await;
            assert_proxy_traffic_quiet(&namenode_control).await;
            assert_proxy_traffic_quiet(&datanode_control).await;
        }
    }

    #[tokio::test]
    #[ignore = "run through tests/sftp-contract.sh with a digest-pinned OpenSSH server"]
    async fn fixed_sftp_transfer_contract() {
        let (app, state, directory) = build_sftp_contract_app().await;
        app.state::<FileTransferRuntime>()
            .ensure_recovered(&state, app.state::<FileManagerRuntime>().inner())
            .await
            .unwrap();
        let connection = state.storage.load_file_connection("sftp-contract").await.unwrap().unwrap();
        let file_manager = app.state::<FileManagerRuntime>();
        let prepared = file_manager
            .prepare_file_mutation_operation(&state, "sftp-contract", "fixture.txt", connection.revision)
            .await
            .unwrap();
        assert_eq!(prepared.remote_path, "home/dbx/files/fixture.txt");
        let operator = &prepared.operator;
        let local_root = directory.path().canonicalize().unwrap();

        let download_target = local_root.join("sftp-large-download.bin");
        let (_, download_worker) = create_worker_transfer_for_connection(
            &app,
            "sftp-contract",
            "sftp-large-download",
            "large.bin",
            &download_target,
        )
        .await;
        download_worker.await.unwrap();
        let download = state.storage.get_file_transfer("sftp-large-download").await.unwrap().unwrap();
        assert_eq!(download.status, "completed", "{download:?}");
        assert_eq!(tokio::fs::metadata(&download_target).await.unwrap().len(), 32 * 1024 * 1024);
        assert_no_owned_temp(&local_root, "sftp-large-download");

        let upload_source = local_root.join("sftp-upload-source.bin");
        let upload_bytes = vec![0x5a_u8; 4 * 1024 * 1024 + 137];
        tokio::fs::write(&upload_source, &upload_bytes).await.unwrap();
        let (_, upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "sftp-contract",
            "sftp-upload-success",
            "worker-upload-success.bin",
            &upload_source,
        )
        .await;
        upload_worker.await.unwrap();
        let upload = state.storage.get_file_transfer("sftp-upload-success").await.unwrap().unwrap();
        assert_eq!(upload.status, "completed", "{upload:?}");
        assert_eq!(upload.publish_outcome.as_deref(), Some("completed"), "{upload:?}");
        let upload_path = prepared.configured_path("worker-upload-success.bin").unwrap();
        assert_eq!(operator.read(&upload_path).await.unwrap().to_vec(), upload_bytes);
        assert!(upload.partial_destination.is_none(), "{upload:?}");

        create_remote_worker_transfer_for_connection(
            &app,
            "sftp-contract",
            "sftp-copy-success",
            "copy",
            "large.bin",
            "worker-copy-target.bin",
        )
        .await
        .await
        .unwrap();
        let copy = state.storage.get_file_transfer("sftp-copy-success").await.unwrap().unwrap();
        assert_eq!(copy.status, "completed", "{copy:?}");
        assert_eq!(copy.operation_outcome.as_deref(), Some("completed"), "{copy:?}");
        let copy_path = prepared.configured_path("worker-copy-target.bin").unwrap();
        assert_eq!(operator.stat(&copy_path).await.unwrap().content_length(), 32 * 1024 * 1024);
        let container = std::env::var("DBX_TEST_SFTP_CONTAINER").expect("DBX_TEST_SFTP_CONTAINER is required");
        let source_inode = Command::new("docker")
            .args(["exec", &container, "stat", "-c", "%i", "/home/dbx/files/worker-copy-target.bin"])
            .output()
            .unwrap();
        assert!(source_inode.status.success(), "{}", String::from_utf8_lossy(&source_inode.stderr));

        create_remote_worker_transfer_for_connection(
            &app,
            "sftp-contract",
            "sftp-rename-success",
            "rename",
            "worker-copy-target.bin",
            "worker-rename-target.bin",
        )
        .await
        .await
        .unwrap();
        let renamed = state.storage.get_file_transfer("sftp-rename-success").await.unwrap().unwrap();
        assert_eq!(renamed.status, "completed", "{renamed:?}");
        assert_eq!(renamed.operation_outcome.as_deref(), Some("completed"), "{renamed:?}");
        assert_eq!(operator.stat(&copy_path).await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        let renamed_path = prepared.configured_path("worker-rename-target.bin").unwrap();
        assert_eq!(operator.stat(&renamed_path).await.unwrap().content_length(), 32 * 1024 * 1024);
        let destination_inode = Command::new("docker")
            .args(["exec", &container, "stat", "-c", "%i", "/home/dbx/files/worker-rename-target.bin"])
            .output()
            .unwrap();
        assert!(destination_inode.status.success(), "{}", String::from_utf8_lossy(&destination_inode.stderr));
        assert_eq!(source_inode.stdout, destination_inode.stdout, "SFTP rename must preserve the server inode");

        let no_clobber_source = prepared.configured_path("worker-no-clobber-source.bin").unwrap();
        let no_clobber_target = prepared.configured_path("worker-no-clobber-target.bin").unwrap();
        operator.write(&no_clobber_source, "source").await.unwrap();
        operator.write(&no_clobber_target, "keep").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "sftp-contract",
            "sftp-copy-no-clobber",
            "copy",
            "worker-no-clobber-source.bin",
            "worker-no-clobber-target.bin",
        )
        .await
        .await
        .unwrap();
        let no_clobber = state.storage.get_file_transfer("sftp-copy-no-clobber").await.unwrap().unwrap();
        assert_eq!(no_clobber.status, "failed", "{no_clobber:?}");
        assert_eq!(no_clobber.operation_outcome.as_deref(), Some("failed_before_copy"), "{no_clobber:?}");
        assert_eq!(no_clobber.partial_destination, None);
        assert_eq!(operator.read(&no_clobber_target).await.unwrap().to_vec(), b"keep");

        {
            use super::super::file_manager::{
                password_scope, FileConnectionConfig, SftpAuthentication, SftpConnectionConfig,
            };

            fn post_fault_control(control: &str, route: &str) {
                let output = Command::new("curl")
                    .args(["--silent", "--show-error", "--fail", "--request", "POST"])
                    .arg(format!("{control}{route}"))
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "SFTP fault control request failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            async fn wait_for_fault_trigger(trace: &Path, label: &str, expect_bound_pair: bool) {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let trace = tokio::fs::read_to_string(trace).await.unwrap_or_default();
                        let events = trace
                            .lines()
                            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                            .collect::<Vec<_>>();
                        if let Some(trigger) = events.iter().find(|event| {
                            event.get("event").and_then(serde_json::Value::as_str) == Some("trigger")
                                && event.get("label").and_then(serde_json::Value::as_str) == Some(label)
                        }) {
                            if !expect_bound_pair {
                                break;
                            }
                            if let Some(binding) = events.iter().find(|event| {
                                event.get("event").and_then(serde_json::Value::as_str) == Some("bind")
                                    && event.get("label").and_then(serde_json::Value::as_str) == Some(label)
                            }) {
                                let bound_pair = binding
                                    .get("pairId")
                                    .and_then(serde_json::Value::as_u64)
                                    .expect("next-pair fault binding must identify its SSH pair");
                                let trigger_pair = trigger
                                    .get("pairId")
                                    .and_then(serde_json::Value::as_u64)
                                    .expect("next-pair fault trigger must identify its SSH pair");
                                assert_eq!(trigger_pair, bound_pair, "{label} triggered on a pair it was not bound to");
                                assert_eq!(
                                    trigger.get("boundPairId").and_then(serde_json::Value::as_u64),
                                    Some(bound_pair),
                                    "{label} trigger did not retain its bound pair"
                                );
                                assert_eq!(
                                    trigger.get("scope").and_then(serde_json::Value::as_str),
                                    Some("next"),
                                    "{label} trigger did not use next-pair scope"
                                );
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .expect("the TCP fault proxy did not trigger the armed worker fault");
            }

            async fn await_fault_worker(
                control: &str,
                trace: &Path,
                label: &str,
                kind: &str,
                expect_bound_pair: bool,
                worker: tokio::task::JoinHandle<()>,
            ) {
                wait_for_fault_trigger(trace, label, expect_bound_pair).await;
                let deadline = if kind == "timeout" {
                    IO_PROGRESS_WATCHDOG + Duration::from_secs(15)
                } else {
                    Duration::from_secs(10)
                };
                tokio::time::timeout(deadline, worker)
                    .await
                    .unwrap_or_else(|_| panic!("{label} worker did not reach a terminal state"))
                    .unwrap();
                if kind == "timeout" {
                    post_fault_control(control, "/drop");
                    wait_for_fault_proxy_idle(control).await;
                }
            }

            async fn wait_for_fault_proxy_idle(control: &str) {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let output = Command::new("curl")
                            .args(["--silent", "--show-error", "--fail"])
                            .arg(format!("{control}/health"))
                            .output()
                            .unwrap();
                        assert!(
                            output.status.success(),
                            "SFTP fault health request failed: {}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                        let health: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                        if health.get("activeConnections").and_then(serde_json::Value::as_u64) == Some(0)
                            && health.get("armedFault").is_some_and(serde_json::Value::is_null)
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .expect("SFTP fault proxy retained a connection after control drop");
            }

            async fn assert_fault_connection_recovered(
                file_manager: &FileManagerRuntime,
                state: &AppState,
                revision: i64,
            ) {
                let recovered = tokio::time::timeout(
                    Duration::from_secs(20),
                    file_manager.prepare_file_mutation_operation(state, "sftp-fault", "large.bin", revision),
                )
                .await
                .expect("SFTP fault connection did not rebuild within the recovery budget")
                .unwrap();
                let source = recovered.configured_path("large.bin").unwrap();
                assert_eq!(recovered.operator.stat(&source).await.unwrap().content_length(), 32 * 1024 * 1024);
                drop(recovered);
                assert_eq!(file_manager.operator_count(), 0, "recovered SFTP mutation operator must not remain cached");
            }

            fn assert_fault_operator_evicted(file_manager: &FileManagerRuntime, transfer_id: &str) {
                assert_eq!(
                    file_manager.operator_count(),
                    0,
                    "{transfer_id} fault worker retained a cached SFTP operator"
                );
            }

            fn assert_fault_error(record: &FileTransferStorageRecord, kind: &str) {
                assert!(matches!(record.status.as_str(), "failed" | "partial"), "{record:?}");
                let error = record.error.as_deref().expect("faulted transfer must persist a classified error");
                assert!(!error.contains("dbx-sftp-keys-"), "temporary key path leaked: {error}");
                if kind == "disconnect" {
                    assert!(error.contains("SftpDisconnected:"), "disconnect was not classified: {error}");
                } else {
                    let lower = error.to_ascii_lowercase();
                    assert!(
                        error.contains("SftpTimeout:")
                            || lower.contains("timed out")
                            || lower.contains("watchdog expired"),
                        "timeout was not classified: {error}"
                    );
                }
            }

            async fn assert_no_remote_owned_partial(
                operator: &opendal::Operator,
                configured_root: &str,
                transfer_id: &str,
            ) {
                let residuals = operator
                    .list_with(&format!("{}/", configured_root.trim_matches('/')))
                    .recursive(true)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|entry| entry.path().to_string())
                    .filter(|path| path.contains(transfer_id) && path.ends_with(".part"))
                    .collect::<Vec<_>>();
                assert!(residuals.is_empty(), "unexpected operation-owned partials: {residuals:?}");
            }

            let fault_endpoint =
                std::env::var("DBX_TEST_SFTP_FAULT_ENDPOINT").expect("DBX_TEST_SFTP_FAULT_ENDPOINT is required");
            let fault_control =
                std::env::var("DBX_TEST_SFTP_FAULT_CONTROL").expect("DBX_TEST_SFTP_FAULT_CONTROL is required");
            let fault_trace = PathBuf::from(
                std::env::var("DBX_TEST_SFTP_PROXY_TRACE").expect("DBX_TEST_SFTP_PROXY_TRACE is required"),
            );
            let username = std::env::var("DBX_TEST_SFTP_USERNAME").expect("DBX_TEST_SFTP_USERNAME is required");
            let root = std::env::var("DBX_TEST_SFTP_ROOT").expect("DBX_TEST_SFTP_ROOT is required");
            let private_key_file =
                std::env::var("DBX_TEST_SFTP_PRIVATE_KEY_FILE").expect("DBX_TEST_SFTP_PRIVATE_KEY_FILE is required");
            let private_key = std::fs::read_to_string(private_key_file).unwrap();
            let passphrase = std::env::var("DBX_TEST_SFTP_PRIVATE_KEY_PASSPHRASE")
                .expect("DBX_TEST_SFTP_PRIVATE_KEY_PASSPHRASE is required");
            let fault_config = FileConnectionConfig::Sftp(SftpConnectionConfig {
                endpoint: fault_endpoint,
                root: root.clone(),
                username,
                authentication: SftpAuthentication::PrivateKey,
            });
            state
                .storage
                .save_file_connection_with_secret_bundle(
                    "sftp-fault".into(),
                    "SFTP fault contract".into(),
                    "sftp".into(),
                    serde_json::to_string(&fault_config).unwrap(),
                    vec![
                        ("sftp_private_key".to_string(), private_key),
                        ("sftp_private_key_passphrase".to_string(), passphrase),
                    ],
                    vec!["sftp_private_key".to_string(), "sftp_private_key_passphrase".to_string()],
                    "sftp_scope".to_string(),
                    password_scope(&fault_config).unwrap(),
                    true,
                    None,
                )
                .await
                .unwrap();

            let fault_connection = state.storage.load_file_connection("sftp-fault").await.unwrap().unwrap();
            let warm_fault_connection = || async {
                let warm = file_manager
                    .prepare_file_mutation_operation(&state, "sftp-fault", "large.bin", fault_connection.revision)
                    .await
                    .unwrap();
                let configured = warm.configured_path("large.bin").unwrap();
                assert_eq!(warm.operator.stat(&configured).await.unwrap().content_length(), 32 * 1024 * 1024);
            };
            let arm_fault = |operation: &str, kind: &str, direction: &str, bytes: usize, scope: Option<&str>| {
                let action = if kind == "disconnect" { "reset" } else { "blackhole" };
                let label = format!("{operation}-{kind}");
                let scope = scope.map(|scope| format!("&scope={scope}")).unwrap_or_default();
                post_fault_control(
                    &fault_control,
                    &format!("/arm?action={action}&direction={direction}&bytes={bytes}&label={label}{scope}"),
                );
                label
            };

            for kind in ["disconnect", "timeout"] {
                warm_fault_connection().await;
                let label = arm_fault("download", kind, "downstream", 128 * 1024, None);
                let transfer_id = format!("sftp-download-{kind}");
                let target = local_root.join(format!("{transfer_id}.bin"));
                let (_, worker) =
                    create_worker_transfer_for_connection(&app, "sftp-fault", &transfer_id, "large.bin", &target).await;
                await_fault_worker(&fault_control, &fault_trace, &label, kind, false, worker).await;
                let record = state.storage.get_file_transfer(&transfer_id).await.unwrap().unwrap();
                assert_fault_error(&record, kind);
                assert_fault_operator_evicted(file_manager.inner(), &transfer_id);
                assert_fault_connection_recovered(file_manager.inner(), state.as_ref(), fault_connection.revision)
                    .await;
                assert!(!target.exists(), "faulted download published its final target");
                assert_no_owned_temp(&local_root, &transfer_id);
            }

            let fault_upload_source = local_root.join("sftp-fault-upload-source.bin");
            let fault_upload_payload = vec![0x6b_u8; 32 * 1024 * 1024 + 137];
            tokio::fs::write(&fault_upload_source, &fault_upload_payload).await.unwrap();
            for kind in ["disconnect", "timeout"] {
                warm_fault_connection().await;
                let label = arm_fault("upload", kind, "upstream", 128 * 1024, None);
                let transfer_id = format!("sftp-upload-{kind}");
                let destination = format!("worker-{transfer_id}.bin");
                let (_, worker) = create_upload_worker_transfer_for_connection(
                    &app,
                    "sftp-fault",
                    &transfer_id,
                    &destination,
                    &fault_upload_source,
                )
                .await;
                await_fault_worker(&fault_control, &fault_trace, &label, kind, false, worker).await;
                let record = state.storage.get_file_transfer(&transfer_id).await.unwrap().unwrap();
                assert_fault_error(&record, kind);
                assert_fault_operator_evicted(file_manager.inner(), &transfer_id);
                assert_fault_connection_recovered(file_manager.inner(), state.as_ref(), fault_connection.revision)
                    .await;
                let oracle = file_manager
                    .prepare_file_mutation_operation(&state, "sftp-contract", &destination, connection.revision)
                    .await
                    .unwrap();
                let configured_destination = oracle.configured_path(&destination).unwrap();
                assert!(
                    !oracle.operator.exists(&configured_destination).await.unwrap(),
                    "faulted upload published its target"
                );
                if let Some(partial) = record.partial_destination.as_deref() {
                    assert!(
                        partial.contains(&format!(".dbx-upload-{transfer_id}-")) && partial.ends_with(".part"),
                        "{record:?}"
                    );
                    let configured_partial = oracle.configured_path(partial).unwrap();
                    if oracle.operator.exists(&configured_partial).await.unwrap() {
                        oracle.operator.delete(&configured_partial).await.unwrap();
                    }
                } else {
                    assert_no_remote_owned_partial(&oracle.operator, &root, &transfer_id).await;
                }
                drop(oracle);
            }

            for kind in ["disconnect", "timeout"] {
                warm_fault_connection().await;
                let label = arm_fault("copy", kind, "either", 256 * 1024, None);
                let transfer_id = format!("sftp-copy-{kind}");
                let destination = format!("worker-{transfer_id}.bin");
                let worker = create_remote_worker_transfer_for_connection(
                    &app,
                    "sftp-fault",
                    &transfer_id,
                    "copy",
                    "large.bin",
                    &destination,
                )
                .await;
                await_fault_worker(&fault_control, &fault_trace, &label, kind, false, worker).await;
                let record = state.storage.get_file_transfer(&transfer_id).await.unwrap().unwrap();
                assert_fault_error(&record, kind);
                assert_fault_operator_evicted(file_manager.inner(), &transfer_id);
                assert_fault_connection_recovered(file_manager.inner(), state.as_ref(), fault_connection.revision)
                    .await;
                let oracle = file_manager
                    .prepare_file_mutation_operation(&state, "sftp-contract", &destination, connection.revision)
                    .await
                    .unwrap();
                assert_eq!(
                    oracle.operator.stat(&oracle.configured_path("large.bin").unwrap()).await.unwrap().content_length(),
                    32 * 1024 * 1024
                );
                let configured_destination = oracle.configured_path(&destination).unwrap();
                assert!(
                    !oracle.operator.exists(&configured_destination).await.unwrap(),
                    "faulted copy published its target"
                );
                if let Some(partial) = record.partial_destination.as_deref() {
                    assert!(
                        partial.contains(&format!(".dbx-copy-{transfer_id}-")) && partial.ends_with(".part"),
                        "{record:?}"
                    );
                    let configured_partial = oracle.configured_path(partial).unwrap();
                    if oracle.operator.exists(&configured_partial).await.unwrap() {
                        oracle.operator.delete(&configured_partial).await.unwrap();
                    }
                } else {
                    assert_no_remote_owned_partial(&oracle.operator, &root, &transfer_id).await;
                }
                drop(oracle);
            }

            for kind in ["disconnect", "timeout"] {
                let transfer_id = format!("sftp-rename-{kind}");
                let source = format!("worker-{transfer_id}-source.bin");
                let destination = format!("worker-{transfer_id}-target.bin");
                let payload = format!("{transfer_id} payload");
                operator.write(&prepared.configured_path(&source).unwrap(), payload.clone()).await.unwrap();
                let dispatch_barrier = install_test_sftp_rename_before_dispatch_barrier();
                let worker = create_remote_worker_transfer_for_connection(
                    &app,
                    "sftp-fault",
                    &transfer_id,
                    "rename",
                    &source,
                    &destination,
                )
                .await;
                tokio::time::timeout(Duration::from_secs(10), dispatch_barrier.opened.notified())
                    .await
                    .expect("SFTP rename must finish preflight before the fault is armed");
                post_fault_control(&fault_control, "/drop");
                let label = arm_fault("rename", kind, "upstream", 1, Some("next"));
                dispatch_barrier.release.notify_one();
                await_fault_worker(&fault_control, &fault_trace, &label, kind, true, worker).await;
                let record = state.storage.get_file_transfer(&transfer_id).await.unwrap().unwrap();
                assert_fault_error(&record, kind);
                assert_fault_operator_evicted(file_manager.inner(), &transfer_id);
                assert_fault_connection_recovered(file_manager.inner(), state.as_ref(), fault_connection.revision)
                    .await;
                assert!(
                    matches!(
                        record.operation_outcome.as_deref(),
                        Some("failed_before_copy")
                            | Some("destination_state_unknown")
                            | Some("move_committed_response_unknown")
                    ),
                    "{record:?}"
                );
                let oracle = file_manager
                    .prepare_file_mutation_operation(&state, "sftp-contract", &source, connection.revision)
                    .await
                    .unwrap();
                let source = oracle.configured_path(&source).unwrap();
                let destination = oracle.configured_path(&destination).unwrap();
                let source_exists = oracle.operator.exists(&source).await.unwrap();
                let destination_exists = oracle.operator.exists(&destination).await.unwrap();
                assert_ne!(source_exists, destination_exists, "rename must leave exactly one complete name");
                let surviving_path = if source_exists { &source } else { &destination };
                assert_eq!(oracle.operator.read(surviving_path).await.unwrap().to_vec(), payload.as_bytes());
                drop(oracle);
            }
        }

        let cancel_download_target = local_root.join("sftp-download-cancelled.bin");
        let download_cancel_barrier = install_test_remote_reader_barrier();
        let (_, download_cancel_worker) = create_worker_transfer_for_connection(
            &app,
            "sftp-contract",
            "sftp-download-cancelled",
            "large.bin",
            &cancel_download_target,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), download_cancel_barrier.opened.notified())
            .await
            .expect("SFTP download must reach its active reader barrier");
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "sftp-download-cancelled",
        )
        .await
        .unwrap();
        download_cancel_barrier.release.notify_one();
        download_cancel_worker.await.unwrap();
        let cancelled_download = state.storage.get_file_transfer("sftp-download-cancelled").await.unwrap().unwrap();
        assert_eq!(cancelled_download.status, "cancelled", "{cancelled_download:?}");
        assert_eq!(cancelled_download.bytes_transferred, 0, "{cancelled_download:?}");
        assert!(!cancel_download_target.exists());
        assert_no_owned_temp(&local_root, "sftp-download-cancelled");
        assert!(!cancelled_download.error.as_deref().is_some_and(|error| error.contains("dbx-sftp-keys-")));

        let cancel_upload_source = local_root.join("sftp-upload-cancelled-source.bin");
        tokio::fs::write(&cancel_upload_source, vec![0x37_u8; UPLOAD_BUFFER_SIZE * 2 + 19]).await.unwrap();
        let upload_cancel_barrier = install_test_upload_after_chunk_barrier();
        let (_, upload_cancel_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "sftp-contract",
            "sftp-upload-cancelled",
            "worker-upload-cancelled.bin",
            &cancel_upload_source,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), upload_cancel_barrier.opened.notified())
            .await
            .expect("SFTP upload must reach its first-chunk barrier");
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "sftp-upload-cancelled",
        )
        .await
        .unwrap();
        upload_cancel_barrier.release.notify_one();
        upload_cancel_worker.await.unwrap();
        let cancelled_upload = state.storage.get_file_transfer("sftp-upload-cancelled").await.unwrap().unwrap();
        assert_eq!(cancelled_upload.status, "cancelled", "{cancelled_upload:?}");
        assert!(cancelled_upload.bytes_transferred > 0, "{cancelled_upload:?}");
        assert_eq!(cancelled_upload.partial_destination, None, "{cancelled_upload:?}");
        assert!(!operator.exists(&prepared.configured_path("worker-upload-cancelled.bin").unwrap()).await.unwrap());
        assert!(!cancelled_upload.error.as_deref().is_some_and(|error| error.contains("dbx-sftp-keys-")));

        let copy_cancel_barrier = install_test_remote_copy_after_close_barrier();
        let copy_cancel_worker = create_remote_worker_transfer_for_connection(
            &app,
            "sftp-contract",
            "sftp-copy-cancelled",
            "copy",
            "large.bin",
            "worker-copy-cancelled.bin",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(15), copy_cancel_barrier.opened.notified())
            .await
            .expect("SFTP copy must reach its post-close barrier");
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "sftp-copy-cancelled",
        )
        .await
        .unwrap();
        copy_cancel_barrier.release.notify_one();
        copy_cancel_worker.await.unwrap();
        let cancelled_copy = state.storage.get_file_transfer("sftp-copy-cancelled").await.unwrap().unwrap();
        assert_eq!(cancelled_copy.status, "cancelled", "{cancelled_copy:?}");
        assert!(cancelled_copy.bytes_transferred > 0, "{cancelled_copy:?}");
        assert_eq!(cancelled_copy.partial_destination, None, "{cancelled_copy:?}");
        assert!(!operator.exists(&prepared.configured_path("worker-copy-cancelled.bin").unwrap()).await.unwrap());
        assert!(!cancelled_copy.error.as_deref().is_some_and(|error| error.contains("dbx-sftp-keys-")));

        let root = std::env::var("DBX_TEST_SFTP_ROOT").unwrap();
        let residuals = operator
            .list_with(&format!("{}/", root.trim_matches('/')))
            .recursive(true)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.path().to_string())
            .filter(|path| path.contains(".dbx-upload-") || path.contains(".dbx-copy-"))
            .collect::<Vec<_>>();
        assert!(residuals.is_empty(), "residual SFTP operation-owned partials: {residuals:?}");
    }

    async fn assert_no_s3_owned_partial(operator: &opendal::Operator, transfer_id: &str) {
        let upload_prefix = format!(".dbx-upload-{transfer_id}-");
        let copy_prefix = format!(".dbx-copy-{transfer_id}-");
        let residuals = operator
            .list_with("/")
            .recursive(true)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.path().to_string())
            .filter(|path| (path.contains(&upload_prefix) || path.contains(&copy_prefix)) && path.ends_with(".part"))
            .collect::<Vec<_>>();
        assert!(residuals.is_empty(), "residual S3 operation-owned partials: {residuals:?}");
    }

    async fn create_remote_worker_transfer<R: Runtime>(
        app: &tauri::App<R>,
        transfer_id: &str,
        operation: &'static str,
        source_path: &str,
        destination_path: &str,
    ) -> tokio::task::JoinHandle<()> {
        create_remote_worker_transfer_for_connection(
            app,
            "ftp-contract",
            transfer_id,
            operation,
            source_path,
            destination_path,
        )
        .await
    }

    async fn create_remote_worker_transfer_for_connection<R: Runtime>(
        app: &tauri::App<R>,
        connection_id: &str,
        transfer_id: &str,
        operation: &'static str,
        source_path: &str,
        destination_path: &str,
    ) -> tokio::task::JoinHandle<()> {
        create_remote_worker_transfer_with_policy(
            app,
            connection_id,
            transfer_id,
            operation,
            source_path,
            destination_path,
            RemoteMutationPolicy::BestEffortNoClobber { atomic_no_clobber: false, external_toctou_risk: true },
        )
        .await
    }

    async fn create_remote_worker_transfer_with_policy<R: Runtime>(
        app: &tauri::App<R>,
        connection_id: &str,
        transfer_id: &str,
        operation: &'static str,
        source_path: &str,
        destination_path: &str,
        policy: RemoteMutationPolicy,
    ) -> tokio::task::JoinHandle<()> {
        let state = app.state::<Arc<AppState>>();
        let connection = state.storage.load_file_connection(connection_id).await.unwrap().unwrap();
        state
            .storage
            .create_file_remote_transfer(
                transfer_id.to_string(),
                connection_id.to_string(),
                operation.to_string(),
                source_path.to_string(),
                destination_path.to_string(),
                connection.revision,
            )
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        app.state::<FileTransferRuntime>().register(
            transfer_id.to_string(),
            connection_id.to_string(),
            cancellation.clone(),
        );
        let app_handle = app.handle().clone();
        let transfer_id = transfer_id.to_string();
        let connection_id = connection_id.to_string();
        tokio::spawn(async move {
            run_remote_transfer_worker(app_handle, transfer_id, connection_id, cancellation, operation, policy).await;
        })
    }

    #[tokio::test]
    #[ignore = "run through tests/webhdfs-contract.sh with the fixed Hadoop service"]
    async fn fixed_webhdfs_file_transfer_worker_contract() {
        use super::super::file_manager::{
            create_file_directory, delete_file_entry, list_file_entries, stat_file_entry,
        };
        use super::super::file_manager_webhdfs::{reset_test_open_request_count, test_open_request_count};

        async fn assert_cancelled_webhdfs_artifacts_absent(
            app: &tauri::App<tauri::test::MockRuntime>,
            transfer_id: &str,
            destination: &str,
        ) {
            let upload_prefix = format!(".dbx-upload-{transfer_id}-");
            let copy_prefix = format!(".dbx-copy-{transfer_id}-");
            for _ in 0..8 {
                let page = list_file_entries(
                    app.state::<Arc<AppState>>(),
                    app.state::<FileManagerRuntime>(),
                    "webhdfs-contract".to_string(),
                    "worker".to_string(),
                    None,
                )
                .await
                .unwrap();
                let residuals = page
                    .entries
                    .iter()
                    .filter(|entry| {
                        let name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
                        entry.path == destination
                            || ((name.starts_with(&upload_prefix) || name.starts_with(&copy_prefix))
                                && name.ends_with(".part"))
                    })
                    .map(|entry| entry.path.clone())
                    .collect::<Vec<_>>();
                assert!(
                    residuals.is_empty(),
                    "cancelled WebHDFS transfer left destination or operation-owned partials: {residuals:?}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }

        let (app, state, directory) = build_webhdfs_contract_app().await;
        app.state::<FileTransferRuntime>()
            .ensure_recovered(&state, app.state::<FileManagerRuntime>().inner())
            .await
            .unwrap();
        create_file_directory(
            app.state::<Arc<AppState>>(),
            app.state::<FileManagerRuntime>(),
            "webhdfs-contract".to_string(),
            "worker".to_string(),
        )
        .await
        .unwrap();

        let local_root = directory.path().canonicalize().unwrap();
        let source_path = local_root.join("webhdfs-upload-source.bin");
        let mut payload = vec![0x5a; 9 * 1024 * 1024 + 17];
        payload[0] = 0x11;
        payload[9 * 1024 * 1024] = 0x22;
        tokio::fs::write(&source_path, &payload).await.unwrap();

        let (_, upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "webhdfs-contract",
            "webhdfs-upload-success",
            "worker/source 100%+#?&=.bin",
            &source_path,
        )
        .await;
        upload_worker.await.unwrap();
        let upload = state.storage.get_file_transfer("webhdfs-upload-success").await.unwrap().unwrap();
        assert_eq!(upload.status, "completed", "{upload:?}");
        assert_eq!(upload.publish_outcome.as_deref(), Some("completed"), "{upload:?}");
        assert!(upload.partial_destination.is_none(), "{upload:?}");

        let upload_cancel_barrier = install_test_upload_after_chunk_barrier();
        let (_, cancelled_upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "webhdfs-contract",
            "webhdfs-upload-cancelled",
            "worker/upload-cancelled.bin",
            &source_path,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(15), upload_cancel_barrier.opened.notified())
            .await
            .expect("WebHDFS upload did not complete its first configured chunk");
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "webhdfs-upload-cancelled",
        )
        .await
        .unwrap();
        upload_cancel_barrier.release.notify_one();
        cancelled_upload_worker.await.unwrap();
        let cancelled_upload = state.storage.get_file_transfer("webhdfs-upload-cancelled").await.unwrap().unwrap();
        assert_cancelled_webhdfs_artifacts_absent(&app, "webhdfs-upload-cancelled", "worker/upload-cancelled.bin")
            .await;
        assert_eq!(cancelled_upload.status, "cancelled", "{cancelled_upload:?}");
        assert!((1..=4 * 1024 * 1024).contains(&cancelled_upload.bytes_transferred), "{cancelled_upload:?}");
        assert_eq!(cancelled_upload.partial_destination, None, "{cancelled_upload:?}");
        assert_eq!(
            cancelled_upload.abort_outcome.as_deref(),
            Some("unsupported; operation_owned_partial_cleaned"),
            "{cancelled_upload:?}"
        );
        assert_eq!(cancelled_upload.publish_outcome, None, "{cancelled_upload:?}");

        let download_path = local_root.join("webhdfs-download.bin");
        reset_test_open_request_count();
        let (_, download_worker) = create_worker_transfer_for_connection(
            &app,
            "webhdfs-contract",
            "webhdfs-download-success",
            "worker/source 100%+#?&=.bin",
            &download_path,
        )
        .await;
        download_worker.await.unwrap();
        assert_eq!(test_open_request_count(), 1, "WebHDFS download must use one OPEN request");
        let download = state.storage.get_file_transfer("webhdfs-download-success").await.unwrap().unwrap();
        assert_eq!(download.status, "completed", "{download:?}");
        assert_eq!(tokio::fs::read(&download_path).await.unwrap(), payload);
        assert_no_owned_temp(&local_root, "webhdfs-download-success");

        reset_test_remote_copy_high_water();
        reset_test_open_request_count();
        create_remote_worker_transfer_for_connection(
            &app,
            "webhdfs-contract",
            "webhdfs-copy-success",
            "copy",
            "worker/source 100%+#?&=.bin",
            "worker/copy 100%+#?&=.bin",
        )
        .await
        .await
        .unwrap();
        let copy = state.storage.get_file_transfer("webhdfs-copy-success").await.unwrap().unwrap();
        assert_eq!(copy.status, "completed", "{copy:?}");
        assert_eq!(copy.operation_outcome.as_deref(), Some("completed"), "{copy:?}");
        assert!(copy.partial_destination.is_none(), "{copy:?}");
        assert_eq!(
            test_open_request_count(),
            3,
            "WebHDFS copy must use one source, one partial-verification, and one destination-verification OPEN"
        );
        let configured_chunk = 4 * 1024 * 1024;
        let (max_read_chunk, max_write_chunk, max_relay_payload) = test_remote_copy_high_water();
        assert!((1..=configured_chunk).contains(&max_read_chunk), "{max_read_chunk}");
        assert!((1..=configured_chunk).contains(&max_write_chunk), "{max_write_chunk}");
        assert!((1..=configured_chunk).contains(&max_relay_payload), "{max_relay_payload}");

        reset_test_remote_copy_high_water();
        let copy_cancel_barrier = install_test_remote_copy_after_chunk_barrier();
        let cancelled_copy_worker = create_remote_worker_transfer_for_connection(
            &app,
            "webhdfs-contract",
            "webhdfs-copy-cancelled",
            "copy",
            "worker/source 100%+#?&=.bin",
            "worker/copy-cancelled.bin",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(15), copy_cancel_barrier.opened.notified())
            .await
            .expect("WebHDFS copy did not complete its first relay chunk");
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "webhdfs-copy-cancelled",
        )
        .await
        .unwrap();
        copy_cancel_barrier.release.notify_one();
        cancelled_copy_worker.await.unwrap();
        let cancelled_copy = state.storage.get_file_transfer("webhdfs-copy-cancelled").await.unwrap().unwrap();
        assert_eq!(cancelled_copy.status, "cancelled", "{cancelled_copy:?}");
        assert!((1..=configured_chunk as i64).contains(&cancelled_copy.bytes_transferred), "{cancelled_copy:?}");
        assert_eq!(cancelled_copy.partial_destination, None, "{cancelled_copy:?}");
        assert_eq!(cancelled_copy.operation_outcome.as_deref(), Some("failed_before_copy"), "{cancelled_copy:?}");
        assert_eq!(cancelled_copy.operation_phase.as_deref(), Some("copying"), "{cancelled_copy:?}");
        assert_eq!(cancelled_copy.destination_fingerprint, None, "{cancelled_copy:?}");
        let (max_read_chunk, max_write_chunk, max_relay_payload) = test_remote_copy_high_water();
        assert!((1..=configured_chunk).contains(&max_read_chunk), "{max_read_chunk}");
        assert!((1..=configured_chunk).contains(&max_write_chunk), "{max_write_chunk}");
        assert!((1..=configured_chunk).contains(&max_relay_payload), "{max_relay_payload}");
        assert_cancelled_webhdfs_artifacts_absent(&app, "webhdfs-copy-cancelled", "worker/copy-cancelled.bin").await;

        create_remote_worker_transfer_with_policy(
            &app,
            "webhdfs-contract",
            "webhdfs-rename-success",
            "rename",
            "worker/copy 100%+#?&=.bin",
            "worker/renamed 100%+#?&=.bin",
            RemoteMutationPolicy::BestEffortNoClobber { atomic_no_clobber: false, external_toctou_risk: true },
        )
        .await
        .await
        .unwrap();
        let renamed = state.storage.get_file_transfer("webhdfs-rename-success").await.unwrap().unwrap();
        assert_eq!(renamed.status, "completed", "{renamed:?}");
        assert_eq!(renamed.operation_outcome.as_deref(), Some("completed"), "{renamed:?}");

        reset_test_remote_copy_high_water();
        reset_test_open_request_count();
        create_remote_worker_transfer_with_policy(
            &app,
            "webhdfs-contract",
            "webhdfs-rename-replace-existing",
            "rename",
            "worker/source 100%+#?&=.bin",
            "worker/renamed 100%+#?&=.bin",
            RemoteMutationPolicy::Replace { confirmed: true },
        )
        .await
        .await
        .unwrap();
        let rejected = state.storage.get_file_transfer("webhdfs-rename-replace-existing").await.unwrap().unwrap();
        assert_eq!(rejected.status, "failed", "{rejected:?}");
        assert!(rejected.error.as_deref().is_some_and(|error| error.contains(WEBHDFS_REPLACE_UNSUPPORTED)));
        assert_eq!(rejected.bytes_transferred, 0, "{rejected:?}");
        assert_eq!(rejected.partial_destination, None, "{rejected:?}");
        assert_eq!(test_open_request_count(), 0, "WebHDFS Replace must fail before opening the source");
        assert_eq!(test_remote_copy_high_water(), (0, 0, 0));

        let page = list_file_entries(
            app.state::<Arc<AppState>>(),
            app.state::<FileManagerRuntime>(),
            "webhdfs-contract".to_string(),
            "worker".to_string(),
            None,
        )
        .await
        .unwrap();
        assert!(page.entries.iter().any(|entry| entry.path == "worker/source 100%+#?&=.bin"));
        assert!(page.entries.iter().any(|entry| entry.path == "worker/renamed 100%+#?&=.bin"));
        let stat = stat_file_entry(
            app.state::<Arc<AppState>>(),
            app.state::<FileManagerRuntime>(),
            "webhdfs-contract".to_string(),
            "worker/renamed 100%+#?&=.bin".to_string(),
        )
        .await
        .unwrap();
        assert_eq!(stat.size, payload.len() as u64);

        for path in ["worker/source 100%+#?&=.bin", "worker/renamed 100%+#?&=.bin"] {
            delete_file_entry(
                app.state::<Arc<AppState>>(),
                app.state::<FileManagerRuntime>(),
                "webhdfs-contract".to_string(),
                path.to_string(),
                Some(false),
                Some("file".to_string()),
            )
            .await
            .unwrap();
        }
        delete_file_entry(
            app.state::<Arc<AppState>>(),
            app.state::<FileManagerRuntime>(),
            "webhdfs-contract".to_string(),
            "worker".to_string(),
            Some(false),
            Some("directory".to_string()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[ignore = "run through tests/webhdfs-contract.sh with the fixed Hadoop service"]
    async fn fixed_webhdfs_permission_failure_contract() {
        let connection_id = "webhdfs-permission-contract";
        let root =
            std::env::var("DBX_TEST_WEBHDFS_PERMISSION_ROOT").expect("DBX_TEST_WEBHDFS_PERMISSION_ROOT is required");
        let (app, state, directory) = build_webhdfs_contract_app_for(connection_id, root).await;
        app.state::<FileTransferRuntime>()
            .ensure_recovered(&state, app.state::<FileManagerRuntime>().inner())
            .await
            .unwrap();
        let source = directory.path().canonicalize().unwrap().join("permission-denied-source.bin");
        let payload = vec![0x61_u8; 2 * 1024 * 1024 + 17];
        tokio::fs::write(&source, &payload).await.unwrap();
        let transfer_id = "webhdfs-upload-permission-denied";
        let destination = "permission-denied-target.bin";

        let (_, worker) =
            create_upload_worker_transfer_for_connection(&app, connection_id, transfer_id, destination, &source).await;
        tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .expect("WebHDFS permission-denied upload worker timed out")
            .unwrap();
        let transfer = state.storage.get_file_transfer(transfer_id).await.unwrap().unwrap();
        assert_eq!(transfer.status, "failed", "{transfer:?}");
        assert_ne!(transfer.status, "completed", "{transfer:?}");
        assert_eq!(Some(transfer.bytes_transferred), transfer.total_bytes, "{transfer:?}");
        assert_eq!(transfer.partial_destination, None, "{transfer:?}");
        let error = transfer.error.as_deref().unwrap_or_default().to_ascii_lowercase();
        assert!(
            error.contains("permission") || error.contains("accesscontrol") || error.contains("forbidden"),
            "{transfer:?}"
        );
        assert_webhdfs_upload_artifacts_absent(&app, connection_id, "", transfer_id, destination).await;
    }

    #[tokio::test]
    #[ignore = "run through tests/webhdfs-contract.sh with the fixed Hadoop service"]
    async fn fixed_webhdfs_quota_failure_contract() {
        let connection_id = "webhdfs-quota-contract";
        let root = std::env::var("DBX_TEST_WEBHDFS_QUOTA_ROOT").expect("DBX_TEST_WEBHDFS_QUOTA_ROOT is required");
        let (app, state, directory) = build_webhdfs_contract_app_for(connection_id, root).await;
        app.state::<FileTransferRuntime>()
            .ensure_recovered(&state, app.state::<FileManagerRuntime>().inner())
            .await
            .unwrap();
        let source = directory.path().canonicalize().unwrap().join("quota-source.bin");
        let payload = vec![0x71_u8; 2 * 1024 * 1024 + 17];
        tokio::fs::write(&source, &payload).await.unwrap();
        let transfer_id = "webhdfs-upload-quota-exceeded";
        let destination = "quota-target.bin";

        let (_, worker) =
            create_upload_worker_transfer_for_connection(&app, connection_id, transfer_id, destination, &source).await;
        tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .expect("WebHDFS quota upload worker timed out")
            .unwrap();
        let transfer = state.storage.get_file_transfer(transfer_id).await.unwrap().unwrap();
        assert_eq!(transfer.status, "failed", "{transfer:?}");
        assert_ne!(transfer.status, "completed", "{transfer:?}");
        assert_eq!(transfer.partial_destination, None, "{transfer:?}");
        let error = transfer.error.as_deref().unwrap_or_default().to_ascii_lowercase();
        assert!(error.contains("quota") || error.contains("space"), "{transfer:?}");
        assert_webhdfs_upload_artifacts_absent(&app, connection_id, "", transfer_id, destination).await;
    }

    #[tokio::test]
    #[ignore = "run through tests/webhdfs-contract.sh with the fixed Hadoop service and DataNode fault proxy"]
    async fn fixed_webhdfs_datanode_disconnect_contract() {
        let connection_id = "webhdfs-disconnect-contract";
        let root = std::env::var("DBX_TEST_WEBHDFS_ROOT").expect("DBX_TEST_WEBHDFS_ROOT is required");
        let fault_control =
            std::env::var("DBX_TEST_WEBHDFS_FAULT_CONTROL").expect("DBX_TEST_WEBHDFS_FAULT_CONTROL is required");
        let fault_trace = PathBuf::from(
            std::env::var("DBX_TEST_WEBHDFS_FAULT_TRACE").expect("DBX_TEST_WEBHDFS_FAULT_TRACE is required"),
        );
        let (app, state, directory) = build_webhdfs_contract_app_for(connection_id, root).await;
        app.state::<FileTransferRuntime>()
            .ensure_recovered(&state, app.state::<FileManagerRuntime>().inner())
            .await
            .unwrap();
        assert_webhdfs_fault_proxy_idle(&fault_control).await;
        let source = directory.path().canonicalize().unwrap().join("datanode-disconnect-source.bin");
        let payload = vec![0x64_u8; 8 * 1024 * 1024 + 17];
        tokio::fs::write(&source, &payload).await.unwrap();
        let transfer_id = "webhdfs-upload-datanode-disconnect";
        let destination = "datanode-disconnect-target.bin";
        let fault_label = "webhdfs-upload-reset";
        let armed = webhdfs_fault_control(
            &fault_control,
            "POST",
            &format!("/arm?action=reset&direction=upstream&bytes={}&label={fault_label}&scope=next", 256 * 1024),
        );
        assert_eq!(armed.pointer("/armedFault/label").and_then(serde_json::Value::as_str), Some(fault_label));

        let (_, worker) =
            create_upload_worker_transfer_for_connection(&app, connection_id, transfer_id, destination, &source).await;
        wait_for_webhdfs_fault_trigger(&fault_trace, fault_label).await;
        tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .expect("WebHDFS DataNode-disconnect upload worker timed out")
            .unwrap();
        let transfer = state.storage.get_file_transfer(transfer_id).await.unwrap().unwrap();
        assert_eq!(transfer.status, "failed", "{transfer:?}");
        assert_ne!(transfer.status, "completed", "{transfer:?}");
        assert!(transfer.bytes_transferred > 0, "{transfer:?}");
        assert_eq!(transfer.partial_destination, None, "{transfer:?}");
        let abort_outcome = transfer.abort_outcome.as_deref().unwrap_or_default();
        assert!(
            (abort_outcome.starts_with("unsupported;") || abort_outcome.starts_with("failed:"))
                && abort_outcome.ends_with("operation_owned_partial_cleaned"),
            "{transfer:?}"
        );
        assert!(transfer.error.is_some(), "{transfer:?}");
        let health = webhdfs_fault_control(&fault_control, "GET", "/health");
        assert!(health.get("armedFault").is_none_or(serde_json::Value::is_null));
        assert_webhdfs_upload_artifacts_absent(&app, connection_id, "", transfer_id, destination).await;
        assert_webhdfs_fault_proxy_idle(&fault_control).await;
    }

    #[tokio::test]
    #[ignore = "run through tests/webdav-contract.sh with a digest-pinned WebDAV server"]
    async fn fixed_webdav_file_transfer_worker_contract() {
        let (app, state, operator, directory) = build_webdav_contract_app().await;
        app.state::<FileTransferRuntime>()
            .ensure_recovered(&state, app.state::<FileManagerRuntime>().inner())
            .await
            .unwrap();
        let local_root = directory.path().canonicalize().unwrap();

        let upload_source = local_root.join("webdav-upload-success.bin");
        tokio::fs::write(&upload_source, b"worker upload").await.unwrap();
        let (_, upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-upload-success",
            "worker-upload-success.bin",
            &upload_source,
        )
        .await;
        upload_worker.await.unwrap();
        let upload = state.storage.get_file_transfer("webdav-upload-success").await.unwrap().unwrap();
        assert_eq!(upload.status, "completed", "{upload:?}");
        assert_eq!(upload.publish_outcome.as_deref(), Some("completed"), "{upload:?}");
        assert_eq!(operator.read("worker-upload-success.bin").await.unwrap().to_vec(), b"worker upload");
        assert!(upload.partial_destination.is_none(), "{upload:?}");

        operator.write("worker-copy-source.bin", "copy payload").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-copy-success",
            "copy",
            "worker-copy-source.bin",
            "worker-copy-target.bin",
        )
        .await
        .await
        .unwrap();
        let copy = state.storage.get_file_transfer("webdav-copy-success").await.unwrap().unwrap();
        assert_eq!(copy.status, "completed", "{copy:?}");
        assert_eq!(copy.operation_outcome.as_deref(), Some("completed"), "{copy:?}");
        assert_eq!(operator.read("worker-copy-target.bin").await.unwrap().to_vec(), b"copy payload");

        operator.write("worker-move-source.bin", "move payload").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-move-success",
            "rename",
            "worker-move-source.bin",
            "worker-move-target.bin",
        )
        .await
        .await
        .unwrap();
        let moved = state.storage.get_file_transfer("webdav-move-success").await.unwrap().unwrap();
        assert_eq!(moved.status, "completed", "{moved:?}");
        assert_eq!(operator.stat("worker-move-source.bin").await.unwrap_err().kind(), opendal::ErrorKind::NotFound);

        operator.write("worker-no-clobber-source.bin", "source").await.unwrap();
        operator.write("worker-no-clobber-target.bin", "keep").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-copy-no-clobber",
            "copy",
            "worker-no-clobber-source.bin",
            "worker-no-clobber-target.bin",
        )
        .await
        .await
        .unwrap();
        let no_clobber = state.storage.get_file_transfer("webdav-copy-no-clobber").await.unwrap().unwrap();
        assert_eq!(no_clobber.status, "failed", "{no_clobber:?}");
        assert_eq!(no_clobber.operation_outcome.as_deref(), Some("failed_before_copy"), "{no_clobber:?}");
        assert_eq!(no_clobber.partial_destination, None);
        assert_eq!(operator.read("worker-no-clobber-target.bin").await.unwrap().to_vec(), b"keep");

        operator.write("worker-reject-copy-source.bin", "reject").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-copy-reject-403",
            "copy",
            "worker-reject-copy-source.bin",
            "worker-copy-reject-403.bin",
        )
        .await
        .await
        .unwrap();
        let rejected_copy = state.storage.get_file_transfer("webdav-copy-reject-403").await.unwrap().unwrap();
        assert_eq!(rejected_copy.status, "failed", "{rejected_copy:?}");
        assert_eq!(rejected_copy.operation_outcome.as_deref(), Some("failed_before_copy"), "{rejected_copy:?}");
        assert_eq!(rejected_copy.partial_destination, None);
        assert!(rejected_copy.error.as_deref().is_some_and(|error| error.contains("HTTP 403")));

        operator.write("worker-reject-move-source.bin", "reject").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-move-reject-507",
            "rename",
            "worker-reject-move-source.bin",
            "worker-move-reject-507.bin",
        )
        .await
        .await
        .unwrap();
        let rejected_move = state.storage.get_file_transfer("webdav-move-reject-507").await.unwrap().unwrap();
        assert_eq!(rejected_move.status, "failed", "{rejected_move:?}");
        assert_eq!(rejected_move.operation_outcome.as_deref(), Some("failed_before_copy"), "{rejected_move:?}");
        assert_eq!(rejected_move.partial_destination, None);
        assert!(rejected_move.error.as_deref().is_some_and(|error| error.contains("HTTP 507")));
        assert!(operator.exists("worker-reject-move-source.bin").await.unwrap());

        let rejected_upload_source = local_root.join("webdav-upload-reject-403.bin");
        tokio::fs::write(&rejected_upload_source, b"rejected upload").await.unwrap();
        let (_, rejected_upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-upload-reject-403",
            "worker-upload-reject-403.bin",
            &rejected_upload_source,
        )
        .await;
        rejected_upload_worker.await.unwrap();
        let rejected_upload = state.storage.get_file_transfer("webdav-upload-reject-403").await.unwrap().unwrap();
        assert_eq!(rejected_upload.status, "failed", "{rejected_upload:?}");
        assert_eq!(rejected_upload.partial_destination, None, "{rejected_upload:?}");
        assert!(rejected_upload.error.as_deref().is_some_and(|error| error.contains("HTTP 403")));

        operator.write("worker-copy-loss-source.bin", "response loss").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-copy-response-loss",
            "copy",
            "worker-copy-loss-source.bin",
            "worker-response-loss-copy-target.bin",
        )
        .await
        .await
        .unwrap();
        let response_loss_copy = state.storage.get_file_transfer("webdav-copy-response-loss").await.unwrap().unwrap();
        assert_eq!(response_loss_copy.status, "partial", "{response_loss_copy:?}");
        assert_eq!(
            response_loss_copy.operation_outcome.as_deref(),
            Some("destination_present_unproven"),
            "{response_loss_copy:?}"
        );
        assert_eq!(operator.read("worker-response-loss-copy-target.bin").await.unwrap().to_vec(), b"response loss");

        operator.write("worker-move-loss-source.bin", "move response loss").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-move-response-loss",
            "rename",
            "worker-move-loss-source.bin",
            "worker-response-loss-move-target.bin",
        )
        .await
        .await
        .unwrap();
        let response_loss_move = state.storage.get_file_transfer("webdav-move-response-loss").await.unwrap().unwrap();
        assert_eq!(response_loss_move.status, "partial", "{response_loss_move:?}");
        assert_eq!(
            response_loss_move.operation_outcome.as_deref(),
            Some("move_committed_response_unknown"),
            "{response_loss_move:?}"
        );
        assert!(!operator.exists("worker-move-loss-source.bin").await.unwrap());
        assert_eq!(
            operator.read("worker-response-loss-move-target.bin").await.unwrap().to_vec(),
            b"move response loss"
        );

        let response_loss_upload_source = local_root.join("webdav-upload-response-loss.bin");
        tokio::fs::write(&response_loss_upload_source, b"upload response loss").await.unwrap();
        let (_, response_loss_upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-upload-response-loss",
            "worker-upload-response-loss-target.bin",
            &response_loss_upload_source,
        )
        .await;
        response_loss_upload_worker.await.unwrap();
        let response_loss_upload =
            state.storage.get_file_transfer("webdav-upload-response-loss").await.unwrap().unwrap();
        assert_eq!(response_loss_upload.status, "partial", "{response_loss_upload:?}");
        let response_loss_partial = response_loss_upload
            .partial_destination
            .as_deref()
            .expect("PUT response loss must retain the operation-owned partial");
        assert!(response_loss_partial.contains(".dbx-upload-webdav-upload-response-loss-"));
        assert!(operator.exists(response_loss_partial).await.unwrap());
        assert!(response_loss_upload
            .abort_outcome
            .as_deref()
            .is_some_and(|outcome| outcome.contains("put_outcome_uncertain")));

        operator.write("worker-timeout-copy-source.bin", "timeout").await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-copy-timeout",
            "copy",
            "worker-timeout-copy-source.bin",
            "worker-timeout-copy-target.bin",
        )
        .await
        .await
        .unwrap();
        let timeout = state.storage.get_file_transfer("webdav-copy-timeout").await.unwrap().unwrap();
        assert_eq!(timeout.status, "partial", "{timeout:?}");
        assert_eq!(timeout.operation_outcome.as_deref(), Some("destination_state_unknown"), "{timeout:?}");
        assert!(!operator.exists("worker-timeout-copy-target.bin").await.unwrap());

        operator.write("worker-cancel-lock-source.bin", "cancel").await.unwrap();
        let connection = state.storage.load_file_connection("webdav-contract").await.unwrap().unwrap();
        let file_manager = app.state::<FileManagerRuntime>();
        let prepared = file_manager
            .prepare_file_mutation_operation(
                &state,
                "webdav-contract",
                "worker-cancel-lock-source.bin",
                connection.revision,
            )
            .await
            .unwrap();
        let guard = prepared.acquire_mutation_guard().await;
        let cancelled_worker = create_remote_worker_transfer_for_connection(
            &app,
            "webdav-contract",
            "webdav-copy-cancelled-lock",
            "copy",
            "worker-cancel-lock-source.bin",
            "worker-cancelled-before-dispatch.bin",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel_file_transfer_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "webdav-copy-cancelled-lock",
        )
        .await
        .unwrap();
        cancelled_worker.await.unwrap();
        drop(guard);
        drop(prepared);
        let cancelled = state.storage.get_file_transfer("webdav-copy-cancelled-lock").await.unwrap().unwrap();
        assert_eq!(cancelled.status, "cancelled", "{cancelled:?}");
        assert_eq!(cancelled.operation_outcome.as_deref(), Some("failed_before_copy"), "{cancelled:?}");
        assert_eq!(cancelled.partial_destination, None);
        assert!(!operator.exists("worker-cancelled-before-dispatch.bin").await.unwrap());

        let recovery_id = "webdav-upload-recovery";
        let recovery_partial = format!(".dbx-upload-{recovery_id}-owned.part");
        let recovery_target = "worker-recovery-target.bin";
        let recovery_payload = b"recovery payload";
        operator.write(&recovery_partial, Bytes::from_static(recovery_payload)).await.unwrap();
        state
            .storage
            .create_file_upload_transfer(
                recovery_id.into(),
                "webdav-contract".into(),
                recovery_target.into(),
                local_root.join("recovery-source.bin").to_string_lossy().into_owned(),
                canonical_directory_identity(&local_root),
                "recovery-source-fingerprint".into(),
                i64::try_from(recovery_payload.len()).unwrap(),
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .start_file_upload_transfer(
                recovery_id,
                recovery_partial.clone(),
                "recovery-source-fingerprint".into(),
                i64::try_from(recovery_payload.len()).unwrap(),
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                recovery_id,
                "publishing".into(),
                i64::try_from(recovery_payload.len()).unwrap(),
                Some(i64::try_from(recovery_payload.len()).unwrap()),
                Some(recovery_partial.clone()),
                Some("recovery-source-fingerprint".into()),
                None,
                false,
            )
            .await
            .unwrap();
        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        let interrupted = interrupted.iter().find(|transfer| transfer.id == recovery_id).unwrap();
        recover_interrupted_transfer(&state, app.state::<FileManagerRuntime>().inner(), interrupted).await.unwrap();
        let recovered = state.storage.get_file_transfer(recovery_id).await.unwrap().unwrap();
        assert_eq!(recovered.status, "partial", "{recovered:?}");
        assert_eq!(recovered.publish_outcome.as_deref(), Some("partial_source"), "{recovered:?}");
        assert_eq!(recovered.partial_destination.as_deref(), Some(recovery_partial.as_str()));
    }

    async fn write_ftp_contract_fixture(operator: &opendal::Operator, path: &str, payload: &[u8], replace: bool) {
        if replace {
            operator.delete(path).await.unwrap();
        }
        let mut writer =
            operator.writer_with(path).append(true).chunk(REMOTE_COPY_BUFFER_SIZE).concurrent(1).await.unwrap();
        writer.write(Bytes::copy_from_slice(payload)).await.unwrap();
        writer.close().await.unwrap();
    }

    fn rewrite_ftp_fixture_preserving_mtime(container: &str, path: &str, payload: &str) {
        let output = Command::new("docker")
            .args([
                "exec",
                container,
                "sh",
                "-c",
                r#"
                    set -eu
                    path="$1"
                    payload="$2"
                    reference="/tmp/dbx-mtime-$$"
                    cp -p "$path" "$reference"
                    printf '%s' "$payload" > "$path"
                    touch -r "$reference" "$path"
                    rm -f "$reference"
                "#,
                "sh",
                path,
                payload,
            ])
            .output()
            .unwrap();
        assert!(output.status.success(), "FTP fixture rewrite failed: {}", String::from_utf8_lossy(&output.stderr));
    }

    #[tokio::test]
    #[ignore = "run through tests/s3-contract.sh with digest-pinned MinIO"]
    async fn fixed_s3_transfer_contract() {
        let (app, state, operator, bucket_operator, directory) = build_s3_contract_app().await;
        let local_root = directory.path().canonicalize().unwrap();
        let prefix = format!("runtime-{}", Uuid::new_v4());
        let proxy_faults = std::env::var("DBX_TEST_S3_FAULT_PROXY").as_deref() == Ok("1");
        let small_payload = b"dbx s3 runtime contract\n".to_vec();
        let large_payload = (0..(S3_UPLOAD_BUFFER_SIZE * 2 + 257)).map(|index| (index % 251) as u8).collect::<Vec<_>>();

        for (name, payload) in
            [("empty", Vec::new()), ("small", small_payload.clone()), ("large", large_payload.clone())]
        {
            let remote_path = format!("{prefix}/download-{name}.bin");
            operator.write(&remote_path, Bytes::from(payload.clone())).await.unwrap();
            let transfer_id = format!("s3-download-{name}");
            let local_path = local_root.join(format!("download-{name}.bin"));
            let (_, worker) =
                create_worker_transfer_for_connection(&app, "s3-contract", &transfer_id, &remote_path, &local_path)
                    .await;
            worker.await.unwrap();
            let transfer = state.storage.get_file_transfer(&transfer_id).await.unwrap().unwrap();
            assert_eq!(transfer.status, "completed", "{transfer:?}");
            assert_eq!(transfer.bytes_transferred, i64::try_from(payload.len()).unwrap());
            assert_eq!(tokio::fs::read(&local_path).await.unwrap(), payload);
            assert_no_owned_temp(&local_root, &transfer_id);
        }

        for (name, payload) in
            [("empty", Vec::new()), ("small", small_payload.clone()), ("large", large_payload.clone())]
        {
            let transfer_id = format!("s3-upload-{name}");
            let local_path = local_root.join(format!("upload-{name}.bin"));
            let remote_path = format!("{prefix}/upload-{name}.bin");
            tokio::fs::write(&local_path, &payload).await.unwrap();
            let (_, worker) = create_upload_worker_transfer_for_connection(
                &app,
                "s3-contract",
                &transfer_id,
                &remote_path,
                &local_path,
            )
            .await;
            worker.await.unwrap();
            let transfer = state.storage.get_file_transfer(&transfer_id).await.unwrap().unwrap();
            assert_eq!(transfer.status, "completed", "{transfer:?}");
            assert_eq!(transfer.publish_outcome.as_deref(), Some("completed"), "{transfer:?}");
            assert_eq!(transfer.bytes_transferred, i64::try_from(payload.len()).unwrap());
            assert_eq!(operator.read(&remote_path).await.unwrap().to_vec(), payload);
            assert_no_s3_owned_partial(&operator, &transfer_id).await;
        }

        for (name, payload) in [("small", small_payload.clone()), ("large", large_payload.clone())] {
            let source = format!("{prefix}/copy-{name}-source.bin");
            let destination = format!("{prefix}/copy-{name}-destination.bin");
            let transfer_id = format!("s3-copy-{name}");
            operator.write(&source, Bytes::from(payload.clone())).await.unwrap();
            create_remote_worker_transfer_for_connection(
                &app,
                "s3-contract",
                &transfer_id,
                "copy",
                &source,
                &destination,
            )
            .await
            .await
            .unwrap();
            let transfer = state.storage.get_file_transfer(&transfer_id).await.unwrap().unwrap();
            assert_eq!(transfer.status, "completed", "{transfer:?}");
            assert_eq!(transfer.operation_outcome.as_deref(), Some("completed"));
            assert_eq!(operator.read(&source).await.unwrap().to_vec(), payload);
            assert_eq!(operator.read(&destination).await.unwrap().to_vec(), payload);
            assert_no_s3_owned_partial(&operator, &transfer_id).await;
        }

        let response_loss_copy_source = format!("{prefix}/response-loss-copy-source.bin");
        let response_loss_copy_destination = format!("{prefix}/response-loss-copy-destination.bin");
        operator.write(&response_loss_copy_source, Bytes::from_static(b"response-loss-copy")).await.unwrap();
        if !proxy_faults {
            install_test_s3_copy_after_commit_response_loss(&response_loss_copy_destination);
        }
        create_remote_worker_transfer_for_connection(
            &app,
            "s3-contract",
            "s3-copy-response-loss",
            "copy",
            &response_loss_copy_source,
            &response_loss_copy_destination,
        )
        .await
        .await
        .unwrap();
        let response_loss_copy = state.storage.get_file_transfer("s3-copy-response-loss").await.unwrap().unwrap();
        assert_eq!(response_loss_copy.status, "partial", "{response_loss_copy:?}");
        assert_eq!(response_loss_copy.operation_outcome.as_deref(), Some("copy_committed_response_unknown"));
        assert_eq!(operator.read(&response_loss_copy_destination).await.unwrap().to_vec(), b"response-loss-copy");
        assert!(operator.exists(&response_loss_copy_source).await.unwrap());

        let response_loss_rename_source = format!("{prefix}/response-loss-rename-source.bin");
        let response_loss_rename_destination = format!("{prefix}/response-loss-rename-destination.bin");
        operator.write(&response_loss_rename_source, Bytes::from_static(b"response-loss-rename")).await.unwrap();
        if !proxy_faults {
            install_test_s3_copy_after_commit_response_loss(&response_loss_rename_destination);
        }
        create_remote_worker_transfer_for_connection(
            &app,
            "s3-contract",
            "s3-rename-response-loss",
            "rename",
            &response_loss_rename_source,
            &response_loss_rename_destination,
        )
        .await
        .await
        .unwrap();
        let response_loss_rename = state.storage.get_file_transfer("s3-rename-response-loss").await.unwrap().unwrap();
        assert_eq!(response_loss_rename.status, "partial", "{response_loss_rename:?}");
        assert_eq!(response_loss_rename.operation_outcome.as_deref(), Some("copy_committed_response_unknown"));
        assert!(operator.exists(&response_loss_rename_source).await.unwrap());
        assert_eq!(operator.read(&response_loss_rename_destination).await.unwrap().to_vec(), b"response-loss-rename");

        let response_loss_upload_source = local_root.join("response-loss-upload.bin");
        let response_loss_upload_target = format!("{prefix}/response-loss-upload-target.bin");
        tokio::fs::write(&response_loss_upload_source, b"response-loss-upload").await.unwrap();
        if !proxy_faults {
            super::super::file_manager::install_test_s3_publish_after_commit_response_loss(
                &response_loss_upload_target,
            );
        }
        let (_, response_loss_upload_worker) = create_upload_worker_transfer_for_connection(
            &app,
            "s3-contract",
            "s3-upload-response-loss",
            &response_loss_upload_target,
            &response_loss_upload_source,
        )
        .await;
        response_loss_upload_worker.await.unwrap();
        let response_loss_upload = state.storage.get_file_transfer("s3-upload-response-loss").await.unwrap().unwrap();
        assert_eq!(response_loss_upload.status, "partial", "{response_loss_upload:?}");
        assert_eq!(response_loss_upload.publish_outcome.as_deref(), Some("partial_target_unproven"));
        assert_eq!(operator.read(&response_loss_upload_target).await.unwrap().to_vec(), b"response-loss-upload");

        if proxy_faults {
            let protocol_error_source = format!("{prefix}/fault-200-error-source.bin");
            let protocol_error_destination = format!("{prefix}/fault-200-error-destination.bin");
            operator.write(&protocol_error_source, Bytes::from_static(b"protocol error source")).await.unwrap();
            create_remote_worker_transfer_for_connection(
                &app,
                "s3-contract",
                "s3-copy-200-error",
                "copy",
                &protocol_error_source,
                &protocol_error_destination,
            )
            .await
            .await
            .unwrap();
            let protocol_error = state.storage.get_file_transfer("s3-copy-200-error").await.unwrap().unwrap();
            assert_eq!(protocol_error.status, "failed", "{protocol_error:?}");
            assert_eq!(protocol_error.operation_outcome.as_deref(), Some("failed_before_copy"));
            assert!(
                protocol_error.error.as_deref().is_some_and(|error| error.contains("InvalidRequest")),
                "{protocol_error:?}"
            );
            assert_eq!(
                operator.stat(&protocol_error_destination).await.unwrap_err().kind(),
                opendal::ErrorKind::NotFound
            );

            let abort_error_source = format!("{prefix}/fault-abort-error-source.bin");
            let abort_error_destination = format!("{prefix}/fault-abort-error-destination.bin");
            operator.write(&abort_error_source, Bytes::from(vec![11_u8; 8 * 1024 * 1024 + 31])).await.unwrap();
            install_test_s3_copy_chunk(&abort_error_destination, 5 * 1024 * 1024);
            create_remote_worker_transfer_for_connection(
                &app,
                "s3-contract",
                "s3-copy-abort-error",
                "copy",
                &abort_error_source,
                &abort_error_destination,
            )
            .await
            .await
            .unwrap();
            let abort_error = state.storage.get_file_transfer("s3-copy-abort-error").await.unwrap().unwrap();
            assert_eq!(abort_error.status, "failed", "{abort_error:?}");
            assert_eq!(abort_error.operation_outcome.as_deref(), Some("failed_before_copy"));
            assert!(
                abort_error.error.as_deref().is_some_and(|error| error.contains("abort failed")),
                "{abort_error:?}"
            );
            assert_eq!(operator.stat(&abort_error_destination).await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        }

        let no_clobber_source = format!("{prefix}/no-clobber-source.bin");
        let no_clobber_destination = format!("{prefix}/no-clobber-destination.bin");
        operator.write(&no_clobber_source, Bytes::from_static(b"source")).await.unwrap();
        operator.write(&no_clobber_destination, Bytes::from_static(b"keep")).await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "s3-contract",
            "s3-copy-no-clobber",
            "copy",
            &no_clobber_source,
            &no_clobber_destination,
        )
        .await
        .await
        .unwrap();
        let no_clobber = state.storage.get_file_transfer("s3-copy-no-clobber").await.unwrap().unwrap();
        assert_eq!(no_clobber.status, "failed", "{no_clobber:?}");
        assert_eq!(no_clobber.operation_outcome.as_deref(), Some("failed_before_copy"));
        assert_eq!(operator.read(&no_clobber_destination).await.unwrap().to_vec(), b"keep");
        assert_eq!(operator.read(&no_clobber_source).await.unwrap().to_vec(), b"source");

        let rename_source = format!("{prefix}/rename-source.bin");
        let rename_destination = format!("{prefix}/rename-destination.bin");
        operator.write(&rename_source, Bytes::from_static(b"old-rename-version")).await.unwrap();
        operator.write(&rename_source, Bytes::from_static(b"rename")).await.unwrap();
        create_remote_worker_transfer_for_connection(
            &app,
            "s3-contract",
            "s3-rename-success",
            "rename",
            &rename_source,
            &rename_destination,
        )
        .await
        .await
        .unwrap();
        let rename = state.storage.get_file_transfer("s3-rename-success").await.unwrap().unwrap();
        assert_eq!(rename.status, "completed", "{rename:?}");
        assert_eq!(operator.stat(&rename_source).await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        assert_eq!(operator.read(&rename_destination).await.unwrap().to_vec(), b"rename");

        let connection = state.storage.load_file_connection("s3-contract").await.unwrap().unwrap();
        let file_manager = app.state::<FileManagerRuntime>();
        let prepared = file_manager
            .prepare_file_mutation_operation(&state, "s3-contract", &rename_destination, connection.revision)
            .await
            .unwrap();

        let retry_source = format!("{prefix}/versioned-retry-source.bin");
        let retry_destination = format!("{prefix}/versioned-retry-destination.bin");
        operator.write(&retry_source, Bytes::from_static(b"old-retry-version")).await.unwrap();
        operator.write(&retry_source, Bytes::from_static(b"current-retry-version")).await.unwrap();
        operator.copy(&retry_source, &retry_destination).await.unwrap();
        let retry_source_fingerprint = prepared.fingerprint_remote_file(&retry_source).await.unwrap().encode();
        let retry_destination_fingerprint =
            prepared.fingerprint_remote_file(&retry_destination).await.unwrap().encode();
        state
            .storage
            .create_file_remote_transfer(
                "s3-versioned-rename-retry".into(),
                "s3-contract".into(),
                "rename".into(),
                retry_source.clone(),
                retry_destination.clone(),
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .finish_file_remote_transfer(
                "s3-versioned-rename-retry",
                "partial".into(),
                i64::try_from(b"current-retry-version".len()).unwrap(),
                Some(i64::try_from(b"current-retry-version".len()).unwrap()),
                Some(RedactedFileText::from_static("injected source delete failure")),
                Some(retry_destination.clone()),
                "copied_source_delete_failed".into(),
                "delete_uncertain".into(),
                Some(retry_source_fingerprint),
                Some(retry_destination_fingerprint),
            )
            .await
            .unwrap();
        let retry_result = retry_file_rename_source_delete_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            file_manager.inner(),
            "s3-versioned-rename-retry",
        )
        .await
        .unwrap();
        assert_eq!(retry_result.status, "completed");
        assert_eq!(operator.stat(&retry_source).await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        assert_eq!(operator.read(&retry_destination).await.unwrap().to_vec(), b"current-retry-version");

        let recovery_copy_source = format!("{prefix}/recovery-copy-source.bin");
        operator.write(&recovery_copy_source, Bytes::from_static(b"copy recovery")).await.unwrap();
        let recovery_copy_fingerprint = prepared.fingerprint_remote_file(&recovery_copy_source).await.unwrap().encode();
        state
            .storage
            .create_file_remote_transfer(
                "s3-recovery-copying".into(),
                "s3-contract".into(),
                "copy".into(),
                recovery_copy_source,
                format!("{prefix}/recovery-copy-destination.bin"),
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_remote_transfer_phase(
                "s3-recovery-copying",
                "running".into(),
                "copying".into(),
                1,
                Some(13),
                None,
                Some(recovery_copy_fingerprint),
                None,
            )
            .await
            .unwrap();

        let recovery_rename_source = format!("{prefix}/recovery-rename-source.bin");
        let recovery_rename_destination = format!("{prefix}/recovery-rename-destination.bin");
        operator.write(&recovery_rename_source, Bytes::from_static(b"rename recovery")).await.unwrap();
        operator.copy(&recovery_rename_source, &recovery_rename_destination).await.unwrap();
        let recovery_rename_source_fingerprint =
            prepared.fingerprint_remote_file(&recovery_rename_source).await.unwrap().encode();
        let recovery_rename_destination_fingerprint =
            prepared.fingerprint_remote_file(&recovery_rename_destination).await.unwrap().encode();
        state
            .storage
            .create_file_remote_transfer(
                "s3-recovery-rename".into(),
                "s3-contract".into(),
                "rename".into(),
                recovery_rename_source.clone(),
                recovery_rename_destination.clone(),
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_remote_transfer_phase(
                "s3-recovery-rename",
                "publishing".into(),
                "delete_uncertain".into(),
                15,
                Some(15),
                None,
                Some(recovery_rename_source_fingerprint),
                Some(recovery_rename_destination_fingerprint),
            )
            .await
            .unwrap();
        operator.delete(&recovery_rename_source).await.unwrap();

        let recovery_upload_id = "s3-recovery-upload-publishing";
        let recovery_upload_partial = format!("{prefix}/.dbx-upload-{recovery_upload_id}-random.part");
        let recovery_upload_target = format!("{prefix}/recovery-upload-target.bin");
        let recovery_upload_payload = b"upload recovery";
        operator.write(&recovery_upload_partial, Bytes::from_static(recovery_upload_payload)).await.unwrap();
        state
            .storage
            .create_file_upload_transfer(
                recovery_upload_id.into(),
                "s3-contract".into(),
                recovery_upload_target,
                local_root.join("recovery-source.bin").to_string_lossy().into_owned(),
                canonical_directory_identity(&local_root),
                "recovery-source-fingerprint".into(),
                i64::try_from(recovery_upload_payload.len()).unwrap(),
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .start_file_upload_transfer(
                recovery_upload_id,
                recovery_upload_partial.clone(),
                "recovery-source-fingerprint".into(),
                i64::try_from(recovery_upload_payload.len()).unwrap(),
                connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                recovery_upload_id,
                "publishing".into(),
                i64::try_from(recovery_upload_payload.len()).unwrap(),
                Some(i64::try_from(recovery_upload_payload.len()).unwrap()),
                Some(recovery_upload_partial.clone()),
                Some("recovery-source-fingerprint".into()),
                None,
                false,
            )
            .await
            .unwrap();
        drop(prepared);

        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        for id in ["s3-recovery-copying", "s3-recovery-rename", recovery_upload_id] {
            let transfer = interrupted.iter().find(|transfer| transfer.id == id).unwrap();
            recover_interrupted_transfer(&state, app.state::<FileManagerRuntime>().inner(), transfer).await.unwrap();
        }
        let recovered_copy = state.storage.get_file_transfer("s3-recovery-copying").await.unwrap().unwrap();
        assert_eq!(recovered_copy.status, "partial", "{recovered_copy:?}");
        assert_eq!(recovered_copy.operation_outcome.as_deref(), Some("failed_with_partial_destination"));
        let recovered_rename = state.storage.get_file_transfer("s3-recovery-rename").await.unwrap().unwrap();
        assert_eq!(recovered_rename.status, "completed", "{recovered_rename:?}");
        let recovered_upload = state.storage.get_file_transfer(recovery_upload_id).await.unwrap().unwrap();
        assert_eq!(recovered_upload.status, "partial", "{recovered_upload:?}");
        assert_eq!(recovered_upload.publish_outcome.as_deref(), Some("partial_source"));
        assert_eq!(recovered_upload.partial_destination.as_deref(), Some(recovery_upload_partial.as_str()));

        let outside_canary_key =
            std::env::var("DBX_TEST_S3_OUTSIDE_CANARY_KEY").expect("DBX_TEST_S3_OUTSIDE_CANARY_KEY is required");
        let bucket_canary_key =
            std::env::var("DBX_TEST_S3_BUCKET_CANARY_KEY").expect("DBX_TEST_S3_BUCKET_CANARY_KEY is required");
        assert_eq!(bucket_operator.read(&outside_canary_key).await.unwrap().to_vec(), b"tenant-canary");
        assert_eq!(bucket_operator.read(&bucket_canary_key).await.unwrap().to_vec(), b"bucket-canary");

        let serialized =
            serde_json::to_string(&state.storage.list_file_transfers(Some("s3-contract"), 100).await.unwrap()).unwrap();
        for secret in [
            std::env::var("DBX_TEST_S3_ACCESS_KEY_ID").unwrap(),
            std::env::var("DBX_TEST_S3_SECRET_ACCESS_KEY").unwrap(),
        ] {
            assert!(!serialized.contains(&secret), "serialized transfer DTO leaked S3 credentials");
        }
    }

    #[tokio::test]
    #[ignore = "run through tests/ftp-contract.sh with a pinned FTP image"]
    async fn fixed_ftp_copy_rename_contract() {
        let (app, state, operator, _directory, container) = build_ftp_contract_app().await;

        let copy_worker =
            create_remote_worker_transfer(&app, "ftp-copy-success", "copy", "fixture.txt", "copy-success.txt").await;
        copy_worker.await.unwrap();
        let copy = state.storage.get_file_transfer("ftp-copy-success").await.unwrap().unwrap();
        assert_eq!(copy.status, "completed", "{copy:?}");
        assert_eq!(copy.operation_outcome.as_deref(), Some("completed"));
        assert_eq!(operator.read("ftp/dbx/copy-success.txt").await.unwrap().to_vec(), b"dbx ftp fixture\n");
        assert!(operator.stat("ftp/dbx/fixture.txt").await.is_ok());
        assert!(copy.source_fingerprint.as_deref().is_some_and(|value| value.contains("relay_sha256:")));
        assert!(copy.destination_fingerprint.as_deref().is_some_and(|value| value.contains("relay_sha256:")));
        assert_no_remote_copy_partial(&container, "ftp-copy-success");

        TEST_REMOTE_COPY_WRITER_OPEN_SIDE_EFFECT_FAILURE.store(true, Ordering::SeqCst);
        let writer_open_worker = create_remote_worker_transfer(
            &app,
            "ftp-copy-writer-open-side-effect",
            "copy",
            "fixture.txt",
            "writer-open-side-effect.txt",
        )
        .await;
        writer_open_worker.await.unwrap();
        let writer_open = state.storage.get_file_transfer("ftp-copy-writer-open-side-effect").await.unwrap().unwrap();
        assert_eq!(writer_open.operation_outcome.as_deref(), Some("failed_before_copy"), "{writer_open:?}");
        assert_eq!(writer_open.partial_destination, None);
        assert_no_remote_copy_partial(&container, "ftp-copy-writer-open-side-effect");

        TEST_REMOTE_COPY_PERSISTENCE_FAILURE_AFTER_VERIFY.store(true, Ordering::SeqCst);
        let persistence_worker = create_remote_worker_transfer(
            &app,
            "ftp-copy-persistence-failure",
            "copy",
            "fixture.txt",
            "persistence-failure.txt",
        )
        .await;
        persistence_worker.await.unwrap();
        let persistence = state.storage.get_file_transfer("ftp-copy-persistence-failure").await.unwrap().unwrap();
        assert_eq!(persistence.operation_outcome.as_deref(), Some("failed_before_copy"), "{persistence:?}");
        assert_eq!(persistence.partial_destination, None);
        assert_no_remote_copy_partial(&container, "ftp-copy-persistence-failure");

        write_ftp_contract_fixture(&operator, "ftp/dbx/existing.txt", b"existing", false).await;
        let existing_worker =
            create_remote_worker_transfer(&app, "ftp-copy-existing", "copy", "fixture.txt", "existing.txt").await;
        existing_worker.await.unwrap();
        let existing = state.storage.get_file_transfer("ftp-copy-existing").await.unwrap().unwrap();
        assert_eq!(existing.operation_outcome.as_deref(), Some("failed_before_copy"));
        assert_eq!(operator.read("ftp/dbx/existing.txt").await.unwrap().to_vec(), b"existing");

        let target_race = install_test_remote_copy_after_close_barrier();
        let target_race_worker =
            create_remote_worker_transfer(&app, "ftp-copy-target-race", "copy", "fixture.txt", "target-race.txt").await;
        tokio::time::timeout(Duration::from_secs(10), target_race.opened.notified()).await.unwrap();
        write_ftp_contract_fixture(&operator, "ftp/dbx/target-race.txt", b"external writer", false).await;
        target_race.release.notify_one();
        target_race_worker.await.unwrap();
        let raced = state.storage.get_file_transfer("ftp-copy-target-race").await.unwrap().unwrap();
        assert_eq!(raced.operation_outcome.as_deref(), Some("failed_before_copy"), "{raced:?}");
        assert_eq!(operator.read("ftp/dbx/target-race.txt").await.unwrap().to_vec(), b"external writer");
        assert_no_remote_copy_partial(&container, "ftp-copy-target-race");

        write_ftp_contract_fixture(&operator, "ftp/dbx/source-mutation.txt", b"original source", false).await;
        let source_race = install_test_remote_copy_after_close_barrier();
        let source_race_worker = create_remote_worker_transfer(
            &app,
            "ftp-copy-source-race",
            "copy",
            "source-mutation.txt",
            "source-mutation-copy.txt",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), source_race.opened.notified()).await.unwrap();
        write_ftp_contract_fixture(&operator, "ftp/dbx/source-mutation.txt", b"changed and longer source", true).await;
        source_race.release.notify_one();
        source_race_worker.await.unwrap();
        let source_changed = state.storage.get_file_transfer("ftp-copy-source-race").await.unwrap().unwrap();
        assert_eq!(source_changed.operation_outcome.as_deref(), Some("failed_before_copy"), "{source_changed:?}");
        assert_eq!(
            operator.stat("ftp/dbx/source-mutation-copy.txt").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );
        assert_no_remote_copy_partial(&container, "ftp-copy-source-race");

        let mismatch_barrier = install_test_remote_copy_after_close_barrier();
        let mismatch_worker =
            create_remote_worker_transfer(&app, "ftp-copy-mismatch", "copy", "fixture.txt", "mismatch.txt").await;
        tokio::time::timeout(Duration::from_secs(10), mismatch_barrier.opened.notified()).await.unwrap();
        let copying = state.storage.get_file_transfer("ftp-copy-mismatch").await.unwrap().unwrap();
        let mismatch_partial = copying.temp_path.clone().expect("copying phase persists its partial path");
        write_ftp_contract_fixture(
            &operator,
            &format!("ftp/dbx/{mismatch_partial}"),
            b"externally replaced mismatched partial",
            true,
        )
        .await;
        mismatch_barrier.release.notify_one();
        mismatch_worker.await.unwrap();
        let mismatch = state.storage.get_file_transfer("ftp-copy-mismatch").await.unwrap().unwrap();
        assert_eq!(mismatch.operation_outcome.as_deref(), Some("failed_with_partial_destination"), "{mismatch:?}");
        assert_eq!(mismatch.partial_destination.as_deref(), Some(mismatch_partial.as_str()));
        assert_eq!(
            operator.read(&format!("ftp/dbx/{mismatch_partial}")).await.unwrap().to_vec(),
            b"externally replaced mismatched partial"
        );

        write_ftp_contract_fixture(&operator, "ftp/dbx/rename-source.txt", b"rename payload", false).await;
        let rename_worker = create_remote_worker_transfer(
            &app,
            "ftp-rename-success",
            "rename",
            "rename-source.txt",
            "rename-destination.txt",
        )
        .await;
        rename_worker.await.unwrap();
        let rename = state.storage.get_file_transfer("ftp-rename-success").await.unwrap().unwrap();
        assert_eq!(rename.operation_outcome.as_deref(), Some("completed"), "{rename:?}");
        assert_eq!(operator.stat("ftp/dbx/rename-source.txt").await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        assert_eq!(operator.read("ftp/dbx/rename-destination.txt").await.unwrap().to_vec(), b"rename payload");

        write_ftp_contract_fixture(&operator, "ftp/dbx/rehash-source.txt", b"source-original!!", false).await;
        let source_rehash_barrier = install_test_remote_rename_after_publish_barrier();
        let source_rehash_worker = create_remote_worker_transfer(
            &app,
            "ftp-rename-source-rehash",
            "rename",
            "rehash-source.txt",
            "rehash-source-destination.txt",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), source_rehash_barrier.opened.notified()).await.unwrap();
        rewrite_ftp_fixture_preserving_mtime(&container, "/ftp/dbx/rehash-source.txt", "source-replaced!!");
        source_rehash_barrier.release.notify_one();
        source_rehash_worker.await.unwrap();
        let source_rehash = state.storage.get_file_transfer("ftp-rename-source-rehash").await.unwrap().unwrap();
        assert_eq!(
            source_rehash.operation_outcome.as_deref(),
            Some("copied_source_delete_failed"),
            "{source_rehash:?}"
        );
        assert_eq!(source_rehash.operation_phase.as_deref(), Some("published_before_delete"));
        assert!(operator.stat("ftp/dbx/rehash-source.txt").await.is_ok(), "source must not be deleted");

        write_ftp_contract_fixture(&operator, "ftp/dbx/rehash-destination-source.txt", b"source-original!!", false)
            .await;
        let destination_rehash_barrier = install_test_remote_rename_after_publish_barrier();
        let destination_rehash_worker = create_remote_worker_transfer(
            &app,
            "ftp-rename-destination-rehash",
            "rename",
            "rehash-destination-source.txt",
            "rehash-destination.txt",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), destination_rehash_barrier.opened.notified()).await.unwrap();
        rewrite_ftp_fixture_preserving_mtime(&container, "/ftp/dbx/rehash-destination.txt", "target-replaced!!");
        destination_rehash_barrier.release.notify_one();
        destination_rehash_worker.await.unwrap();
        let destination_rehash =
            state.storage.get_file_transfer("ftp-rename-destination-rehash").await.unwrap().unwrap();
        assert_eq!(
            destination_rehash.operation_outcome.as_deref(),
            Some("copied_source_delete_failed"),
            "{destination_rehash:?}"
        );
        assert_eq!(destination_rehash.operation_phase.as_deref(), Some("published_before_delete"));
        assert!(operator.stat("ftp/dbx/rehash-destination-source.txt").await.is_ok(), "source must not be deleted");

        write_ftp_contract_fixture(&operator, "ftp/dbx/delete-failure-source.txt", b"delete failure payload", false)
            .await;
        let delete_barrier = install_test_remote_rename_after_publish_barrier();
        let delete_worker = create_remote_worker_transfer(
            &app,
            "ftp-rename-delete-failure",
            "rename",
            "delete-failure-source.txt",
            "delete-failure-destination.txt",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), delete_barrier.opened.notified()).await.unwrap();
        assert!(Command::new("docker")
            .args(["exec", &container, "chmod", "0555", "/ftp/dbx"])
            .status()
            .unwrap()
            .success());
        delete_barrier.release.notify_one();
        delete_worker.await.unwrap();
        assert!(Command::new("docker")
            .args(["exec", &container, "chmod", "0775", "/ftp/dbx"])
            .status()
            .unwrap()
            .success());
        let delete_failed = state.storage.get_file_transfer("ftp-rename-delete-failure").await.unwrap().unwrap();
        assert_eq!(
            delete_failed.operation_outcome.as_deref(),
            Some("copied_source_delete_failed"),
            "{delete_failed:?}"
        );
        assert!(operator.stat("ftp/dbx/delete-failure-source.txt").await.is_ok());
        assert!(operator.stat("ftp/dbx/delete-failure-destination.txt").await.is_ok());
        rewrite_ftp_fixture_preserving_mtime(
            &container,
            "/ftp/dbx/delete-failure-destination.txt",
            "target failure payload",
        );
        let destination_retry_error = retry_file_rename_source_delete_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "ftp-rename-delete-failure",
        )
        .await
        .unwrap_err();
        assert!(destination_retry_error.contains("Destination content"), "{destination_retry_error}");
        assert!(operator.stat("ftp/dbx/delete-failure-source.txt").await.is_ok());
        rewrite_ftp_fixture_preserving_mtime(
            &container,
            "/ftp/dbx/delete-failure-destination.txt",
            "delete failure payload",
        );
        rewrite_ftp_fixture_preserving_mtime(
            &container,
            "/ftp/dbx/delete-failure-source.txt",
            "source failure payload",
        );
        let source_retry_error = retry_file_rename_source_delete_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "ftp-rename-delete-failure",
        )
        .await
        .unwrap_err();
        assert!(source_retry_error.contains("Source content"), "{source_retry_error}");
        assert!(operator.stat("ftp/dbx/delete-failure-source.txt").await.is_ok());
        rewrite_ftp_fixture_preserving_mtime(
            &container,
            "/ftp/dbx/delete-failure-source.txt",
            "delete failure payload",
        );
        let recovered = retry_file_rename_source_delete_inner(
            app.handle(),
            &state,
            app.state::<FileTransferRuntime>().inner(),
            app.state::<FileManagerRuntime>().inner(),
            "ftp-rename-delete-failure",
        )
        .await
        .unwrap();
        assert_eq!(recovered.operation_outcome.as_deref(), Some("completed"));
        assert_eq!(
            operator.stat("ftp/dbx/delete-failure-source.txt").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );

        let recovery_connection = state.storage.load_file_connection("ftp-contract").await.unwrap().unwrap();
        state
            .storage
            .create_file_remote_transfer(
                "ftp-recovery-queued".into(),
                "ftp-contract".into(),
                "copy".into(),
                "fixture.txt".into(),
                "recovery-queued.txt".into(),
                recovery_connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .create_file_remote_transfer(
                "ftp-recovery-copying".into(),
                "ftp-contract".into(),
                "copy".into(),
                "fixture.txt".into(),
                "recovery-copying.txt".into(),
                recovery_connection.revision,
            )
            .await
            .unwrap();
        let recovery_partial = ".dbx-copy-ftp-recovery-copying-random.part";
        state
            .storage
            .update_file_remote_transfer_phase(
                "ftp-recovery-copying",
                "running".into(),
                "copying".into(),
                4,
                Some(16),
                Some(recovery_partial.into()),
                None,
                None,
            )
            .await
            .unwrap();

        write_ftp_contract_fixture(&operator, "ftp/dbx/recovery-published-source.txt", b"recovery-payload", false)
            .await;
        write_ftp_contract_fixture(&operator, "ftp/dbx/recovery-published-destination.txt", b"recovery-payload", false)
            .await;
        write_ftp_contract_fixture(&operator, "ftp/dbx/recovery-uncertain-source.txt", b"recovery-payload", false)
            .await;
        write_ftp_contract_fixture(&operator, "ftp/dbx/recovery-uncertain-destination.txt", b"recovery-payload", false)
            .await;
        write_ftp_contract_fixture(&operator, "ftp/dbx/recovery-mismatch-source.txt", b"recovery-payload", false).await;
        write_ftp_contract_fixture(&operator, "ftp/dbx/recovery-mismatch-destination.txt", b"recovery-payload", false)
            .await;
        let recovery_file_manager = app.state::<FileManagerRuntime>();
        let recovery_prepared = recovery_file_manager
            .prepare_file_mutation_operation(
                &state,
                "ftp-contract",
                "recovery-published-source.txt",
                recovery_connection.revision,
            )
            .await
            .unwrap();
        let recovery_cancellation = CancellationToken::new();
        let published_source =
            verify_remote_content(&recovery_prepared, "recovery-published-source.txt", &recovery_cancellation)
                .await
                .unwrap();
        let published_destination =
            verify_remote_content(&recovery_prepared, "recovery-published-destination.txt", &recovery_cancellation)
                .await
                .unwrap();
        let uncertain_source =
            verify_remote_content(&recovery_prepared, "recovery-uncertain-source.txt", &recovery_cancellation)
                .await
                .unwrap();
        let uncertain_destination =
            verify_remote_content(&recovery_prepared, "recovery-uncertain-destination.txt", &recovery_cancellation)
                .await
                .unwrap();
        let mismatch_source =
            verify_remote_content(&recovery_prepared, "recovery-mismatch-source.txt", &recovery_cancellation)
                .await
                .unwrap();
        let mismatch_destination =
            verify_remote_content(&recovery_prepared, "recovery-mismatch-destination.txt", &recovery_cancellation)
                .await
                .unwrap();
        state
            .storage
            .create_file_remote_transfer(
                "ftp-recovery-published".into(),
                "ftp-contract".into(),
                "rename".into(),
                "recovery-published-source.txt".into(),
                "recovery-published-destination.txt".into(),
                recovery_connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_remote_transfer_phase(
                "ftp-recovery-published",
                "publishing".into(),
                "published_before_delete".into(),
                16,
                Some(16),
                None,
                Some(published_source.durable_fingerprint()),
                Some(published_destination.durable_fingerprint()),
            )
            .await
            .unwrap();
        state
            .storage
            .create_file_remote_transfer(
                "ftp-recovery-uncertain".into(),
                "ftp-contract".into(),
                "rename".into(),
                "recovery-uncertain-source.txt".into(),
                "recovery-uncertain-destination.txt".into(),
                recovery_connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_remote_transfer_phase(
                "ftp-recovery-uncertain",
                "publishing".into(),
                "delete_uncertain".into(),
                16,
                Some(16),
                None,
                Some(uncertain_source.durable_fingerprint()),
                Some(uncertain_destination.durable_fingerprint()),
            )
            .await
            .unwrap();
        operator.delete("ftp/dbx/recovery-uncertain-source.txt").await.unwrap();
        state
            .storage
            .create_file_remote_transfer(
                "ftp-recovery-mismatch".into(),
                "ftp-contract".into(),
                "rename".into(),
                "recovery-mismatch-source.txt".into(),
                "recovery-mismatch-destination.txt".into(),
                recovery_connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_remote_transfer_phase(
                "ftp-recovery-mismatch",
                "publishing".into(),
                "delete_uncertain".into(),
                16,
                Some(16),
                None,
                Some(mismatch_source.durable_fingerprint()),
                Some(mismatch_destination.durable_fingerprint()),
            )
            .await
            .unwrap();
        operator.delete("ftp/dbx/recovery-mismatch-source.txt").await.unwrap();
        rewrite_ftp_fixture_preserving_mtime(
            &container,
            "/ftp/dbx/recovery-mismatch-destination.txt",
            "recovery-mutated",
        );

        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        let recovery_ids = [
            "ftp-recovery-queued",
            "ftp-recovery-copying",
            "ftp-recovery-published",
            "ftp-recovery-uncertain",
            "ftp-recovery-mismatch",
        ];
        for recovery_id in recovery_ids {
            let transfer = interrupted.iter().find(|transfer| transfer.id == recovery_id).unwrap();
            assert!(
                matches!(transfer.status.as_str(), "queued" | "running" | "publishing"),
                "protocol recovery must receive the active durable state: {transfer:?}"
            );
            recover_interrupted_transfer(&state, app.state::<FileManagerRuntime>().inner(), transfer).await.unwrap();
        }
        let recovered_queued = state.storage.get_file_transfer("ftp-recovery-queued").await.unwrap().unwrap();
        assert_eq!(recovered_queued.status, "failed");
        assert_eq!(recovered_queued.operation_outcome.as_deref(), Some("failed_before_copy"));
        assert_eq!(recovered_queued.operation_phase.as_deref(), Some("queued"));
        assert_eq!(recovered_queued.partial_destination, None);
        let recovered_copying = state.storage.get_file_transfer("ftp-recovery-copying").await.unwrap().unwrap();
        assert_eq!(recovered_copying.status, "partial");
        assert_eq!(recovered_copying.operation_outcome.as_deref(), Some("failed_with_partial_destination"));
        assert_eq!(recovered_copying.operation_phase.as_deref(), Some("copying"));
        assert_eq!(recovered_copying.partial_destination.as_deref(), Some(recovery_partial));
        let recovered_published = state.storage.get_file_transfer("ftp-recovery-published").await.unwrap().unwrap();
        assert_eq!(recovered_published.status, "partial");
        assert_eq!(recovered_published.operation_outcome.as_deref(), Some("copied_source_delete_failed"));
        assert_eq!(recovered_published.operation_phase.as_deref(), Some("delete_uncertain"));
        assert_eq!(recovered_published.partial_destination.as_deref(), Some("recovery-published-destination.txt"));
        let recovered_uncertain = state.storage.get_file_transfer("ftp-recovery-uncertain").await.unwrap().unwrap();
        assert_eq!(recovered_uncertain.status, "completed");
        assert_eq!(recovered_uncertain.operation_outcome.as_deref(), Some("completed"));
        assert_eq!(recovered_uncertain.operation_phase.as_deref(), Some("completed"));
        let recovered_mismatch = state.storage.get_file_transfer("ftp-recovery-mismatch").await.unwrap().unwrap();
        assert_eq!(recovered_mismatch.status, "partial");
        assert_eq!(recovered_mismatch.operation_outcome.as_deref(), Some("copied_source_delete_failed"));
        assert_eq!(recovered_mismatch.operation_phase.as_deref(), Some("delete_uncertain"));
        assert_eq!(recovered_mismatch.partial_destination.as_deref(), Some("recovery-mismatch-destination.txt"));
    }

    #[tokio::test]
    #[ignore = "run through tests/ftp-contract.sh with a pinned FTP image"]
    async fn fixed_ftp_upload_contract() {
        let (app, state, operator, directory, container) = build_ftp_contract_app().await;
        let source_directory = directory.path().canonicalize().unwrap();
        let payload = (0..(UPLOAD_BUFFER_SIZE + 257)).map(|index| (index % 251) as u8).collect::<Vec<_>>();

        let success_source = source_directory.join("upload-success.bin");
        tokio::fs::write(&success_source, &payload).await.unwrap();
        let (_, success_worker) =
            create_upload_worker_transfer(&app, "ftp-upload-success", "upload-success.bin", &success_source).await;
        success_worker.await.unwrap();
        let success = state.storage.get_file_transfer("ftp-upload-success").await.unwrap().unwrap();
        assert_eq!(success.status, "completed", "{success:?}");
        assert_eq!(success.bytes_transferred, i64::try_from(payload.len()).unwrap());
        assert_eq!(operator.read("ftp/dbx/upload-success.bin").await.unwrap().to_vec(), payload);
        assert_no_remote_upload_partial(&container, "ftp-upload-success");

        let empty_source = source_directory.join("upload-empty.bin");
        tokio::fs::write(&empty_source, []).await.unwrap();
        let (_, empty_worker) =
            create_upload_worker_transfer(&app, "ftp-upload-empty", "upload-empty.bin", &empty_source).await;
        empty_worker.await.unwrap();
        let empty = state.storage.get_file_transfer("ftp-upload-empty").await.unwrap().unwrap();
        assert_eq!(empty.status, "completed", "{empty:?}");
        assert_eq!(empty.bytes_transferred, 0);
        assert_eq!(operator.stat("ftp/dbx/upload-empty.bin").await.unwrap().content_length(), 0);
        assert_no_remote_upload_partial(&container, "ftp-upload-empty");

        let empty_changed_source = source_directory.join("upload-empty-changed.bin");
        tokio::fs::write(&empty_changed_source, []).await.unwrap();
        let empty_changed_barrier = install_test_upload_after_chunk_barrier();
        let (_, empty_changed_worker) = create_upload_worker_transfer(
            &app,
            "ftp-upload-empty-changed",
            "upload-empty-changed.bin",
            &empty_changed_source,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), empty_changed_barrier.opened.notified())
            .await
            .expect("empty upload must reach its pre-create barrier");
        tokio::fs::write(&empty_changed_source, b"changed").await.unwrap();
        empty_changed_barrier.release.notify_one();
        empty_changed_worker.await.unwrap();
        let empty_changed = state.storage.get_file_transfer("ftp-upload-empty-changed").await.unwrap().unwrap();
        assert_eq!(empty_changed.status, "failed", "{empty_changed:?}");
        assert!(empty_changed.error.as_deref().is_some_and(|error| error.contains("source changed")));
        assert_eq!(
            operator.stat("ftp/dbx/upload-empty-changed.bin").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );
        assert_no_remote_upload_partial(&container, "ftp-upload-empty-changed");

        let cancel_source = source_directory.join("upload-cancel.bin");
        tokio::fs::write(&cancel_source, vec![7_u8; UPLOAD_BUFFER_SIZE * 2 + 17]).await.unwrap();
        let cancel_barrier = install_test_upload_after_chunk_barrier();
        let (cancel_token, cancel_worker) =
            create_upload_worker_transfer(&app, "ftp-upload-cancel", "upload-cancel.bin", &cancel_source).await;
        tokio::time::timeout(Duration::from_secs(10), cancel_barrier.opened.notified())
            .await
            .expect("upload must reach the first-chunk barrier");
        state.storage.request_file_transfer_cancel("ftp-upload-cancel").await.unwrap();
        cancel_token.cancel();
        cancel_barrier.release.notify_one();
        cancel_worker.await.unwrap();
        let cancelled = state.storage.get_file_transfer("ftp-upload-cancel").await.unwrap().unwrap();
        assert_eq!(cancelled.status, "cancelled", "{cancelled:?}");
        assert_eq!(cancelled.partial_destination, None);
        assert_eq!(cancelled.abort_outcome.as_deref(), Some("unsupported; operation_owned_partial_cleaned"));
        assert!(cancelled.bytes_transferred > 0);
        assert!(cancelled.bytes_transferred <= i64::try_from(UPLOAD_BUFFER_SIZE).unwrap());
        assert_eq!(operator.stat("ftp/dbx/upload-cancel.bin").await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        assert_no_remote_upload_partial(&container, "ftp-upload-cancel");

        let after_close_source = source_directory.join("upload-cancel-after-close.bin");
        tokio::fs::write(&after_close_source, b"closed upload cancellation").await.unwrap();
        let after_close_barrier = install_test_upload_after_close_barrier();
        let (after_close_token, after_close_worker) = create_upload_worker_transfer(
            &app,
            "ftp-upload-cancel-after-close",
            "upload-cancel-after-close.bin",
            &after_close_source,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), after_close_barrier.opened.notified())
            .await
            .expect("upload must reach the post-close barrier");
        state.storage.request_file_transfer_cancel("ftp-upload-cancel-after-close").await.unwrap();
        after_close_token.cancel();
        after_close_barrier.release.notify_one();
        after_close_worker.await.unwrap();
        let after_close = state.storage.get_file_transfer("ftp-upload-cancel-after-close").await.unwrap().unwrap();
        assert_eq!(after_close.status, "cancelled", "{after_close:?}");
        assert_eq!(after_close.partial_destination, None);
        assert_eq!(
            operator.stat("ftp/dbx/upload-cancel-after-close.bin").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );
        assert_no_remote_upload_partial(&container, "ftp-upload-cancel-after-close");

        let changed_source = source_directory.join("upload-changed.bin");
        tokio::fs::write(&changed_source, vec![3_u8; UPLOAD_BUFFER_SIZE * 2 + 17]).await.unwrap();
        let changed_barrier = install_test_upload_after_chunk_barrier();
        let (_, changed_worker) =
            create_upload_worker_transfer(&app, "ftp-upload-changed", "upload-changed.bin", &changed_source).await;
        tokio::time::timeout(Duration::from_secs(10), changed_barrier.opened.notified())
            .await
            .expect("upload must reach the source-change barrier");
        tokio::fs::write(&changed_source, vec![5_u8; UPLOAD_BUFFER_SIZE * 2 + 17]).await.unwrap();
        changed_barrier.release.notify_one();
        changed_worker.await.unwrap();
        let changed = state.storage.get_file_transfer("ftp-upload-changed").await.unwrap().unwrap();
        assert_eq!(changed.status, "failed", "{changed:?}");
        assert!(changed.error.as_deref().is_some_and(|error| error.contains("source changed")));
        assert_eq!(changed.partial_destination, None);
        assert_eq!(changed.abort_outcome.as_deref(), Some("unsupported; operation_owned_partial_cleaned"));
        assert_eq!(operator.stat("ftp/dbx/upload-changed.bin").await.unwrap_err().kind(), opendal::ErrorKind::NotFound);
        assert_no_remote_upload_partial(&container, "ftp-upload-changed");

        let revision_source = source_directory.join("upload-revision.bin");
        tokio::fs::write(&revision_source, vec![6_u8; UPLOAD_BUFFER_SIZE * 2 + 17]).await.unwrap();
        let revision_barrier = install_test_upload_after_chunk_barrier();
        let (_, revision_worker) =
            create_upload_worker_transfer(&app, "ftp-upload-revision", "upload-revision.bin", &revision_source).await;
        tokio::time::timeout(Duration::from_secs(10), revision_barrier.opened.notified())
            .await
            .expect("upload must reach the revision-change barrier");
        let current = state.storage.load_file_connection("ftp-contract").await.unwrap().unwrap();
        let current_config: super::super::file_manager::FileConnectionConfig =
            serde_json::from_str(&current.config_json).unwrap();
        let current_scope = super::super::file_manager::password_scope(&current_config).unwrap();
        state
            .storage
            .save_file_connection(
                current.id,
                current.name,
                current.kind,
                current.config_json,
                None,
                current_scope,
                false,
                Some(current.revision),
            )
            .await
            .unwrap();
        revision_barrier.release.notify_one();
        revision_worker.await.unwrap();
        let revision = state.storage.get_file_transfer("ftp-upload-revision").await.unwrap().unwrap();
        assert_eq!(revision.status, "failed", "{revision:?}");
        assert!(revision.error.as_deref().is_some_and(|error| error.contains("revision changed")));
        assert_eq!(revision.partial_destination, None);
        assert_eq!(
            operator.stat("ftp/dbx/upload-revision.bin").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );
        assert_no_remote_upload_partial(&container, "ftp-upload-revision");

        let recovery_id = "ftp-upload-publishing-recovery";
        let recovery_partial = format!(".dbx-upload-{recovery_id}-random.part");
        let recovery_payload = b"recovery!";
        let recovery_fixture = Command::new("docker")
            .args([
                "exec",
                &container,
                "sh",
                "-c",
                &format!(
                    "printf recovery > /ftp/dbx/{recovery_partial}; printf external > /ftp/dbx/upload-recovery-target.bin"
                ),
            ])
            .status()
            .unwrap();
        assert!(recovery_fixture.success());
        let recovery_connection = state.storage.load_file_connection("ftp-contract").await.unwrap().unwrap();
        state
            .storage
            .create_file_upload_transfer(
                recovery_id.into(),
                "ftp-contract".into(),
                "upload-recovery-target.bin".into(),
                source_directory.join("recovery-source.bin").to_string_lossy().into_owned(),
                canonical_directory_identity(&source_directory),
                "recovery-source-fingerprint".into(),
                i64::try_from(recovery_payload.len()).unwrap(),
                recovery_connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .start_file_upload_transfer(
                recovery_id,
                recovery_partial.clone(),
                "recovery-source-fingerprint".into(),
                i64::try_from(recovery_payload.len()).unwrap(),
                recovery_connection.revision,
            )
            .await
            .unwrap();
        state
            .storage
            .update_file_transfer(
                recovery_id,
                "publishing".into(),
                i64::try_from(recovery_payload.len()).unwrap(),
                Some(i64::try_from(recovery_payload.len()).unwrap()),
                Some(recovery_partial.clone()),
                Some("recovery-source-fingerprint".into()),
                None,
                false,
            )
            .await
            .unwrap();
        let interrupted = state.storage.recover_interrupted_file_transfers().await.unwrap();
        let interrupted = interrupted.iter().find(|transfer| transfer.id == recovery_id).unwrap();
        recover_interrupted_transfer(&state, app.state::<FileManagerRuntime>().inner(), interrupted).await.unwrap();
        let recovered = state.storage.get_file_transfer(recovery_id).await.unwrap().unwrap();
        assert_eq!(recovered.status, "partial");
        assert_eq!(recovered.partial_destination.as_deref(), Some(recovery_partial.as_str()));
        assert_eq!(recovered.publish_outcome.as_deref(), Some("partial_source"));
        assert!(recovered.error.as_deref().is_some_and(|error| error.contains("expected 9, actual 8")));
        operator.delete(&format!("ftp/dbx/{recovery_partial}")).await.unwrap();
        operator.delete("ftp/dbx/upload-recovery-target.bin").await.unwrap();

        let readonly = Command::new("docker")
            .args([
                "exec",
                &container,
                "sh",
                "-c",
                "mkdir -p /ftp/dbx/readonly && chown root:root /ftp/dbx/readonly && chmod 0555 /ftp/dbx/readonly",
            ])
            .status()
            .unwrap();
        assert!(readonly.success());
        let denied_source = source_directory.join("upload-denied.bin");
        let denied_payload = b"permission denied fixture";
        tokio::fs::write(&denied_source, denied_payload).await.unwrap();
        let (_, denied_worker) =
            create_upload_worker_transfer(&app, "ftp-upload-denied", "readonly/upload-denied.bin", &denied_source)
                .await;
        denied_worker.await.unwrap();
        let denied = state.storage.get_file_transfer("ftp-upload-denied").await.unwrap().unwrap();
        assert_eq!(denied.status, "failed", "{denied:?}");
        assert_eq!(denied.bytes_transferred, i64::try_from(denied_payload.len()).unwrap());
        assert_ne!(denied.status, "completed");
        assert_eq!(
            operator.stat("ftp/dbx/readonly/upload-denied.bin").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );
        assert_no_remote_upload_partial(&container, "ftp-upload-denied");
    }

    #[tokio::test]
    #[ignore = "run through tests/ftp-contract.sh with two pinned FTP services"]
    async fn fixed_ftp_upload_queued_revision_contract() {
        use super::super::file_manager::{build_operator, password_scope, FileConnectionConfig, FtpConnectionConfig};

        let (app, state, primary_operator, directory, primary_container) = build_ftp_contract_app().await;
        let source_directory = directory.path().canonicalize().unwrap();
        let mut barriers = Vec::new();
        let mut workers = Vec::new();
        for index in 0..CONNECTION_TRANSFER_LIMIT {
            let source = source_directory.join(format!("queue-holder-{index}.bin"));
            tokio::fs::write(&source, vec![u8::try_from(index + 1).unwrap(); UPLOAD_BUFFER_SIZE + 17]).await.unwrap();
            let barrier = install_test_upload_after_chunk_barrier();
            let (_, worker) = create_upload_worker_transfer(
                &app,
                &format!("ftp-upload-queue-holder-{index}"),
                &format!("queue-holder-{index}.bin"),
                &source,
            )
            .await;
            tokio::time::timeout(Duration::from_secs(10), barrier.opened.notified())
                .await
                .expect("holder upload must occupy a connection permit");
            barriers.push(barrier);
            workers.push(worker);
        }

        let queued_id = "ftp-upload-queued-revision";
        let queued_source = source_directory.join("queued-revision.bin");
        tokio::fs::write(&queued_source, b"must never reach either FTP service").await.unwrap();
        let (_, queued_worker) =
            create_upload_worker_transfer(&app, queued_id, "queued-revision.bin", &queued_source).await;
        tokio::task::yield_now().await;
        let queued = state.storage.get_file_transfer(queued_id).await.unwrap().unwrap();
        assert_eq!(queued.status, "queued", "{queued:?}");

        let secondary_endpoint =
            std::env::var("DBX_TEST_FTP_SECONDARY_ENDPOINT").expect("secondary FTP endpoint is required");
        let secondary_container =
            std::env::var("DBX_TEST_FTP_SECONDARY_CONTAINER").expect("secondary FTP container is required");
        let current = state.storage.load_file_connection("ftp-contract").await.unwrap().unwrap();
        let secondary_config = FileConnectionConfig::Ftp(FtpConnectionConfig {
            endpoint: secondary_endpoint,
            root: "/ftp/dbx".to_string(),
            username: "dbx".to_string(),
        });
        state
            .storage
            .save_file_connection(
                current.id,
                current.name,
                current.kind,
                serde_json::to_string(&secondary_config).unwrap(),
                Some("dbx-password".to_string()),
                password_scope(&secondary_config).unwrap(),
                true,
                Some(current.revision),
            )
            .await
            .unwrap();

        for barrier in barriers {
            barrier.release.notify_one();
        }
        for worker in workers {
            worker.await.unwrap();
        }
        queued_worker.await.unwrap();

        let failed = state.storage.get_file_transfer(queued_id).await.unwrap().unwrap();
        assert_eq!(failed.status, "failed", "{failed:?}");
        assert_eq!(failed.bytes_transferred, 0);
        assert!(failed.error.as_deref().is_some_and(|error| error.contains("connection revision changed")));
        assert_eq!(
            primary_operator.stat("ftp/dbx/queued-revision.bin").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );
        assert_no_remote_upload_partial(&primary_container, queued_id);

        let secondary_operator = build_operator(&secondary_config, Some("dbx-password")).unwrap();
        assert_eq!(
            secondary_operator.stat("ftp/dbx/queued-revision.bin").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );
        assert_no_remote_upload_partial(&secondary_container, queued_id);
    }

    #[tokio::test]
    #[ignore = "run through tests/ftp-contract.sh with a pinned FTP image"]
    async fn fixed_ftp_upload_disconnect_contract() {
        let (app, state, operator, directory, container) = build_ftp_contract_app().await;
        let source = directory.path().canonicalize().unwrap().join("upload-disconnect.bin");
        tokio::fs::write(&source, vec![9_u8; UPLOAD_BUFFER_SIZE * 2 + 17]).await.unwrap();
        let barrier = install_test_upload_after_chunk_barrier();
        let (_, worker) =
            create_upload_worker_transfer(&app, "ftp-upload-disconnect", "upload-disconnect.bin", &source).await;
        tokio::time::timeout(Duration::from_secs(10), barrier.opened.notified())
            .await
            .expect("upload must reach the disconnect barrier");
        assert_eq!(
            operator.stat("ftp/dbx/upload-disconnect.bin").await.unwrap_err().kind(),
            opendal::ErrorKind::NotFound
        );
        let killed = tokio::task::spawn_blocking(move || {
            Command::new("docker").args(["kill", &container]).stdout(Stdio::null()).stderr(Stdio::null()).status()
        })
        .await
        .unwrap()
        .unwrap();
        assert!(killed.success());
        barrier.release.notify_one();
        tokio::time::timeout(Duration::from_secs(45), worker)
            .await
            .expect("disconnected upload must terminate within its watchdog")
            .unwrap();
        let disconnected = state.storage.get_file_transfer("ftp-upload-disconnect").await.unwrap().unwrap();
        assert_eq!(disconnected.status, "partial", "{disconnected:?}");
        assert_ne!(disconnected.status, "completed");
        assert!(disconnected
            .partial_destination
            .as_deref()
            .is_some_and(|path| path.starts_with(".dbx-upload-ftp-upload-disconnect-") && path.ends_with(".part")));
        assert!(disconnected.abort_outcome.as_deref().is_some_and(|outcome| outcome.starts_with("unsupported")));
        assert!(disconnected.error.as_deref().is_some_and(|error| error.contains("cleanup failed safely")));
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
        let storage = Storage::open_with_file_secret_key(&directory.path().join("dbx.sqlite"), TEST_FILE_SECRET_KEY)
            .await
            .unwrap();
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
        let kill = tokio::task::spawn_blocking(move || {
            Command::new("docker").args(["kill", &container]).stdout(Stdio::null()).stderr(Stdio::null()).status()
        })
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
