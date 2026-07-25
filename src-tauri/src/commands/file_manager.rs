use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use super::file_manager_paths::{reject_ftp_command_injection, reject_recursive_delete, RemotePath};
pub use super::file_manager_s3::S3ConnectionConfig;
use super::file_manager_s3::{
    build_operator as build_s3_operator, capabilities as s3_capabilities, delete_current as delete_s3_current,
    delete_entry as delete_s3_backend_entry, endpoint_host_port as endpoint_host_port_for_s3,
    file_size_if_exists as s3_file_size_if_exists, normalize_root as normalize_s3_root,
    stat_directory_or_virtual as stat_s3_directory_or_virtual, test_connection as test_s3_connection,
    validate_config as validate_s3_config, write_object_exact as write_s3_object_exact,
};
use super::file_manager_webdav::{
    build_operator as build_webdav_operator, capabilities as webdav_capabilities, copy_file as copy_webdav_file,
    delete_entry as delete_webdav_backend_entry, endpoint_host_port as endpoint_host_port_for_webdav,
    move_file as move_webdav_file, normalize_endpoint as normalize_webdav_endpoint,
    normalize_root as normalize_webdav_root, put_file as put_webdav_file, test_connection as test_webdav_connection,
    validate_config as validate_webdav_config, WebdavMutationError,
};
pub use super::file_manager_webdav::{WebdavAuthentication, WebdavConnectionConfig};
use dbx_core::connection::AppState;
use dbx_core::storage::{FileConnectionStorageRecord, FileTransferStorageRecord};
use futures::StreamExt;
use opendal::services::Ftp;
use opendal::{Buffer, ErrorKind, Metadata, Operator};
use serde::{Deserialize, Serialize};
use suppaftp::tokio::AsyncFtpStream;
use suppaftp::{FtpError, Status};
use tauri::State;
use tokio::net::lookup_host;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::file_manager_list::{
    FileListOptions, FileListPage, ListSessionBinding, ListSessionRegistry, NormalizedFileListOptions, CURSOR_EXPIRED,
};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const LIST_TIMEOUT: Duration = Duration::from_secs(30);
const MUTATION_TIMEOUT: Duration = Duration::from_secs(30);
const DELETE_WAIT_TIMEOUT: Duration = Duration::from_secs(3);
const FTP_SESSION_ATTEMPTS: usize = 3;
const FTP_SESSION_RETRY_DELAY: Duration = Duration::from_millis(100);
#[cfg(test)]
static FTP_SESSION_ESTABLISHMENT_COUNT: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TEST_S3_PUBLISH_AFTER_COMMIT_RESPONSE_LOSS: std::sync::OnceLock<Mutex<Option<String>>> =
    std::sync::OnceLock::new();

#[derive(Default)]
pub struct FileManagerRuntime {
    operators: RwLock<HashMap<String, CachedOperator>>,
    lifecycles: Arc<Mutex<HashMap<String, Arc<ConnectionRuntime>>>>,
    list_sessions: ListSessionRegistry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionLifecycle {
    Active,
    Deleting,
}

struct ConnectionRuntime {
    state: Mutex<ConnectionRuntimeState>,
    idle: Notify,
    list_lock: AsyncMutex<()>,
    mutation_lock: Arc<AsyncMutex<()>>,
}

struct ConnectionRuntimeState {
    lifecycle: ConnectionLifecycle,
    in_flight: usize,
    cancellation: Arc<CancellationSignal>,
}

#[derive(Default)]
pub(super) struct CancellationSignal {
    cancelled: AtomicBool,
    notify: Notify,
}

struct OperationLease {
    connection_id: String,
    entry: Arc<ConnectionRuntime>,
    lifecycles: Arc<Mutex<HashMap<String, Arc<ConnectionRuntime>>>>,
    cancellation: Arc<CancellationSignal>,
}

struct DeleteLease {
    connection_id: String,
    entry: Arc<ConnectionRuntime>,
    lifecycles: Arc<Mutex<HashMap<String, Arc<ConnectionRuntime>>>>,
}

struct CachedOperator {
    revision: i64,
    operator: Operator,
}

struct CachedOperatorRetirement<'a> {
    runtime: &'a FileManagerRuntime,
    connection_id: &'a str,
    revision: i64,
}

pub(super) struct PreparedFileOperation {
    pub operator: Operator,
    pub revision: i64,
    pub remote_path: String,
    pub cancellation: Arc<CancellationSignal>,
    secrets: ResolvedFileSecrets,
    _lease: OperationLease,
}

pub(super) struct PreparedFileMutation<'a> {
    pub operator: Operator,
    pub revision: i64,
    pub remote_path: String,
    pub cancellation: Arc<CancellationSignal>,
    pub config_json: String,
    config: FileConnectionConfig,
    password: Option<String>,
    secrets: ResolvedFileSecrets,
    mutation_lock: Arc<AsyncMutex<()>>,
    connection_id: String,
    runtime: &'a FileManagerRuntime,
    _lease: OperationLease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UploadPublishState {
    Completed,
    PartialSource,
    PartialTarget,
    Unknown,
}

#[derive(Debug)]
pub(super) struct UploadPublishResolution {
    pub state: UploadPublishState,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RemoteFileFingerprint {
    pub size: u64,
    pub modified: String,
    pub etag: Option<String>,
    pub version: Option<String>,
}

impl RemoteFileFingerprint {
    pub(super) fn encode(&self) -> String {
        let mut encoded = format!("size:{};modified:{}", self.size, self.modified);
        if let Some(etag) = &self.etag {
            encoded.push_str(";etag:");
            encoded.push_str(etag);
        }
        if let Some(version) = &self.version {
            encoded.push_str(";version:");
            encoded.push_str(version);
        }
        encoded
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UploadConflictMode {
    BestEffortNoClobber,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct UploadPolicy {
    pub mode: UploadConflictMode,
    pub atomic_no_clobber: bool,
    pub external_toctou_risk: bool,
}

impl UploadPolicy {
    #[cfg(test)]
    pub(super) const fn best_effort_no_clobber() -> Self {
        Self { mode: UploadConflictMode::BestEffortNoClobber, atomic_no_clobber: false, external_toctou_risk: true }
    }

    pub(super) fn validate(self) -> Result<(), String> {
        if self.mode != UploadConflictMode::BestEffortNoClobber {
            return Err("Unsupported upload conflict policy".to_string());
        }
        if self.atomic_no_clobber || !self.external_toctou_risk {
            return Err(
                "FTP uploads require best_effort_no_clobber with atomicNoClobber=false and externalToctouRisk=true"
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileConnectionConfig {
    Ftp(FtpConnectionConfig),
    S3(S3ConnectionConfig),
    Webdav(WebdavConnectionConfig),
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FtpConnectionConfig {
    pub endpoint: String,
    pub root: String,
    #[serde(default)]
    pub username: String,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileConnectionSecrets {
    pub password: Option<String>,
    #[serde(default)]
    pub clear_password: Option<bool>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    #[serde(default)]
    pub clear_s3_credentials: Option<bool>,
    pub webdav_token: Option<String>,
    #[serde(default)]
    pub clear_webdav_credentials: Option<bool>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileConnectionInput {
    pub id: Option<String>,
    pub expected_revision: Option<i64>,
    pub name: String,
    pub config: FileConnectionConfig,
    pub secrets: Option<FileConnectionSecrets>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileConnection {
    pub id: String,
    pub name: String,
    pub config: FileConnectionConfig,
    pub revision: i64,
    pub created_at: String,
    pub updated_at: String,
    pub has_password: bool,
    pub has_credentials: bool,
    pub capabilities: FileConnectionCapabilities,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileConnectionCapabilities {
    pub read: bool,
    pub write: bool,
    pub stat: bool,
    pub list: bool,
    pub create_directory: bool,
    pub delete: bool,
    pub copy: bool,
    pub rename: bool,
    pub server_side_copy: bool,
    pub atomic_rename: bool,
    pub atomic_no_clobber: bool,
}

#[derive(Clone, Default)]
pub(super) struct ResolvedFileSecrets {
    pub(super) password: Option<String>,
    pub(super) access_key_id: Option<String>,
    pub(super) secret_access_key: Option<String>,
    pub(super) session_token: Option<String>,
    pub(super) webdav_token: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub path: String,
    pub name: String,
    pub kind: String,
    pub size: u64,
    pub last_modified: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileStat {
    pub path: String,
    pub name: String,
    pub kind: String,
    pub size: u64,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub version: Option<String>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub cache_control: Option<String>,
    pub content_md5: Option<String>,
    pub user_metadata: HashMap<String, String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTestStage {
    pub stage: &'static str,
    pub status: &'static str,
    pub message: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileConnectionTestResult {
    pub success: bool,
    pub stages: Vec<ConnectionTestStage>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FileMutationOutcome {
    Completed,
    #[allow(dead_code)]
    NoOp,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMutationResult {
    pub outcome: FileMutationOutcome,
}

#[tauri::command]
pub async fn list_file_connections(state: State<'_, std::sync::Arc<AppState>>) -> Result<Vec<FileConnection>, String> {
    state.storage.list_file_connections().await?.into_iter().map(file_connection_from_storage).collect()
}

#[tauri::command]
pub async fn save_file_connection(
    state: State<'_, std::sync::Arc<AppState>>,
    runtime: State<'_, FileManagerRuntime>,
    mut input: FileConnectionInput,
) -> Result<FileConnection, String> {
    validate_input(&input)?;
    normalize_input(&mut input)?;
    let id = input.id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
    let lease = runtime.begin_operation(&id)?;

    let config_json = serde_json::to_string(&input.config).map_err(|error| error.to_string())?;
    let cancellation = lease.cancellation();
    let record = run_mutation_operation(&cancellation, "Save connection", async {
        let _mutation_guard = lease.entry.mutation_lock.lock().await;
        cancellation.ensure_active()?;
        let record = match &input.config {
            FileConnectionConfig::Ftp(_) => {
                let password = input.secrets.as_ref().and_then(|secrets| secrets.password.clone());
                let replace_secret = password.is_some()
                    || input.secrets.as_ref().is_some_and(|secrets| secrets.clear_password == Some(true));
                state
                    .storage
                    .save_file_connection_with_secret_bundle(
                        id.clone(),
                        input.name.trim().to_string(),
                        config_kind(&input.config).to_string(),
                        config_json,
                        password.into_iter().map(|value| ("password".to_string(), value)).collect(),
                        vec![
                            "password".to_string(),
                            "access_key_id".to_string(),
                            "secret_access_key".to_string(),
                            "session_token".to_string(),
                            "s3_scope".to_string(),
                            "webdav_token".to_string(),
                            "webdav_scope".to_string(),
                        ],
                        "password_scope".to_string(),
                        password_scope(&input.config)?,
                        replace_secret,
                        input.expected_revision,
                    )
                    .await?
            }
            FileConnectionConfig::S3(config) => {
                let supplied = input.secrets.as_ref();
                let replace_secrets = supplied.is_some_and(|secrets| {
                    secrets.clear_s3_credentials == Some(true)
                        || secrets.access_key_id.is_some()
                        || secrets.secret_access_key.is_some()
                        || secrets.session_token.is_some()
                });
                let secrets =
                    if config.anonymous || supplied.is_some_and(|secrets| secrets.clear_s3_credentials == Some(true)) {
                        Vec::new()
                    } else {
                        let mut values = Vec::new();
                        if let Some(value) = supplied.and_then(|secrets| secrets.access_key_id.clone()) {
                            values.push(("access_key_id".to_string(), value));
                        }
                        if let Some(value) = supplied.and_then(|secrets| secrets.secret_access_key.clone()) {
                            values.push(("secret_access_key".to_string(), value));
                        }
                        if let Some(value) = supplied.and_then(|secrets| secrets.session_token.clone()) {
                            values.push(("session_token".to_string(), value));
                        }
                        values
                    };
                state
                    .storage
                    .save_file_connection_with_secret_bundle(
                        id.clone(),
                        input.name.trim().to_string(),
                        config_kind(&input.config).to_string(),
                        config_json,
                        secrets,
                        vec![
                            "password".to_string(),
                            "password_scope".to_string(),
                            "access_key_id".to_string(),
                            "secret_access_key".to_string(),
                            "session_token".to_string(),
                            "webdav_token".to_string(),
                            "webdav_scope".to_string(),
                        ],
                        "s3_scope".to_string(),
                        password_scope(&input.config)?,
                        replace_secrets || config.anonymous,
                        input.expected_revision,
                    )
                    .await?
            }
            FileConnectionConfig::Webdav(config) => {
                let supplied = input.secrets.as_ref();
                let clear = supplied.is_some_and(|secrets| secrets.clear_webdav_credentials == Some(true));
                let replace_secrets = clear
                    || supplied.is_some_and(|secrets| secrets.password.is_some() || secrets.webdav_token.is_some())
                    || config.authentication == WebdavAuthentication::None;
                let mut values = Vec::new();
                if !clear && config.authentication != WebdavAuthentication::None {
                    if let Some(value) = supplied.and_then(|secrets| secrets.password.clone()) {
                        values.push(("password".to_string(), value));
                    }
                    if let Some(value) = supplied.and_then(|secrets| secrets.webdav_token.clone()) {
                        values.push(("webdav_token".to_string(), value));
                    }
                }
                state
                    .storage
                    .save_file_connection_with_secret_bundle(
                        id.clone(),
                        input.name.trim().to_string(),
                        config_kind(&input.config).to_string(),
                        config_json,
                        values,
                        vec![
                            "password".to_string(),
                            "password_scope".to_string(),
                            "access_key_id".to_string(),
                            "secret_access_key".to_string(),
                            "session_token".to_string(),
                            "s3_scope".to_string(),
                            "webdav_token".to_string(),
                        ],
                        "webdav_scope".to_string(),
                        password_scope(&input.config)?,
                        replace_secrets,
                        input.expected_revision,
                    )
                    .await?
            }
        };
        runtime.evict(&id);
        Ok(record)
    })
    .await?;
    file_connection_from_storage(record)
}

#[tauri::command]
pub async fn delete_file_connection(
    state: State<'_, std::sync::Arc<AppState>>,
    runtime: State<'_, FileManagerRuntime>,
    connection_id: String,
) -> Result<(), String> {
    let deleting = runtime.start_delete(&connection_id)?;
    runtime.invalidate_list_sessions(&connection_id);
    if let Err(error) = deleting.wait_for_idle().await {
        deleting.restore_active();
        return Err(error);
    }
    let result = state.storage.delete_file_connection(&connection_id).await;
    match result {
        Ok(true) => {
            runtime.evict(&connection_id);
            deleting.finish();
            Ok(())
        }
        Ok(false) => {
            deleting.restore_active();
            Err("File connection not found".to_string())
        }
        Err(error) => {
            deleting.restore_active();
            Err(error)
        }
    }
}

#[tauri::command]
pub async fn test_file_connection(
    state: State<'_, std::sync::Arc<AppState>>,
    runtime: State<'_, FileManagerRuntime>,
    mut input: FileConnectionInput,
) -> Result<FileConnectionTestResult, String> {
    let lease = match input.id.as_deref() {
        Some(id) => Some(runtime.begin_operation(id)?),
        None => None,
    };
    if validate_input(&input).is_err() {
        return Ok(test_connection_for_input(&input, ResolvedFileSecrets::default()).await);
    }
    normalize_input(&mut input)?;
    let secrets = resolve_input_secrets(&state, &input).await?;
    match lease {
        Some(lease) => {
            let cancellation = lease.cancellation();
            tokio::select! {
                result = test_connection_for_input(&input, secrets) => Ok(result),
                _ = cancellation.cancelled() => Err("File connection is being deleted".to_string()),
            }
        }
        None => Ok(test_connection_for_input(&input, secrets).await),
    }
}

#[tauri::command]
pub async fn list_file_entries(
    state: State<'_, std::sync::Arc<AppState>>,
    runtime: State<'_, FileManagerRuntime>,
    connection_id: String,
    path: String,
    options: Option<FileListOptions>,
) -> Result<FileListPage, String> {
    let options = options.unwrap_or_default().normalize()?;
    let path = normalize_relative_remote_path(&path, true)?;
    let generation = runtime.list_sessions.generation(&connection_id);
    let lease = runtime.begin_operation(&connection_id)?;
    let record = state
        .storage
        .load_file_connection(&connection_id)
        .await?
        .ok_or_else(|| "File connection not found".to_string())?;
    let revision = record.revision;
    let config = match parse_storage_config(&record) {
        Ok(config) => config,
        Err(error) => {
            runtime.evict_revision(&connection_id, revision);
            return Err(error);
        }
    };
    let binding = list_session_binding(&connection_id, revision, &path, options.clone());
    let cancellation = lease.cancellation();
    run_list_operation(&runtime, &connection_id, revision, &cancellation, LIST_TIMEOUT, async {
        let _list_guard = lease.entry.list_lock.lock().await;
        let secrets = load_file_connection_secrets(&state.storage, &connection_id, &config).await?;
        let operator = runtime.operator_for(&record, &config, &secrets)?;
        if path.is_empty() && matches!(config, FileConnectionConfig::Ftp(_)) {
            let password = secrets.password.as_deref();
            verify_ftp_root_read_only(&config, password).await?;
        }
        let list_path = configured_directory_path(&config, &path);
        let lister = operator
            .lister_with(&list_path)
            .limit(options.page_size)
            .await
            .map_err(|error| redact_secrets(error.to_string(), &secrets))?;
        let error_secrets = secrets.clone();
        let configured_root = configured_root_list_path(&config);
        let object_store = matches!(config, FileConnectionConfig::S3(_));
        let seen_entries = Arc::new(Mutex::new(HashSet::<(String, String)>::new()));
        let stream = lister.filter_map(move |result| {
            let seen_entries = seen_entries.clone();
            if result.as_ref().is_ok_and(|entry| entry.path() == configured_root) {
                return futures::future::ready(None);
            }
            let result = result
                .map_err(|error| redact_secrets(error.to_string(), &error_secrets))
                .and_then(|entry| file_entry_from_opendal(&configured_root, entry, object_store));
            futures::future::ready(match result {
                Ok(entry) => {
                    let key = (entry.kind.clone(), entry.path.clone());
                    seen_entries.lock().unwrap_or_else(|error| error.into_inner()).insert(key).then_some(Ok(entry))
                }
                Err(error) => Some(Err(error)),
            })
        });
        runtime.list_sessions.open(binding, generation, stream).await
    })
    .await
}

#[tauri::command]
pub async fn list_file_entries_next(
    state: State<'_, std::sync::Arc<AppState>>,
    runtime: State<'_, FileManagerRuntime>,
    connection_id: String,
    cursor: String,
    path: String,
    options: Option<FileListOptions>,
) -> Result<FileListPage, String> {
    let options = options.unwrap_or_default().normalize()?;
    let path = normalize_relative_remote_path(&path, true)?;
    let record = state
        .storage
        .load_file_connection(&connection_id)
        .await
        .map_err(|_| CURSOR_EXPIRED.to_string())?
        .ok_or_else(|| CURSOR_EXPIRED.to_string())?;
    let binding = list_session_binding(&connection_id, record.revision, &path, options);
    runtime.list_sessions.validate(&cursor, &binding)?;
    let lease = runtime.begin_operation(&connection_id).map_err(|_| CURSOR_EXPIRED.to_string())?;
    let cancellation = lease.cancellation();
    let result = run_list_operation(&runtime, &connection_id, record.revision, &cancellation, LIST_TIMEOUT, async {
        let _list_guard = lease.entry.list_lock.lock().await;
        runtime.list_sessions.next(&cursor, &binding).await
    })
    .await;
    if result.is_err() {
        let _ = runtime.list_sessions.invalidate_cursor(&connection_id, &cursor);
    }
    result
}

#[tauri::command]
pub fn close_file_list_cursor(
    runtime: State<'_, FileManagerRuntime>,
    connection_id: String,
    cursor: String,
) -> Result<(), String> {
    runtime.list_sessions.invalidate_cursor(&connection_id, &cursor)
}

#[tauri::command]
pub async fn stat_file_entry(
    state: State<'_, std::sync::Arc<AppState>>,
    runtime: State<'_, FileManagerRuntime>,
    connection_id: String,
    path: String,
) -> Result<FileStat, String> {
    let path = normalize_relative_remote_path(&path, true)?;
    let lease = runtime.begin_operation(&connection_id)?;
    let record = state
        .storage
        .load_file_connection(&connection_id)
        .await?
        .ok_or_else(|| "File connection not found".to_string())?;
    let revision = record.revision;
    let config = parse_storage_config(&record)?;
    let cancellation = lease.cancellation();
    run_list_operation(&runtime, &connection_id, revision, &cancellation, LIST_TIMEOUT, async {
        let secrets = load_file_connection_secrets(&state.storage, &connection_id, &config).await?;
        let operator = runtime.operator_for(&record, &config, &secrets)?;
        let metadata = stat_remote_metadata(&operator, &config, &path, secrets.password.as_deref())
            .await
            .map_err(|error| redact_secrets(error, &secrets))?;
        Ok(file_stat_from_metadata(&path, &metadata))
    })
    .await
}

#[tauri::command]
pub async fn create_file_directory(
    state: State<'_, std::sync::Arc<AppState>>,
    runtime: State<'_, FileManagerRuntime>,
    connection_id: String,
    path: String,
) -> Result<FileMutationResult, String> {
    let path = RemotePath::parse(&path)?;
    let lease = runtime.begin_operation(&connection_id)?;
    let cancellation = lease.cancellation();

    run_mutation_operation(&cancellation, "Create directory", async {
        run_locked_mutation(
            &state,
            &runtime,
            &lease.entry.mutation_lock,
            &connection_id,
            &cancellation,
            move |config, secrets| async move {
                create_directory_entry(&config, &path, &secrets).await?;
                Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
            },
        )
        .await
    })
    .await
}

#[tauri::command]
pub async fn delete_file_entry(
    state: State<'_, std::sync::Arc<AppState>>,
    runtime: State<'_, FileManagerRuntime>,
    connection_id: String,
    path: String,
    recursive: Option<bool>,
    expected_kind: Option<String>,
) -> Result<FileMutationResult, String> {
    reject_recursive_delete(recursive.unwrap_or(false))?;
    let path = RemotePath::parse(&path)?;
    let lease = runtime.begin_operation(&connection_id)?;
    let cancellation = lease.cancellation();

    run_mutation_operation(&cancellation, "Delete", async {
        run_locked_mutation(
            &state,
            &runtime,
            &lease.entry.mutation_lock,
            &connection_id,
            &cancellation,
            move |config, secrets| async move {
                delete_entry(&config, &path, expected_kind.as_deref(), &secrets).await
            },
        )
        .await
    })
    .await
}

impl FileManagerRuntime {
    fn begin_operation(&self, connection_id: &str) -> Result<OperationLease, String> {
        let mut lifecycles = self.lifecycles.lock().unwrap_or_else(|error| error.into_inner());
        let entry = lifecycles
            .entry(connection_id.to_string())
            .or_insert_with(|| Arc::new(ConnectionRuntime::default()))
            .clone();
        let mut state = entry.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.lifecycle == ConnectionLifecycle::Deleting {
            return Err("File connection is being deleted".to_string());
        }
        state.in_flight += 1;
        let cancellation = state.cancellation.clone();
        drop(state);
        drop(lifecycles);
        Ok(OperationLease {
            connection_id: connection_id.to_string(),
            entry,
            lifecycles: self.lifecycles.clone(),
            cancellation,
        })
    }

    fn start_delete(&self, connection_id: &str) -> Result<DeleteLease, String> {
        let mut lifecycles = self.lifecycles.lock().unwrap_or_else(|error| error.into_inner());
        let entry = lifecycles
            .entry(connection_id.to_string())
            .or_insert_with(|| Arc::new(ConnectionRuntime::default()))
            .clone();
        let mut state = entry.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.lifecycle == ConnectionLifecycle::Deleting {
            return Err("File connection is being deleted".to_string());
        }
        state.lifecycle = ConnectionLifecycle::Deleting;
        state.cancellation.cancel();
        drop(state);
        drop(lifecycles);
        Ok(DeleteLease { connection_id: connection_id.to_string(), entry, lifecycles: self.lifecycles.clone() })
    }

    fn evict(&self, connection_id: &str) {
        self.operators.write().unwrap_or_else(|error| error.into_inner()).remove(connection_id);
        self.invalidate_list_sessions(connection_id);
    }

    fn invalidate_list_sessions(&self, connection_id: &str) {
        self.list_sessions.invalidate_connection(connection_id);
    }

    pub(super) fn evict_revision(&self, connection_id: &str, revision: i64) {
        let mut operators = self.operators.write().unwrap_or_else(|error| error.into_inner());
        if operators.get(connection_id).is_some_and(|cached| cached.revision == revision) {
            operators.remove(connection_id);
        }
    }

    pub(super) async fn prepare_file_operation(
        &self,
        state: &AppState,
        connection_id: &str,
        remote_path: &str,
    ) -> Result<PreparedFileOperation, String> {
        let relative_path = validate_remote_relative_path(remote_path)?;
        let lease = self.begin_operation(connection_id)?;
        let record = state
            .storage
            .load_file_connection(connection_id)
            .await?
            .ok_or_else(|| "File connection not found".to_string())?;
        let revision = record.revision;
        let config = parse_storage_config(&record).inspect_err(|_| self.evict_revision(connection_id, revision))?;
        let secrets = load_file_connection_secrets(&state.storage, connection_id, &config).await?;
        let operator = self.operator_for(&record, &config, &secrets)?;
        let remote_path = configured_entry_path(&config, &relative_path, false);
        Ok(PreparedFileOperation {
            operator,
            revision,
            remote_path,
            cancellation: lease.cancellation(),
            secrets,
            _lease: lease,
        })
    }

    pub(super) async fn prepare_file_mutation_operation<'a>(
        &'a self,
        state: &AppState,
        connection_id: &str,
        remote_path: &str,
        expected_revision: i64,
    ) -> Result<PreparedFileMutation<'a>, String> {
        let relative_path = validate_remote_relative_path(remote_path)?;
        let lease = self.begin_operation(connection_id)?;
        let cancellation = lease.cancellation();
        let mutation_lock = lease.entry.mutation_lock.clone();
        cancellation.ensure_active()?;
        let record = state
            .storage
            .load_file_connection(connection_id)
            .await?
            .ok_or_else(|| "File connection not found".to_string())?;
        if record.revision != expected_revision {
            return Err(format!(
                "File connection revision changed while the upload was queued: expected {expected_revision}, current {}",
                record.revision
            ));
        }
        let revision = record.revision;
        let config = parse_storage_config(&record).inspect_err(|_| self.evict_revision(connection_id, revision))?;
        let secrets = load_file_connection_secrets(&state.storage, connection_id, &config).await?;
        let password = secrets.password.clone();
        self.evict_revision(connection_id, revision);
        let operator = build_operator_with_secrets(&config, &secrets)?;
        let remote_path = configured_entry_path(&config, &relative_path, false);
        Ok(PreparedFileMutation {
            operator,
            revision,
            remote_path,
            cancellation,
            config_json: record.config_json,
            config,
            password,
            secrets,
            mutation_lock,
            connection_id: connection_id.to_string(),
            runtime: self,
            _lease: lease,
        })
    }

    pub(super) async fn reconcile_interrupted_upload(
        &self,
        state: &AppState,
        transfer: &FileTransferStorageRecord,
    ) -> Result<UploadPublishResolution, String> {
        let expected_revision = transfer
            .connection_revision
            .ok_or_else(|| "Interrupted upload has no durable connection revision".to_string())?;
        let partial = RemotePath::parse(
            transfer
                .temp_path
                .as_deref()
                .ok_or_else(|| "Interrupted upload has no durable partial path".to_string())?,
        )?;
        let target = RemotePath::parse(&transfer.remote_path)?;
        let expected_size = usize::try_from(
            transfer.total_bytes.ok_or_else(|| "Interrupted upload has no durable source size".to_string())?,
        )
        .map_err(|_| "Upload size is not representable on this platform".to_string())?;
        let lease = self.begin_operation(&transfer.connection_id)?;
        let cancellation = lease.cancellation();
        let _mutation_guard = tokio::select! {
            _ = cancellation.cancelled() => return Err("File connection is being deleted".to_string()),
            guard = lease.entry.mutation_lock.lock() => guard,
        };
        cancellation.ensure_active()?;
        let current = state
            .storage
            .load_file_connection(&transfer.connection_id)
            .await?
            .ok_or_else(|| "File connection not found".to_string())?;
        if current.revision != expected_revision {
            return Err("File connection revision changed after the interrupted upload".to_string());
        }
        let config = parse_storage_config(&current)?;
        let secrets = load_file_connection_secrets(&state.storage, &transfer.connection_id, &config).await?;
        let detail = "The application exited while upload publish was in progress".to_string();
        match &config {
            FileConnectionConfig::Ftp(_) => {
                reconcile_ftp_upload_publish(
                    &config,
                    &partial,
                    &target,
                    expected_size,
                    secrets.password.as_deref(),
                    detail,
                )
                .await
            }
            FileConnectionConfig::S3(_) => {
                let operator = build_operator_with_secrets(&config, &secrets)?;
                let source = configured_entry_path(&config, partial.as_str(), false);
                let target = configured_entry_path(&config, target.as_str(), false);
                let source_size = s3_file_size_if_exists(&operator, &source, &secrets).await?;
                let target_size = s3_file_size_if_exists(&operator, &target, &secrets).await?;
                Ok(resolve_upload_publish_observation(source_size, target_size, expected_size, detail))
            }
            FileConnectionConfig::Webdav(_) => {
                let operator = build_operator_with_secrets(&config, &secrets)?;
                let source = configured_entry_path(&config, partial.as_str(), false);
                let target = configured_entry_path(&config, target.as_str(), false);
                let source_size = file_size_if_exists(&operator, &source, &secrets).await?;
                let target_size = file_size_if_exists(&operator, &target, &secrets).await?;
                Ok(resolve_upload_publish_observation(source_size, target_size, expected_size, detail))
            }
        }
    }

    fn operator_for(
        &self,
        record: &FileConnectionStorageRecord,
        config: &FileConnectionConfig,
        secrets: &ResolvedFileSecrets,
    ) -> Result<Operator, String> {
        if let Some(cached) = self
            .operators
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&record.id)
            .filter(|cached| cached.revision == record.revision)
        {
            return Ok(cached.operator.clone());
        }

        let operator = build_operator_with_secrets(config, secrets)?;
        self.operators
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(record.id.clone(), CachedOperator { revision: record.revision, operator: operator.clone() });
        Ok(operator)
    }

    #[cfg(test)]
    fn lifecycle_count(&self) -> usize {
        self.lifecycles.lock().unwrap_or_else(|error| error.into_inner()).len()
    }

    #[cfg(test)]
    fn operator_count(&self) -> usize {
        self.operators.read().unwrap_or_else(|error| error.into_inner()).len()
    }
}

impl Default for ConnectionRuntime {
    fn default() -> Self {
        Self {
            state: Mutex::new(ConnectionRuntimeState {
                lifecycle: ConnectionLifecycle::Active,
                in_flight: 0,
                cancellation: Arc::new(CancellationSignal::default()),
            }),
            idle: Notify::new(),
            list_lock: AsyncMutex::new(()),
            mutation_lock: Arc::new(AsyncMutex::new(())),
        }
    }
}

impl Drop for CachedOperatorRetirement<'_> {
    fn drop(&mut self) {
        self.runtime.evict_revision(self.connection_id, self.revision);
    }
}

impl Drop for PreparedFileMutation<'_> {
    fn drop(&mut self) {
        self.runtime.evict_revision(&self.connection_id, self.revision);
    }
}

impl PreparedFileOperation {
    pub(super) fn redact_remote_error(&self, message: String) -> String {
        redact_secrets(message, &self.secrets)
    }
}

impl PreparedFileMutation<'_> {
    pub(super) fn redact_remote_error(&self, message: String) -> String {
        redact_secrets(message, &self.secrets)
    }
    #[cfg(test)]
    pub(super) fn mutation_lock_is_available(&self) -> bool {
        self.mutation_lock.try_lock().is_ok()
    }

    pub(super) async fn delete_owned_upload_partial(&self, path: &str) -> Result<(), String> {
        self.delete_owned_remote_partial(path).await
    }

    pub(super) async fn delete_owned_remote_partial(&self, path: &str) -> Result<(), String> {
        let path = RemotePath::parse(path)?;
        let _mutation_guard = self.mutation_lock.lock().await;
        match &self.config {
            FileConnectionConfig::Ftp(_) => {
                delete_ftp_file_if_exists(&self.config, &path, self.password.as_deref()).await
            }
            FileConnectionConfig::S3(_) => {
                let configured = self.configured_path(path.as_str())?;
                match self.operator.stat(&configured).await {
                    Ok(metadata) => {
                        delete_s3_current(&self.operator, &configured, Some(&metadata), &self.secrets).await
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(self.redact_remote_error(error.to_string())),
                }
            }
            FileConnectionConfig::Webdav(_) => {
                let configured = self.configured_path(path.as_str())?;
                match self.operator.stat(&configured).await {
                    Ok(metadata) if metadata.mode().is_file() => self
                        .operator
                        .delete(&configured)
                        .await
                        .map_err(|error| self.redact_remote_error(error.to_string())),
                    Ok(_) => Err("Operation-owned WebDAV partial is not a file".to_string()),
                    Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(self.redact_remote_error(error.to_string())),
                }
            }
        }
    }

    pub(super) async fn create_empty_owned_upload_partial(&self, path: &str) -> Result<(), String> {
        let path = RemotePath::parse(path)?;
        let _mutation_guard = self.mutation_lock.lock().await;
        match &self.config {
            FileConnectionConfig::Ftp(_) => {
                create_empty_ftp_file_exact(&self.config, &path, self.password.as_deref()).await
            }
            FileConnectionConfig::S3(_) => {
                let configured = self.configured_path(path.as_str())?;
                if self
                    .operator
                    .exists(&configured)
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?
                {
                    return Err("Operation-owned empty upload partial already exists".to_string());
                }
                self.operator
                    .write(&configured, Vec::<u8>::new())
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?;
                Ok(())
            }
            FileConnectionConfig::Webdav(_) => {
                let configured = self.configured_path(path.as_str())?;
                if self
                    .operator
                    .exists(&configured)
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?
                {
                    return Err("Operation-owned empty upload partial already exists".to_string());
                }
                self.operator
                    .write(&configured, Vec::<u8>::new())
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?;
                Ok(())
            }
        }
    }

    pub(super) fn uses_streaming_webdav_upload(&self) -> bool {
        matches!(self.config, FileConnectionConfig::Webdav(_))
    }

    pub(super) async fn put_webdav_upload_partial(
        &self,
        path: &str,
        file: tokio::fs::File,
        size: u64,
        progress: Arc<dyn Fn(u64) + Send + Sync>,
        dispatch_started: Arc<AtomicBool>,
    ) -> Result<(), WebdavMutationError> {
        let FileConnectionConfig::Webdav(config) = &self.config else {
            return Err(WebdavMutationError::definitive(
                "WebDAV streaming upload received a non-WebDAV connection".to_string(),
            ));
        };
        put_webdav_file(config, path, file, size, progress, &self.secrets, dispatch_started).await
    }

    pub(super) async fn publish_owned_upload_partial(
        &self,
        state: &AppState,
        partial_path: &str,
        target_path: &str,
        expected_size: i64,
        policy: UploadPolicy,
        transfer_cancellation: &CancellationToken,
    ) -> Result<UploadPublishResolution, String> {
        policy.validate()?;
        let partial = RemotePath::parse(partial_path)?;
        let target = RemotePath::parse(target_path)?;
        let expected_size = usize::try_from(expected_size)
            .map_err(|_| "Upload size is not representable on this platform".to_string())?;
        let _mutation_guard = tokio::select! {
            _ = transfer_cancellation.cancelled() => return Err("Upload was cancelled before publish".to_string()),
            _ = self.cancellation.cancelled() => return Err("The file connection was removed before upload publish".to_string()),
            guard = self.mutation_lock.lock() => guard,
        };
        self.cancellation.ensure_active()?;
        let current = state
            .storage
            .load_file_connection(&self.connection_id)
            .await?
            .ok_or_else(|| "File connection not found".to_string())?;
        if current.revision != self.revision || current.config_json != self.config_json {
            return Err("File connection revision changed before upload publish".to_string());
        }
        if matches!(self.config, FileConnectionConfig::S3(_)) {
            return self
                .publish_s3_owned_partial(
                    &partial,
                    &target,
                    expected_size,
                    false,
                    "Upload publish",
                    transfer_cancellation,
                )
                .await;
        }
        if matches!(self.config, FileConnectionConfig::Webdav(_)) {
            return self
                .publish_webdav_owned_partial(
                    &partial,
                    &target,
                    expected_size,
                    false,
                    "Upload publish",
                    transfer_cancellation,
                )
                .await;
        }
        let mutation = tokio::time::timeout(
            MUTATION_TIMEOUT,
            rename_ftp_file_exact(&self.config, &partial, &target, expected_size, self.password.as_deref()),
        )
        .await;
        match mutation {
            Ok(Ok(())) => Ok(UploadPublishResolution {
                state: UploadPublishState::Completed,
                detail: "Upload publish rename completed and was verified".to_string(),
            }),
            Ok(Err(error)) => {
                reconcile_ftp_upload_publish(
                    &self.config,
                    &partial,
                    &target,
                    expected_size,
                    self.password.as_deref(),
                    error,
                )
                .await
            }
            Err(_) => {
                reconcile_ftp_upload_publish(
                    &self.config,
                    &partial,
                    &target,
                    expected_size,
                    self.password.as_deref(),
                    "Upload publish rename timed out; mutation response was not observed".to_string(),
                )
                .await
            }
        }
    }

    pub(super) fn configured_path(&self, relative_path: &str) -> Result<String, String> {
        let relative_path = validate_remote_relative_path(relative_path)?;
        Ok(configured_entry_path(&self.config, &relative_path, false))
    }

    pub(super) async fn stat_remote_file(&self, relative_path: &str) -> Result<RemoteFileFingerprint, String> {
        let relative_path = validate_remote_relative_path(relative_path)?;
        let metadata = stat_remote_metadata(&self.operator, &self.config, &relative_path, self.password.as_deref())
            .await
            .map_err(|error| self.redact_remote_error(error))?;
        if !metadata.mode().is_file() {
            return Err("Unsupported: directory copy and rename are not available in v1".to_string());
        }
        self.fingerprint_remote_file(relative_path.as_str()).await
    }

    pub(super) async fn remote_entry_exists(&self, relative_path: &str) -> Result<bool, String> {
        let relative_path = validate_remote_relative_path(relative_path)?;
        match stat_remote_metadata_once(&self.operator, &self.config, &relative_path).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(self.redact_remote_error(error.to_string())),
        }
    }

    pub(super) async fn fingerprint_remote_file(&self, relative_path: &str) -> Result<RemoteFileFingerprint, String> {
        let path = RemotePath::parse(relative_path)?;
        match &self.config {
            FileConnectionConfig::Ftp(config) => {
                let mut ftp = open_ftp_root_session(config, self.password.as_deref()).await?;
                let fingerprint = ftp_file_fingerprint_in_session(&mut ftp, &path, self.password.as_deref()).await?;
                let _ = ftp.quit().await;
                fingerprint.ok_or_else(|| "Remote file no longer exists".to_string())
            }
            FileConnectionConfig::S3(_) | FileConnectionConfig::Webdav(_) => {
                let configured = self.configured_path(path.as_str())?;
                match self.operator.stat(&configured).await {
                    Ok(metadata) if metadata.mode().is_file() => Ok(remote_fingerprint_from_metadata(&metadata)),
                    Ok(_) => Err("Unsupported: directory copy and rename are not available in v1".to_string()),
                    Err(error) if error.kind() == ErrorKind::NotFound => {
                        Err("Remote file no longer exists".to_string())
                    }
                    Err(error) => Err(self.redact_remote_error(error.to_string())),
                }
            }
        }
    }

    pub(super) async fn open_exact_ftp_read_session(&self) -> Result<AsyncFtpStream, String> {
        let FileConnectionConfig::Ftp(config) = &self.config else {
            return Err("FTP relay session is unavailable for this connection".to_string());
        };
        open_ftp_root_session(config, self.password.as_deref()).await
    }

    pub(super) fn uses_server_side_copy(&self) -> bool {
        matches!(self.config, FileConnectionConfig::S3(_) | FileConnectionConfig::Webdav(_))
    }

    pub(super) fn uses_native_rename(&self) -> bool {
        matches!(self.config, FileConnectionConfig::Webdav(_))
    }

    pub(super) fn uses_native_webdav_copy(&self) -> bool {
        matches!(self.config, FileConnectionConfig::Webdav(_))
    }

    pub(super) async fn acquire_mutation_guard(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.mutation_lock.clone().lock_owned().await
    }

    pub(super) async fn preflight_native_webdav_mutation(
        &self,
        source_path: &str,
        destination_path: &str,
        replace: bool,
    ) -> Result<(), String> {
        if !self.uses_native_webdav_copy() {
            return Err("Native WebDAV mutation received a non-WebDAV connection".to_string());
        }
        let source = self.configured_path(source_path)?;
        let destination = self.configured_path(destination_path)?;
        let source_metadata =
            self.operator.stat(&source).await.map_err(|error| self.redact_remote_error(error.to_string()))?;
        if !source_metadata.mode().is_file() {
            return Err("Unsupported: directory copy and rename are not available in v1".to_string());
        }
        if !replace
            && self.operator.exists(&destination).await.map_err(|error| self.redact_remote_error(error.to_string()))?
        {
            return Err("Remote destination already exists; best_effort_no_clobber does not replace it".to_string());
        }
        Ok(())
    }

    pub(super) async fn dispatch_native_webdav_copy(
        &self,
        source_path: &str,
        destination_path: &str,
        dispatch_started: Arc<AtomicBool>,
    ) -> Result<(), WebdavMutationError> {
        let FileConnectionConfig::Webdav(config) = &self.config else {
            return Err(WebdavMutationError::definitive(
                "Native WebDAV COPY received a non-WebDAV connection".to_string(),
            ));
        };
        let source = self.configured_path(source_path).map_err(WebdavMutationError::definitive)?;
        let destination = self.configured_path(destination_path).map_err(WebdavMutationError::definitive)?;
        copy_webdav_file(config, &source, &destination, &self.secrets, dispatch_started).await
    }

    pub(super) async fn dispatch_native_webdav_rename(
        &self,
        source_path: &str,
        destination_path: &str,
        dispatch_started: Arc<AtomicBool>,
    ) -> Result<(), WebdavMutationError> {
        let FileConnectionConfig::Webdav(config) = &self.config else {
            return Err(WebdavMutationError::definitive(
                "Native WebDAV rename received a non-WebDAV connection".to_string(),
            ));
        };
        let source = self.configured_path(source_path).map_err(WebdavMutationError::definitive)?;
        let destination = self.configured_path(destination_path).map_err(WebdavMutationError::definitive)?;
        move_webdav_file(config, &source, &destination, &self.secrets, dispatch_started).await
    }

    pub(super) fn redact_exact_ftp_error(&self, error: FtpError) -> String {
        redact_ftp_error(error, self.password.as_deref())
    }

    pub(super) async fn publish_owned_remote_partial(
        &self,
        state: &AppState,
        partial_path: &str,
        target_path: &str,
        expected_size: i64,
        replace: bool,
        transfer_cancellation: &CancellationToken,
    ) -> Result<UploadPublishResolution, String> {
        let partial = RemotePath::parse(partial_path)?;
        let target = RemotePath::parse(target_path)?;
        let expected_size = usize::try_from(expected_size)
            .map_err(|_| "Remote copy size is not representable on this platform".to_string())?;
        let _mutation_guard = tokio::select! {
            _ = transfer_cancellation.cancelled() => {
                return Err("Remote copy was cancelled before publish".to_string())
            },
            _ = self.cancellation.cancelled() => {
                return Err("The file connection was removed before remote copy publish".to_string())
            },
            guard = self.mutation_lock.lock() => guard,
        };
        self.cancellation.ensure_active()?;
        let current = state
            .storage
            .load_file_connection(&self.connection_id)
            .await?
            .ok_or_else(|| "File connection not found".to_string())?;
        if current.revision != self.revision || current.config_json != self.config_json {
            return Err("File connection revision changed before remote copy publish".to_string());
        }
        if matches!(self.config, FileConnectionConfig::S3(_)) {
            return self
                .publish_s3_owned_partial(
                    &partial,
                    &target,
                    expected_size,
                    replace,
                    "Remote copy publish",
                    transfer_cancellation,
                )
                .await;
        }
        if matches!(self.config, FileConnectionConfig::Webdav(_)) {
            return self
                .publish_webdav_owned_partial(
                    &partial,
                    &target,
                    expected_size,
                    replace,
                    "Remote copy publish",
                    transfer_cancellation,
                )
                .await;
        }
        let mutation = tokio::time::timeout(
            MUTATION_TIMEOUT,
            rename_ftp_file_exact_with_replace(
                &self.config,
                &partial,
                &target,
                expected_size,
                self.password.as_deref(),
                replace,
                "Remote copy publish",
            ),
        )
        .await;
        match mutation {
            Ok(Ok(())) => Ok(UploadPublishResolution {
                state: UploadPublishState::Completed,
                detail: "Remote copy publish rename completed and was verified".to_string(),
            }),
            Ok(Err(error)) => {
                reconcile_ftp_upload_publish(
                    &self.config,
                    &partial,
                    &target,
                    expected_size,
                    self.password.as_deref(),
                    error,
                )
                .await
            }
            Err(_) => {
                reconcile_ftp_upload_publish(
                    &self.config,
                    &partial,
                    &target,
                    expected_size,
                    self.password.as_deref(),
                    "Remote copy publish rename timed out; mutation response was not observed".to_string(),
                )
                .await
            }
        }
    }

    pub(super) async fn delete_source_if_fingerprints_match(
        &self,
        state: &AppState,
        source_path: &str,
        destination_path: &str,
        expected_source: &RemoteFileFingerprint,
        expected_destination: &RemoteFileFingerprint,
    ) -> Result<(), String> {
        let source = RemotePath::parse(source_path)?;
        let destination = RemotePath::parse(destination_path)?;
        let _mutation_guard = self.mutation_lock.lock().await;
        self.cancellation.ensure_active()?;
        let current = state
            .storage
            .load_file_connection(&self.connection_id)
            .await?
            .ok_or_else(|| "File connection not found".to_string())?;
        if current.revision != self.revision || current.config_json != self.config_json {
            return Err("File connection revision changed before rename source deletion".to_string());
        }
        match &self.config {
            FileConnectionConfig::Ftp(config) => {
                let mut ftp = open_ftp_root_session(config, self.password.as_deref()).await?;
                let current_source =
                    ftp_file_fingerprint_in_session(&mut ftp, &source, self.password.as_deref()).await?;
                let current_destination =
                    ftp_file_fingerprint_in_session(&mut ftp, &destination, self.password.as_deref()).await?;
                if current_source.as_ref() != Some(expected_source)
                    || current_destination.as_ref() != Some(expected_destination)
                {
                    let _ = ftp.quit().await;
                    return Err(
                        "Source or destination fingerprint changed; source deletion was not attempted".to_string()
                    );
                }
                let result = ftp.rm(source.as_str()).await.map_err(|error| {
                    format!("Copied source could not be deleted: {}", redact_ftp_error(error, self.password.as_deref()))
                });
                let _ = ftp.quit().await;
                result
            }
            FileConnectionConfig::S3(_) => {
                let source_path = self.configured_path(source.as_str())?;
                let destination_path = self.configured_path(destination.as_str())?;
                let source_metadata = self
                    .operator
                    .stat(&source_path)
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?;
                let destination_metadata = self
                    .operator
                    .stat(&destination_path)
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?;
                if remote_fingerprint_from_metadata(&source_metadata) != *expected_source
                    || remote_fingerprint_from_metadata(&destination_metadata) != *expected_destination
                {
                    return Err(
                        "Source or destination fingerprint changed; source deletion was not attempted".to_string()
                    );
                }
                delete_s3_current(&self.operator, &source_path, Some(&source_metadata), &self.secrets)
                    .await
                    .map_err(|error| format!("Copied source could not be deleted: {error}"))
            }
            FileConnectionConfig::Webdav(_) => {
                let source_path = self.configured_path(source.as_str())?;
                let destination_path = self.configured_path(destination.as_str())?;
                let source_metadata = self
                    .operator
                    .stat(&source_path)
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?;
                let destination_metadata = self
                    .operator
                    .stat(&destination_path)
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?;
                if remote_fingerprint_from_metadata(&source_metadata) != *expected_source
                    || remote_fingerprint_from_metadata(&destination_metadata) != *expected_destination
                {
                    return Err(
                        "Source or destination fingerprint changed; source deletion was not attempted".to_string()
                    );
                }
                self.operator.delete(&source_path).await.map_err(|error| {
                    format!("Copied source could not be deleted: {}", self.redact_remote_error(error.to_string()))
                })
            }
        }
    }

    async fn publish_webdav_owned_partial(
        &self,
        partial: &RemotePath,
        target: &RemotePath,
        expected_size: usize,
        replace: bool,
        operation: &str,
        transfer_cancellation: &CancellationToken,
    ) -> Result<UploadPublishResolution, String> {
        let partial_path = self.configured_path(partial.as_str())?;
        let target_path = self.configured_path(target.as_str())?;
        let partial_metadata =
            self.operator.stat(&partial_path).await.map_err(|error| self.redact_remote_error(error.to_string()))?;
        if !partial_metadata.mode().is_file() || partial_metadata.content_length() != expected_size as u64 {
            return Ok(UploadPublishResolution {
                state: UploadPublishState::PartialSource,
                detail: format!("{operation} WebDAV partial is missing or changed"),
            });
        }
        if !replace
            && self.operator.exists(&target_path).await.map_err(|error| self.redact_remote_error(error.to_string()))?
        {
            return Err(format!("{operation} destination already exists"));
        }
        if transfer_cancellation.is_cancelled() {
            return Err(format!("{operation} was cancelled before WebDAV MOVE dispatch"));
        }
        self.cancellation.ensure_active()?;
        let FileConnectionConfig::Webdav(config) = &self.config else {
            return Err(format!("{operation} WebDAV publish received a non-WebDAV connection"));
        };
        let dispatch_started = Arc::new(AtomicBool::new(false));
        let move_request =
            move_webdav_file(config, &partial_path, &target_path, &self.secrets, dispatch_started.clone());
        tokio::pin!(move_request);
        let result = tokio::select! {
            biased;
            result = tokio::time::timeout(MUTATION_TIMEOUT, &mut move_request) => result,
            _ = transfer_cancellation.cancelled() => {
                if dispatch_started.load(Ordering::Acquire) {
                    return self
                        .reconcile_uncertain_webdav_move(
                            &partial_path,
                            &target_path,
                            expected_size,
                            format!("{operation} MOVE was cancelled after dispatch"),
                        )
                        .await;
                }
                return Err(format!("{operation} was cancelled before WebDAV MOVE dispatch"));
            }
            _ = self.cancellation.cancelled() => {
                if dispatch_started.load(Ordering::Acquire) {
                    return self
                        .reconcile_uncertain_webdav_move(
                            &partial_path,
                            &target_path,
                            expected_size,
                            format!("The file connection was removed after {operation} MOVE dispatch"),
                        )
                        .await;
                }
                return Err(format!("The file connection was removed before {operation} MOVE dispatch"));
            }
        };
        match result {
            Ok(Ok(())) => {
                let target_metadata = self
                    .operator
                    .stat(&target_path)
                    .await
                    .map_err(|error| self.redact_remote_error(error.to_string()))?;
                if !target_metadata.mode().is_file() || target_metadata.content_length() != expected_size as u64 {
                    return Ok(UploadPublishResolution {
                        state: UploadPublishState::PartialTarget,
                        detail: format!("{operation} MOVE completed but destination verification failed"),
                    });
                }
                Ok(UploadPublishResolution {
                    state: UploadPublishState::Completed,
                    detail: format!("{operation} completed with native WebDAV MOVE"),
                })
            }
            Ok(Err(error)) if !error.is_outcome_unknown() => Err(error.message),
            Ok(Err(error)) => {
                self.reconcile_uncertain_webdav_move(&partial_path, &target_path, expected_size, error.message).await
            }
            Err(_) => {
                self.reconcile_uncertain_webdav_move(
                    &partial_path,
                    &target_path,
                    expected_size,
                    format!("{operation} MOVE timed out"),
                )
                .await
            }
        }
    }

    async fn reconcile_uncertain_webdav_move(
        &self,
        partial_path: &str,
        target_path: &str,
        expected_size: usize,
        detail: String,
    ) -> Result<UploadPublishResolution, String> {
        let source = file_size_if_exists(&self.operator, partial_path, &self.secrets).await;
        let target = file_size_if_exists(&self.operator, target_path, &self.secrets).await;
        match (source, target) {
            (Ok(None), Ok(Some(size))) if size == expected_size => Ok(UploadPublishResolution {
                state: UploadPublishState::PartialTarget,
                detail: format!(
                    "{detail}; destination is present and source is absent, but the MOVE response was lost"
                ),
            }),
            (Ok(Some(size)), Ok(None)) if size == expected_size => Ok(UploadPublishResolution {
                state: UploadPublishState::PartialSource,
                detail: format!("{detail}; owned partial remains and destination is absent"),
            }),
            (Ok(source), Ok(target)) => Ok(UploadPublishResolution {
                state: UploadPublishState::Unknown,
                detail: format!("{detail}; source size {source:?}, destination size {target:?}"),
            }),
            (Err(error), _) | (_, Err(error)) => Ok(UploadPublishResolution {
                state: UploadPublishState::Unknown,
                detail: format!("{detail}; reconciliation failed: {error}"),
            }),
        }
    }

    async fn publish_s3_owned_partial(
        &self,
        partial: &RemotePath,
        target: &RemotePath,
        expected_size: usize,
        replace: bool,
        operation: &str,
        transfer_cancellation: &CancellationToken,
    ) -> Result<UploadPublishResolution, String> {
        let partial_path = self.configured_path(partial.as_str())?;
        let target_path = self.configured_path(target.as_str())?;
        let partial_metadata =
            self.operator.stat(&partial_path).await.map_err(|error| self.redact_remote_error(error.to_string()))?;
        if partial_metadata.content_length() != expected_size as u64 {
            return Ok(UploadPublishResolution {
                state: UploadPublishState::PartialSource,
                detail: format!(
                    "{operation} partial size changed: expected {expected_size}, actual {}",
                    partial_metadata.content_length()
                ),
            });
        }
        if !replace
            && self.operator.exists(&target_path).await.map_err(|error| self.redact_remote_error(error.to_string()))?
        {
            return Err(format!("{operation} destination already exists"));
        }
        let mut copier = self
            .operator
            .copier_with(&partial_path, &target_path)
            .if_not_exists(!replace)
            .source_content_length_hint(partial_metadata.content_length())
            .concurrent(1)
            .await
            .map_err(|error| self.redact_remote_error(error.to_string()))?;
        loop {
            let step = tokio::select! {
                _ = transfer_cancellation.cancelled() => {
                    let abort = copier.abort().await;
                    let detail = abort.err().map(|error| format!("; abort failed: {error}")).unwrap_or_default();
                    return self.reconcile_uncertain_s3_publish(
                        &partial_path, &target_path, &partial_metadata,
                        format!("{operation} cancelled{detail}"),
                    ).await;
                }
                _ = self.cancellation.cancelled() => {
                    let abort = copier.abort().await;
                    let detail = abort.err().map(|error| format!("; abort failed: {error}")).unwrap_or_default();
                    return self.reconcile_uncertain_s3_publish(
                        &partial_path, &target_path, &partial_metadata,
                        format!("{operation} connection cancelled{detail}"),
                    ).await;
                }
                result = tokio::time::timeout(MUTATION_TIMEOUT, copier.next()) => result,
            };
            match step {
                Ok(Ok(Some(_))) => continue,
                Ok(Ok(None)) => break,
                Ok(Err(error)) => {
                    let abort = copier.abort().await;
                    let detail = abort.err().map(|abort| format!("; abort failed: {abort}")).unwrap_or_default();
                    return self
                        .reconcile_uncertain_s3_publish(
                            &partial_path,
                            &target_path,
                            &partial_metadata,
                            self.redact_remote_error(format!("{operation} failed: {error}{detail}")),
                        )
                        .await;
                }
                Err(_) => {
                    let abort = copier.abort().await;
                    let detail = abort.err().map(|error| format!("; abort failed: {error}")).unwrap_or_default();
                    return self
                        .reconcile_uncertain_s3_publish(
                            &partial_path,
                            &target_path,
                            &partial_metadata,
                            format!("{operation} timed out{detail}"),
                        )
                        .await;
                }
            }
        }
        #[cfg(test)]
        if take_test_s3_publish_after_commit_response_loss(&target_path) {
            return self
                .reconcile_uncertain_s3_publish(
                    &partial_path,
                    &target_path,
                    &partial_metadata,
                    format!("{operation} injected after-commit response loss"),
                )
                .await;
        }
        let target_metadata =
            self.operator.stat(&target_path).await.map_err(|error| self.redact_remote_error(error.to_string()))?;
        if target_metadata.content_length() != expected_size as u64 {
            return Ok(UploadPublishResolution {
                state: UploadPublishState::PartialTarget,
                detail: format!(
                    "{operation} destination size mismatch: expected {expected_size}, actual {}",
                    target_metadata.content_length()
                ),
            });
        }
        delete_s3_current(&self.operator, &partial_path, Some(&partial_metadata), &self.secrets)
            .await
            .map_err(|error| format!("{operation} completed but owned partial cleanup failed: {error}"))?;
        Ok(UploadPublishResolution {
            state: UploadPublishState::Completed,
            detail: format!("{operation} completed with server-side S3 copy"),
        })
    }

    async fn reconcile_uncertain_s3_publish(
        &self,
        partial_path: &str,
        target_path: &str,
        partial: &Metadata,
        detail: String,
    ) -> Result<UploadPublishResolution, String> {
        match self.operator.stat(target_path).await {
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(UploadPublishResolution {
                state: UploadPublishState::PartialSource,
                detail: format!("{detail}; destination is absent and the owned partial remains"),
            }),
            Ok(target)
                if target.content_length() == partial.content_length()
                    && partial.etag().is_some()
                    && target.etag() == partial.etag() =>
            {
                Ok(UploadPublishResolution {
                    state: UploadPublishState::PartialTarget,
                    detail: format!(
                        "{detail}; destination is committed and fingerprint-matching, but publish response was lost; owned partial remains at {partial_path}"
                    ),
                })
            }
            Ok(_) => Ok(UploadPublishResolution {
                state: UploadPublishState::PartialTarget,
                detail: format!("{detail}; destination exists but is unproven; owned partial remains at {partial_path}"),
            }),
            Err(error) => Ok(UploadPublishResolution {
                state: UploadPublishState::Unknown,
                detail: self.redact_remote_error(format!("{detail}; destination reconciliation failed: {error}")),
            }),
        }
    }
}

#[cfg(test)]
pub(super) fn install_test_s3_publish_after_commit_response_loss(target_path: &str) {
    *TEST_S3_PUBLISH_AFTER_COMMIT_RESPONSE_LOSS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(target_path.to_string());
}

#[cfg(test)]
fn take_test_s3_publish_after_commit_response_loss(target_path: &str) -> bool {
    let mut target = TEST_S3_PUBLISH_AFTER_COMMIT_RESPONSE_LOSS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if target.as_deref() == Some(target_path) {
        target.take();
        true
    } else {
        false
    }
}

impl CancellationSignal {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(super) async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            if self.cancelled.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn ensure_active(&self) -> Result<(), String> {
        if self.cancelled.load(Ordering::Acquire) {
            Err("File connection is being deleted".to_string())
        } else {
            Ok(())
        }
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl OperationLease {
    fn cancellation(&self) -> Arc<CancellationSignal> {
        self.cancellation.clone()
    }
}

impl Drop for OperationLease {
    fn drop(&mut self) {
        let mut lifecycles = self.lifecycles.lock().unwrap_or_else(|error| error.into_inner());
        let mut state = self.entry.state.lock().unwrap_or_else(|error| error.into_inner());
        debug_assert!(state.in_flight > 0);
        state.in_flight = state.in_flight.saturating_sub(1);
        let became_idle = state.in_flight == 0;
        let remove = became_idle
            && state.lifecycle == ConnectionLifecycle::Active
            && lifecycles.get(&self.connection_id).is_some_and(|entry| Arc::ptr_eq(entry, &self.entry));
        drop(state);
        if became_idle {
            self.entry.idle.notify_waiters();
        }
        if remove {
            lifecycles.remove(&self.connection_id);
        }
    }
}

impl DeleteLease {
    async fn wait_for_idle(&self) -> Result<(), String> {
        tokio::time::timeout(DELETE_WAIT_TIMEOUT, async {
            loop {
                let notified = self.entry.idle.notified();
                if self.entry.state.lock().unwrap_or_else(|error| error.into_inner()).in_flight == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| "Timed out waiting for file operations to stop; connection was not deleted".to_string())
    }

    fn restore_active(&self) {
        let mut lifecycles = self.lifecycles.lock().unwrap_or_else(|error| error.into_inner());
        let mut state = self.entry.state.lock().unwrap_or_else(|error| error.into_inner());
        state.lifecycle = ConnectionLifecycle::Active;
        state.cancellation = Arc::new(CancellationSignal::default());
        let remove = state.in_flight == 0
            && lifecycles.get(&self.connection_id).is_some_and(|entry| Arc::ptr_eq(entry, &self.entry));
        drop(state);
        if remove {
            lifecycles.remove(&self.connection_id);
        }
    }

    fn finish(&self) {
        let mut lifecycles = self.lifecycles.lock().unwrap_or_else(|error| error.into_inner());
        if lifecycles.get(&self.connection_id).is_some_and(|entry| Arc::ptr_eq(entry, &self.entry)) {
            lifecycles.remove(&self.connection_id);
        }
    }
}

async fn resolve_input_secrets(state: &AppState, input: &FileConnectionInput) -> Result<ResolvedFileSecrets, String> {
    match &input.config {
        FileConnectionConfig::Ftp(_) => {
            if input.secrets.as_ref().is_some_and(|secrets| secrets.clear_password == Some(true)) {
                return Ok(ResolvedFileSecrets::default());
            }
            if let Some(password) = input.secrets.as_ref().and_then(|secrets| secrets.password.clone()) {
                return Ok(ResolvedFileSecrets { password: Some(password), ..ResolvedFileSecrets::default() });
            }
        }
        FileConnectionConfig::S3(config) => {
            if config.anonymous {
                return Ok(ResolvedFileSecrets::default());
            }
            if input.secrets.as_ref().is_some_and(|secrets| secrets.clear_s3_credentials == Some(true)) {
                return Ok(ResolvedFileSecrets::default());
            }
            if input.secrets.as_ref().is_some_and(|secrets| {
                secrets.access_key_id.is_some()
                    || secrets.secret_access_key.is_some()
                    || secrets.session_token.is_some()
            }) {
                let secrets = input.secrets.as_ref().expect("checked");
                return Ok(ResolvedFileSecrets {
                    access_key_id: secrets.access_key_id.clone(),
                    secret_access_key: secrets.secret_access_key.clone(),
                    session_token: secrets.session_token.clone(),
                    ..ResolvedFileSecrets::default()
                });
            }
        }
        FileConnectionConfig::Webdav(config) => {
            if config.authentication == WebdavAuthentication::None
                || input.secrets.as_ref().is_some_and(|secrets| secrets.clear_webdav_credentials == Some(true))
            {
                return Ok(ResolvedFileSecrets::default());
            }
            if input
                .secrets
                .as_ref()
                .is_some_and(|secrets| secrets.password.is_some() || secrets.webdav_token.is_some())
            {
                let secrets = input.secrets.as_ref().expect("checked");
                return Ok(ResolvedFileSecrets {
                    password: secrets.password.clone(),
                    webdav_token: secrets.webdav_token.clone(),
                    ..ResolvedFileSecrets::default()
                });
            }
        }
    }

    let Some(id) = input.id.as_deref() else {
        return Ok(ResolvedFileSecrets::default());
    };
    let record =
        state.storage.load_file_connection(id).await?.ok_or_else(|| "File connection not found".to_string())?;
    if input.expected_revision != Some(record.revision) {
        return Err("Saved credentials cannot be reused after the connection revision changed".to_string());
    }
    let stored_config = parse_storage_config(&record)?;
    if password_scope(&input.config)? != password_scope(&stored_config)? {
        return Err("Re-enter or clear the credentials after changing the connection endpoint or identity".to_string());
    }
    load_file_connection_secrets(&state.storage, id, &input.config).await
}

async fn load_file_connection_secrets(
    storage: &dbx_core::storage::Storage,
    id: &str,
    config: &FileConnectionConfig,
) -> Result<ResolvedFileSecrets, String> {
    match config {
        FileConnectionConfig::Ftp(_) => Ok(ResolvedFileSecrets {
            password: storage.load_file_connection_password(id, &password_scope(config)?).await?,
            ..ResolvedFileSecrets::default()
        }),
        FileConnectionConfig::S3(_) => {
            let stored_scope = storage.load_file_connection_secret(id, "s3_scope").await?;
            let access_key_id = storage.load_file_connection_secret(id, "access_key_id").await?;
            let secret_access_key = storage.load_file_connection_secret(id, "secret_access_key").await?;
            let session_token = storage.load_file_connection_secret(id, "session_token").await?;
            if (access_key_id.is_some() || secret_access_key.is_some() || session_token.is_some())
                && stored_scope.as_deref() != Some(password_scope(config)?.as_str())
            {
                return Err("Stored S3 credentials do not match this endpoint and bucket; re-enter them".to_string());
            }
            Ok(ResolvedFileSecrets {
                password: None,
                access_key_id,
                secret_access_key,
                session_token,
                webdav_token: None,
            })
        }
        FileConnectionConfig::Webdav(_) => {
            let stored_scope = storage.load_file_connection_secret(id, "webdav_scope").await?;
            let password = storage.load_file_connection_secret(id, "password").await?;
            let webdav_token = storage.load_file_connection_secret(id, "webdav_token").await?;
            if (password.is_some() || webdav_token.is_some())
                && stored_scope.as_deref() != Some(password_scope(config)?.as_str())
            {
                return Err(
                    "Stored WebDAV credentials do not match this endpoint and identity; re-enter them".to_string()
                );
            }
            Ok(ResolvedFileSecrets { password, webdav_token, ..ResolvedFileSecrets::default() })
        }
    }
}

async fn file_size_if_exists(
    operator: &Operator,
    path: &str,
    secrets: &ResolvedFileSecrets,
) -> Result<Option<usize>, String> {
    match operator.stat(path).await {
        Ok(metadata) if metadata.mode().is_file() => usize::try_from(metadata.content_length())
            .map(Some)
            .map_err(|_| "Remote file size is not representable".to_string()),
        Ok(_) => Err("Remote resource is not a file".to_string()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(redact_secrets(error.to_string(), secrets)),
    }
}

async fn run_with_deadline_and_cancellation<T, F>(
    cancellation: &CancellationSignal,
    deadline: Duration,
    future: F,
) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    tokio::select! {
        _ = cancellation.cancelled() => Err("File connection is being deleted".to_string()),
        result = tokio::time::timeout(deadline, future) => {
            result.map_err(|_| "FTP root listing timed out".to_string())?
        }
    }
}

async fn run_list_operation<T, F>(
    runtime: &FileManagerRuntime,
    connection_id: &str,
    revision: i64,
    cancellation: &CancellationSignal,
    deadline: Duration,
    future: F,
) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    let result = run_with_deadline_and_cancellation(cancellation, deadline, future).await;
    if result.as_ref().err().is_some_and(|error| !error.starts_with("CursorExpired:")) {
        runtime.evict_revision(connection_id, revision);
    }
    result
}

async fn run_mutation_operation<T, F>(
    cancellation: &CancellationSignal,
    operation: &'static str,
    future: F,
) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    tokio::select! {
        _ = cancellation.cancelled() => Err("File connection is being deleted".to_string()),
        result = tokio::time::timeout(MUTATION_TIMEOUT, future) => {
            result.map_err(|_| format!("{operation} timed out"))?
        }
    }
}

async fn run_locked_mutation<T, Mutate, Mutation>(
    state: &AppState,
    runtime: &FileManagerRuntime,
    mutation_lock: &AsyncMutex<()>,
    connection_id: &str,
    cancellation: &CancellationSignal,
    mutate: Mutate,
) -> Result<T, String>
where
    Mutate: FnOnce(FileConnectionConfig, ResolvedFileSecrets) -> Mutation,
    Mutation: Future<Output = Result<T, String>>,
{
    let mutation_guard = mutation_lock.lock().await;
    run_locked_mutation_with_guard(state, runtime, mutation_guard, connection_id, cancellation, mutate).await
}

async fn run_locked_mutation_with_guard<T, Mutate, Mutation>(
    state: &AppState,
    runtime: &FileManagerRuntime,
    _mutation_guard: tokio::sync::MutexGuard<'_, ()>,
    connection_id: &str,
    cancellation: &CancellationSignal,
    mutate: Mutate,
) -> Result<T, String>
where
    Mutate: FnOnce(FileConnectionConfig, ResolvedFileSecrets) -> Mutation,
    Mutation: Future<Output = Result<T, String>>,
{
    cancellation.ensure_active()?;
    let record = state
        .storage
        .load_file_connection(connection_id)
        .await?
        .ok_or_else(|| "File connection not found".to_string())?;
    let revision = record.revision;
    let config = parse_storage_config(&record)?;
    let secrets = load_file_connection_secrets(&state.storage, connection_id, &config).await?;
    // Declared after the lock guard so every return/cancellation path evicts
    // the cached operator before the per-connection lock is released.
    let _retirement = CachedOperatorRetirement { runtime, connection_id, revision };
    // FTP mutations use exact, short-lived protocol sessions and never share
    // OpenDAL's pooled browsing connections.
    runtime.evict_revision(connection_id, revision);
    mutate(config, secrets).await
}

async fn delete_entry(
    config: &FileConnectionConfig,
    path: &RemotePath,
    expected_kind: Option<&str>,
    secrets: &ResolvedFileSecrets,
) -> Result<FileMutationResult, String> {
    match config {
        FileConnectionConfig::Ftp(ftp_config) => {
            let password = secrets.password.as_deref();
            let mut ftp = open_ftp_root_session(ftp_config, password).await?;
            let kind = prepare_ftp_delete_in_session(&mut ftp, ftp_config, path, password).await?;
            delete_ftp_entry_in_session(ftp, ftp_config, path, kind, password).await?;
            Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
        }
        FileConnectionConfig::S3(config) => delete_s3_backend_entry(config, path, expected_kind, secrets).await,
        FileConnectionConfig::Webdav(config) => {
            let operator = build_webdav_operator(config, secrets)?;
            delete_webdav_backend_entry(config, &operator, path, expected_kind, secrets).await
        }
    }
}

#[cfg(test)]
async fn delete_s3_entry(
    config: &FileConnectionConfig,
    path: &RemotePath,
    expected_kind: Option<&str>,
    secrets: &ResolvedFileSecrets,
) -> Result<FileMutationResult, String> {
    let FileConnectionConfig::S3(config) = config else {
        return Err("S3 delete received a non-S3 configuration".to_string());
    };
    delete_s3_backend_entry(config, path, expected_kind, secrets).await
}

async fn create_directory_entry(
    config: &FileConnectionConfig,
    path: &RemotePath,
    secrets: &ResolvedFileSecrets,
) -> Result<(), String> {
    match config {
        FileConnectionConfig::Ftp(_) => create_ftp_directory_exact(config, path, secrets.password.as_deref()).await,
        FileConnectionConfig::S3(config) => {
            let operator = build_s3_operator(config, secrets)?;
            let marker = format!("{}/", path.as_str().trim_end_matches('/'));
            write_s3_object_exact(&operator, &marker, Buffer::new(), true, secrets).await.map(|_| ())
        }
        FileConnectionConfig::Webdav(config) => {
            let operator = build_webdav_operator(config, secrets)?;
            let directory = format!("{}/", path.as_str().trim_end_matches('/'));
            match operator.stat(&directory).await {
                Ok(_) => Err("WebDAV destination already exists".to_string()),
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    operator.create_dir(&directory).await.map_err(|error| redact_secrets(error.to_string(), secrets))
                }
                Err(error) => Err(redact_secrets(error.to_string(), secrets)),
            }
        }
    }
}

#[derive(Clone, Copy)]
enum FtpEntryKind {
    File,
    Directory,
}

async fn prepare_ftp_delete_in_session(
    ftp: &mut AsyncFtpStream,
    config: &FtpConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<FtpEntryKind, String> {
    let directory_result = ftp.cwd(path.as_str()).await;
    if directory_result.is_ok() {
        ftp.cwd(&config.root)
            .await
            .map_err(|error| format!("Could not return to the FTP root: {}", redact_ftp_error(error, password)))?;
        let entries = ftp
            .nlst(Some(path.as_str()))
            .await
            .map_err(|error| format!("Directory preflight failed: {}", redact_ftp_error(error, password)))?;
        if !entries.is_empty() {
            return Err("Directory is not empty; recursive delete is unsupported".to_string());
        }
        return Ok(FtpEntryKind::Directory);
    }

    let directory_error = directory_result.unwrap_err();
    if !ftp_error_is_file_unavailable(&directory_error) {
        return Err(format!(
            "File or directory classification failed: {}",
            redact_ftp_error(directory_error, password)
        ));
    }
    match ftp_entry_exists_in_session(ftp, config, path).await {
        Ok(true) => Ok(FtpEntryKind::File),
        Ok(false) => Err("File or directory not found".to_string()),
        Err(error) => Err(format!("File classification failed: {}", redact_ftp_error(error, password))),
    }
}

#[cfg(test)]
async fn delete_ftp_directory_exact(
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config else {
        return Err("FTP partial cleanup helper received a non-FTP connection".to_string());
    };
    let ftp = open_ftp_root_session(config, password).await?;
    delete_ftp_entry_in_session(ftp, config, path, FtpEntryKind::Directory, password).await
}

async fn delete_ftp_file_if_exists(
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config else {
        return Err("FTP empty-file helper received a non-FTP connection".to_string());
    };
    let mut ftp = open_ftp_root_session(config, password).await?;
    let exists = ftp_file_size_if_exists(&mut ftp, path)
        .await
        .map_err(|error| format!("Upload partial classification failed: {}", redact_ftp_error(error, password)))?;
    if exists.is_none() {
        let _ = ftp.quit().await;
        return Ok(());
    }
    delete_ftp_entry_in_session(ftp, config, path, FtpEntryKind::File, password).await
}

#[cfg(test)]
async fn delete_ftp_file_exact(
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config else {
        return Err("FTP rename helper received a non-FTP connection".to_string());
    };
    let ftp = open_ftp_root_session(config, password).await?;
    delete_ftp_entry_in_session(ftp, config, path, FtpEntryKind::File, password).await
}

async fn create_empty_ftp_file_exact(
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config else {
        return Err("FTP empty-file helper received a non-FTP connection".to_string());
    };
    let mut ftp = open_ftp_root_session(config, password).await?;
    if ftp_file_size_if_exists(&mut ftp, path)
        .await
        .map_err(|error| format!("Empty upload partial preflight failed: {}", redact_ftp_error(error, password)))?
        .is_some()
    {
        let _ = ftp.quit().await;
        return Err("Operation-owned empty upload partial already exists".to_string());
    }
    let mut empty = tokio::io::empty();
    ftp.put_file(path.as_str(), &mut empty)
        .await
        .map_err(|error| format!("Creating empty upload partial failed: {}", redact_ftp_error(error, password)))?;
    let size = ftp
        .size(path.as_str())
        .await
        .map_err(|error| format!("Empty upload partial verification failed: {}", redact_ftp_error(error, password)))?;
    let _ = ftp.quit().await;
    if size == 0 {
        Ok(())
    } else {
        Err("Empty upload partial has an unexpected non-zero size".to_string())
    }
}

async fn rename_ftp_file_exact(
    config: &FileConnectionConfig,
    source: &RemotePath,
    target: &RemotePath,
    expected_size: usize,
    password: Option<&str>,
) -> Result<(), String> {
    rename_ftp_file_exact_with_replace(config, source, target, expected_size, password, false, "Upload publish").await
}

async fn rename_ftp_file_exact_with_replace(
    config: &FileConnectionConfig,
    source: &RemotePath,
    target: &RemotePath,
    expected_size: usize,
    password: Option<&str>,
    replace: bool,
    operation: &str,
) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config else {
        return Err("FTP rename helper received a non-FTP connection".to_string());
    };
    let mut ftp = open_ftp_root_session(config, password).await?;
    if !replace
        && ftp_file_size_if_exists(&mut ftp, target)
            .await
            .map_err(|error| format!("{operation} target preflight failed: {}", redact_ftp_error(error, password)))?
            .is_some()
    {
        let _ = ftp.quit().await;
        return Err("Remote destination already exists".to_string());
    }
    let source_size = ftp_file_size_if_exists(&mut ftp, source)
        .await
        .map_err(|error| format!("{operation} partial preflight failed: {}", redact_ftp_error(error, password)))?;
    let Some(source_size) = source_size else {
        let _ = ftp.quit().await;
        return Err("Operation-owned remote partial is missing".to_string());
    };
    if source_size != expected_size {
        let _ = ftp.quit().await;
        return Err("Operation-owned remote partial size does not match the validated source".to_string());
    }

    let mutation = ftp.rename(source.as_str(), target.as_str()).await;
    let mutation_context = mutation.as_ref().err().map(|error| redact_error(error.to_string(), password));
    if mutation.as_ref().is_err_and(ftp_session_is_unusable) {
        drop(ftp);
        return Err(format!(
            "{}; upload publish rename outcome is unknown and was not inferred from path existence",
            mutation_context.unwrap_or_else(|| "Upload publish session disconnected".to_string())
        ));
    }
    if let Err(error) = mutation {
        let _ = ftp.quit().await;
        return Err(format!("{operation} rename failed: {}", redact_ftp_error(error, password)));
    }

    match verify_ftp_rename_in_session(&mut ftp, config, source, target, expected_size).await {
        Ok(true) => {
            let _ = ftp.quit().await;
            Ok(())
        }
        Ok(false) => {
            let _ = ftp.quit().await;
            Err("Upload publish rename could not be verified".to_string())
        }
        Err(error) if ftp_session_is_unusable(&error) => {
            drop(ftp);
            verify_successful_ftp_rename_in_fresh_session(config, source, target, expected_size, password).await
        }
        Err(error) => {
            let _ = ftp.quit().await;
            Err(format!("Upload publish verification failed: {}", redact_ftp_error(error, password)))
        }
    }
}

async fn ftp_file_fingerprint_in_session(
    ftp: &mut AsyncFtpStream,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<Option<RemoteFileFingerprint>, String> {
    let Some(size) = ftp_file_size_if_exists(ftp, path)
        .await
        .map_err(|error| format!("Remote file size check failed: {}", redact_ftp_error(error, password)))?
    else {
        return Ok(None);
    };
    let modified = ftp
        .mdtm(path.as_str())
        .await
        .map_err(|error| {
            format!(
                "FTP server must support MDTM for safe copy/rename fingerprint checks: {}",
                redact_ftp_error(error, password)
            )
        })?
        .and_utc()
        .to_rfc3339();
    Ok(Some(RemoteFileFingerprint {
        size: u64::try_from(size).map_err(|_| "Remote file size is not representable".to_string())?,
        modified,
        etag: None,
        version: None,
    }))
}

async fn verify_ftp_rename_in_session(
    ftp: &mut AsyncFtpStream,
    config: &FtpConnectionConfig,
    source: &RemotePath,
    target: &RemotePath,
    expected_size: usize,
) -> Result<bool, FtpError> {
    let _ = config;
    let source_size = ftp_file_size_if_exists(ftp, source).await?;
    let target_size = ftp_file_size_if_exists(ftp, target).await?;
    Ok(source_size.is_none() && target_size == Some(expected_size))
}

async fn verify_successful_ftp_rename_in_fresh_session(
    config: &FtpConnectionConfig,
    source: &RemotePath,
    target: &RemotePath,
    expected_size: usize,
    password: Option<&str>,
) -> Result<(), String> {
    let mut verify = open_ftp_root_session(config, password)
        .await
        .map_err(|error| format!("Successful upload publish could not be verified in a fresh session: {error}"))?;
    let result =
        verify_ftp_rename_in_session(&mut verify, config, source, target, expected_size).await.map_err(|error| {
            format!("Upload publish read-only verification failed: {}", redact_ftp_error(error, password))
        });
    let _ = verify.quit().await;
    match result? {
        true => Ok(()),
        false => {
            Err("Upload publish rename returned success but its target fingerprint could not be verified".to_string())
        }
    }
}

async fn reconcile_ftp_upload_publish(
    config: &FileConnectionConfig,
    source: &RemotePath,
    target: &RemotePath,
    expected_size: usize,
    password: Option<&str>,
    detail: String,
) -> Result<UploadPublishResolution, String> {
    let FileConnectionConfig::Ftp(config) = config else {
        return Err("FTP publish reconciliation received a non-FTP connection".to_string());
    };
    let mut ftp = match open_ftp_root_session(config, password).await {
        Ok(ftp) => ftp,
        Err(error) => {
            return Ok(UploadPublishResolution {
                state: UploadPublishState::Unknown,
                detail: format!("{detail}; read-only reconciliation could not connect: {error}"),
            })
        }
    };
    let source_size = match ftp_file_size_if_exists(&mut ftp, source).await {
        Ok(size) => size,
        Err(error) => {
            let _ = ftp.quit().await;
            return Ok(UploadPublishResolution {
                state: UploadPublishState::Unknown,
                detail: format!("{detail}; source reconciliation failed: {}", redact_ftp_error(error, password)),
            });
        }
    };
    let target_size = match ftp_file_size_if_exists(&mut ftp, target).await {
        Ok(size) => size,
        Err(error) => {
            let _ = ftp.quit().await;
            return Ok(UploadPublishResolution {
                state: UploadPublishState::Unknown,
                detail: format!("{detail}; target reconciliation failed: {}", redact_ftp_error(error, password)),
            });
        }
    };
    let _ = ftp.quit().await;
    Ok(resolve_upload_publish_observation(source_size, target_size, expected_size, detail))
}

fn resolve_upload_publish_observation(
    source_size: Option<usize>,
    target_size: Option<usize>,
    expected_size: usize,
    detail: String,
) -> UploadPublishResolution {
    if let Some(actual_size) = source_size {
        let size_detail = if actual_size == expected_size {
            format!("operation-owned source partial exists with expected size {expected_size}")
        } else {
            format!(
                "operation-owned source partial size mismatch: expected {expected_size}, actual {actual_size}; partial was preserved"
            )
        };
        return UploadPublishResolution {
            state: UploadPublishState::PartialSource,
            detail: format!("{detail}; {size_detail}"),
        };
    }
    let state = match target_size {
        Some(target_size) if target_size == expected_size => UploadPublishState::PartialTarget,
        _ => UploadPublishState::Unknown,
    };
    UploadPublishResolution { state, detail }
}

async fn delete_ftp_entry_in_session(
    mut ftp: AsyncFtpStream,
    config: &FtpConnectionConfig,
    path: &RemotePath,
    kind: FtpEntryKind,
    password: Option<&str>,
) -> Result<(), String> {
    let mutation = match kind {
        FtpEntryKind::File => ftp.rm(path.as_str()).await,
        FtpEntryKind::Directory => ftp.rmdir(path.as_str()).await,
    };
    let mutation_error = match mutation {
        Ok(()) => None,
        Err(error) if ftp_session_is_unusable(&error) => Some(format_ftp_delete_error(kind, error, password)),
        Err(error) => {
            let _ = ftp.quit().await;
            return Err(format_ftp_delete_error(kind, error, password));
        }
    };

    let fallback_context = if let Some(error) = mutation_error.as_ref() {
        error.clone()
    } else {
        match ftp_entry_exists_in_session(&mut ftp, config, path).await {
            Ok(exists) => {
                let _ = ftp.quit().await;
                return finish_ftp_delete_verification(kind, exists, None);
            }
            Err(error) if ftp_session_is_unusable(&error) => {
                format!("Mutation verification failed: {}", redact_ftp_error(error, password))
            }
            Err(error) => {
                let _ = ftp.quit().await;
                return Err(format!("Mutation verification failed: {}", redact_ftp_error(error, password)));
            }
        }
    };

    drop(ftp);
    let mut fallback = open_ftp_root_session(config, password).await.map_err(|fallback_error| {
        format!("{fallback_context}; read-only verification fallback could not start: {fallback_error}")
    })?;
    let fallback_result = ftp_entry_exists_in_session(&mut fallback, config, path)
        .await
        .map_err(|error| format!("Read-only verification fallback failed: {}", redact_ftp_error(error, password)));
    let _ = fallback.quit().await;
    let exists = fallback_result?;
    finish_ftp_delete_verification(kind, exists, mutation_error)
}

fn finish_ftp_delete_verification(
    kind: FtpEntryKind,
    exists: bool,
    mutation_error: Option<String>,
) -> Result<(), String> {
    if !exists {
        return Ok(());
    }
    Err(mutation_error.unwrap_or_else(|| match kind {
        FtpEntryKind::File => "File delete could not be verified because the target still exists".to_string(),
        FtpEntryKind::Directory => "Directory delete could not be verified because the target still exists".to_string(),
    }))
}

fn format_ftp_delete_error(kind: FtpEntryKind, error: FtpError, password: Option<&str>) -> String {
    match kind {
        FtpEntryKind::File => format!("File could not be deleted: {}", redact_ftp_error(error, password)),
        FtpEntryKind::Directory => format!(
            "Directory changed, is not empty, or cannot be removed; recursive delete is unsupported: {}",
            redact_ftp_error(error, password)
        ),
    }
}

async fn create_ftp_directory_exact(
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config else {
        return Err("FTP directory helper received a non-FTP connection".to_string());
    };
    let mut ftp = open_ftp_root_session(config, password).await?;
    let create_result = ftp.mkdir(path.as_str()).await;
    let create_error = create_result
        .as_ref()
        .err()
        .map(|error| format!("Directory could not be created: {}", redact_error(error.to_string(), password)));
    if create_result.as_ref().is_err_and(ftp_session_is_unusable) {
        drop(ftp);
        return verify_created_directory_in_fresh_session(
            config,
            path,
            password,
            create_error.expect("failed create has an error"),
        )
        .await;
    }

    match ftp.cwd(path.as_str()).await {
        Ok(()) => {
            let _ = ftp.quit().await;
            Ok(())
        }
        Err(error) if ftp_session_is_unusable(&error) => {
            let context = create_error.unwrap_or_else(|| {
                format!("Directory creation could not be verified: {}", redact_ftp_error(error, password))
            });
            drop(ftp);
            verify_created_directory_in_fresh_session(config, path, password, context).await
        }
        Err(error) => {
            let _ = ftp.quit().await;
            Err(create_error.unwrap_or_else(|| {
                format!("Directory creation could not be verified: {}", redact_ftp_error(error, password))
            }))
        }
    }
}

async fn verify_created_directory_in_fresh_session(
    config: &FtpConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
    context: String,
) -> Result<(), String> {
    let mut verify = open_ftp_root_session(config, password)
        .await
        .map_err(|error| format!("{context}; read-only verification fallback could not start: {error}"))?;
    let result = match verify.cwd(path.as_str()).await {
        Ok(()) => Ok(()),
        Err(error) if ftp_error_is_file_unavailable(&error) => Err(context),
        Err(error) => {
            Err(format!("{context}; read-only verification fallback failed: {}", redact_ftp_error(error, password)))
        }
    };
    let _ = verify.quit().await;
    result
}

async fn ftp_entry_exists_in_session(
    ftp: &mut AsyncFtpStream,
    config: &FtpConnectionConfig,
    path: &RemotePath,
) -> Result<bool, FtpError> {
    let (parent, basename) = path.as_str().rsplit_once('/').unwrap_or(("", path.as_str()));
    let entries = ftp.nlst((!parent.is_empty()).then_some(parent)).await?;
    let relative = path.as_str();
    let rooted = if config.root == "/" {
        format!("/{relative}")
    } else {
        format!("{}/{relative}", config.root.trim_end_matches('/'))
    };
    Ok(entries.iter().any(|entry| {
        let entry = entry.trim_end_matches('/');
        entry == basename || entry == relative || entry == format!("/{relative}") || entry == rooted
    }))
}

async fn ftp_file_size_if_exists(ftp: &mut AsyncFtpStream, path: &RemotePath) -> Result<Option<usize>, FtpError> {
    match ftp.size(path.as_str()).await {
        Ok(size) => Ok(Some(size)),
        Err(error) if ftp_error_is_file_unavailable(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

fn ftp_error_is_file_unavailable(error: &FtpError) -> bool {
    matches!(error, FtpError::UnexpectedResponse(response) if response.status == Status::FileUnavailable)
}

fn ftp_session_is_unusable(error: &FtpError) -> bool {
    !matches!(error, FtpError::UnexpectedResponse(_))
}

fn redact_ftp_error(error: FtpError, password: Option<&str>) -> String {
    redact_error(error.to_string(), password)
}

async fn open_ftp_root_session(config: &FtpConnectionConfig, password: Option<&str>) -> Result<AsyncFtpStream, String> {
    validate_ftp_session_arguments(config, password)?;
    let (host, port) = endpoint_host_port(&config.endpoint)?;
    let addresses = resolve_addresses(&host, port).await.map_err(|error| format!("DNS stage failed: {error}"))?;
    open_ftp_root_session_with_addresses(config, password, &addresses).await.map_err(|failure| failure.message)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum FtpConnectionStage {
    Tcp,
    Authentication,
    Root,
}

#[derive(Debug)]
struct FtpSessionFailure {
    stage: FtpConnectionStage,
    message: String,
}

impl FtpSessionFailure {
    fn new(stage: FtpConnectionStage, message: String) -> Self {
        Self { stage, message }
    }
}

enum FtpSessionOpenError {
    Retryable(FtpSessionFailure),
    Definite(FtpSessionFailure),
}

fn retain_deepest_ftp_failure(current: &mut FtpSessionFailure, candidate: FtpSessionFailure) {
    if candidate.stage >= current.stage {
        *current = candidate;
    }
}

async fn open_ftp_root_session_with_addresses(
    config: &FtpConnectionConfig,
    password: Option<&str>,
    addresses: &[SocketAddr],
) -> Result<AsyncFtpStream, FtpSessionFailure> {
    let mut deepest_failure =
        FtpSessionFailure::new(FtpConnectionStage::Tcp, "TCP stage failed: No address available".to_string());
    for attempt in 0..FTP_SESSION_ATTEMPTS {
        match open_ftp_root_session_once(config, password, addresses).await {
            Ok(ftp) => return Ok(ftp),
            Err(FtpSessionOpenError::Definite(failure)) => return Err(failure),
            Err(FtpSessionOpenError::Retryable(failure)) => {
                retain_deepest_ftp_failure(&mut deepest_failure, failure);
            }
        }
        if attempt + 1 < FTP_SESSION_ATTEMPTS {
            tokio::time::sleep(FTP_SESSION_RETRY_DELAY * (attempt as u32 + 1)).await;
        }
    }
    Err(deepest_failure)
}

async fn open_ftp_root_session_once(
    config: &FtpConnectionConfig,
    password: Option<&str>,
    addresses: &[SocketAddr],
) -> Result<AsyncFtpStream, FtpSessionOpenError> {
    let mut deepest_failure =
        FtpSessionFailure::new(FtpConnectionStage::Tcp, "TCP stage failed: No address available".to_string());
    let mut connected = None;
    for address in addresses {
        match tokio::time::timeout(CONNECTION_TIMEOUT, AsyncFtpStream::connect(address)).await {
            Ok(Ok(ftp)) => {
                connected = Some(ftp);
                break;
            }
            Ok(Err(error)) => {
                let stage = match &error {
                    FtpError::ConnectionError(io_error) if io_error.kind() != std::io::ErrorKind::UnexpectedEof => {
                        FtpConnectionStage::Tcp
                    }
                    _ => FtpConnectionStage::Authentication,
                };
                let stage_name = match stage {
                    FtpConnectionStage::Tcp => "TCP",
                    FtpConnectionStage::Authentication => "Authentication",
                    FtpConnectionStage::Root => unreachable!("connect cannot reach the root stage"),
                };
                retain_deepest_ftp_failure(
                    &mut deepest_failure,
                    FtpSessionFailure::new(
                        stage,
                        format!("{stage_name} stage failed: {}", redact_ftp_error(error, password)),
                    ),
                );
            }
            Err(_) => {
                retain_deepest_ftp_failure(
                    &mut deepest_failure,
                    FtpSessionFailure::new(
                        FtpConnectionStage::Authentication,
                        "Authentication stage failed: FTP greeting timed out".to_string(),
                    ),
                );
            }
        }
    }
    let mut ftp = connected.ok_or(FtpSessionOpenError::Retryable(deepest_failure))?;
    let (username, resolved_password) = ftp_credentials(config, password);
    match tokio::time::timeout(CONNECTION_TIMEOUT, ftp.login(&username, &resolved_password)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let retryable = ftp_session_is_unusable(&error);
            let failure = FtpSessionFailure::new(
                FtpConnectionStage::Authentication,
                format!("Authentication stage failed: {}", redact_ftp_error(error, password)),
            );
            return Err(if retryable {
                FtpSessionOpenError::Retryable(failure)
            } else {
                FtpSessionOpenError::Definite(failure)
            });
        }
        Err(_) => {
            return Err(FtpSessionOpenError::Retryable(FtpSessionFailure::new(
                FtpConnectionStage::Authentication,
                "Authentication stage failed: FTP login timed out".to_string(),
            )));
        }
    }
    match tokio::time::timeout(CONNECTION_TIMEOUT, ftp.cwd(&config.root)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let retryable = ftp_session_is_unusable(&error);
            let failure = FtpSessionFailure::new(
                FtpConnectionStage::Root,
                format!("Root stage failed: {}", redact_ftp_error(error, password)),
            );
            return Err(if retryable {
                FtpSessionOpenError::Retryable(failure)
            } else {
                FtpSessionOpenError::Definite(failure)
            });
        }
        Err(_) => {
            return Err(FtpSessionOpenError::Retryable(FtpSessionFailure::new(
                FtpConnectionStage::Root,
                "Root stage failed: FTP root check timed out".to_string(),
            )));
        }
    }
    #[cfg(test)]
    FTP_SESSION_ESTABLISHMENT_COUNT.fetch_add(1, Ordering::Relaxed);
    Ok(ftp)
}

async fn test_ftp_connection(input: &FileConnectionInput, password: Option<&str>) -> FileConnectionTestResult {
    let mut stages = Vec::with_capacity(5);
    let FileConnectionConfig::Ftp(config) = &input.config else {
        return FileConnectionTestResult {
            success: false,
            stages: vec![failed_stage("configuration", "Expected an FTP configuration".to_string())],
        };
    };
    if let Err(error) = validate_input(input).and_then(|_| validate_ftp_session_arguments(config, password)) {
        stages.push(failed_stage("configuration", error));
        append_skipped_stages(&mut stages, &["dns", "tcp", "authentication", "root"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("configuration"));

    let (host, port) = endpoint_host_port(&config.endpoint).expect("validated endpoint");
    let addresses = match resolve_addresses(&host, port).await {
        Ok(addresses) => addresses,
        Err(error) => {
            stages.push(failed_stage("dns", error));
            append_skipped_stages(&mut stages, &["tcp", "authentication", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    if addresses.is_empty() {
        stages.push(failed_stage("dns", "No addresses returned".to_string()));
        append_skipped_stages(&mut stages, &["tcp", "authentication", "root"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("dns"));

    match open_ftp_root_session_with_addresses(config, password, &addresses).await {
        Ok(mut ftp) => {
            stages.push(passed_stage("tcp"));
            stages.push(passed_stage("authentication"));
            stages.push(passed_stage("root"));
            let _ = ftp.quit().await;
            FileConnectionTestResult { success: true, stages }
        }
        Err(failure) if failure.stage == FtpConnectionStage::Tcp => {
            stages.push(failed_stage("tcp", failure.message));
            append_skipped_stages(&mut stages, &["authentication", "root"]);
            FileConnectionTestResult { success: false, stages }
        }
        Err(failure) if failure.stage == FtpConnectionStage::Authentication => {
            stages.push(passed_stage("tcp"));
            stages.push(failed_stage("authentication", failure.message));
            stages.push(skipped_stage("root"));
            FileConnectionTestResult { success: false, stages }
        }
        Err(failure) if failure.stage == FtpConnectionStage::Root => {
            stages.push(passed_stage("tcp"));
            stages.push(passed_stage("authentication"));
            stages.push(failed_stage("root", failure.message));
            FileConnectionTestResult { success: false, stages }
        }
        Err(_) => unreachable!("all FTP connection stages are handled"),
    }
}

async fn test_connection_for_input(
    input: &FileConnectionInput,
    secrets: ResolvedFileSecrets,
) -> FileConnectionTestResult {
    match &input.config {
        FileConnectionConfig::Ftp(_) => test_ftp_connection(input, secrets.password.as_deref()).await,
        FileConnectionConfig::S3(config) => match validate_input(input) {
            Ok(()) => test_s3_connection(config, &secrets).await,
            Err(error) => FileConnectionTestResult {
                success: false,
                stages: std::iter::once(failed_stage("configuration", error))
                    .chain(["dns", "tcp", "authentication", "bucket", "root"].into_iter().map(skipped_stage))
                    .collect(),
            },
        },
        FileConnectionConfig::Webdav(config) => match validate_input(input) {
            Ok(()) => test_webdav_connection(config, &secrets).await,
            Err(error) => FileConnectionTestResult {
                success: false,
                stages: std::iter::once(failed_stage("configuration", error))
                    .chain(["dns", "tcp", "authentication", "root"].into_iter().map(skipped_stage))
                    .collect(),
            },
        },
    }
}

async fn resolve_addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    tokio::time::timeout(CONNECTION_TIMEOUT, lookup_host((host, port)))
        .await
        .map_err(|_| "DNS lookup timed out".to_string())?
        .map(|addresses| addresses.collect())
        .map_err(|error| error.to_string())
}

async fn verify_ftp_root_read_only(config: &FileConnectionConfig, password: Option<&str>) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config else {
        return Err("FTP root verification received a non-FTP connection".to_string());
    };
    let mut ftp = open_ftp_root_session(config, password).await?;
    let _ = ftp.quit().await;
    Ok(())
}

fn validate_input(input: &FileConnectionInput) -> Result<(), String> {
    if input.name.trim().is_empty() {
        return Err("Connection name is required".to_string());
    }
    match &input.config {
        FileConnectionConfig::Ftp(config) => {
            if input.secrets.as_ref().is_some_and(|secrets| {
                secrets.access_key_id.is_some()
                    || secrets.secret_access_key.is_some()
                    || secrets.session_token.is_some()
            }) {
                return Err("FTP connections cannot include S3 credentials".to_string());
            }
            if input.secrets.as_ref().is_some_and(|secrets| secrets.clear_s3_credentials.is_some()) {
                return Err("FTP connections cannot include clearS3Credentials".to_string());
            }
            if input
                .secrets
                .as_ref()
                .is_some_and(|secrets| secrets.webdav_token.is_some() || secrets.clear_webdav_credentials.is_some())
            {
                return Err("FTP connections cannot include WebDAV credentials".to_string());
            }
            endpoint_host_port(&config.endpoint)?;
            normalize_ftp_root(&config.root)?;
            reject_ftp_command_injection(&config.username, "FTP username")?;
            if let Some(password) = input.secrets.as_ref().and_then(|secrets| secrets.password.as_deref()) {
                reject_ftp_command_injection(password, "FTP password")?;
            }
            Ok(())
        }
        FileConnectionConfig::S3(config) => {
            if input.secrets.as_ref().is_some_and(|secrets| {
                secrets.password.is_some()
                    || secrets.clear_password.is_some()
                    || secrets.webdav_token.is_some()
                    || secrets.clear_webdav_credentials.is_some()
            }) {
                return Err("S3 connections cannot include an FTP password or clearPassword".to_string());
            }
            let access_key_id = input.secrets.as_ref().and_then(|secrets| secrets.access_key_id.as_deref());
            let secret_access_key = input.secrets.as_ref().and_then(|secrets| secrets.secret_access_key.as_deref());
            validate_s3_config(
                config,
                input.id.is_none(),
                access_key_id,
                secret_access_key,
                input.secrets.as_ref().and_then(|secrets| secrets.session_token.as_deref()),
            )
        }
        FileConnectionConfig::Webdav(config) => {
            if input.secrets.as_ref().is_some_and(|secrets| {
                secrets.access_key_id.is_some()
                    || secrets.secret_access_key.is_some()
                    || secrets.session_token.is_some()
                    || secrets.clear_s3_credentials.is_some()
                    || secrets.clear_password.is_some()
            }) {
                return Err("WebDAV connections cannot include FTP or S3 secret fields".to_string());
            }
            if let Some(secrets) = input.secrets.as_ref() {
                if secrets.clear_webdav_credentials == Some(true)
                    && (secrets.password.is_some() || secrets.webdav_token.is_some())
                {
                    return Err(
                        "clearWebdavCredentials=true cannot be combined with a WebDAV password or token".to_string()
                    );
                }
                if secrets.password.as_deref() == Some("") || secrets.webdav_token.as_deref() == Some("") {
                    return Err(
                        "WebDAV password and token fields cannot be empty; omit the field to preserve credentials or use clearWebdavCredentials=true"
                            .to_string(),
                    );
                }
                match config.authentication {
                    WebdavAuthentication::Basic
                        if secrets.webdav_token.is_some()
                            || matches!(secrets.clear_webdav_credentials, Some(false)) =>
                    {
                        return Err("WebDAV Basic authentication only accepts the password secret field".to_string());
                    }
                    WebdavAuthentication::Bearer
                        if secrets.password.is_some() || matches!(secrets.clear_webdav_credentials, Some(false)) =>
                    {
                        return Err(
                            "WebDAV bearer authentication only accepts the webdavToken secret field".to_string()
                        );
                    }
                    WebdavAuthentication::None
                        if secrets.password.is_some()
                            || secrets.webdav_token.is_some()
                            || secrets.clear_webdav_credentials != Some(true) =>
                    {
                        return Err("Anonymous WebDAV only accepts clearWebdavCredentials=true".to_string());
                    }
                    _ => {}
                }
            }
            let password = input
                .secrets
                .as_ref()
                .and_then(|secrets| secrets.password.as_deref())
                .filter(|value| !value.is_empty());
            let token = input
                .secrets
                .as_ref()
                .and_then(|secrets| secrets.webdav_token.as_deref())
                .filter(|value| !value.is_empty());
            validate_webdav_config(config, input.id.is_none(), password, token)
        }
    }
}

fn endpoint_host_port(endpoint: &str) -> Result<(String, u16), String> {
    let url = Url::parse(endpoint).map_err(|_| "FTP endpoint must be a valid ftp:// URL".to_string())?;
    if url.scheme() != "ftp" {
        return Err("Only unencrypted ftp:// endpoints are supported".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("Credentials must not be embedded in the FTP endpoint".to_string());
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err("FTP endpoint must not contain a path, query, or fragment; use the root field".to_string());
    }
    let host = url.host_str().ok_or_else(|| "FTP endpoint host is required".to_string())?;
    Ok((host.to_string(), url.port().unwrap_or(21)))
}

#[cfg(test)]
pub(super) fn build_operator(config: &FileConnectionConfig, password: Option<&str>) -> Result<Operator, String> {
    let secrets = ResolvedFileSecrets { password: password.map(ToString::to_string), ..ResolvedFileSecrets::default() };
    build_operator_with_secrets(config, &secrets)
}

fn build_operator_with_secrets(
    config: &FileConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<Operator, String> {
    match config {
        FileConnectionConfig::Ftp(config) => {
            let password = secrets.password.as_deref();
            validate_ftp_session_arguments(config, password)?;
            let (username, resolved_password) = ftp_credentials(config, password);
            let builder =
                Ftp::default().endpoint(&config.endpoint).root("/").user(&username).password(&resolved_password);
            Operator::new(builder)
                .map(|builder| builder.finish())
                .map_err(|error| redact_error(error.to_string(), password))
        }
        FileConnectionConfig::S3(config) => build_s3_operator(config, secrets),
        FileConnectionConfig::Webdav(config) => build_webdav_operator(config, secrets),
    }
}

fn normalize_input(input: &mut FileConnectionInput) -> Result<(), String> {
    match &mut input.config {
        FileConnectionConfig::Ftp(config) => {
            config.endpoint = config.endpoint.trim().trim_end_matches('/').to_string();
            config.root = normalize_ftp_root(&config.root)?;
            reject_ftp_command_injection(&config.username, "FTP username")?;
            config.username = config.username.trim().to_string();
        }
        FileConnectionConfig::S3(config) => {
            config.endpoint = config.endpoint.trim().trim_end_matches('/').to_string();
            config.region = config.region.trim().to_string();
            config.bucket = config.bucket.trim().to_string();
            config.root = normalize_s3_root(&config.root)?;
        }
        FileConnectionConfig::Webdav(config) => {
            config.endpoint = normalize_webdav_endpoint(&config.endpoint)?;
            config.root = normalize_webdav_root(&config.root)?;
            config.username = config.username.trim().to_string();
        }
    }
    Ok(())
}

fn normalize_ftp_root(root: &str) -> Result<String, String> {
    reject_ftp_command_injection(root, "FTP root")?;
    let decoded = percent_encoding::percent_decode_str(root.trim())
        .decode_utf8()
        .map_err(|_| "FTP root contains invalid percent-encoded UTF-8".to_string())?;
    if !decoded.starts_with('/') {
        return Err("FTP root must be an absolute path beginning with '/'".to_string());
    }
    if decoded.contains('\0') || decoded.contains('\\') {
        return Err("FTP root contains an invalid character".to_string());
    }
    let mut normalized = Vec::new();
    for segment in decoded.split('/').filter(|segment| !segment.is_empty()) {
        if matches!(segment, "." | "..") {
            return Err("FTP root cannot contain '.' or '..' path segments".to_string());
        }
        normalized.push(segment);
    }
    Ok(if normalized.is_empty() { "/".to_string() } else { format!("/{}", normalized.join("/")) })
}

fn configured_root_list_path(config: &FileConnectionConfig) -> String {
    match config {
        FileConnectionConfig::Ftp(config) => {
            let relative = config.root.trim_matches('/');
            if relative.is_empty() {
                "/".to_string()
            } else {
                format!("{relative}/")
            }
        }
        FileConnectionConfig::S3(_) | FileConnectionConfig::Webdav(_) => "/".to_string(),
    }
}

fn configured_directory_path(config: &FileConnectionConfig, path: &str) -> String {
    configured_entry_path(config, path, true)
}

fn configured_entry_path(config: &FileConnectionConfig, path: &str, is_directory: bool) -> String {
    let root = configured_root_list_path(config);
    let path = path.trim_matches('/');
    if path.is_empty() {
        return root;
    }
    let joined = if root == "/" { path.to_string() } else { format!("{}{path}", root.trim_start_matches('/')) };
    if is_directory {
        format!("{}/", joined.trim_end_matches('/'))
    } else {
        joined.trim_end_matches('/').to_string()
    }
}

fn normalize_relative_remote_path(path: &str, allow_root: bool) -> Result<String, String> {
    // Remote paths are opaque storage keys, not URLs. Keep the raw value for
    // backend lookup and use a decoded byte shadow only for safety checks.
    reject_ftp_command_injection(path, "Remote path")?;
    if path.trim() != path {
        return Err("Remote path cannot begin or end with whitespace".to_string());
    }
    if path.starts_with('/') {
        return Err("Remote path must be relative to the configured root".to_string());
    }
    if path.contains('\0') || path.contains('\\') {
        return Err("Remote path contains an invalid character".to_string());
    }
    if path.is_empty() {
        return if allow_root { Ok(String::new()) } else { Err("Remote path is required".to_string()) };
    }
    if path.ends_with("//") {
        return Err("Remote path cannot contain empty path segments".to_string());
    }
    let validation_path = path.strip_suffix('/').unwrap_or(path);
    for segment in validation_path.split('/') {
        if segment.is_empty() {
            return Err("Remote path cannot contain empty path segments".to_string());
        }
    }
    validate_decoded_path_shadow(path)?;
    Ok(path.to_string())
}

pub(super) fn validate_remote_relative_path(path: &str) -> Result<String, String> {
    normalize_relative_remote_path(path, false)
}

fn validate_decoded_path_shadow(path: &str) -> Result<(), String> {
    let decoded = percent_encoding::percent_decode_str(path).collect::<Vec<_>>();
    if decoded.first() == Some(&b'/') {
        return Err("Remote path must be relative to the configured root".to_string());
    }
    if decoded.contains(&b'\0') || decoded.contains(&b'\\') {
        return Err("Remote path contains an invalid character".to_string());
    }
    if decoded.split(|byte| *byte == b'/').any(|segment| segment == b"." || segment == b"..") {
        return Err("Remote path cannot contain '.' or '..' path segments".to_string());
    }
    Ok(())
}

fn list_session_binding(
    connection_id: &str,
    revision: i64,
    path: &str,
    options: NormalizedFileListOptions,
) -> ListSessionBinding {
    ListSessionBinding { connection_id: connection_id.to_string(), revision, path: path.to_string(), options }
}

fn file_entry_from_opendal(
    list_path: &str,
    entry: opendal::Entry,
    preserve_directory_suffix: bool,
) -> Result<FileEntry, String> {
    let metadata = entry.metadata();
    let kind = if metadata.mode().is_dir() {
        "directory"
    } else if metadata.mode().is_file() {
        "file"
    } else {
        return Err("Storage returned an entry with an unknown type".to_string());
    };
    let relative_path = root_relative_entry_path(list_path, entry.path())?;
    let relative_path = if kind == "directory" && !preserve_directory_suffix {
        relative_path
            .strip_suffix('/')
            .ok_or_else(|| "Storage returned a directory path without a trailing slash".to_string())?
    } else {
        relative_path.as_str()
    };
    let path = normalize_relative_remote_path(relative_path, false)?;
    let name_path = path.strip_suffix('/').unwrap_or(&path);
    let name = name_path.rsplit('/').next().unwrap_or(name_path).to_string();
    Ok(FileEntry {
        path,
        name,
        kind: kind.to_string(),
        size: if metadata.mode().is_file() { metadata.content_length() } else { 0 },
        last_modified: metadata.last_modified().map(|value| value.to_string()),
    })
}

async fn stat_remote_metadata(
    operator: &Operator,
    config: &FileConnectionConfig,
    path: &str,
    password: Option<&str>,
) -> Result<Metadata, String> {
    let mut last_error = None;
    for attempt in 0..FTP_SESSION_ATTEMPTS {
        match stat_remote_metadata_once(operator, config, path).await {
            Ok(metadata) => return Ok(metadata),
            Err(error) if should_retry_ftp_stat(&error) && attempt + 1 < FTP_SESSION_ATTEMPTS => {
                last_error = Some(error);
                tokio::time::sleep(FTP_SESSION_RETRY_DELAY * (attempt as u32 + 1)).await;
            }
            Err(error) => return Err(redact_error(error.to_string(), password)),
        }
    }
    Err(redact_error(last_error.expect("a transient stat failure is recorded before retry").to_string(), password))
}

fn should_retry_ftp_stat(error: &opendal::Error) -> bool {
    error.kind() == ErrorKind::Unexpected && error.is_temporary()
}

async fn stat_remote_metadata_once(
    operator: &Operator,
    config: &FileConnectionConfig,
    path: &str,
) -> Result<Metadata, opendal::Error> {
    let is_directory = path.is_empty() || path.ends_with('/');
    let file_path = configured_entry_path(config, path, is_directory);
    match operator.stat(&file_path).await {
        Ok(metadata) => Ok(metadata),
        Err(error)
            if is_directory && error.kind() == ErrorKind::NotFound && matches!(config, FileConnectionConfig::S3(_)) =>
        {
            stat_s3_directory_or_virtual(operator, &file_path).await
        }
        Err(error) if !path.is_empty() && !is_directory && error.kind() == ErrorKind::NotFound => {
            let directory_path = configured_entry_path(config, path, true);
            match config {
                FileConnectionConfig::S3(_) => stat_s3_directory_or_virtual(operator, &directory_path).await,
                FileConnectionConfig::Ftp(_) | FileConnectionConfig::Webdav(_) => operator.stat(&directory_path).await,
            }
        }
        Err(error) => Err(error),
    }
}

fn file_stat_from_metadata(path: &str, metadata: &Metadata) -> FileStat {
    let named_path = path.trim_end_matches('/');
    FileStat {
        path: path.to_string(),
        name: if named_path.is_empty() {
            "/".to_string()
        } else {
            named_path.rsplit('/').next().unwrap_or(named_path).to_string()
        },
        kind: if metadata.mode().is_dir() {
            "directory"
        } else if metadata.mode().is_file() {
            "file"
        } else {
            "unknown"
        }
        .to_string(),
        size: if metadata.mode().is_file() { metadata.content_length() } else { 0 },
        last_modified: metadata.last_modified().map(|value| value.to_string()),
        etag: metadata.etag().map(ToString::to_string),
        version: metadata.version().map(ToString::to_string),
        content_type: metadata.content_type().map(ToString::to_string),
        content_encoding: metadata.content_encoding().map(ToString::to_string),
        content_disposition: metadata.content_disposition().map(ToString::to_string),
        cache_control: metadata.cache_control().map(ToString::to_string),
        content_md5: metadata.content_md5().map(ToString::to_string),
        user_metadata: metadata.user_metadata().cloned().unwrap_or_default(),
    }
}

fn remote_fingerprint_from_metadata(metadata: &Metadata) -> RemoteFileFingerprint {
    RemoteFileFingerprint {
        size: metadata.content_length(),
        modified: metadata.last_modified().map(|value| value.to_string()).unwrap_or_default(),
        etag: metadata.etag().map(ToString::to_string),
        version: metadata.version().map(ToString::to_string),
    }
}

pub(super) fn password_scope(config: &FileConnectionConfig) -> Result<String, String> {
    match config {
        FileConnectionConfig::Ftp(config) => {
            let (host, port) = endpoint_host_port(&config.endpoint)?;
            reject_ftp_command_injection(&config.username, "FTP username")?;
            Ok(format!("ftp\n{}\n{port}\n{}", host.to_ascii_lowercase(), config.username))
        }
        FileConnectionConfig::S3(config) => {
            let (host, port) = endpoint_host_port_for_s3(&config.endpoint)?;
            Ok(format!("s3\n{}\n{port}\n{}\n{}", host.to_ascii_lowercase(), config.region, config.bucket))
        }
        FileConnectionConfig::Webdav(config) => {
            let (host, port) = endpoint_host_port_for_webdav(&config.endpoint)?;
            Ok(format!(
                "webdav\n{}\n{port}\n{}\n{}\n{:?}\n{}",
                host.to_ascii_lowercase(),
                config.endpoint,
                config.root,
                config.authentication,
                config.username
            ))
        }
    }
}

fn root_relative_entry_path(list_path: &str, entry_path: &str) -> Result<String, String> {
    let root = list_path.trim_matches('/');
    let candidate = entry_path.trim_start_matches('/');
    let relative = if root.is_empty() {
        candidate
    } else {
        candidate
            .strip_prefix(root)
            .and_then(|path| path.strip_prefix('/'))
            .ok_or_else(|| "FTP server returned an entry outside the configured root".to_string())?
    };
    if relative.is_empty() || relative.starts_with('/') || relative.contains('\0') || relative.contains('\\') {
        return Err("FTP server returned an invalid entry path".to_string());
    }
    validate_decoded_path_shadow(relative).map_err(|_| "FTP server returned an unsafe entry path".to_string())?;
    Ok(relative.to_string())
}

fn ftp_credentials(config: &FtpConnectionConfig, password: Option<&str>) -> (String, String) {
    if config.username.is_empty() {
        ("anonymous".to_string(), "anonymous@".to_string())
    } else {
        (config.username.clone(), password.unwrap_or_default().to_string())
    }
}

fn validate_ftp_session_arguments(config: &FtpConnectionConfig, password: Option<&str>) -> Result<(), String> {
    reject_ftp_command_injection(&config.root, "FTP root")?;
    reject_ftp_command_injection(&config.username, "FTP username")?;
    reject_ftp_command_injection(password.unwrap_or_default(), "FTP password")
}

fn parse_storage_config(record: &FileConnectionStorageRecord) -> Result<FileConnectionConfig, String> {
    serde_json::from_str(&record.config_json).map_err(|_| "Stored file connection configuration is invalid".to_string())
}

fn file_connection_from_storage(record: FileConnectionStorageRecord) -> Result<FileConnection, String> {
    let config = parse_storage_config(&record)?;
    let capabilities = file_connection_capabilities(&config);
    let has_password = record.has_secret
        && matches!(
            config,
            FileConnectionConfig::Ftp(_)
                | FileConnectionConfig::Webdav(WebdavConnectionConfig {
                    authentication: WebdavAuthentication::Basic,
                    ..
                })
        );
    Ok(FileConnection {
        id: record.id,
        name: record.name,
        config,
        revision: record.revision,
        created_at: record.created_at,
        updated_at: record.updated_at,
        has_password,
        has_credentials: record.has_secret,
        capabilities,
    })
}

fn config_kind(config: &FileConnectionConfig) -> &'static str {
    match config {
        FileConnectionConfig::Ftp(_) => "ftp",
        FileConnectionConfig::S3(_) => "s3",
        FileConnectionConfig::Webdav(_) => "webdav",
    }
}

fn file_connection_capabilities(config: &FileConnectionConfig) -> FileConnectionCapabilities {
    match config {
        FileConnectionConfig::S3(_) => s3_capabilities(),
        FileConnectionConfig::Webdav(_) => webdav_capabilities(),
        FileConnectionConfig::Ftp(_) => FileConnectionCapabilities {
            read: true,
            write: true,
            stat: true,
            list: true,
            create_directory: true,
            delete: true,
            copy: true,
            rename: true,
            server_side_copy: false,
            atomic_rename: false,
            atomic_no_clobber: false,
        },
    }
}

fn redact_error(mut message: String, password: Option<&str>) -> String {
    if let Some(password) = password.filter(|password| !password.is_empty()) {
        message = message.replace(password, "[REDACTED]");
    }
    message
}

fn redact_secrets(mut message: String, secrets: &ResolvedFileSecrets) -> String {
    for secret in [
        secrets.password.as_deref(),
        secrets.access_key_id.as_deref(),
        secrets.secret_access_key.as_deref(),
        secrets.session_token.as_deref(),
        secrets.webdav_token.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter(|value| !value.is_empty())
    {
        message = message.replace(secret, "[REDACTED]");
        let percent_encoded =
            percent_encoding::utf8_percent_encode(secret, percent_encoding::NON_ALPHANUMERIC).to_string();
        message = message.replace(&percent_encoded, "[REDACTED]");
        let form_encoded = url::form_urlencoded::byte_serialize(secret.as_bytes()).collect::<String>();
        message = message.replace(&form_encoded, "[REDACTED]");
    }
    message
}

fn passed_stage(stage: &'static str) -> ConnectionTestStage {
    ConnectionTestStage { stage, status: "passed", message: None }
}

fn failed_stage(stage: &'static str, message: String) -> ConnectionTestStage {
    ConnectionTestStage { stage, status: "failed", message: Some(message) }
}

fn skipped_stage(stage: &'static str) -> ConnectionTestStage {
    ConnectionTestStage { stage, status: "skipped", message: None }
}

fn append_skipped_stages(stages: &mut Vec<ConnectionTestStage>, remaining: &[&'static str]) {
    stages.extend(remaining.iter().map(|stage| skipped_stage(stage)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::io::AsyncWriteExt;

    fn input(endpoint: &str, root: &str) -> FileConnectionInput {
        FileConnectionInput {
            id: None,
            expected_revision: None,
            name: "FTP".to_string(),
            config: FileConnectionConfig::Ftp(FtpConnectionConfig {
                endpoint: endpoint.to_string(),
                root: root.to_string(),
                username: "demo".to_string(),
            }),
            secrets: None,
        }
    }

    fn s3_input() -> FileConnectionInput {
        FileConnectionInput {
            id: None,
            expected_revision: None,
            name: "S3".to_string(),
            config: FileConnectionConfig::S3(S3ConnectionConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: "us-east-1".to_string(),
                bucket: "dbx-test".to_string(),
                root: "/tenant/".to_string(),
                virtual_host_style: false,
                anonymous: false,
            }),
            secrets: Some(FileConnectionSecrets {
                access_key_id: Some("dbx-access".to_string()),
                secret_access_key: Some("s3cr3t/+ token".to_string()),
                session_token: Some("session/+ value".to_string()),
                ..FileConnectionSecrets::default()
            }),
        }
    }

    fn webdav_input(authentication: WebdavAuthentication) -> FileConnectionInput {
        FileConnectionInput {
            id: None,
            expected_revision: None,
            name: "WebDAV".to_string(),
            config: FileConnectionConfig::Webdav(WebdavConnectionConfig {
                endpoint: "https://dav.example.test/service".to_string(),
                root: "/tenant/".to_string(),
                authentication,
                username: if authentication == WebdavAuthentication::Basic {
                    "CaseSensitiveUser".to_string()
                } else {
                    String::new()
                },
            }),
            secrets: Some(match authentication {
                WebdavAuthentication::None => {
                    FileConnectionSecrets { clear_webdav_credentials: Some(true), ..FileConnectionSecrets::default() }
                }
                WebdavAuthentication::Basic => {
                    FileConnectionSecrets { password: Some("password".to_string()), ..FileConnectionSecrets::default() }
                }
                WebdavAuthentication::Bearer => FileConnectionSecrets {
                    webdav_token: Some("token".to_string()),
                    ..FileConnectionSecrets::default()
                },
            }),
        }
    }

    #[test]
    fn file_connection_secrets_reject_unknown_fields() {
        let secrets = serde_json::json!({
            "password": "secret",
            "credentialProvider": "environment"
        });

        assert!(serde_json::from_value::<FileConnectionSecrets>(secrets).is_err());
    }

    #[test]
    fn file_connection_configs_reject_unknown_fields_for_ftp_s3_and_webdav() {
        let ftp = serde_json::json!({
            "type": "ftp",
            "endpoint": "ftp://example.test:21",
            "root": "/",
            "username": "demo",
            "passiveMode": true
        });
        let s3 = serde_json::json!({
            "type": "s3",
            "endpoint": "http://127.0.0.1:9000",
            "region": "us-east-1",
            "bucket": "dbx-test",
            "root": "/tenant/",
            "virtualHostStyle": false,
            "anonymous": false,
            "credentialProvider": "environment"
        });
        let webdav = serde_json::json!({
            "type": "webdav",
            "endpoint": "https://dav.example.test/service",
            "root": "/",
            "authentication": "basic",
            "username": "dbx",
            "digest": true
        });

        assert!(serde_json::from_value::<FileConnectionConfig>(ftp).is_err());
        assert!(serde_json::from_value::<FileConnectionConfig>(s3).is_err());
        assert!(serde_json::from_value::<FileConnectionConfig>(webdav).is_err());
    }

    #[test]
    fn file_connection_validation_rejects_cross_protocol_secrets() {
        let mut ftp = input("ftp://example.test:21", "/");
        ftp.secrets = Some(FileConnectionSecrets {
            access_key_id: Some("access".to_string()),
            ..FileConnectionSecrets::default()
        });
        assert!(validate_input(&ftp).unwrap_err().contains("S3 credentials"));

        ftp.secrets =
            Some(FileConnectionSecrets { clear_s3_credentials: Some(false), ..FileConnectionSecrets::default() });
        assert!(validate_input(&ftp).unwrap_err().contains("clearS3Credentials"));

        let mut s3 = s3_input();
        let secrets = s3.secrets.as_mut().unwrap();
        secrets.password = Some("password".to_string());
        assert!(validate_input(&s3).unwrap_err().contains("FTP password"));

        let secrets = s3.secrets.as_mut().unwrap();
        secrets.password = None;
        secrets.clear_password = Some(false);
        assert!(validate_input(&s3).unwrap_err().contains("clearPassword"));

        let mut webdav = webdav_input(WebdavAuthentication::Basic);
        webdav.secrets.as_mut().unwrap().clear_password = Some(false);
        assert!(validate_input(&webdav).unwrap_err().contains("FTP or S3"));
        webdav.secrets.as_mut().unwrap().clear_password = None;
        webdav.secrets.as_mut().unwrap().webdav_token = Some("wrong-mode".to_string());
        assert!(validate_input(&webdav).unwrap_err().contains("only accepts"));

        let mut bearer = webdav_input(WebdavAuthentication::Bearer);
        bearer.secrets.as_mut().unwrap().clear_webdav_credentials = Some(false);
        assert!(validate_input(&bearer).unwrap_err().contains("only accepts"));
        bearer.secrets.as_mut().unwrap().clear_webdav_credentials = Some(true);
        assert!(validate_input(&bearer).unwrap_err().contains("cannot be combined"));

        let mut empty_basic = webdav_input(WebdavAuthentication::Basic);
        empty_basic.id = Some("existing-basic".to_string());
        empty_basic.secrets.as_mut().unwrap().password = Some(String::new());
        assert!(validate_input(&empty_basic).unwrap_err().contains("cannot be empty"));

        let mut empty_bearer = webdav_input(WebdavAuthentication::Bearer);
        empty_bearer.id = Some("existing-bearer".to_string());
        empty_bearer.secrets.as_mut().unwrap().webdav_token = Some(String::new());
        assert!(validate_input(&empty_bearer).unwrap_err().contains("cannot be empty"));
    }

    #[tokio::test]
    async fn anonymous_s3_edit_does_not_reuse_stored_credentials() {
        let directory = tempfile::tempdir().unwrap();
        let storage = dbx_core::storage::Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        let stored_config = s3_input().config;
        let record = storage
            .save_file_connection_with_secret_bundle(
                "s3-existing".to_string(),
                "S3".to_string(),
                "s3".to_string(),
                serde_json::to_string(&stored_config).unwrap(),
                vec![
                    ("access_key_id".to_string(), "stored-access".to_string()),
                    ("secret_access_key".to_string(), "stored-secret".to_string()),
                    ("session_token".to_string(), "stored-session".to_string()),
                ],
                vec!["access_key_id".to_string(), "secret_access_key".to_string(), "session_token".to_string()],
                "s3_scope".to_string(),
                password_scope(&stored_config).unwrap(),
                true,
                None,
            )
            .await
            .unwrap();
        let state = AppState::new(storage);
        assert_eq!(
            state.storage.load_file_connection_secret(&record.id, "access_key_id").await.unwrap().as_deref(),
            Some("stored-access")
        );

        let mut edited = s3_input();
        edited.id = Some(record.id);
        edited.expected_revision = Some(record.revision);
        edited.secrets = None;
        let FileConnectionConfig::S3(config) = &mut edited.config else { unreachable!() };
        config.anonymous = true;

        let resolved = resolve_input_secrets(&state, &edited).await.unwrap();
        assert!(resolved.access_key_id.is_none());
        assert!(resolved.secret_access_key.is_none());
        assert!(resolved.session_token.is_none());
    }

    #[test]
    fn s3_configuration_and_secrets_are_strictly_separated() {
        let mut input = s3_input();
        validate_input(&input).unwrap();
        normalize_input(&mut input).unwrap();
        let config_json = serde_json::to_string(&input.config).unwrap();
        assert!(!config_json.contains("dbx-access"));
        assert!(!config_json.contains("s3cr3t"));
        assert!(!config_json.contains("session"));

        let FileConnectionConfig::S3(config) = &mut input.config else { unreachable!() };
        config.endpoint = "http://user:password@127.0.0.1:9000".to_string();
        assert!(validate_input(&input).unwrap_err().contains("embedded"));
    }

    #[test]
    fn s3_secret_redaction_covers_raw_and_encoded_values() {
        let input = s3_input();
        let secrets = input.secrets.unwrap();
        let resolved = ResolvedFileSecrets {
            access_key_id: secrets.access_key_id,
            secret_access_key: secrets.secret_access_key,
            session_token: secrets.session_token,
            ..ResolvedFileSecrets::default()
        };
        let redacted =
            redact_secrets("dbx-access s3cr3t%2F%2B%20token session%2F%2B+value s3cr3t/+ token".to_string(), &resolved);
        assert!(!redacted.contains("dbx-access"));
        assert!(!redacted.contains("s3cr3t"));
        assert!(!redacted.contains("session%2F"));
    }

    #[test]
    fn s3_anonymous_mode_is_explicit_and_rejects_credentials() {
        let mut input = s3_input();
        let FileConnectionConfig::S3(config) = &mut input.config else { unreachable!() };
        config.anonymous = true;
        assert!(validate_input(&input).unwrap_err().contains("cannot include credentials"));
        input.secrets = None;
        assert!(validate_input(&input).is_ok());
    }

    async fn direct_ftp(input: &FileConnectionInput, password: &str) -> AsyncFtpStream {
        let FileConnectionConfig::Ftp(config) = &input.config else { unreachable!() };
        open_ftp_root_session(config, Some(password)).await.unwrap()
    }

    fn ftp_resolved(password: &str) -> ResolvedFileSecrets {
        ResolvedFileSecrets { password: Some(password.to_string()), ..ResolvedFileSecrets::default() }
    }

    async fn direct_ftp_write(ftp: &mut AsyncFtpStream, path: &str, content: &[u8]) {
        let mut stream = ftp.put_with_stream(path).await.unwrap();
        stream.write_all(content).await.unwrap();
        ftp.finalize_put_stream(stream).await.unwrap();
    }

    fn ftp_session_establishment_count() -> usize {
        FTP_SESSION_ESTABLISHMENT_COUNT.load(Ordering::Relaxed)
    }

    #[tokio::test]
    #[ignore = "requires the digest-pinned MinIO contract harness"]
    async fn fixed_s3_service_contract() {
        let endpoint = std::env::var("DBX_TEST_S3_ENDPOINT").expect("DBX_TEST_S3_ENDPOINT is required");
        let direct_endpoint = std::env::var("DBX_TEST_S3_DIRECT_ENDPOINT").unwrap_or_else(|_| endpoint.clone());
        let bucket = std::env::var("DBX_TEST_S3_BUCKET").expect("DBX_TEST_S3_BUCKET is required");
        let access_key_id = std::env::var("DBX_TEST_S3_ACCESS_KEY_ID").expect("DBX_TEST_S3_ACCESS_KEY_ID is required");
        let secret_access_key =
            std::env::var("DBX_TEST_S3_SECRET_ACCESS_KEY").expect("DBX_TEST_S3_SECRET_ACCESS_KEY is required");
        let session_token = std::env::var("DBX_TEST_S3_SESSION_TOKEN").ok();
        let region = std::env::var("DBX_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let root = std::env::var("DBX_TEST_S3_ROOT").expect("DBX_TEST_S3_ROOT is required");
        let config = FileConnectionConfig::S3(S3ConnectionConfig {
            endpoint: endpoint.clone(),
            region: region.clone(),
            bucket: bucket.clone(),
            root,
            virtual_host_style: false,
            anonymous: false,
        });
        let secrets = ResolvedFileSecrets {
            access_key_id: Some(access_key_id.clone()),
            secret_access_key: Some(secret_access_key.clone()),
            session_token: session_token.clone(),
            ..ResolvedFileSecrets::default()
        };
        let operator = build_operator_with_secrets(&config, &secrets).unwrap();

        let entries = operator.list("/").await.unwrap();
        let canonical = entries
            .into_iter()
            .filter(|entry| entry.path() != "/")
            .map(|entry| {
                let converted = file_entry_from_opendal("/", entry, true).unwrap();
                (converted.kind, converted.path)
            })
            .collect::<Vec<_>>();
        assert_eq!(canonical.iter().filter(|entry| *entry == &("file".to_string(), "a".to_string())).count(), 1);
        assert_eq!(canonical.iter().filter(|entry| *entry == &("directory".to_string(), "a/".to_string())).count(), 1);

        let directory_delete =
            delete_s3_entry(&config, &RemotePath::parse("a/").unwrap(), Some("directory"), &secrets).await.unwrap_err();
        assert!(directory_delete.contains("cannot be proven"));
        assert_eq!(operator.read("a/child.txt").await.unwrap().to_vec(), b"child-a");
        assert_eq!(operator.read("a").await.unwrap().to_vec(), b"file-a");

        let virtual_delete =
            delete_s3_entry(&config, &RemotePath::parse("virtual/").unwrap(), Some("directory"), &secrets)
                .await
                .unwrap_err();
        assert!(virtual_delete.contains("not empty"));
        assert_eq!(operator.read("virtual/child.txt").await.unwrap().to_vec(), b"virtual-child");
        let virtual_metadata = stat_remote_metadata_once(&operator, &config, "virtual/").await.unwrap();
        let virtual_stat = file_stat_from_metadata("virtual/", &virtual_metadata);
        assert_eq!(virtual_stat.kind, "directory");
        assert_eq!(virtual_stat.name, "virtual");

        let deleted =
            delete_s3_entry(&config, &RemotePath::parse("empty/").unwrap(), Some("directory"), &secrets).await.unwrap();
        assert!(matches!(deleted.outcome, FileMutationOutcome::Completed));
        assert!(!operator.exists("empty/").await.unwrap());

        let nonzero = delete_s3_entry(&config, &RemotePath::parse("nonzero/").unwrap(), Some("directory"), &secrets)
            .await
            .unwrap_err();
        assert!(nonzero.contains("contains data"));
        let nonzero_entries = operator.list_with("nonzero/").recursive(true).await.unwrap();
        let nonzero_metadata = nonzero_entries
            .iter()
            .find(|entry| entry.path() == "nonzero/")
            .expect("the rejected non-zero marker must survive")
            .metadata();
        assert_eq!(nonzero_metadata.content_length(), b"nonzero-marker".len() as u64);

        let transient = format!("contract-{}", Uuid::new_v4());
        let created_directory = format!("{transient}/created");
        create_directory_entry(&config, &RemotePath::parse(&created_directory).unwrap(), &secrets).await.unwrap();
        let created_marker = format!("{created_directory}/");
        let created_entries = operator.list_with(&created_marker).recursive(true).await.unwrap();
        assert_eq!(
            created_entries
                .iter()
                .find(|entry| entry.path() == created_marker)
                .expect("production directory creation must write an exact marker")
                .metadata()
                .content_length(),
            0
        );
        let marker_metadata = stat_remote_metadata_once(&operator, &config, &created_marker).await.unwrap();
        let marker_stat = file_stat_from_metadata(&created_marker, &marker_metadata);
        assert_eq!(marker_stat.kind, "directory");
        assert_eq!(marker_stat.name, "created");
        let created_delete =
            delete_s3_entry(&config, &RemotePath::parse(&created_marker).unwrap(), Some("directory"), &secrets)
                .await
                .unwrap();
        assert!(matches!(created_delete.outcome, FileMutationOutcome::Completed));

        let same_name = format!("{transient}/same-name");
        let same_name_marker = format!("{same_name}/");
        operator.write(&same_name, Bytes::from_static(b"same-name-file")).await.unwrap();
        write_s3_object_exact(&operator, &same_name_marker, Buffer::new(), true, &secrets).await.unwrap();
        let same_name_delete =
            delete_s3_entry(&config, &RemotePath::parse(&same_name_marker).unwrap(), Some("directory"), &secrets)
                .await
                .unwrap_err();
        assert!(same_name_delete.contains("cannot be proven"));
        let same_name_marker_entries = operator.list_with(&same_name_marker).recursive(true).await.unwrap();
        assert!(same_name_marker_entries.iter().any(|entry| entry.path() == same_name_marker));
        assert_eq!(operator.read(&same_name).await.unwrap().to_vec(), b"same-name-file");

        let versioned_file = format!("{transient}/versioned-file");
        operator.write(&versioned_file, Bytes::from_static(b"old-version")).await.unwrap();
        operator.write(&versioned_file, Bytes::from_static(b"current-version")).await.unwrap();
        delete_s3_entry(&config, &RemotePath::parse(&versioned_file).unwrap(), Some("file"), &secrets).await.unwrap();
        assert_eq!(operator.stat(&versioned_file).await.unwrap_err().kind(), ErrorKind::NotFound);

        let versioned_marker = format!("{transient}/versioned-marker/");
        write_s3_object_exact(&operator, &versioned_marker, Buffer::new(), false, &secrets).await.unwrap();
        write_s3_object_exact(&operator, &versioned_marker, Buffer::new(), false, &secrets).await.unwrap();
        delete_s3_entry(&config, &RemotePath::parse(&versioned_marker).unwrap(), Some("directory"), &secrets)
            .await
            .unwrap();
        assert_eq!(operator.stat(&versioned_marker).await.unwrap_err().kind(), ErrorKind::NotFound);

        let large_source = format!("{transient}/large-source");
        let large_copy = format!("{transient}/large-copy");
        let source = vec![7_u8; 12 * 1024 * 1024 + 17];
        let mut writer = operator.writer_with(&large_source).chunk(8 * 1024 * 1024).concurrent(1).await.unwrap();
        for chunk in source.chunks(8 * 1024 * 1024) {
            writer.write(Bytes::copy_from_slice(chunk)).await.unwrap();
        }
        writer.close().await.unwrap();
        let mut copier = operator
            .copier_with(&large_source, &large_copy)
            .if_not_exists(true)
            .source_content_length_hint(source.len() as u64)
            .chunk(8 * 1024 * 1024)
            .concurrent(1)
            .await
            .unwrap();
        while copier.next().await.unwrap().is_some() {}
        assert_eq!(operator.stat(&large_copy).await.unwrap().content_length(), source.len() as u64);
        assert_eq!(operator.read(&large_copy).await.unwrap().to_vec(), source);

        let existing = format!("{transient}/existing");
        operator.write(&existing, Bytes::from_static(b"keep")).await.unwrap();
        assert!(operator.exists(&existing).await.unwrap());
        assert_eq!(operator.read(&existing).await.unwrap().to_vec(), b"keep");

        let aborted_upload = format!("{transient}/aborted-upload");
        let mut aborted = operator.writer_with(&aborted_upload).chunk(8 * 1024 * 1024).concurrent(1).await.unwrap();
        aborted.write(Bytes::from(vec![1_u8; 8 * 1024 * 1024])).await.unwrap();
        aborted.write(Bytes::from(vec![2_u8; 8 * 1024 * 1024])).await.unwrap();
        aborted.abort().await.unwrap();
        assert!(!operator.exists(&aborted_upload).await.unwrap());

        let outside_key =
            std::env::var("DBX_TEST_S3_OUTSIDE_CANARY_KEY").expect("DBX_TEST_S3_OUTSIDE_CANARY_KEY is required");
        let bucket_canary_key =
            std::env::var("DBX_TEST_S3_BUCKET_CANARY_KEY").expect("DBX_TEST_S3_BUCKET_CANARY_KEY is required");
        let bucket_config = FileConnectionConfig::S3(S3ConnectionConfig {
            endpoint: direct_endpoint,
            region,
            bucket,
            root: "/".to_string(),
            virtual_host_style: false,
            anonymous: false,
        });
        let bucket_operator = build_operator_with_secrets(&bucket_config, &secrets).unwrap();
        assert_eq!(bucket_operator.read(&outside_key).await.unwrap().to_vec(), b"tenant-canary");
        assert_eq!(bucket_operator.read(&bucket_canary_key).await.unwrap().to_vec(), b"bucket-canary");
    }

    #[test]
    fn mismatched_owned_upload_source_is_partial_source_when_target_is_absent() {
        let resolution = resolve_upload_publish_observation(Some(7), None, 9, "publish response lost".to_string());
        assert_eq!(resolution.state, UploadPublishState::PartialSource);
        assert!(resolution.detail.contains("expected 9, actual 7"), "{}", resolution.detail);
        assert!(resolution.detail.contains("preserved"), "{}", resolution.detail);
    }

    #[test]
    fn mismatched_owned_upload_source_is_partial_source_when_target_is_present() {
        let resolution = resolve_upload_publish_observation(Some(7), Some(9), 9, "publish response lost".to_string());
        assert_eq!(resolution.state, UploadPublishState::PartialSource);
        assert!(resolution.detail.contains("expected 9, actual 7"), "{}", resolution.detail);
        assert!(resolution.detail.contains("preserved"), "{}", resolution.detail);
    }

    #[test]
    fn ftp_validation_rejects_embedded_credentials_and_non_ftp_transport() {
        assert!(validate_input(&input("ftp://demo:secret@example.test:21", "/")).unwrap_err().contains("embedded"));
        assert!(validate_input(&input("ftps://example.test:21", "/")).unwrap_err().contains("unencrypted"));
    }

    #[test]
    fn ftp_validation_keeps_endpoint_and_root_separate() {
        assert!(validate_input(&input("ftp://example.test:21/files", "/")).unwrap_err().contains("root field"));
        assert!(validate_input(&input("ftp://example.test:21", "relative")).unwrap_err().contains("absolute"));
        assert!(validate_input(&input("ftp://example.test:21", "/")).is_ok());
        assert!(validate_input(&input("ftp://example.test:21", "/safe/%2e%2e/escape"))
            .unwrap_err()
            .contains("path segments"));
        assert_eq!(normalize_ftp_root("//safe///folder/").unwrap(), "/safe/folder");
    }

    #[test]
    fn ftp_validation_rejects_raw_and_encoded_command_delimiters() {
        for injected in ["safe\r\nDELE victim", "safe\nDELE victim", "safe%0d%0aDELE%20victim", "safe%0ADELE%20victim"]
        {
            let root_input = input("ftp://example.test:21", &format!("/{injected}"));
            assert!(validate_input(&root_input).unwrap_err().contains("FTP root"));

            let mut username_input = input("ftp://example.test:21", "/");
            let FileConnectionConfig::Ftp(config) = &mut username_input.config else { unreachable!() };
            config.username = injected.to_string();
            assert!(validate_input(&username_input).unwrap_err().contains("FTP username"));

            let mut password_input = input("ftp://example.test:21", "/");
            password_input.secrets = Some(FileConnectionSecrets {
                password: Some(injected.to_string()),
                clear_password: None,
                ..FileConnectionSecrets::default()
            });
            assert!(validate_input(&password_input).unwrap_err().contains("FTP password"));
        }
    }

    #[test]
    fn multi_address_failures_keep_the_deepest_completed_stage() {
        let mut failure =
            FtpSessionFailure::new(FtpConnectionStage::Tcp, "last address refused the connection".to_string());
        retain_deepest_ftp_failure(
            &mut failure,
            FtpSessionFailure::new(
                FtpConnectionStage::Authentication,
                "an earlier address reached the FTP greeting".to_string(),
            ),
        );
        retain_deepest_ftp_failure(
            &mut failure,
            FtpSessionFailure::new(FtpConnectionStage::Tcp, "final address refused the connection".to_string()),
        );

        assert_eq!(failure.stage, FtpConnectionStage::Authentication);
        assert!(failure.message.contains("earlier address"));
    }

    #[test]
    fn ftp_stat_retry_requires_a_temporary_unexpected_error() {
        let temporary = opendal::Error::new(ErrorKind::Unexpected, "temporary protocol desync").set_temporary();
        let permanent = opendal::Error::new(ErrorKind::Unexpected, "permanent FTP error");
        let temporary_not_found = opendal::Error::new(ErrorKind::NotFound, "temporary missing entry").set_temporary();

        assert!(should_retry_ftp_stat(&temporary));
        assert!(!should_retry_ftp_stat(&permanent));
        assert!(!should_retry_ftp_stat(&temporary_not_found));
    }

    #[tokio::test]
    async fn runtime_blocks_new_work_while_deleting() {
        let runtime = FileManagerRuntime::default();
        let deleting = runtime.start_delete("ftp-1").unwrap();
        assert!(runtime.begin_operation("ftp-1").err().unwrap().contains("being deleted"));
        deleting.restore_active();
        assert_eq!(runtime.lifecycle_count(), 0);
        drop(runtime.begin_operation("ftp-1").unwrap());
        assert_eq!(runtime.lifecycle_count(), 0);
    }

    #[tokio::test]
    async fn tracer_serializes_root_lists_per_connection() {
        let runtime = FileManagerRuntime::default();
        let first = runtime.begin_operation("ftp-1").unwrap();
        let second = runtime.begin_operation("ftp-1").unwrap();
        let guard = first.entry.list_lock.try_lock().unwrap();
        assert!(second.entry.list_lock.try_lock().is_err());
        drop(guard);
        drop(first);
        drop(second);
        assert_eq!(runtime.lifecycle_count(), 0);
    }

    #[tokio::test]
    async fn mutations_are_serialized_per_connection_but_not_globally() {
        let runtime = FileManagerRuntime::default();
        let first = runtime.begin_operation("ftp-1").unwrap();
        let second = runtime.begin_operation("ftp-1").unwrap();
        let other = runtime.begin_operation("ftp-2").unwrap();
        let guard = first.entry.mutation_lock.try_lock().unwrap();
        assert!(second.entry.mutation_lock.try_lock().is_err());
        assert!(other.entry.mutation_lock.try_lock().is_ok());
        drop(guard);
        drop(first);
        drop(second);
        drop(other);
        assert_eq!(runtime.lifecycle_count(), 0);
    }

    #[tokio::test]
    async fn queued_mutation_reloads_latest_revision_after_predecessor_retires_cache() {
        let runtime = Arc::new(FileManagerRuntime::default());
        let db_path = std::env::temp_dir().join(format!("dbx-queued-mutation-{}.db", Uuid::new_v4()));
        let storage = dbx_core::storage::Storage::open(&db_path).await.unwrap();
        let config = input("ftp://revision-1.example.test:21", "/").config;
        let record = storage
            .save_file_connection(
                "ftp-1".to_string(),
                "FTP".to_string(),
                "ftp".to_string(),
                serde_json::to_string(&config).unwrap(),
                None,
                password_scope(&config).unwrap(),
                false,
                None,
            )
            .await
            .unwrap();
        runtime.operator_for(&record, &config, &ResolvedFileSecrets::default()).unwrap();
        assert_eq!(runtime.operator_count(), 1);
        let state = Arc::new(AppState::new(storage));
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let second_waiting = Arc::new(Notify::new());
        let second_acquired = Arc::new(AtomicBool::new(false));

        let first_runtime = runtime.clone();
        let first_state = state.clone();
        let first_lease = runtime.begin_operation("ftp-1").unwrap();
        let first_cancellation = first_lease.cancellation();
        let first_started_signal = first_started.clone();
        let first_release_signal = release_first.clone();
        let first = tokio::spawn(async move {
            let mutation_runtime = first_runtime.clone();
            let result = run_mutation_operation(
                &first_cancellation,
                "Mutation",
                run_locked_mutation(
                    &first_state,
                    &first_runtime,
                    &first_lease.entry.mutation_lock,
                    "ftp-1",
                    &first_cancellation,
                    move |config, _password| async move {
                        assert_eq!(mutation_runtime.operator_count(), 0);
                        let FileConnectionConfig::Ftp(config) = config else { unreachable!() };
                        assert_eq!(config.endpoint, "ftp://revision-1.example.test:21");
                        first_started_signal.notify_one();
                        first_release_signal.notified().await;
                        Ok::<_, String>(())
                    },
                ),
            )
            .await;
            drop(first_lease);
            result
        });
        first_started.notified().await;
        assert_eq!(runtime.operator_count(), 0);

        let second_runtime = runtime.clone();
        let second_state = state.clone();
        let second_lease = runtime.begin_operation("ftp-1").unwrap();
        let second_cancellation = second_lease.cancellation();
        let second_waiting_signal = second_waiting.clone();
        let second_acquired_flag = second_acquired.clone();
        let second = tokio::spawn(async move {
            let mutation_runtime = second_runtime.clone();
            let result = run_mutation_operation(&second_cancellation, "Mutation", async {
                second_waiting_signal.notify_one();
                let mutation_guard = second_lease.entry.mutation_lock.lock().await;
                run_locked_mutation_with_guard(
                    &second_state,
                    &second_runtime,
                    mutation_guard,
                    "ftp-1",
                    &second_cancellation,
                    move |config, _password| async move {
                        assert_eq!(mutation_runtime.operator_count(), 0);
                        let FileConnectionConfig::Ftp(config) = config else { unreachable!() };
                        assert_eq!(config.endpoint, "ftp://revision-2.example.test:21");
                        second_acquired_flag.store(true, Ordering::Release);
                        Ok::<_, String>(())
                    },
                )
                .await
            })
            .await;
            drop(second_lease);
            result
        });
        second_waiting.notified().await;
        assert!(!second_acquired.load(Ordering::Acquire));
        assert_eq!(runtime.operator_count(), 0);

        let revised_config = input("ftp://revision-2.example.test:21", "/").config;
        let revised = state
            .storage
            .save_file_connection(
                "ftp-1".to_string(),
                "FTP revised".to_string(),
                "ftp".to_string(),
                serde_json::to_string(&revised_config).unwrap(),
                None,
                password_scope(&revised_config).unwrap(),
                false,
                Some(record.revision),
            )
            .await
            .unwrap();
        assert_eq!(revised.revision, 2);
        release_first.notify_one();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert!(second_acquired.load(Ordering::Acquire));
        assert_eq!(runtime.operator_count(), 0);
        assert_eq!(runtime.lifecycle_count(), 0);
        drop(state);
        std::fs::remove_file(db_path).ok();
    }

    #[tokio::test]
    async fn queued_mutation_is_cancelled_when_connection_deletion_starts() {
        let runtime = Arc::new(FileManagerRuntime::default());
        let acquired = Arc::new(Notify::new());

        let first_acquired = acquired.clone();
        let first_lease = runtime.begin_operation("ftp-1").unwrap();
        let first_cancellation = first_lease.cancellation();
        let first = tokio::spawn(async move {
            let result = run_mutation_operation(&first_cancellation, "Mutation", async {
                let _guard = first_lease.entry.mutation_lock.lock().await;
                first_acquired.notify_one();
                std::future::pending::<Result<(), String>>().await
            })
            .await;
            drop(first_lease);
            result
        });
        acquired.notified().await;

        let second_lease = runtime.begin_operation("ftp-1").unwrap();
        let second_cancellation = second_lease.cancellation();
        let second = tokio::spawn(async move {
            let result = run_mutation_operation(&second_cancellation, "Mutation", async {
                let _guard = second_lease.entry.mutation_lock.lock().await;
                second_cancellation.ensure_active()?;
                std::future::pending::<Result<(), String>>().await
            })
            .await;
            drop(second_lease);
            result
        });
        tokio::task::yield_now().await;

        let deleting = runtime.start_delete("ftp-1").unwrap();
        tokio::time::timeout(Duration::from_secs(1), deleting.wait_for_idle()).await.unwrap().unwrap();
        assert!(first.await.unwrap().unwrap_err().contains("being deleted"));
        assert!(second.await.unwrap().unwrap_err().contains("being deleted"));
        deleting.finish();
        assert_eq!(runtime.lifecycle_count(), 0);
    }

    #[tokio::test]
    async fn deletion_cancels_a_hanging_list_and_cleans_lifecycle_state() {
        let runtime = FileManagerRuntime::default();
        let lease = runtime.begin_operation("ftp-1").unwrap();
        let cancellation = lease.cancellation();
        let hanging = tokio::spawn(async move {
            let result = run_with_deadline_and_cancellation(
                &cancellation,
                Duration::from_secs(60),
                std::future::pending::<Result<(), String>>(),
            )
            .await;
            drop(lease);
            result
        });
        let deleting = runtime.start_delete("ftp-1").unwrap();
        deleting.wait_for_idle().await.unwrap();
        assert!(hanging.await.unwrap().unwrap_err().contains("being deleted"));
        deleting.finish();
        assert_eq!(runtime.lifecycle_count(), 0);
    }

    #[tokio::test]
    async fn list_deadline_is_bounded_and_invalid_ids_do_not_accumulate() {
        let runtime = FileManagerRuntime::default();
        let lease = runtime.begin_operation("missing").unwrap();
        let config = input("ftp://127.0.0.1:21", "/").config;
        let record = FileConnectionStorageRecord {
            id: "missing".to_string(),
            name: "Missing".to_string(),
            kind: "ftp".to_string(),
            config_json: serde_json::to_string(&config).unwrap(),
            revision: 7,
            created_at: String::new(),
            updated_at: String::new(),
            has_secret: false,
        };
        runtime.operator_for(&record, &config, &ResolvedFileSecrets::default()).unwrap();
        assert_eq!(runtime.operator_count(), 1);
        let cancellation = lease.cancellation();
        let error = run_list_operation(
            &runtime,
            "missing",
            7,
            &cancellation,
            Duration::from_millis(10),
            std::future::pending::<Result<(), String>>(),
        )
        .await
        .unwrap_err();
        assert!(error.contains("timed out"));
        assert_eq!(runtime.operator_count(), 0);
        drop(lease);
        assert_eq!(runtime.lifecycle_count(), 0);
    }

    #[test]
    fn ftp_entry_paths_are_relative_to_the_configured_root() {
        assert_eq!(root_relative_entry_path("ftp/dbx/", "ftp/dbx/fixture.txt").unwrap(), "fixture.txt");
        assert_eq!(root_relative_entry_path("/", "fixture.txt").unwrap(), "fixture.txt");
        assert!(root_relative_entry_path("ftp/dbx/", "ftp/other/fixture.txt").is_err());
        assert!(root_relative_entry_path("ftp/dbx/", "ftp/dbx/%2e%2e/escape").is_err());
        assert!(root_relative_entry_path("ftp/dbx/", "ftp/dbx/safe%5cescape").is_err());
        assert!(root_relative_entry_path("ftp/dbx/", "ftp/dbx/safe%00escape").is_err());
    }

    #[test]
    fn remote_paths_reject_opendal_trim_ambiguity_but_preserve_inner_spaces() {
        assert!(normalize_relative_remote_path(" leading/trailing", false).is_err());
        assert!(normalize_relative_remote_path("leading/trailing ", false).is_err());
        assert_eq!(normalize_relative_remote_path("folder /file name.txt", false).unwrap(), "folder /file name.txt");
        assert_eq!(normalize_relative_remote_path("folder/file%20name.txt", false).unwrap(), "folder/file%20name.txt");
        assert_eq!(normalize_relative_remote_path("%20leading", false).unwrap(), "%20leading");
        assert_eq!(normalize_relative_remote_path("trailing%20", false).unwrap(), "trailing%20");
        assert_eq!(normalize_relative_remote_path("a%2Fb", false).unwrap(), "a%2Fb");
        assert!(normalize_relative_remote_path("%2Fabsolute", false).is_err());
        assert!(normalize_relative_remote_path("safe/%2e%2e/escape", false).is_err());
        assert!(normalize_relative_remote_path("safe//file", false).is_err());
        assert_eq!(normalize_relative_remote_path("folder/", false).unwrap(), "folder/");
        assert!(normalize_relative_remote_path(" folder /../escape", false).is_err());
        assert!(normalize_relative_remote_path("safe\r\nDELE victim", false).is_err());
        assert!(normalize_relative_remote_path("safe%0d%0aDELE%20victim", false).is_err());
    }

    #[test]
    fn transfer_paths_cannot_escape_the_configured_root() {
        assert_eq!(validate_remote_relative_path("reports/2026.csv").unwrap(), "reports/2026.csv");
        assert_eq!(validate_remote_relative_path("a%20b").unwrap(), "a%20b");
        assert_eq!(validate_remote_relative_path("a%2Fb").unwrap(), "a%2Fb");
        assert_eq!(validate_remote_relative_path("literal%FFname").unwrap(), "literal%FFname");
        assert!(validate_remote_relative_path(" reports/final.csv ").unwrap_err().contains("whitespace"));
        for path in ["/absolute", "../escape", "safe/../escape", "safe\\escape", "safe/%2e%2e/escape", "safe//file"] {
            assert!(validate_remote_relative_path(path).is_err(), "{path} should be rejected");
        }
    }

    #[tokio::test]
    #[ignore = "run through tests/ftp-contract.sh with a pinned FTP image"]
    async fn fixed_ftp_service_contract() {
        let endpoint = std::env::var("DBX_TEST_FTP_ENDPOINT").expect("DBX_TEST_FTP_ENDPOINT is required");
        let username = std::env::var("DBX_TEST_FTP_USERNAME").unwrap_or_else(|_| "dbx".to_string());
        let password = std::env::var("DBX_TEST_FTP_PASSWORD").unwrap_or_else(|_| "dbx-password".to_string());
        let input = FileConnectionInput {
            id: None,
            expected_revision: None,
            name: "FTP contract".to_string(),
            config: FileConnectionConfig::Ftp(FtpConnectionConfig { endpoint, root: "/ftp/dbx".to_string(), username }),
            secrets: Some(FileConnectionSecrets {
                password: Some(password.clone()),
                clear_password: None,
                ..FileConnectionSecrets::default()
            }),
        };

        let result = test_ftp_connection(&input, Some(&password)).await;
        assert!(result.success, "stages: {}", serde_json::to_string(&result.stages).unwrap());

        let db_path = std::env::temp_dir().join(format!("dbx-ftp-contract-{}.db", Uuid::new_v4()));
        let storage = dbx_core::storage::Storage::open(&db_path).await.unwrap();
        let config_json = serde_json::to_string(&input.config).unwrap();
        let created = storage
            .save_file_connection(
                "ftp-contract".to_string(),
                input.name.clone(),
                "ftp".to_string(),
                config_json.clone(),
                Some(password.clone()),
                password_scope(&input.config).unwrap(),
                true,
                None,
            )
            .await
            .unwrap();
        assert_eq!(created.revision, 1);
        assert_eq!(storage.list_file_connections().await.unwrap().len(), 1);
        let updated = storage
            .save_file_connection(
                created.id.clone(),
                "FTP contract edited".to_string(),
                "ftp".to_string(),
                config_json,
                None,
                password_scope(&input.config).unwrap(),
                false,
                Some(created.revision),
            )
            .await
            .unwrap();
        assert_eq!(updated.revision, 2);

        let mut operator = build_operator(&input.config, Some(&password)).unwrap();
        let list_path = configured_root_list_path(&input.config);
        let entries = operator.list(&list_path).await.unwrap();
        let paths =
            entries.iter().map(|entry| root_relative_entry_path(&list_path, entry.path()).unwrap()).collect::<Vec<_>>();
        assert!(paths.iter().any(|path| path == "fixture.txt"), "entries: {paths:?}");

        let lister = operator.lister_with(&list_path).limit(50).await.unwrap();
        let list_root = list_path.clone();
        let stream = lister.map(move |result| {
            result
                .map_err(|error| error.to_string())
                .and_then(|entry| file_entry_from_opendal(&list_root, entry, false))
        });
        let registry = ListSessionRegistry::default();
        let binding = list_session_binding(
            "ftp-contract",
            updated.revision,
            "",
            FileListOptions { page_size: Some(50) }.normalize().unwrap(),
        );
        let mut page = registry.open(binding.clone(), registry.generation("ftp-contract"), stream).await.unwrap();
        let mut paged_paths = page.entries.into_iter().map(|entry| entry.path).collect::<Vec<_>>();
        while let Some(cursor) = page.cursor {
            page = registry.next(&cursor, &binding).await.unwrap();
            assert!(page.entries.len() <= 50);
            paged_paths.extend(page.entries.iter().map(|entry| entry.path.clone()));
        }
        assert_eq!(paged_paths.len(), 211);
        let unique_paths = paged_paths.iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(unique_paths.len(), paged_paths.len());
        assert!(paged_paths.iter().any(|path| path == "nested"));
        assert!(paged_paths.iter().any(|path| path == "a%20b"));
        assert!(paged_paths.iter().any(|path| path == "a b"));
        assert!(paged_paths.iter().any(|path| path == "a%2Fb"));
        assert!(paged_paths.iter().any(|path| path == "a"));

        let metadata = stat_remote_metadata(&operator, &input.config, "a%20b", Some(&password)).await.unwrap();
        assert_eq!(metadata.content_length(), b"literal-percent-space\n".len() as u64);
        let metadata = stat_remote_metadata(&operator, &input.config, "a b", Some(&password)).await.unwrap();
        assert_eq!(metadata.content_length(), b"actual-space\n".len() as u64);
        let metadata = stat_remote_metadata(&operator, &input.config, "a%2Fb", Some(&password)).await.unwrap();
        assert_eq!(metadata.content_length(), b"literal-percent-slash\n".len() as u64);
        let metadata = stat_remote_metadata(&operator, &input.config, "a/b", Some(&password)).await.unwrap();
        assert_eq!(metadata.content_length(), b"nested-slash\n".len() as u64);

        let metadata = stat_remote_metadata(&operator, &input.config, "fixture.txt", Some(&password)).await.unwrap();
        let stat = file_stat_from_metadata("fixture.txt", &metadata);
        assert_eq!(stat.kind, "file");
        assert_eq!(stat.name, "fixture.txt");
        assert!(stat.size > 0);

        let metadata = stat_remote_metadata(&operator, &input.config, "nested", Some(&password)).await.unwrap();
        let stat = file_stat_from_metadata("nested", &metadata);
        assert_eq!(stat.kind, "directory");
        assert_eq!(stat.name, "nested");
        assert_eq!(stat.size, 0);

        let metadata = stat_remote_metadata(&operator, &input.config, "", Some(&password)).await.unwrap();
        let stat = file_stat_from_metadata("", &metadata);
        assert_eq!(stat.kind, "directory");
        assert_eq!(stat.name, "/");
        assert_eq!(stat.size, 0);
        drop(operator);

        let injection_victim = "/ftp/dbx/ticket-4-injection-victim";
        let mut direct = direct_ftp(&input, &password).await;
        direct_ftp_write(&mut direct, injection_victim, b"must survive").await;
        direct.quit().await.unwrap();

        for injected_path in
            ["ticket-4-safe\r\nDELE ticket-4-injection-victim", "ticket-4-safe%0d%0aDELE%20ticket-4-injection-victim"]
        {
            assert!(RemotePath::parse(injected_path).unwrap_err().contains("CR or LF"));
        }

        let FileConnectionConfig::Ftp(base_config) = &input.config else { unreachable!() };
        for injected_root in [
            "/ftp/dbx\r\nDELE /ftp/dbx/ticket-4-injection-victim",
            "/ftp/dbx%0d%0aDELE%20/ftp/dbx/ticket-4-injection-victim",
        ] {
            let mut injected_config = base_config.clone();
            injected_config.root = injected_root.to_string();
            let error = match open_ftp_root_session(&injected_config, Some(&password)).await {
                Ok(mut ftp) => {
                    let _ = ftp.quit().await;
                    panic!("injected FTP root reached the protocol session");
                }
                Err(error) => error,
            };
            assert!(error.contains("FTP root") && error.contains("CR or LF"), "{error}");
        }

        direct = direct_ftp(&input, &password).await;
        assert_eq!(direct.size(injection_victim).await.unwrap(), b"must survive".len());
        direct.rm(injection_victim).await.unwrap();
        direct.quit().await.unwrap();

        let empty_directory = RemotePath::parse("ticket-4-empty").unwrap();
        let sessions_before_create = ftp_session_establishment_count();
        create_ftp_directory_exact(&input.config, &empty_directory, Some(&password)).await.unwrap();
        assert_eq!(
            ftp_session_establishment_count() - sessions_before_create,
            1,
            "normal directory create must use one control session"
        );
        let sessions_before_directory_delete = ftp_session_establishment_count();
        assert!(matches!(
            delete_entry(&input.config, &empty_directory, None, &ftp_resolved(&password)).await.unwrap().outcome,
            FileMutationOutcome::Completed
        ));
        assert_eq!(
            ftp_session_establishment_count() - sessions_before_directory_delete,
            1,
            "normal directory delete must use one control session"
        );

        let removable_file = RemotePath::parse("ticket-4-file.txt").unwrap();
        direct = direct_ftp(&input, &password).await;
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-file.txt", b"delete me").await;
        direct.quit().await.unwrap();
        let sessions_before_file_delete = ftp_session_establishment_count();
        assert!(matches!(
            delete_entry(&input.config, &removable_file, None, &ftp_resolved(&password)).await.unwrap().outcome,
            FileMutationOutcome::Completed
        ));
        assert_eq!(
            ftp_session_establishment_count() - sessions_before_file_delete,
            1,
            "normal file delete must use one control session"
        );
        operator = build_operator(&input.config, Some(&password)).unwrap();
        assert!(stat_remote_metadata(&operator, &input.config, removable_file.as_str(), Some(&password))
            .await
            .is_err());

        for (literal_name, decoded_name) in
            [("ticket-4%20literal-dir", "ticket-4 literal-dir"), ("ticket-4%2Fliteral-dir", "ticket-4/literal-dir")]
        {
            let literal_directory = RemotePath::parse(literal_name).unwrap();
            drop(operator);
            create_ftp_directory_exact(&input.config, &literal_directory, Some(&password)).await.unwrap();
            operator = build_operator(&input.config, Some(&password)).unwrap();
            assert!(stat_remote_metadata(&operator, &input.config, literal_name, Some(&password))
                .await
                .unwrap()
                .mode()
                .is_dir());
            drop(operator);
            direct = direct_ftp(&input, &password).await;
            direct.cwd(format!("/ftp/dbx/{literal_name}")).await.unwrap();
            direct.cwd("/ftp/dbx").await.unwrap();
            assert!(direct.cwd(format!("/ftp/dbx/{decoded_name}")).await.is_err());
            let _ = direct.quit().await;
            assert!(matches!(
                delete_entry(&input.config, &literal_directory, None, &ftp_resolved(&password)).await.unwrap().outcome,
                FileMutationOutcome::Completed
            ));
            direct = direct_ftp(&input, &password).await;
            assert!(direct.cwd(format!("/ftp/dbx/{literal_name}")).await.is_err());
            let _ = direct.quit().await;
            direct = direct_ftp(&input, &password).await;
            operator = build_operator(&input.config, Some(&password)).unwrap();
        }

        let percent_space_file = RemotePath::parse("ticket-4%20literal-file").unwrap();
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4%20literal-file", b"literal").await;
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4 literal-file", b"decoded-target").await;
        assert_eq!(
            stat_remote_metadata(&operator, &input.config, percent_space_file.as_str(), Some(&password))
                .await
                .unwrap()
                .content_length(),
            7
        );
        drop(operator);
        direct.quit().await.unwrap();
        delete_entry(&input.config, &percent_space_file, None, &ftp_resolved(&password)).await.unwrap();
        direct = direct_ftp(&input, &password).await;
        assert!(direct.size("/ftp/dbx/ticket-4%20literal-file").await.is_err());
        let _ = direct.quit().await;
        direct = direct_ftp(&input, &password).await;
        assert_eq!(direct.size("/ftp/dbx/ticket-4 literal-file").await.unwrap(), 14);
        direct.rm("/ftp/dbx/ticket-4 literal-file").await.unwrap();
        direct.quit().await.unwrap();
        operator = build_operator(&input.config, Some(&password)).unwrap();

        direct = direct_ftp(&input, &password).await;
        direct.mkdir("/ftp/dbx/ticket-4-decoded").await.unwrap();
        let percent_slash_file = RemotePath::parse("ticket-4-decoded%2Fliteral-file").unwrap();
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-decoded%2Fliteral-file", b"raw").await;
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-decoded/literal-file", b"decoded-target").await;
        assert_eq!(
            stat_remote_metadata(&operator, &input.config, percent_slash_file.as_str(), Some(&password))
                .await
                .unwrap()
                .content_length(),
            3
        );
        drop(operator);
        direct.quit().await.unwrap();
        delete_entry(&input.config, &percent_slash_file, None, &ftp_resolved(&password)).await.unwrap();
        direct = direct_ftp(&input, &password).await;
        assert!(direct.size("/ftp/dbx/ticket-4-decoded%2Fliteral-file").await.is_err());
        let _ = direct.quit().await;
        direct = direct_ftp(&input, &password).await;
        assert_eq!(direct.size("/ftp/dbx/ticket-4-decoded/literal-file").await.unwrap(), 14);
        direct.rm("/ftp/dbx/ticket-4-decoded/literal-file").await.unwrap();
        direct.rmdir("/ftp/dbx/ticket-4-decoded").await.unwrap();
        direct.quit().await.unwrap();

        let nonempty_directory = RemotePath::parse("ticket-4-nonempty").unwrap();
        create_ftp_directory_exact(&input.config, &nonempty_directory, Some(&password)).await.unwrap();
        direct = direct_ftp(&input, &password).await;
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-nonempty/child.txt", b"keep me").await;
        direct.quit().await.unwrap();
        assert!(delete_entry(&input.config, &nonempty_directory, None, &ftp_resolved(&password))
            .await
            .unwrap_err()
            .contains("not empty"));
        operator = build_operator(&input.config, Some(&password)).unwrap();
        stat_remote_metadata(&operator, &input.config, "ticket-4-nonempty/child.txt", Some(&password)).await.unwrap();

        // Model a remote writer racing the preflight. The exact FTP RMD must
        // fail once the child exists; no recursive fallback is attempted.
        let raced_directory = RemotePath::parse("ticket-4-raced").unwrap();
        drop(operator);
        create_ftp_directory_exact(&input.config, &raced_directory, Some(&password)).await.unwrap();
        let FileConnectionConfig::Ftp(ftp_config) = &input.config else { unreachable!() };
        let mut preflight = direct_ftp(&input, &password).await;
        assert!(matches!(
            prepare_ftp_delete_in_session(&mut preflight, ftp_config, &raced_directory, Some(&password)).await.unwrap(),
            FtpEntryKind::Directory
        ));
        preflight.quit().await.unwrap();
        direct = direct_ftp(&input, &password).await;
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-raced/concurrent.txt", b"created after preflight").await;
        direct.quit().await.unwrap();
        let raced_delete_error =
            delete_ftp_directory_exact(&input.config, &raced_directory, Some(&password)).await.unwrap_err();
        assert!(raced_delete_error.contains("recursive delete is unsupported"), "{raced_delete_error}");
        operator = build_operator(&input.config, Some(&password)).unwrap();
        stat_remote_metadata(&operator, &input.config, "ticket-4-raced/concurrent.txt", Some(&password)).await.unwrap();

        for unsafe_path in ["/ftp/dbx/fixture.txt", "../fixture.txt", "%2e%2e/fixture.txt", "safe%2f..%2ffixture.txt"] {
            assert!(RemotePath::parse(unsafe_path).is_err());
        }
        stat_remote_metadata(&operator, &input.config, "fixture.txt", Some(&password)).await.unwrap();

        direct = direct_ftp(&input, &password).await;
        direct_ftp_write(&mut direct, "/ftp/dbx/whitespace-proof", b"x").await;
        direct_ftp_write(&mut direct, "/ftp/dbx/whitespace-proof ", b"yy").await;
        assert_eq!(direct.size("/ftp/dbx/whitespace-proof").await.unwrap(), 1);
        assert_eq!(direct.size("/ftp/dbx/whitespace-proof ").await.unwrap(), 2);
        assert_eq!(
            stat_remote_metadata(&operator, &input.config, "whitespace-proof ", Some(&password))
                .await
                .unwrap()
                .content_length(),
            1
        );
        assert!(RemotePath::parse("whitespace-proof ").unwrap_err().contains("storage runtime"));
        direct.quit().await.unwrap();
        drop(operator);

        delete_ftp_file_exact(
            &input.config,
            &RemotePath::parse("ticket-4-nonempty/child.txt").unwrap(),
            Some(&password),
        )
        .await
        .unwrap();
        delete_ftp_directory_exact(&input.config, &nonempty_directory, Some(&password)).await.unwrap();
        delete_ftp_file_exact(
            &input.config,
            &RemotePath::parse("ticket-4-raced/concurrent.txt").unwrap(),
            Some(&password),
        )
        .await
        .unwrap();
        delete_ftp_directory_exact(&input.config, &raced_directory, Some(&password)).await.unwrap();
        delete_ftp_file_exact(&input.config, &RemotePath::parse("whitespace-proof").unwrap(), Some(&password))
            .await
            .unwrap();
        direct = direct_ftp(&input, &password).await;
        direct.rm("/ftp/dbx/whitespace-proof ").await.unwrap();
        direct.quit().await.unwrap();

        let missing_config = FileConnectionConfig::Ftp(FtpConnectionConfig {
            endpoint: match &input.config {
                FileConnectionConfig::Ftp(config) => config.endpoint.clone(),
                FileConnectionConfig::S3(_) | FileConnectionConfig::Webdav(_) => unreachable!(),
            },
            root: "/ftp/dbx/must-not-be-created".to_string(),
            username: match &input.config {
                FileConnectionConfig::Ftp(config) => config.username.clone(),
                FileConnectionConfig::S3(_) | FileConnectionConfig::Webdav(_) => unreachable!(),
            },
        });
        direct = direct_ftp(&input, &password).await;
        let before = direct.nlst(None).await.unwrap();
        direct.quit().await.unwrap();
        assert!(verify_ftp_root_read_only(&missing_config, Some(&password)).await.is_err());
        direct = direct_ftp(&input, &password).await;
        let after = direct.nlst(None).await.unwrap();
        direct.quit().await.unwrap();
        assert_eq!(after, before);
        assert!(!after.iter().any(|path| path.contains("must-not-be-created")));

        assert!(storage.delete_file_connection(&created.id).await.unwrap());
        assert!(storage.list_file_connections().await.unwrap().is_empty());
        drop(storage);
        std::fs::remove_file(db_path).ok();
    }
}
