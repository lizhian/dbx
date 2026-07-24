use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use super::file_manager_paths::{reject_ftp_command_injection, reject_recursive_delete, RemotePath};
use dbx_core::connection::AppState;
use dbx_core::storage::FileConnectionStorageRecord;
use futures::StreamExt;
use opendal::services::Ftp;
use opendal::{ErrorKind, Metadata, Operator};
use serde::{Deserialize, Serialize};
use suppaftp::tokio::AsyncFtpStream;
use suppaftp::{FtpError, Status};
use tauri::State;
use tokio::net::lookup_host;
use tokio::sync::{Mutex as AsyncMutex, Notify};
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
    mutation_lock: AsyncMutex<()>,
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
    _lease: OperationLease,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FileConnectionConfig {
    Ftp(FtpConnectionConfig),
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FtpConnectionConfig {
    pub endpoint: String,
    pub root: String,
    #[serde(default)]
    pub username: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileConnectionSecrets {
    pub password: Option<String>,
    #[serde(default)]
    pub clear_password: bool,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    let password = input.secrets.as_ref().and_then(|secrets| secrets.password.clone());
    let replace_secret = password.is_some() || input.secrets.as_ref().is_some_and(|secrets| secrets.clear_password);
    let password_scope = password_scope(&input.config)?;
    let cancellation = lease.cancellation();
    let record = run_mutation_operation(&cancellation, "Save connection", async {
        let _mutation_guard = lease.entry.mutation_lock.lock().await;
        cancellation.ensure_active()?;
        let record = state
            .storage
            .save_file_connection(
                id.clone(),
                input.name.trim().to_string(),
                config_kind(&input.config).to_string(),
                config_json,
                password,
                password_scope,
                replace_secret,
                input.expected_revision,
            )
            .await?;
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
        return Ok(test_ftp_connection(&input, None).await);
    }
    normalize_input(&mut input)?;
    let password = resolve_input_password(&state, &input).await?;
    match lease {
        Some(lease) => {
            let cancellation = lease.cancellation();
            tokio::select! {
                result = test_ftp_connection(&input, password.as_deref()) => Ok(result),
                _ = cancellation.cancelled() => Err("File connection is being deleted".to_string()),
            }
        }
        None => Ok(test_ftp_connection(&input, password.as_deref()).await),
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
        let scope = password_scope(&config)?;
        let password = state.storage.load_file_connection_password(&connection_id, &scope).await?;
        let operator = runtime.operator_for(&record, &config, password.as_deref())?;
        if path.is_empty() {
            verify_ftp_root_read_only(&config, password.as_deref()).await?;
        }
        let list_path = configured_directory_path(&config, &path);
        let lister = operator
            .lister_with(&list_path)
            .limit(options.page_size)
            .await
            .map_err(|error| redact_error(error.to_string(), password.as_deref()))?;
        let error_password = password.clone();
        let configured_root = configured_root_list_path(&config);
        let stream = lister.map(move |result| {
            result
                .map_err(|error| redact_error(error.to_string(), error_password.as_deref()))
                .and_then(|entry| file_entry_from_opendal(&configured_root, entry))
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
        let scope = password_scope(&config)?;
        let password = state.storage.load_file_connection_password(&connection_id, &scope).await?;
        let operator = runtime.operator_for(&record, &config, password.as_deref())?;
        let metadata = stat_remote_metadata(&operator, &config, &path, password.as_deref()).await?;
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
            move |config, password| async move {
                create_ftp_directory_exact(&config, &path, password.as_deref()).await?;
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
            move |config, password| async move { delete_entry(&config, &path, password.as_deref()).await },
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
        let scope = password_scope(&config)?;
        let password = state.storage.load_file_connection_password(connection_id, &scope).await?;
        let operator = self.operator_for(&record, &config, password.as_deref())?;
        let remote_path = configured_entry_path(&config, &relative_path, false);
        Ok(PreparedFileOperation { operator, revision, remote_path, cancellation: lease.cancellation(), _lease: lease })
    }

    fn operator_for(
        &self,
        record: &FileConnectionStorageRecord,
        config: &FileConnectionConfig,
        password: Option<&str>,
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

        let operator = build_operator(config, password)?;
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
            mutation_lock: AsyncMutex::new(()),
        }
    }
}

impl Drop for CachedOperatorRetirement<'_> {
    fn drop(&mut self) {
        self.runtime.evict_revision(self.connection_id, self.revision);
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

async fn resolve_input_password(state: &AppState, input: &FileConnectionInput) -> Result<Option<String>, String> {
    if input.secrets.as_ref().is_some_and(|secrets| secrets.clear_password) {
        return Ok(None);
    }
    if let Some(password) = input.secrets.as_ref().and_then(|secrets| secrets.password.clone()) {
        return Ok(Some(password));
    }
    match input.id.as_deref() {
        Some(id) => {
            let record =
                state.storage.load_file_connection(id).await?.ok_or_else(|| "File connection not found".to_string())?;
            if input.expected_revision != Some(record.revision) {
                return Err("Saved password cannot be reused after the connection revision changed".to_string());
            }
            let stored_config = parse_storage_config(&record)?;
            let input_scope = password_scope(&input.config)?;
            if input_scope != password_scope(&stored_config)? {
                return Err("Re-enter or clear the password after changing the FTP endpoint or username".to_string());
            }
            state.storage.load_file_connection_password(id, &input_scope).await
        }
        None => Ok(None),
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
    Mutate: FnOnce(FileConnectionConfig, Option<String>) -> Mutation,
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
    Mutate: FnOnce(FileConnectionConfig, Option<String>) -> Mutation,
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
    let scope = password_scope(&config)?;
    let password = state.storage.load_file_connection_password(connection_id, &scope).await?;
    // Declared after the lock guard so every return/cancellation path evicts
    // the cached operator before the per-connection lock is released.
    let _retirement = CachedOperatorRetirement { runtime, connection_id, revision };
    // FTP mutations use exact, short-lived protocol sessions and never share
    // OpenDAL's pooled browsing connections.
    runtime.evict_revision(connection_id, revision);
    mutate(config, password).await
}

async fn delete_entry(
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<FileMutationResult, String> {
    let FileConnectionConfig::Ftp(ftp_config) = config;
    let mut ftp = open_ftp_root_session(ftp_config, password).await?;
    let kind = prepare_ftp_delete_in_session(&mut ftp, ftp_config, path, password).await?;
    delete_ftp_entry_in_session(ftp, ftp_config, path, kind, password).await?;
    Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
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
    let FileConnectionConfig::Ftp(config) = config;
    let ftp = open_ftp_root_session(config, password).await?;
    delete_ftp_entry_in_session(ftp, config, path, FtpEntryKind::Directory, password).await
}

#[cfg(test)]
async fn delete_ftp_file_exact(
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config;
    let ftp = open_ftp_root_session(config, password).await?;
    delete_ftp_entry_in_session(ftp, config, path, FtpEntryKind::File, password).await
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
    let FileConnectionConfig::Ftp(config) = config;
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
    let FileConnectionConfig::Ftp(config) = &input.config;
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

async fn resolve_addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    tokio::time::timeout(CONNECTION_TIMEOUT, lookup_host((host, port)))
        .await
        .map_err(|_| "DNS lookup timed out".to_string())?
        .map(|addresses| addresses.collect())
        .map_err(|error| error.to_string())
}

async fn verify_ftp_root_read_only(config: &FileConnectionConfig, password: Option<&str>) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config;
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
            endpoint_host_port(&config.endpoint)?;
            normalize_ftp_root(&config.root)?;
            reject_ftp_command_injection(&config.username, "FTP username")?;
            if let Some(password) = input.secrets.as_ref().and_then(|secrets| secrets.password.as_deref()) {
                reject_ftp_command_injection(password, "FTP password")?;
            }
            Ok(())
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

pub(super) fn build_operator(config: &FileConnectionConfig, password: Option<&str>) -> Result<Operator, String> {
    match config {
        FileConnectionConfig::Ftp(config) => {
            validate_ftp_session_arguments(config, password)?;
            let (username, resolved_password) = ftp_credentials(config, password);
            let builder =
                Ftp::default().endpoint(&config.endpoint).root("/").user(&username).password(&resolved_password);
            Operator::new(builder)
                .map(|builder| builder.finish())
                .map_err(|error| redact_error(error.to_string(), password))
        }
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
    for segment in path.split('/') {
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

fn file_entry_from_opendal(list_path: &str, entry: opendal::Entry) -> Result<FileEntry, String> {
    let metadata = entry.metadata();
    let kind = if metadata.mode().is_dir() {
        "directory"
    } else if metadata.mode().is_file() {
        "file"
    } else {
        return Err("Storage returned an entry with an unknown type".to_string());
    };
    let relative_path = root_relative_entry_path(list_path, entry.path())?;
    let relative_path = if kind == "directory" {
        relative_path
            .strip_suffix('/')
            .ok_or_else(|| "Storage returned a directory path without a trailing slash".to_string())?
    } else {
        relative_path.as_str()
    };
    let path = normalize_relative_remote_path(relative_path, false)?;
    let name = path.rsplit('/').next().unwrap_or(&path).to_string();
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
    let file_path = configured_entry_path(config, path, path.is_empty());
    match operator.stat(&file_path).await {
        Ok(metadata) => Ok(metadata),
        Err(error) if !path.is_empty() && error.kind() == ErrorKind::NotFound => {
            let directory_path = configured_entry_path(config, path, true);
            operator.stat(&directory_path).await
        }
        Err(error) => Err(error),
    }
}

fn file_stat_from_metadata(path: &str, metadata: &Metadata) -> FileStat {
    FileStat {
        path: path.to_string(),
        name: if path.is_empty() { "/".to_string() } else { path.rsplit('/').next().unwrap_or(path).to_string() },
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

fn password_scope(config: &FileConnectionConfig) -> Result<String, String> {
    match config {
        FileConnectionConfig::Ftp(config) => {
            let (host, port) = endpoint_host_port(&config.endpoint)?;
            reject_ftp_command_injection(&config.username, "FTP username")?;
            Ok(format!("ftp\n{}\n{port}\n{}", host.to_ascii_lowercase(), config.username))
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
    Ok(FileConnection {
        id: record.id,
        name: record.name,
        config,
        revision: record.revision,
        created_at: record.created_at,
        updated_at: record.updated_at,
        has_password: record.has_secret,
    })
}

fn config_kind(config: &FileConnectionConfig) -> &'static str {
    match config {
        FileConnectionConfig::Ftp(_) => "ftp",
    }
}

fn redact_error(mut message: String, password: Option<&str>) -> String {
    if let Some(password) = password.filter(|password| !password.is_empty()) {
        message = message.replace(password, "[REDACTED]");
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

    async fn direct_ftp(input: &FileConnectionInput, password: &str) -> AsyncFtpStream {
        let FileConnectionConfig::Ftp(config) = &input.config;
        open_ftp_root_session(config, Some(password)).await.unwrap()
    }

    async fn direct_ftp_write(ftp: &mut AsyncFtpStream, path: &str, content: &[u8]) {
        let mut stream = ftp.put_with_stream(path).await.unwrap();
        stream.write_all(content).await.unwrap();
        ftp.finalize_put_stream(stream).await.unwrap();
    }

    fn ftp_session_establishment_count() -> usize {
        FTP_SESSION_ESTABLISHMENT_COUNT.load(Ordering::Relaxed)
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
            let FileConnectionConfig::Ftp(config) = &mut username_input.config;
            config.username = injected.to_string();
            assert!(validate_input(&username_input).unwrap_err().contains("FTP username"));

            let mut password_input = input("ftp://example.test:21", "/");
            password_input.secrets =
                Some(FileConnectionSecrets { password: Some(injected.to_string()), clear_password: false });
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
        runtime.operator_for(&record, &config, None).unwrap();
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
                        let FileConnectionConfig::Ftp(config) = config;
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
                        let FileConnectionConfig::Ftp(config) = config;
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
        runtime.operator_for(&record, &config, None).unwrap();
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
        assert!(normalize_relative_remote_path("folder/", false).is_err());
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
            secrets: Some(FileConnectionSecrets { password: Some(password.clone()), clear_password: false }),
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
            result.map_err(|error| error.to_string()).and_then(|entry| file_entry_from_opendal(&list_root, entry))
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

        let FileConnectionConfig::Ftp(base_config) = &input.config;
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
            delete_entry(&input.config, &empty_directory, Some(&password)).await.unwrap().outcome,
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
            delete_entry(&input.config, &removable_file, Some(&password)).await.unwrap().outcome,
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
                delete_entry(&input.config, &literal_directory, Some(&password)).await.unwrap().outcome,
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
        delete_entry(&input.config, &percent_space_file, Some(&password)).await.unwrap();
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
        delete_entry(&input.config, &percent_slash_file, Some(&password)).await.unwrap();
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
        assert!(delete_entry(&input.config, &nonempty_directory, Some(&password))
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
        let FileConnectionConfig::Ftp(ftp_config) = &input.config;
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
            },
            root: "/ftp/dbx/must-not-be-created".to_string(),
            username: match &input.config {
                FileConnectionConfig::Ftp(config) => config.username.clone(),
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
