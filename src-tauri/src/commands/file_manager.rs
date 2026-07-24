use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use super::file_manager_paths::{
    join_configured_root, plan_directory_delete, reject_recursive_delete, DirectoryDeleteEvidence, DirectoryDeletePlan,
    DirectoryStorageModel, RemotePath,
};
use dbx_core::connection::AppState;
use dbx_core::storage::FileConnectionStorageRecord;
use futures::{StreamExt, TryStreamExt};
use opendal::services::Ftp;
use opendal::{ErrorKind, Metadata, Operator};
use serde::{Deserialize, Serialize};
use suppaftp::tokio::AsyncFtpStream;
use tauri::State;
use tokio::net::{lookup_host, TcpStream};
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
struct CancellationSignal {
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
    let _lease = runtime.begin_operation(&id)?;

    let config_json = serde_json::to_string(&input.config).map_err(|error| error.to_string())?;
    let password = input.secrets.as_ref().and_then(|secrets| secrets.password.clone());
    let replace_secret = password.is_some() || input.secrets.as_ref().is_some_and(|secrets| secrets.clear_password);
    let password_scope = password_scope(&input.config)?;
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
    let record = state
        .storage
        .load_file_connection(&connection_id)
        .await?
        .ok_or_else(|| "File connection not found".to_string())?;
    let revision = record.revision;
    let config = parse_storage_config(&record)?;
    let scope = password_scope(&config)?;
    let password = state.storage.load_file_connection_password(&connection_id, &scope).await?;
    let operator = runtime.operator_for(&record, &config, password.as_deref())?;
    let directory_path = configured_operation_path(&config, &path, true);
    let cancellation = lease.cancellation();

    let result = run_mutation_operation(&runtime, &connection_id, revision, &cancellation, "Create directory", async {
        let _mutation_guard = lease.entry.mutation_lock.lock().await;
        cancellation.ensure_active()?;
        operator
            .create_dir(&directory_path)
            .await
            .map_err(|error| redact_error(error.to_string(), password.as_deref()))?;
        let metadata = operator.stat(&directory_path).await.map_err(|error| {
            format!(
                "Directory creation could not be verified: {}",
                redact_error(error.to_string(), password.as_deref())
            )
        })?;
        if !metadata.mode().is_dir() {
            return Err("Directory creation could not be verified because the target is not a directory".to_string());
        }
        Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
    })
    .await;
    result
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
    let record = state
        .storage
        .load_file_connection(&connection_id)
        .await?
        .ok_or_else(|| "File connection not found".to_string())?;
    let revision = record.revision;
    let config = parse_storage_config(&record)?;
    let scope = password_scope(&config)?;
    let password = state.storage.load_file_connection_password(&connection_id, &scope).await?;
    let operator = runtime.operator_for(&record, &config, password.as_deref())?;
    let cancellation = lease.cancellation();

    run_mutation_operation(&runtime, &connection_id, revision, &cancellation, "Delete", async {
        let _mutation_guard = lease.entry.mutation_lock.lock().await;
        cancellation.ensure_active()?;
        delete_entry(&operator, &config, &path, password.as_deref()).await
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

    fn evict_revision(&self, connection_id: &str, revision: i64) {
        let mut operators = self.operators.write().unwrap_or_else(|error| error.into_inner());
        if operators.get(connection_id).is_some_and(|cached| cached.revision == revision) {
            operators.remove(connection_id);
        }
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

impl CancellationSignal {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn cancelled(&self) {
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
    runtime: &FileManagerRuntime,
    connection_id: &str,
    revision: i64,
    cancellation: &CancellationSignal,
    operation: &'static str,
    future: F,
) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    let result = tokio::select! {
        _ = cancellation.cancelled() => Err("File connection is being deleted".to_string()),
        result = tokio::time::timeout(MUTATION_TIMEOUT, future) => {
            result.map_err(|_| format!("{operation} timed out"))?
        }
    };
    // FTP mutations and their verification commonly finish on a 550/NotFound
    // response. OpenDAL 0.57 can leave that pooled control connection
    // unsuitable for the next command, so mutations always retire the cached
    // operator before the UI refreshes.
    runtime.evict_revision(connection_id, revision);
    result
}

async fn delete_entry(
    operator: &Operator,
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<FileMutationResult, String> {
    let file_path = configured_operation_path(config, path, false);
    let directory_path = configured_operation_path(config, path, true);
    let (metadata, exact_path) = match operator.stat(&file_path).await {
        Ok(metadata) => {
            let exact_path = if metadata.mode().is_dir() { directory_path.as_str() } else { file_path.as_str() };
            (metadata, exact_path)
        }
        Err(error) if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            let metadata =
                operator.stat(&directory_path).await.map_err(|error| redact_error(error.to_string(), password))?;
            (metadata, directory_path.as_str())
        }
        Err(error) => return Err(redact_error(error.to_string(), password)),
    };

    if !metadata.mode().is_dir() {
        operator.delete(exact_path).await.map_err(|error| redact_error(error.to_string(), password))?;
        return match operator.stat(exact_path).await {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
            }
            Ok(_) => Err("File delete was not confirmed because the target still exists".to_string()),
            Err(error) => {
                Err(format!("File delete could not be verified: {}", redact_error(error.to_string(), password)))
            }
        };
    }

    let evidence =
        inspect_directory_delete(operator, &directory_path, DirectoryStorageModel::Hierarchical, password).await?;
    match plan_directory_delete(DirectoryStorageModel::Hierarchical, evidence)? {
        DirectoryDeletePlan::DeleteExactDirectory => {
            // OpenDAL 0.57 maps every FTP RMD 550 response to success. Use the
            // protocol's exact RMD so a concurrent child cannot be hidden as a
            // completed delete.
            delete_ftp_directory_exact(config, path, password).await?;
            Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
        }
        DirectoryDeletePlan::DeleteExactMarker => {
            operator.delete(&directory_path).await.map_err(|error| redact_error(error.to_string(), password))?;
            Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
        }
        DirectoryDeletePlan::NoOpVirtualPrefix => Ok(FileMutationResult { outcome: FileMutationOutcome::NoOp }),
    }
}

async fn delete_ftp_directory_exact(
    config: &FileConnectionConfig,
    path: &RemotePath,
    password: Option<&str>,
) -> Result<(), String> {
    let FileConnectionConfig::Ftp(config) = config;
    let (host, port) = endpoint_host_port(&config.endpoint)?;
    let addresses = resolve_addresses(&host, port).await.map_err(|error| format!("DNS stage failed: {error}"))?;
    let address = connect_first(&addresses).await.map_err(|error| format!("TCP stage failed: {error}"))?;
    let mut ftp = AsyncFtpStream::connect(address)
        .await
        .map_err(|error| format!("Authentication stage failed: {}", redact_error(error.to_string(), password)))?;
    let (username, resolved_password) = ftp_credentials(config, password);
    ftp.login(&username, &resolved_password)
        .await
        .map_err(|error| format!("Authentication stage failed: {}", redact_error(error.to_string(), password)))?;
    ftp.cwd(&config.root)
        .await
        .map_err(|error| format!("Root stage failed: {}", redact_error(error.to_string(), password)))?;
    let result = ftp.rmdir(path.as_str()).await;
    let _ = ftp.quit().await;
    result.map_err(|error| {
        format!(
            "Directory changed, is not empty, or cannot be removed; recursive delete is unsupported: {}",
            redact_error(error.to_string(), password)
        )
    })
}

async fn inspect_directory_delete(
    operator: &Operator,
    directory_path: &str,
    model: DirectoryStorageModel,
    password: Option<&str>,
) -> Result<DirectoryDeleteEvidence, String> {
    let target = directory_path.trim_end_matches('/');
    let mut lister =
        operator.lister(directory_path).await.map_err(|error| redact_error(error.to_string(), password))?;
    let mut has_children = false;
    let mut marker_size = None;
    while let Some(entry) = lister.try_next().await.map_err(|error| redact_error(error.to_string(), password))? {
        if entry.path().trim_end_matches('/') == target {
            if model == DirectoryStorageModel::ObjectStore {
                marker_size = Some(entry.metadata().content_length());
            }
        } else {
            has_children = true;
            break;
        }
    }
    Ok(DirectoryDeleteEvidence { has_children, marker_size })
}

async fn test_ftp_connection(input: &FileConnectionInput, password: Option<&str>) -> FileConnectionTestResult {
    let mut stages = Vec::with_capacity(5);
    if let Err(error) = validate_input(input) {
        stages.push(failed_stage("configuration", error));
        append_skipped_stages(&mut stages, &["dns", "tcp", "authentication", "root"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("configuration"));

    let FileConnectionConfig::Ftp(config) = &input.config;
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

    let address = match connect_first(&addresses).await {
        Ok(address) => address,
        Err(error) => {
            stages.push(failed_stage("tcp", error));
            append_skipped_stages(&mut stages, &["authentication", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    stages.push(passed_stage("tcp"));

    let mut ftp = match tokio::time::timeout(CONNECTION_TIMEOUT, AsyncFtpStream::connect(address)).await {
        Ok(Ok(ftp)) => ftp,
        Ok(Err(error)) => {
            stages.push(failed_stage("authentication", redact_error(error.to_string(), password)));
            stages.push(skipped_stage("root"));
            return FileConnectionTestResult { success: false, stages };
        }
        Err(_) => {
            stages.push(failed_stage("authentication", "FTP greeting timed out".to_string()));
            stages.push(skipped_stage("root"));
            return FileConnectionTestResult { success: false, stages };
        }
    };
    let (username, resolved_password) = ftp_credentials(config, password);
    match tokio::time::timeout(CONNECTION_TIMEOUT, ftp.login(&username, &resolved_password)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            stages.push(failed_stage("authentication", redact_error(error.to_string(), password)));
            stages.push(skipped_stage("root"));
            return FileConnectionTestResult { success: false, stages };
        }
        Err(_) => {
            stages.push(failed_stage("authentication", "FTP login timed out".to_string()));
            stages.push(skipped_stage("root"));
            return FileConnectionTestResult { success: false, stages };
        }
    }
    stages.push(passed_stage("authentication"));

    match tokio::time::timeout(CONNECTION_TIMEOUT, ftp.cwd(&config.root)).await {
        Ok(Ok(())) => {
            stages.push(passed_stage("root"));
            let _ = ftp.quit().await;
            FileConnectionTestResult { success: true, stages }
        }
        Ok(Err(error)) => {
            stages.push(failed_stage("root", redact_error(error.to_string(), password)));
            FileConnectionTestResult { success: false, stages }
        }
        Err(_) => {
            stages.push(failed_stage("root", "FTP root check timed out".to_string()));
            FileConnectionTestResult { success: false, stages }
        }
    }
}

async fn connect_first(addresses: &[SocketAddr]) -> Result<SocketAddr, String> {
    let mut last_error = "No address available".to_string();
    for address in addresses {
        match tokio::time::timeout(CONNECTION_TIMEOUT, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                drop(stream);
                return Ok(*address);
            }
            Ok(Err(error)) => last_error = error.to_string(),
            Err(_) => last_error = "TCP connection timed out".to_string(),
        }
    }
    Err(last_error)
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
    let (host, port) = endpoint_host_port(&config.endpoint)?;
    let addresses = resolve_addresses(&host, port).await.map_err(|error| format!("DNS stage failed: {error}"))?;
    let address = connect_first(&addresses).await.map_err(|error| format!("TCP stage failed: {error}"))?;
    let mut ftp = tokio::time::timeout(CONNECTION_TIMEOUT, AsyncFtpStream::connect(address))
        .await
        .map_err(|_| "Authentication stage failed: FTP greeting timed out".to_string())?
        .map_err(|error| format!("Authentication stage failed: {}", redact_error(error.to_string(), password)))?;
    let (username, resolved_password) = ftp_credentials(config, password);
    tokio::time::timeout(CONNECTION_TIMEOUT, ftp.login(&username, &resolved_password))
        .await
        .map_err(|_| "Authentication stage failed: FTP login timed out".to_string())?
        .map_err(|error| format!("Authentication stage failed: {}", redact_error(error.to_string(), password)))?;
    tokio::time::timeout(CONNECTION_TIMEOUT, ftp.cwd(&config.root))
        .await
        .map_err(|_| "Root stage failed: FTP root check timed out".to_string())?
        .map_err(|error| format!("Root stage failed: {}", redact_error(error.to_string(), password)))?;
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

fn build_operator(config: &FileConnectionConfig, password: Option<&str>) -> Result<Operator, String> {
    match config {
        FileConnectionConfig::Ftp(config) => {
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
            config.username = config.username.trim().to_string();
        }
    }
    Ok(())
}

fn normalize_ftp_root(root: &str) -> Result<String, String> {
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
    let file_path = configured_entry_path(config, path, path.is_empty());
    match operator.stat(&file_path).await {
        Ok(metadata) => Ok(metadata),
        Err(error) if !path.is_empty() && error.kind() == ErrorKind::NotFound => {
            let directory_path = configured_entry_path(config, path, true);
            operator.stat(&directory_path).await.map_err(|error| redact_error(error.to_string(), password))
        }
        Err(error) => Err(redact_error(error.to_string(), password)),
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

fn configured_operation_path(config: &FileConnectionConfig, path: &RemotePath, directory: bool) -> String {
    match config {
        FileConnectionConfig::Ftp(config) => join_configured_root(&config.root, path, directory),
    }
}

fn password_scope(config: &FileConnectionConfig) -> Result<String, String> {
    match config {
        FileConnectionConfig::Ftp(config) => {
            let (host, port) = endpoint_host_port(&config.endpoint)?;
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
        let (host, port) = endpoint_host_port(&config.endpoint).unwrap();
        let addresses = resolve_addresses(&host, port).await.unwrap();
        let address = connect_first(&addresses).await.unwrap();
        let mut ftp = AsyncFtpStream::connect(address).await.unwrap();
        let (username, resolved_password) = ftp_credentials(config, Some(password));
        ftp.login(&username, &resolved_password).await.unwrap();
        ftp
    }

    async fn direct_ftp_write(ftp: &mut AsyncFtpStream, path: &str, content: &[u8]) {
        let mut stream = ftp.put_with_stream(path).await.unwrap();
        stream.write_all(content).await.unwrap();
        ftp.finalize_put_stream(stream).await.unwrap();
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
    async fn successful_mutation_retires_the_cached_operator() {
        let runtime = FileManagerRuntime::default();
        let config = input("ftp://example.test:21", "/");
        let operator = build_operator(&config.config, None).unwrap();
        runtime.operators.write().unwrap().insert("ftp-1".to_string(), CachedOperator { revision: 1, operator });
        assert_eq!(runtime.operator_count(), 1);

        run_mutation_operation(&runtime, "ftp-1", 1, &CancellationSignal::default(), "Mutation", async {
            Ok::<_, String>(())
        })
        .await
        .unwrap();
        assert_eq!(runtime.operator_count(), 0);
    }

    #[tokio::test]
    async fn queued_mutation_is_cancelled_when_connection_deletion_starts() {
        let runtime = Arc::new(FileManagerRuntime::default());
        let acquired = Arc::new(Notify::new());

        let first_runtime = runtime.clone();
        let first_acquired = acquired.clone();
        let first_lease = runtime.begin_operation("ftp-1").unwrap();
        let first_cancellation = first_lease.cancellation();
        let first = tokio::spawn(async move {
            let result = run_mutation_operation(&first_runtime, "ftp-1", 1, &first_cancellation, "Mutation", async {
                let _guard = first_lease.entry.mutation_lock.lock().await;
                first_acquired.notify_one();
                std::future::pending::<Result<(), String>>().await
            })
            .await;
            drop(first_lease);
            result
        });
        acquired.notified().await;

        let second_runtime = runtime.clone();
        let second_lease = runtime.begin_operation("ftp-1").unwrap();
        let second_cancellation = second_lease.cancellation();
        let second = tokio::spawn(async move {
            let result = run_mutation_operation(&second_runtime, "ftp-1", 1, &second_cancellation, "Mutation", async {
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
        let mut direct = direct_ftp(&input, &password).await;
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

        operator = build_operator(&input.config, Some(&password)).unwrap();
        let empty_directory = RemotePath::parse("ticket-4-empty").unwrap();
        operator.create_dir(&configured_operation_path(&input.config, &empty_directory, true)).await.unwrap();
        operator = build_operator(&input.config, Some(&password)).unwrap();
        assert!(matches!(
            delete_entry(&operator, &input.config, &empty_directory, Some(&password)).await.unwrap().outcome,
            FileMutationOutcome::Completed
        ));

        operator = build_operator(&input.config, Some(&password)).unwrap();
        let removable_file = RemotePath::parse("ticket-4-file.txt").unwrap();
        let removable_file_path = configured_operation_path(&input.config, &removable_file, false);
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-file.txt", b"delete me").await;
        assert!(matches!(
            delete_entry(&operator, &input.config, &removable_file, Some(&password)).await.unwrap().outcome,
            FileMutationOutcome::Completed
        ));
        assert_eq!(operator.stat(&removable_file_path).await.unwrap_err().kind(), ErrorKind::NotFound);
        operator = build_operator(&input.config, Some(&password)).unwrap();

        for (literal_name, decoded_name) in
            [("ticket-4%20literal-dir", "ticket-4 literal-dir"), ("ticket-4%2Fliteral-dir", "ticket-4/literal-dir")]
        {
            let literal_directory = RemotePath::parse(literal_name).unwrap();
            let literal_directory_path = configured_operation_path(&input.config, &literal_directory, true);
            operator.create_dir(&literal_directory_path).await.unwrap();
            assert!(operator.stat(&literal_directory_path).await.unwrap().mode().is_dir());
            operator = build_operator(&input.config, Some(&password)).unwrap();
            direct.cwd(format!("/ftp/dbx/{literal_name}")).await.unwrap();
            direct.cwd("/ftp/dbx").await.unwrap();
            assert!(direct.cwd(format!("/ftp/dbx/{decoded_name}")).await.is_err());
            assert!(matches!(
                delete_entry(&operator, &input.config, &literal_directory, Some(&password)).await.unwrap().outcome,
                FileMutationOutcome::Completed
            ));
            assert!(direct.cwd(format!("/ftp/dbx/{literal_name}")).await.is_err());
            direct.cwd("/ftp/dbx").await.unwrap();
            operator = build_operator(&input.config, Some(&password)).unwrap();
        }

        let percent_space_file = RemotePath::parse("ticket-4%20literal-file").unwrap();
        let percent_space_file_path = configured_operation_path(&input.config, &percent_space_file, false);
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4%20literal-file", b"literal").await;
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4 literal-file", b"decoded-target").await;
        assert_eq!(operator.stat(&percent_space_file_path).await.unwrap().content_length(), 7);
        delete_entry(&operator, &input.config, &percent_space_file, Some(&password)).await.unwrap();
        assert!(direct.size("/ftp/dbx/ticket-4%20literal-file").await.is_err());
        assert_eq!(direct.size("/ftp/dbx/ticket-4 literal-file").await.unwrap(), 14);
        direct.rm("/ftp/dbx/ticket-4 literal-file").await.unwrap();
        operator = build_operator(&input.config, Some(&password)).unwrap();

        direct.mkdir("/ftp/dbx/ticket-4-decoded").await.unwrap();
        let percent_slash_file = RemotePath::parse("ticket-4-decoded%2Fliteral-file").unwrap();
        let percent_slash_file_path = configured_operation_path(&input.config, &percent_slash_file, false);
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-decoded%2Fliteral-file", b"raw").await;
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-decoded/literal-file", b"decoded-target").await;
        assert_eq!(operator.stat(&percent_slash_file_path).await.unwrap().content_length(), 3);
        delete_entry(&operator, &input.config, &percent_slash_file, Some(&password)).await.unwrap();
        assert!(direct.size("/ftp/dbx/ticket-4-decoded%2Fliteral-file").await.is_err());
        assert_eq!(direct.size("/ftp/dbx/ticket-4-decoded/literal-file").await.unwrap(), 14);
        direct.rm("/ftp/dbx/ticket-4-decoded/literal-file").await.unwrap();
        direct.rmdir("/ftp/dbx/ticket-4-decoded").await.unwrap();
        operator = build_operator(&input.config, Some(&password)).unwrap();

        let nonempty_directory = RemotePath::parse("ticket-4-nonempty").unwrap();
        let nonempty_directory_path = configured_operation_path(&input.config, &nonempty_directory, true);
        operator.create_dir(&nonempty_directory_path).await.unwrap();
        operator = build_operator(&input.config, Some(&password)).unwrap();
        let nonempty_child = format!("{nonempty_directory_path}child.txt");
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-nonempty/child.txt", b"keep me").await;
        assert!(delete_entry(&operator, &input.config, &nonempty_directory, Some(&password))
            .await
            .unwrap_err()
            .contains("not empty"));
        operator = build_operator(&input.config, Some(&password)).unwrap();
        operator.stat(&nonempty_child).await.unwrap();

        // Model a remote writer racing the preflight. The exact FTP RMD must
        // fail once the child exists; no recursive fallback is attempted.
        let raced_directory = RemotePath::parse("ticket-4-raced").unwrap();
        let raced_directory_path = configured_operation_path(&input.config, &raced_directory, true);
        operator.create_dir(&raced_directory_path).await.unwrap();
        operator = build_operator(&input.config, Some(&password)).unwrap();
        let evidence = inspect_directory_delete(
            &operator,
            &raced_directory_path,
            DirectoryStorageModel::Hierarchical,
            Some(&password),
        )
        .await
        .unwrap();
        assert_eq!(
            plan_directory_delete(DirectoryStorageModel::Hierarchical, evidence).unwrap(),
            DirectoryDeletePlan::DeleteExactDirectory
        );
        let raced_child = format!("{raced_directory_path}concurrent.txt");
        direct_ftp_write(&mut direct, "/ftp/dbx/ticket-4-raced/concurrent.txt", b"created after preflight").await;
        let raced_delete_error =
            delete_ftp_directory_exact(&input.config, &raced_directory, Some(&password)).await.unwrap_err();
        assert!(raced_delete_error.contains("recursive delete is unsupported"), "{raced_delete_error}");
        operator = build_operator(&input.config, Some(&password)).unwrap();
        operator.stat(&raced_child).await.unwrap();

        for unsafe_path in ["/ftp/dbx/fixture.txt", "../fixture.txt", "%2e%2e/fixture.txt", "safe%2f..%2ffixture.txt"] {
            assert!(RemotePath::parse(unsafe_path).is_err());
        }
        operator.stat(&format!("{list_path}fixture.txt")).await.unwrap();

        direct_ftp_write(&mut direct, "/ftp/dbx/whitespace-proof", b"x").await;
        direct_ftp_write(&mut direct, "/ftp/dbx/whitespace-proof ", b"yy").await;
        assert_eq!(direct.size("/ftp/dbx/whitespace-proof").await.unwrap(), 1);
        assert_eq!(direct.size("/ftp/dbx/whitespace-proof ").await.unwrap(), 2);
        assert_eq!(operator.stat(&format!("{list_path}whitespace-proof ")).await.unwrap().content_length(), 1);
        assert!(RemotePath::parse("whitespace-proof ").unwrap_err().contains("storage runtime"));

        operator.delete(&nonempty_child).await.unwrap();
        direct.rmdir("/ftp/dbx/ticket-4-nonempty").await.unwrap();
        operator.delete(&raced_child).await.unwrap();
        direct.rmdir("/ftp/dbx/ticket-4-raced").await.unwrap();
        direct.rm("/ftp/dbx/whitespace-proof").await.unwrap();
        direct.rm("/ftp/dbx/whitespace-proof ").await.unwrap();
        direct.quit().await.unwrap();

        operator = build_operator(&input.config, Some(&password)).unwrap();
        let missing_config = FileConnectionConfig::Ftp(FtpConnectionConfig {
            endpoint: match &input.config {
                FileConnectionConfig::Ftp(config) => config.endpoint.clone(),
            },
            root: "/ftp/dbx/must-not-be-created".to_string(),
            username: match &input.config {
                FileConnectionConfig::Ftp(config) => config.username.clone(),
            },
        });
        let before =
            operator.list(&list_path).await.unwrap().iter().map(|entry| entry.path().to_string()).collect::<Vec<_>>();
        assert!(verify_ftp_root_read_only(&missing_config, Some(&password)).await.is_err());
        let after =
            operator.list(&list_path).await.unwrap().iter().map(|entry| entry.path().to_string()).collect::<Vec<_>>();
        assert_eq!(after, before);
        assert!(!after.iter().any(|path| path.contains("must-not-be-created")));

        assert!(storage.delete_file_connection(&created.id).await.unwrap());
        assert!(storage.list_file_connections().await.unwrap().is_empty());
        drop(storage);
        std::fs::remove_file(db_path).ok();
    }
}
