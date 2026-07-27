use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dbx_core::connection::AppState;
use dbx_core::file_connection_config::{FileConnectionConfig, FileSecretUpdates};
use dbx_core::models::connection::{ConnectionConfig, DatabaseType, TransportLayerConfig};
use opendal::Operator;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};

use super::adapter::{build_operator, resolve_secrets, ResolvedSecrets, RuntimeEndpoint};
use super::models::{FileManagerError, TestFileConnectionRequest};
use super::service::file_config_from_connection;

#[derive(Clone)]
pub struct FileOperatorLease {
    pub operator: Arc<Operator>,
    pub operation_timeout: Option<Duration>,
}

struct CachedOperator {
    connection: ConnectionConfig,
    base_fingerprint: [u8; 32],
    #[allow(dead_code)]
    fingerprint: [u8; 32],
    operator: Arc<Operator>,
    operation_timeout: Option<Duration>,
    idle_timeout: Duration,
    last_used: Instant,
}

#[derive(Default)]
struct RegistryState {
    instances: HashMap<String, CachedOperator>,
    generations: HashMap<String, u64>,
}

#[derive(Default)]
pub struct FileOperatorRegistry {
    state: Mutex<RegistryState>,
    build_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
}

struct PreparedOperator {
    connection: ConnectionConfig,
    config: FileConnectionConfig,
    secrets: ResolvedSecrets,
    resolved_transport_layers: Vec<TransportLayerConfig>,
    base_fingerprint: [u8; 32],
    operation_timeout: Option<Duration>,
    idle_timeout: Duration,
}

pub async fn validate_file_connection_config(
    app_state: &AppState,
    connection: &ConnectionConfig,
) -> Result<(), FileManagerError> {
    let config = file_config_from_connection(connection)?;
    resolved_transport_layers(app_state, connection, &config).await?;
    Ok(())
}

impl FileOperatorRegistry {
    pub async fn get_or_build(
        &self,
        app_state: &AppState,
        connection_id: &str,
    ) -> Result<FileOperatorLease, FileManagerError> {
        self.evict_idle(app_state).await;
        let prepared = prepare_saved(app_state, connection_id).await?;
        if let Some(lease) = self.cached(connection_id, prepared.base_fingerprint).await {
            return Ok(lease);
        }

        let build_lock = {
            let mut locks = self.build_locks.write().await;
            locks.entry(connection_id.to_string()).or_insert_with(|| Arc::new(Mutex::new(()))).clone()
        };
        let _build_guard = build_lock.lock().await;

        let prepared = prepare_saved(app_state, connection_id).await?;
        if let Some(lease) = self.cached(connection_id, prepared.base_fingerprint).await {
            return Ok(lease);
        }
        if let Some(stale_connection) = self.remove_stale(connection_id, prepared.base_fingerprint).await {
            app_state.reset_connection_transport_for_config(connection_id, &stale_connection).await;
        }

        let generation = self.generation(connection_id).await;
        let runtime_endpoint = match runtime_endpoint(app_state, connection_id, &prepared).await {
            Ok(endpoint) => endpoint,
            Err(error) => {
                app_state.reset_connection_transport_for_config(connection_id, &prepared.connection).await;
                return Err(error);
            }
        };
        let operator = match build_operator(&prepared.config, &prepared.secrets, runtime_endpoint.as_ref()) {
            Ok(operator) => Arc::new(operator),
            Err(error) => {
                app_state.reset_connection_transport_for_config(connection_id, &prepared.connection).await;
                return Err(error);
            }
        };
        let fingerprint = full_fingerprint(prepared.base_fingerprint, runtime_endpoint.as_ref());

        let mut state = self.state.lock().await;
        if state.generations.get(connection_id).copied().unwrap_or_default() != generation {
            drop(state);
            app_state.reset_connection_transport_for_config(connection_id, &prepared.connection).await;
            return Err(FileManagerError::new(
                "connection_changed",
                "The file connection changed while it was being opened",
            ));
        }
        state.instances.insert(
            connection_id.to_string(),
            CachedOperator {
                connection: prepared.connection,
                base_fingerprint: prepared.base_fingerprint,
                fingerprint,
                operator: operator.clone(),
                operation_timeout: prepared.operation_timeout,
                idle_timeout: prepared.idle_timeout,
                last_used: Instant::now(),
            },
        );
        Ok(FileOperatorLease { operator, operation_timeout: prepared.operation_timeout })
    }

    pub async fn build_transient(
        &self,
        app_state: &AppState,
        connection: &ConnectionConfig,
        secret_updates: &FileSecretUpdates,
    ) -> Result<FileOperatorLease, FileManagerError> {
        let config = file_config_from_connection(connection)?;
        let request = TestFileConnectionRequest {
            id: Some(connection.id.clone()),
            config: config.clone(),
            secrets: secret_updates.clone(),
        };
        let secrets = resolve_secrets(&app_state.storage, &request).await?;
        let resolved_transport_layers = resolved_transport_layers(app_state, connection, &config).await?;
        let prepared = PreparedOperator {
            connection: connection.clone(),
            config,
            secrets,
            resolved_transport_layers,
            base_fingerprint: [0; 32],
            operation_timeout: operation_timeout(connection),
            idle_timeout: Duration::from_secs(connection.idle_timeout_secs),
        };
        let tunnel_id = format!("{}:file-test", connection.id);
        let runtime_endpoint = match runtime_endpoint(app_state, &tunnel_id, &prepared).await {
            Ok(endpoint) => endpoint,
            Err(error) => {
                app_state.reset_connection_transport_for_config(&tunnel_id, connection).await;
                return Err(error);
            }
        };
        match build_operator(&prepared.config, &prepared.secrets, runtime_endpoint.as_ref()) {
            Ok(operator) => {
                Ok(FileOperatorLease { operator: Arc::new(operator), operation_timeout: prepared.operation_timeout })
            }
            Err(error) => {
                app_state.reset_connection_transport_for_config(&tunnel_id, connection).await;
                Err(error)
            }
        }
    }

    pub async fn drop_connection(&self, app_state: &AppState, connection_id: &str) {
        let removed = {
            let mut state = self.state.lock().await;
            let removed = state.instances.remove(connection_id);
            let generation = state.generations.entry(connection_id.to_string()).or_default();
            *generation = generation.wrapping_add(1);
            removed
        };
        if let Some(removed) = removed {
            app_state.reset_connection_transport_for_config(connection_id, &removed.connection).await;
        } else {
            app_state.reset_connection_transport(connection_id).await;
        }
        self.remove_unused_build_lock(connection_id).await;
    }

    pub async fn drop_connections<'a>(&self, app_state: &AppState, connection_ids: impl IntoIterator<Item = &'a str>) {
        for connection_id in connection_ids {
            self.drop_connection(app_state, connection_id).await;
        }
    }

    async fn cached(&self, connection_id: &str, fingerprint: [u8; 32]) -> Option<FileOperatorLease> {
        let mut state = self.state.lock().await;
        let entry = state.instances.get_mut(connection_id)?;
        if entry.base_fingerprint != fingerprint {
            return None;
        }
        entry.last_used = Instant::now();
        Some(FileOperatorLease { operator: entry.operator.clone(), operation_timeout: entry.operation_timeout })
    }

    async fn generation(&self, connection_id: &str) -> u64 {
        self.state.lock().await.generations.get(connection_id).copied().unwrap_or_default()
    }

    async fn remove_unused_build_lock(&self, connection_id: &str) {
        let mut locks = self.build_locks.write().await;
        if locks.get(connection_id).is_some_and(|lock| Arc::strong_count(lock) == 1) {
            locks.remove(connection_id);
        }
    }

    async fn remove_stale(&self, connection_id: &str, fingerprint: [u8; 32]) -> Option<ConnectionConfig> {
        let mut state = self.state.lock().await;
        if state.instances.get(connection_id).is_none_or(|entry| entry.base_fingerprint == fingerprint) {
            return None;
        }
        let connection = state.instances.remove(connection_id)?.connection;
        let generation = state.generations.entry(connection_id.to_string()).or_default();
        *generation = generation.wrapping_add(1);
        Some(connection)
    }

    async fn evict_idle(&self, app_state: &AppState) {
        let now = Instant::now();
        let expired = {
            let mut state = self.state.lock().await;
            let expired_ids: Vec<String> = state
                .instances
                .iter()
                .filter(|(_, entry)| now.duration_since(entry.last_used) >= entry.idle_timeout)
                .map(|(id, _)| id.clone())
                .collect();
            let mut expired = Vec::with_capacity(expired_ids.len());
            for id in expired_ids {
                if let Some(entry) = state.instances.remove(&id) {
                    expired.push((id.clone(), entry.connection));
                }
                let generation = state.generations.entry(id.clone()).or_default();
                *generation = generation.wrapping_add(1);
            }
            expired
        };
        for (id, connection) in expired {
            app_state.reset_connection_transport_for_config(&id, &connection).await;
        }
    }
}

async fn prepare_saved(app_state: &AppState, connection_id: &str) -> Result<PreparedOperator, FileManagerError> {
    let connection = app_state
        .storage
        .load_connections()
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connections"))?
        .into_iter()
        .find(|connection| connection.id == connection_id && connection.db_type == DatabaseType::FileManager)
        .ok_or_else(|| FileManagerError::new("not_found", "The file connection does not exist"))?;
    let config = file_config_from_connection(&connection)?;
    let request = TestFileConnectionRequest {
        id: Some(connection.id.clone()),
        config: config.clone(),
        secrets: FileSecretUpdates::default(),
    };
    let secrets = resolve_secrets(&app_state.storage, &request).await?;
    let resolved_transport_layers = resolved_transport_layers(app_state, &connection, &config).await?;
    let base_fingerprint = base_fingerprint(&connection, &config, &secrets, &resolved_transport_layers)?;
    Ok(PreparedOperator {
        operation_timeout: operation_timeout(&connection),
        idle_timeout: Duration::from_secs(connection.idle_timeout_secs),
        connection,
        config,
        secrets,
        resolved_transport_layers,
        base_fingerprint,
    })
}

async fn resolved_transport_layers(
    app_state: &AppState,
    connection: &ConnectionConfig,
    config: &FileConnectionConfig,
) -> Result<Vec<TransportLayerConfig>, FileManagerError> {
    let layers = app_state
        .resolved_transport_layers(connection)
        .await
        .map_err(|error| FileManagerError::new("configuration", error))?;
    if !layers.is_empty() && !matches!(config, FileConnectionConfig::Ftp { .. } | FileConnectionConfig::Sftp { .. }) {
        return Err(FileManagerError::new(
            "unsupported",
            format!("{} does not support SSH or proxy transport layers", config.driver_label()),
        ));
    }
    Ok(layers)
}

async fn runtime_endpoint(
    app_state: &AppState,
    connection_id: &str,
    prepared: &PreparedOperator,
) -> Result<Option<RuntimeEndpoint>, FileManagerError> {
    if prepared.resolved_transport_layers.is_empty() {
        return Ok(None);
    }
    let (host, port) = app_state
        .connection_host_port(connection_id, &prepared.connection)
        .await
        .map_err(|error| FileManagerError::new("connection_failed", error))?;
    Ok(Some(RuntimeEndpoint { host, port }))
}

fn base_fingerprint(
    connection: &ConnectionConfig,
    config: &FileConnectionConfig,
    secrets: &ResolvedSecrets,
    resolved_transport_layers: &[TransportLayerConfig],
) -> Result<[u8; 32], FileManagerError> {
    let mut hasher = Sha256::new();
    hash_json(&mut hasher, connection)?;
    hash_json(&mut hasher, config)?;
    hash_json(&mut hasher, resolved_transport_layers)?;
    secrets.update_fingerprint(&mut hasher);
    Ok(hasher.finalize().into())
}

fn full_fingerprint(base: [u8; 32], runtime_endpoint: Option<&RuntimeEndpoint>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(base);
    if let Some(runtime_endpoint) = runtime_endpoint {
        hasher.update((runtime_endpoint.host.len() as u64).to_le_bytes());
        hasher.update(runtime_endpoint.host.as_bytes());
        hasher.update(runtime_endpoint.port.to_le_bytes());
    }
    hasher.finalize().into()
}

fn hash_json<T: serde::Serialize + ?Sized>(hasher: &mut Sha256, value: &T) -> Result<(), FileManagerError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|_| FileManagerError::new("configuration", "Failed to fingerprint the file connection"))?;
    hasher.update((encoded.len() as u64).to_le_bytes());
    hasher.update(encoded);
    Ok(())
}

fn operation_timeout(connection: &ConnectionConfig) -> Option<Duration> {
    let seconds = connection.effective_query_timeout_secs();
    (seconds > 0).then(|| Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use dbx_core::connection::AppState;
    use dbx_core::file_connection_config::{FileSecretUpdate, FileSecretUpdates};
    use dbx_core::models::connection::ConnectionConfig;
    use dbx_core::storage::Storage;
    use serde_json::json;
    use uuid::Uuid;

    use super::FileOperatorRegistry;

    async fn state(label: &str) -> AppState {
        let directory = std::env::temp_dir().join(format!("dbx-file-registry-{label}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let storage = Storage::open(&directory.join("storage.db")).await.unwrap();
        AppState::new_with_plugin_dir(storage, directory.join("plugins"))
    }

    fn ftp_config(id: &str, root: &str, idle_timeout_secs: u64) -> ConnectionConfig {
        serde_json::from_value(json!({
            "id": id,
            "name": "FTP",
            "db_type": "file",
            "driver_profile": "ftp",
            "driver_label": "FTP",
            "host": "127.0.0.1",
            "port": 21,
            "username": "dbx",
            "password": "",
            "database": null,
            "query_timeout_secs": 7,
            "idle_timeout_secs": idle_timeout_secs,
            "external_config": {
                "protocol": "ftp",
                "endpoint": "127.0.0.1",
                "port": 21,
                "root": root,
                "username": "dbx"
            }
        }))
        .unwrap()
    }

    async fn save_ftp(state: &AppState, config: &ConnectionConfig, password: Option<&str>) {
        let updates = password
            .map(|password| {
                HashMap::from([(
                    config.id.clone(),
                    FileSecretUpdates { password: FileSecretUpdate::Set(password.to_string()), ..Default::default() },
                )])
            })
            .unwrap_or_default();
        state.storage.save_connections_with_file_secret_updates(std::slice::from_ref(config), &updates).await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_first_use_builds_one_shared_operator() {
        let state = state("concurrent").await;
        let config = ftp_config("ftp", "/", 60);
        save_ftp(&state, &config, Some("one")).await;
        let registry = FileOperatorRegistry::default();

        let (first, second, third) = tokio::join!(
            registry.get_or_build(&state, "ftp"),
            registry.get_or_build(&state, "ftp"),
            registry.get_or_build(&state, "ftp")
        );
        let first = first.unwrap();
        let second = second.unwrap();
        let third = third.unwrap();
        assert!(Arc::ptr_eq(&first.operator, &second.operator));
        assert!(Arc::ptr_eq(&first.operator, &third.operator));
        assert_eq!(first.operation_timeout.unwrap().as_secs(), 7);
    }

    #[tokio::test]
    async fn config_secret_drop_and_idle_changes_rebuild_operator() {
        let state = state("fingerprint").await;
        let mut config = ftp_config("ftp", "/first", 60);
        save_ftp(&state, &config, Some("one")).await;
        let registry = FileOperatorRegistry::default();
        let first = registry.get_or_build(&state, "ftp").await.unwrap();

        save_ftp(&state, &config, Some("two")).await;
        let after_secret = registry.get_or_build(&state, "ftp").await.unwrap();
        assert!(!Arc::ptr_eq(&first.operator, &after_secret.operator));

        config.external_config.as_mut().unwrap()["root"] = json!("/second");
        save_ftp(&state, &config, None).await;
        let after_config = registry.get_or_build(&state, "ftp").await.unwrap();
        assert!(!Arc::ptr_eq(&after_secret.operator, &after_config.operator));

        registry.drop_connection(&state, "ftp").await;
        let after_drop = registry.get_or_build(&state, "ftp").await.unwrap();
        assert!(!Arc::ptr_eq(&after_config.operator, &after_drop.operator));

        config.idle_timeout_secs = 0;
        save_ftp(&state, &config, None).await;
        let zero_idle_first = registry.get_or_build(&state, "ftp").await.unwrap();
        let zero_idle_second = registry.get_or_build(&state, "ftp").await.unwrap();
        assert!(!Arc::ptr_eq(&zero_idle_first.operator, &zero_idle_second.operator));
    }

    #[tokio::test]
    async fn unsupported_transport_is_rejected_before_operator_build() {
        let state = state("transport").await;
        let config: ConnectionConfig = serde_json::from_value(json!({
            "id": "s3",
            "name": "S3",
            "db_type": "file",
            "driver_profile": "s3",
            "driver_label": "S3",
            "host": "127.0.0.1",
            "port": 9000,
            "username": "",
            "password": "",
            "database": null,
            "transport_layers": [{
                "type": "proxy",
                "id": "proxy",
                "host": "127.0.0.1",
                "port": 1080
            }],
            "external_config": {
                "protocol": "s3",
                "endpoint": "http://127.0.0.1:9000",
                "region": "us-east-1",
                "bucket": "dbx",
                "root": "/",
                "pathStyle": true
            }
        }))
        .unwrap();
        state.storage.save_connections(std::slice::from_ref(&config)).await.unwrap();
        let error = match FileOperatorRegistry::default().get_or_build(&state, "s3").await {
            Ok(_) => panic!("S3 transport must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.code, "unsupported");
        assert!(error.message.contains("S3"));
    }
}
