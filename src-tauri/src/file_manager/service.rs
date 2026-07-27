#[cfg(test)]
use std::collections::HashMap;
use std::time::Duration;

use dbx_core::connection::AppState;
use dbx_core::file_connection_config::FileSecretUpdates as GenericFileSecretUpdates;
use dbx_core::models::connection::{ConnectionConfig, DatabaseConnectionInfo, DatabaseType};
use dbx_core::storage::Storage;
#[cfg(test)]
use opendal::Operator;
use opendal::{EntryMode, Metadata};
use percent_encoding::percent_decode_str;

use super::adapter::map_operation_error;
#[cfg(test)]
use super::adapter::{build_operator, resolve_secrets};
use super::models::{
    FileCapabilities, FileConnection, FileConnectionConfig, FileEntry, FileEntryKind, FileManagerError,
    FileSecretStatus, HdfsConfig, StoredFileConnection,
};
#[cfg(test)]
use super::models::{
    FileSecretUpdates, SaveFileConnectionRequest, SecretUpdate, SftpAuthentication, TestFileConnectionRequest,
    WebdavAuthentication,
};
use super::registry::{FileOperatorLease, FileOperatorRegistry};

#[cfg(test)]
const CONNECTION_TEST_TIMEOUT_SECS: u64 = 15;
#[cfg(test)]
const FILE_OPERATION_TIMEOUT_SECS: u64 = 60;

pub async fn list_connections(storage: &Storage) -> Result<Vec<FileConnection>, FileManagerError> {
    let configs = storage
        .load_connections()
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connections"))?;
    let mut connections = Vec::new();
    for config in configs.into_iter().filter(|config| config.db_type == DatabaseType::FileManager) {
        let file_config = file_config_from_connection(&config)?;
        let status = storage
            .file_connection_secret_status(&config.id)
            .await
            .map_err(|_| FileManagerError::new("storage", "Failed to load file connection credential status"))?;
        connections.push(public_connection(
            StoredFileConnection { id: config.id, name: config.name, config: file_config },
            status,
        ));
    }
    #[cfg(test)]
    if connections.is_empty() {
        let legacy_values = storage
            .load_file_connections()
            .await
            .map_err(|_| FileManagerError::new("storage", "Failed to load legacy file connections"))?;
        for value in legacy_values {
            let stored: StoredFileConnection = serde_json::from_value(value)
                .map_err(|_| FileManagerError::new("storage", "A saved legacy file connection is invalid"))?;
            let keys = storage
                .file_connection_secret_keys(&stored.id)
                .await
                .map_err(|_| FileManagerError::new("storage", "Failed to load file connection credential status"))?;
            connections.push(public_connection(stored, FileSecretStatus::from_keys(&keys)));
        }
    }
    Ok(connections)
}

#[cfg(test)]
pub async fn save_connection(
    storage: &Storage,
    mut request: SaveFileConnectionRequest,
) -> Result<FileConnection, FileManagerError> {
    validate_identity(&request.id, &request.name)?;
    clear_inactive_secret_updates(&request.config, &mut request.secrets);
    let validation_request = TestFileConnectionRequest {
        id: Some(request.id.clone()),
        config: request.config.clone(),
        secrets: request.secrets.clone(),
    };
    let resolved = resolve_secrets(storage, &validation_request).await?;
    build_operator(&request.config, &resolved, None)?;
    let updates = request.secrets.persistence_updates().map_err(FileManagerError::configuration)?;
    let stored = StoredFileConnection { id: request.id, name: request.name, config: request.config };
    let (host, port, ssl) = stored.config.projected_host_port_ssl();
    let generic: ConnectionConfig = serde_json::from_value(serde_json::json!({
        "id": stored.id.clone(),
        "name": stored.name.clone(),
        "db_type": "file",
        "driver_profile": stored.config.driver_profile(),
        "driver_label": stored.config.driver_label(),
        "host": host,
        "port": port,
        "username": stored.config.username(),
        "password": "",
        "database": null,
        "ssl": ssl,
        "external_config": stored.config.clone(),
    }))
    .map_err(|_| FileManagerError::new("storage", "Failed to serialize the file connection"))?;
    let mut configs = storage
        .load_connections()
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connections"))?;
    configs.retain(|config| config.id != generic.id);
    configs.push(generic);
    storage
        .save_connections_with_file_secret_updates(
            &configs,
            &HashMap::from([(stored.id.clone(), request.secrets.clone())]),
        )
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to save the file connection"))?;
    let value = serde_json::to_value(&stored)
        .map_err(|_| FileManagerError::new("storage", "Failed to serialize the file connection"))?;
    storage
        .save_file_connection(&stored.id, &value, &updates)
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to save the file connection"))?;
    let keys = storage
        .file_connection_secret_keys(&stored.id)
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connection credential status"))?;
    Ok(public_connection(stored, FileSecretStatus::from_keys(&keys)))
}

#[cfg(test)]
fn clear_inactive_secret_updates(config: &FileConnectionConfig, secrets: &mut FileSecretUpdates) {
    let (password, private_key, access_key, secret_key, session_token, bearer_token, delegation_token) = match config {
        FileConnectionConfig::Ftp { .. } => (true, false, false, false, false, false, false),
        FileConnectionConfig::Sftp { authentication, .. } => {
            (false, matches!(authentication, SftpAuthentication::PrivateKey), false, false, false, false, false)
        }
        FileConnectionConfig::S3 { .. } => (false, false, true, true, true, false, false),
        FileConnectionConfig::Webdav { authentication, .. } => match authentication {
            WebdavAuthentication::Basic { .. } => (true, false, false, false, false, false, false),
            WebdavAuthentication::Bearer => (false, false, false, false, false, true, false),
        },
        FileConnectionConfig::Hdfs { config: HdfsConfig::Webhdfs { use_delegation_token: true, .. } } => {
            (false, false, false, false, false, false, true)
        }
        FileConnectionConfig::Hdfs { .. } => (false, false, false, false, false, false, false),
    };
    for (active, update) in [
        (password, &mut secrets.password),
        (private_key, &mut secrets.private_key),
        (access_key, &mut secrets.access_key),
        (secret_key, &mut secrets.secret_key),
        (session_token, &mut secrets.session_token),
        (bearer_token, &mut secrets.bearer_token),
        (delegation_token, &mut secrets.delegation_token),
    ] {
        if !active {
            *update = SecretUpdate::Clear;
        }
    }
}

#[cfg(test)]
pub async fn delete_connection(storage: &Storage, id: &str) -> Result<(), FileManagerError> {
    if id.trim().is_empty() {
        return Err(FileManagerError::configuration("A file connection ID is required"));
    }
    let mut configs = storage
        .load_connections()
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connections"))?;
    let before = configs.len();
    configs.retain(|config| config.id != id);
    storage
        .save_connections(&configs)
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to delete the file connection"))?;
    let deleted_legacy = storage
        .delete_file_connection(id)
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to delete the file connection"))?;
    if before != configs.len() || deleted_legacy {
        Ok(())
    } else {
        Err(FileManagerError::new("not_found", "The file connection does not exist"))
    }
}

#[cfg(test)]
pub async fn test_connection(storage: &Storage, request: TestFileConnectionRequest) -> Result<(), FileManagerError> {
    let secrets = resolve_secrets(storage, &request).await?;
    let operator = build_operator(&request.config, &secrets, None)?;
    match tokio::time::timeout(Duration::from_secs(CONNECTION_TEST_TIMEOUT_SECS), operator.check()).await {
        Err(_) => Err(FileManagerError::new("timeout", "The file connection test timed out")),
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            let mapped = map_operation_error(error);
            match mapped.code {
                "configuration" | "unsupported" => Err(mapped),
                _ => Err(FileManagerError::new(
                    "connection_failed",
                    "Could not connect or authenticate with the remote file service",
                )),
            }
        }
    }
}

pub async fn test_connection_config(
    state: &AppState,
    registry: &FileOperatorRegistry,
    connection: &ConnectionConfig,
    secrets: &GenericFileSecretUpdates,
) -> Result<DatabaseConnectionInfo, FileManagerError> {
    let file_config = file_config_from_connection(connection)?;
    let lease = registry.build_transient(state, connection, secrets).await?;
    let tunnel_id = format!("{}:file-test", connection.id);
    let result = match tokio::time::timeout(
        Duration::from_secs(connection.effective_connect_timeout_secs()),
        lease.operator.check(),
    )
    .await
    {
        Err(_) => Err(FileManagerError::new("timeout", "The file connection test timed out")),
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            let mapped = map_operation_error(error);
            match mapped.code {
                "configuration" | "unsupported" => Err(mapped),
                _ => Err(FileManagerError::new(
                    "connection_failed",
                    "Could not connect or authenticate with the remote file service",
                )),
            }
        }
    };
    state.reset_connection_transport_for_config(&tunnel_id, connection).await;
    result?;

    let current_database = match &file_config {
        FileConnectionConfig::S3 { bucket, root, .. } if root.is_empty() => Some(bucket.clone()),
        FileConnectionConfig::S3 { bucket, root, .. } => Some(format!("{bucket}/{root}")),
        FileConnectionConfig::Ftp { root, .. }
        | FileConnectionConfig::Sftp { root, .. }
        | FileConnectionConfig::Webdav { root, .. }
        | FileConnectionConfig::Hdfs { config: HdfsConfig::Webhdfs { root, .. } }
        | FileConnectionConfig::Hdfs { config: HdfsConfig::Native { root, .. } } => Some(root.clone()),
    };
    Ok(DatabaseConnectionInfo {
        product_name: Some(file_config.driver_label().to_string()),
        current_database,
        driver_name: Some("Apache OpenDAL".to_string()),
        ..Default::default()
    })
}

#[cfg(test)]
pub async fn stat_path(storage: &Storage, connection_id: &str, path: &str) -> Result<FileEntry, FileManagerError> {
    let path = validate_remote_path(path)?;
    let operator = operator_for_connection(storage, connection_id).await?;
    let metadata = with_operation_timeout(operator.stat(&path)).await?;
    Ok(entry_from_metadata(&path, &metadata))
}

pub async fn stat_path_cached(
    state: &AppState,
    registry: &FileOperatorRegistry,
    connection_id: &str,
    path: &str,
) -> Result<FileEntry, FileManagerError> {
    let path = validate_remote_path(path)?;
    let lease = registry.get_or_build(state, connection_id).await?;
    let metadata = with_configured_timeout(lease.operation_timeout, lease.operator.stat(&path)).await?;
    Ok(entry_from_metadata(&path, &metadata))
}

#[cfg(test)]
pub async fn list_path(storage: &Storage, connection_id: &str, path: &str) -> Result<Vec<FileEntry>, FileManagerError> {
    let path = validate_remote_path(path)?;
    let directory = if path.is_empty() { String::new() } else { format!("{path}/") };
    let operator = operator_for_connection(storage, connection_id).await?;
    let entries = with_operation_timeout(operator.list(&directory)).await?;
    let mut result = entries
        .into_iter()
        .filter(|entry| entry.path() != directory)
        .map(|entry| {
            let path = validate_remote_path(entry.path())?;
            Ok(entry_from_metadata(&path, entry.metadata()))
        })
        .collect::<Result<Vec<_>, FileManagerError>>()?;
    result.sort_by(|left, right| {
        let left_directory = left.kind == FileEntryKind::Directory;
        let right_directory = right.kind == FileEntryKind::Directory;
        right_directory.cmp(&left_directory).then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
    });
    Ok(result)
}

pub async fn list_path_cached(
    state: &AppState,
    registry: &FileOperatorRegistry,
    connection_id: &str,
    path: &str,
) -> Result<Vec<FileEntry>, FileManagerError> {
    let path = validate_remote_path(path)?;
    let directory = if path.is_empty() { String::new() } else { format!("{path}/") };
    let lease = registry.get_or_build(state, connection_id).await?;
    let entries = with_configured_timeout(lease.operation_timeout, lease.operator.list(&directory)).await?;
    entries_from_listing(&directory, entries)
}

pub fn validate_remote_path(path: &str) -> Result<String, FileManagerError> {
    let normalized = path.trim_end_matches('/');
    validate_path_representation(normalized)?;

    let mut decoded = normalized.to_string();
    for _ in 0..3 {
        let next = percent_decode_str(&decoded)
            .decode_utf8()
            .map_err(|_| FileManagerError::configuration("The remote path contains invalid encoding"))?
            .into_owned();
        validate_path_representation(&next)?;
        if next != decoded && (next.matches('/').count() != decoded.matches('/').count() || next.contains('\\')) {
            return Err(FileManagerError::configuration("The remote path contains an encoded separator"));
        }
        if next == decoded {
            break;
        }
        decoded = next;
    }
    Ok(normalized.to_string())
}

fn validate_path_representation(path: &str) -> Result<(), FileManagerError> {
    if path.is_empty() {
        return Ok(());
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(FileManagerError::configuration("Remote paths must be relative to the connection root"));
    }
    if path.contains('\0') || path.contains('\\') {
        return Err(FileManagerError::configuration("The remote path contains an invalid character"));
    }
    if path.split('/').any(|segment| segment.is_empty() || segment == "." || segment == "..") {
        return Err(FileManagerError::configuration("The remote path contains an invalid segment"));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn operator_for_connection(
    storage: &Storage,
    connection_id: &str,
) -> Result<Operator, FileManagerError> {
    let stored = stored_connection(storage, connection_id).await?;
    let request = TestFileConnectionRequest { id: Some(stored.id), config: stored.config, secrets: Default::default() };
    let secrets = resolve_secrets(storage, &request).await?;
    build_operator(&request.config, &secrets, None)
}

pub(crate) async fn operator_lease_for_connection(
    state: &AppState,
    registry: &FileOperatorRegistry,
    connection_id: &str,
) -> Result<FileOperatorLease, FileManagerError> {
    registry.get_or_build(state, connection_id).await
}

#[cfg(test)]
async fn stored_connection(storage: &Storage, connection_id: &str) -> Result<StoredFileConnection, FileManagerError> {
    if let Some(config) = connection_config(storage, connection_id).await? {
        let file_config = file_config_from_connection(&config)?;
        return Ok(StoredFileConnection { id: config.id, name: config.name, config: file_config });
    }

    let values = storage
        .load_file_connections()
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connections"))?;
    values
        .into_iter()
        .filter_map(|value| serde_json::from_value::<StoredFileConnection>(value).ok())
        .find(|connection| connection.id == connection_id)
        .ok_or_else(|| FileManagerError::new("not_found", "The file connection does not exist"))
}

pub(crate) async fn ensure_writable_connection(storage: &Storage, connection_id: &str) -> Result<(), FileManagerError> {
    if connection_config(storage, connection_id).await?.is_some_and(|config| config.read_only) {
        return Err(FileManagerError::new("read_only", "This File Manager connection is read-only"));
    }
    Ok(())
}

async fn connection_config(
    storage: &Storage,
    connection_id: &str,
) -> Result<Option<ConnectionConfig>, FileManagerError> {
    storage
        .load_connections()
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connections"))
        .map(|configs| {
            configs.into_iter().find(|config| config.id == connection_id && config.db_type == DatabaseType::FileManager)
        })
}

pub(crate) fn file_config_from_connection(config: &ConnectionConfig) -> Result<FileConnectionConfig, FileManagerError> {
    let value = config
        .external_config
        .clone()
        .ok_or_else(|| FileManagerError::configuration("The file protocol configuration is missing"))?;
    let file_config: FileConnectionConfig = serde_json::from_value(value)
        .map_err(|_| FileManagerError::configuration("The file protocol configuration is invalid"))?;
    let expected_profile = file_config.driver_profile();
    if config.driver_profile.as_deref() != Some(expected_profile) {
        return Err(FileManagerError::configuration("The file protocol does not match the selected driver profile"));
    }
    Ok(file_config)
}

#[cfg(test)]
async fn with_operation_timeout<T>(
    operation: impl std::future::Future<Output = opendal::Result<T>>,
) -> Result<T, FileManagerError> {
    match tokio::time::timeout(Duration::from_secs(FILE_OPERATION_TIMEOUT_SECS), operation).await {
        Err(_) => Err(FileManagerError::new("timeout", "The remote file operation timed out")),
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(map_operation_error(error)),
    }
}

pub(crate) async fn with_configured_timeout<T>(
    timeout: Option<Duration>,
    operation: impl std::future::Future<Output = opendal::Result<T>>,
) -> Result<T, FileManagerError> {
    match timeout {
        Some(timeout) => match tokio::time::timeout(timeout, operation).await {
            Err(_) => Err(FileManagerError::new("timeout", "The remote file operation timed out")),
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_operation_error(error)),
        },
        None => operation.await.map_err(map_operation_error),
    }
}

fn entries_from_listing(directory: &str, entries: Vec<opendal::Entry>) -> Result<Vec<FileEntry>, FileManagerError> {
    let mut result = entries
        .into_iter()
        .filter(|entry| entry.path() != directory)
        .map(|entry| {
            let path = validate_remote_path(entry.path())?;
            Ok(entry_from_metadata(&path, entry.metadata()))
        })
        .collect::<Result<Vec<_>, FileManagerError>>()?;
    result.sort_by(|left, right| {
        let left_directory = left.kind == FileEntryKind::Directory;
        let right_directory = right.kind == FileEntryKind::Directory;
        right_directory.cmp(&left_directory).then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
    });
    Ok(result)
}

fn entry_from_metadata(path: &str, metadata: &Metadata) -> FileEntry {
    let name = path.rsplit('/').next().filter(|name| !name.is_empty()).unwrap_or("/").to_string();
    let kind = match metadata.mode() {
        EntryMode::FILE => FileEntryKind::File,
        EntryMode::DIR => FileEntryKind::Directory,
        _ => FileEntryKind::Unknown,
    };
    FileEntry {
        path: path.to_string(),
        name,
        kind,
        size: metadata.content_length(),
        modified_at: metadata.last_modified().map(|value| value.to_string()),
    }
}

#[cfg(test)]
fn validate_identity(id: &str, name: &str) -> Result<(), FileManagerError> {
    if id.trim().is_empty() {
        return Err(FileManagerError::configuration("A file connection ID is required"));
    }
    if id.len() > 128 || id.chars().any(|character| character.is_control()) {
        return Err(FileManagerError::configuration("The file connection ID is invalid"));
    }
    if name.trim().is_empty() {
        return Err(FileManagerError::configuration("A file connection name is required"));
    }
    Ok(())
}

fn public_connection(stored: StoredFileConnection, secret_status: FileSecretStatus) -> FileConnection {
    FileConnection {
        id: stored.id,
        name: stored.name,
        capabilities: FileCapabilities::for_config(&stored.config),
        config: stored.config,
        secret_status,
    }
}

#[cfg(test)]
mod tests {
    use dbx_core::storage::Storage;

    use super::{
        delete_connection, list_connections, list_path, operator_for_connection, save_connection, stat_path,
        test_connection, validate_remote_path,
    };
    use crate::file_manager::models::{
        FileConnectionConfig, FileSecretUpdates, HdfsConfig, SaveFileConnectionRequest, SecretUpdate,
        SftpAuthentication, TestFileConnectionRequest, WebdavAuthentication,
    };

    async fn storage(label: &str) -> Storage {
        let path = std::env::temp_dir().join(format!("dbx-file-manager-{label}-{}.db", uuid::Uuid::new_v4()));
        Storage::open(&path).await.unwrap()
    }

    fn ftp_request(id: &str, password: SecretUpdate) -> SaveFileConnectionRequest {
        SaveFileConnectionRequest {
            id: id.to_string(),
            name: "Local FTP".to_string(),
            config: FileConnectionConfig::Ftp {
                endpoint: "127.0.0.1".to_string(),
                port: 2121,
                root: "/ftp/dbx/".to_string(),
                username: "dbx".to_string(),
            },
            secrets: FileSecretUpdates { password, ..FileSecretUpdates::default() },
        }
    }

    #[tokio::test]
    async fn file_connection_queries_never_return_secret_values() {
        let storage = storage("secret-redaction").await;
        save_connection(&storage, ftp_request("ftp-1", SecretUpdate::Set("top-secret".to_string()))).await.unwrap();

        let values = storage.load_file_connections().await.unwrap();
        assert!(!serde_json::to_string(&values).unwrap().contains("top-secret"));
        let connections = list_connections(&storage).await.unwrap();
        assert!(connections[0].secret_status.password);
    }

    #[tokio::test]
    async fn empty_edit_preserves_secret_and_clear_is_explicit() {
        let storage = storage("secret-update").await;
        save_connection(&storage, ftp_request("ftp-1", SecretUpdate::Set("top-secret".to_string()))).await.unwrap();
        save_connection(&storage, ftp_request("ftp-1", SecretUpdate::Keep)).await.unwrap();
        assert_eq!(
            storage.get_file_connection_secret("ftp-1", "password").await.unwrap().as_deref(),
            Some("top-secret")
        );

        let saved = save_connection(&storage, ftp_request("ftp-1", SecretUpdate::Clear)).await.unwrap();
        assert!(!saved.secret_status.password);
        assert_eq!(storage.get_file_connection_secret("ftp-1", "password").await.unwrap(), None);
    }

    #[tokio::test]
    async fn sftp_private_key_is_required_and_never_stored_in_public_config() {
        let storage = storage("sftp-secret").await;
        let mut request = SaveFileConnectionRequest {
            id: "sftp-1".to_string(),
            name: "Local SFTP".to_string(),
            config: FileConnectionConfig::Sftp {
                endpoint: "127.0.0.1".to_string(),
                port: 2222,
                root: "/config".to_string(),
                username: "dbx".to_string(),
                authentication: SftpAuthentication::PrivateKey,
            },
            secrets: FileSecretUpdates::default(),
        };
        assert_eq!(save_connection(&storage, request.clone()).await.unwrap_err().code, "configuration");

        request.secrets.private_key = SecretUpdate::Set("/secret/path/id_ed25519".to_string());
        let saved = save_connection(&storage, request).await.unwrap();
        assert!(saved.secret_status.private_key);
        let public = serde_json::to_string(&list_connections(&storage).await.unwrap()).unwrap();
        assert!(!public.contains("/secret/path/id_ed25519"));
        let stored = serde_json::to_string(&storage.load_file_connections().await.unwrap()).unwrap();
        assert!(!stored.contains("/secret/path/id_ed25519"));
    }

    #[tokio::test]
    async fn s3_credentials_are_required_and_never_stored_in_public_config() {
        let storage = storage("s3-secrets").await;
        let mut request = SaveFileConnectionRequest {
            id: "s3-1".to_string(),
            name: "Local S3".to_string(),
            config: FileConnectionConfig::S3 {
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: "us-east-1".to_string(),
                bucket: "dbx".to_string(),
                root: "/root/".to_string(),
                path_style: true,
            },
            secrets: FileSecretUpdates::default(),
        };
        assert_eq!(save_connection(&storage, request.clone()).await.unwrap_err().code, "configuration");

        request.secrets.access_key = SecretUpdate::Set("secret-access".to_string());
        request.secrets.secret_key = SecretUpdate::Set("secret-value".to_string());
        request.secrets.session_token = SecretUpdate::Set("secret-session".to_string());
        let saved = save_connection(&storage, request).await.unwrap();
        assert!(saved.secret_status.access_key);
        assert!(saved.secret_status.secret_key);
        assert!(saved.secret_status.session_token);
        let public = serde_json::to_string(&list_connections(&storage).await.unwrap()).unwrap();
        for secret in ["secret-access", "secret-value", "secret-session"] {
            assert!(!public.contains(secret));
        }
        let stored = serde_json::to_string(&storage.load_file_connections().await.unwrap()).unwrap();
        for secret in ["secret-access", "secret-value", "secret-session"] {
            assert!(!stored.contains(secret));
        }
    }

    #[tokio::test]
    async fn webdav_authentication_secrets_are_required_exclusive_and_redacted() {
        let storage = storage("webdav-secrets").await;
        let request = |id: &str, authentication, secrets| SaveFileConnectionRequest {
            id: id.to_string(),
            name: "Local WebDAV".to_string(),
            config: FileConnectionConfig::Webdav {
                endpoint: "http://127.0.0.1:8080".to_string(),
                root: "/".to_string(),
                authentication,
            },
            secrets,
        };

        let basic = request(
            "webdav-basic",
            WebdavAuthentication::Basic { username: "dbx".to_string() },
            FileSecretUpdates::default(),
        );
        assert_eq!(save_connection(&storage, basic.clone()).await.unwrap_err().code, "configuration");
        let mut basic_with_password = basic;
        basic_with_password.secrets.password = SecretUpdate::Set("secret-basic".to_string());
        let saved = save_connection(&storage, basic_with_password).await.unwrap();
        assert!(saved.secret_status.password);
        assert!(!saved.secret_status.bearer_token);

        let bearer = request("webdav-bearer", WebdavAuthentication::Bearer, FileSecretUpdates::default());
        assert_eq!(save_connection(&storage, bearer.clone()).await.unwrap_err().code, "configuration");
        let mut bearer_with_token = bearer;
        bearer_with_token.secrets.bearer_token = SecretUpdate::Set("secret-bearer".to_string());
        let saved = save_connection(&storage, bearer_with_token).await.unwrap();
        assert!(saved.secret_status.bearer_token);
        assert!(!saved.secret_status.password);

        let public = serde_json::to_string(&list_connections(&storage).await.unwrap()).unwrap();
        let stored = serde_json::to_string(&storage.load_file_connections().await.unwrap()).unwrap();
        for secret in ["secret-basic", "secret-bearer"] {
            assert!(!public.contains(secret));
            assert!(!stored.contains(secret));
        }
    }

    #[tokio::test]
    async fn webhdfs_authentication_is_structured_and_delegation_token_is_redacted() {
        let storage = storage("webhdfs-secrets").await;
        let request = |id: &str, simple_user: &str, use_delegation_token, delegation_token| SaveFileConnectionRequest {
            id: id.to_string(),
            name: "Local HDFS".to_string(),
            config: FileConnectionConfig::Hdfs {
                config: HdfsConfig::Webhdfs {
                    endpoint: "http://127.0.0.1:9870".to_string(),
                    root: "/".to_string(),
                    simple_user: simple_user.to_string(),
                    use_delegation_token,
                },
            },
            secrets: FileSecretUpdates { delegation_token, ..Default::default() },
        };

        assert_eq!(
            save_connection(&storage, request("webhdfs-simple-invalid", "", false, SecretUpdate::Keep))
                .await
                .unwrap_err()
                .code,
            "configuration"
        );
        let simple =
            save_connection(&storage, request("webhdfs-simple", "dbx", false, SecretUpdate::Clear)).await.unwrap();
        assert!(matches!(
            simple.config,
            FileConnectionConfig::Hdfs { config: HdfsConfig::Webhdfs { use_delegation_token: false, .. } }
        ));

        assert_eq!(
            save_connection(&storage, request("webhdfs-token", "", true, SecretUpdate::Keep)).await.unwrap_err().code,
            "configuration"
        );
        let token = save_connection(
            &storage,
            request("webhdfs-token", "", true, SecretUpdate::Set("secret-delegation-token".to_string())),
        )
        .await
        .unwrap();
        assert!(token.secret_status.delegation_token);
        let public = serde_json::to_string(&list_connections(&storage).await.unwrap()).unwrap();
        let stored = serde_json::to_string(&storage.load_file_connections().await.unwrap()).unwrap();
        assert!(!public.contains("secret-delegation-token"));
        assert!(!stored.contains("secret-delegation-token"));
    }

    #[tokio::test]
    async fn native_hdfs_reuses_the_hdfs_product_contract_and_requires_config_directory() {
        let storage = storage("native-hdfs-config").await;
        let hadoop_config_directory =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../deploy/file-manager/config/hadoop/client");
        save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "hdfs-native".to_string(),
                name: "Local HDFS".to_string(),
                config: FileConnectionConfig::Hdfs {
                    config: HdfsConfig::Webhdfs {
                        endpoint: "http://127.0.0.1:9870".to_string(),
                        root: "/".to_string(),
                        simple_user: String::new(),
                        use_delegation_token: true,
                    },
                },
                secrets: FileSecretUpdates {
                    delegation_token: SecretUpdate::Set("old-delegation-token".to_string()),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
        assert!(storage.get_file_connection_secret("hdfs-native", "delegation_token").await.unwrap().is_some());
        let request = |directory: String| SaveFileConnectionRequest {
            id: "hdfs-native".to_string(),
            name: "Local HDFS".to_string(),
            config: FileConnectionConfig::Hdfs {
                config: HdfsConfig::Native {
                    name_node_uri: "hdfs://127.0.0.1:19000".to_string(),
                    root: "/".to_string(),
                    hadoop_config_directory: directory,
                },
            },
            secrets: FileSecretUpdates::default(),
        };
        assert_eq!(save_connection(&storage, request(String::new())).await.unwrap_err().code, "configuration");
        let saved = save_connection(
            &storage,
            request(hadoop_config_directory.canonicalize().unwrap().to_string_lossy().to_string()),
        )
        .await
        .unwrap();
        assert!(matches!(saved.config, FileConnectionConfig::Hdfs { config: HdfsConfig::Native { .. } }));
        assert!(!saved.capabilities.native_copy);
        assert!(saved.capabilities.native_rename);
        assert!(saved.capabilities.atomic_rename);
        assert!(!saved.secret_status.delegation_token);
        assert_eq!(storage.get_file_connection_secret("hdfs-native", "delegation_token").await.unwrap(), None);
    }

    #[tokio::test]
    async fn deleting_a_file_connection_removes_its_secrets_only() {
        let storage = storage("delete").await;
        save_connection(&storage, ftp_request("ftp-1", SecretUpdate::Set("one".to_string()))).await.unwrap();
        save_connection(&storage, ftp_request("ftp-2", SecretUpdate::Set("two".to_string()))).await.unwrap();

        delete_connection(&storage, "ftp-1").await.unwrap();
        assert_eq!(storage.get_file_connection_secret("ftp-1", "password").await.unwrap(), None);
        assert_eq!(storage.get_file_connection_secret("ftp-2", "password").await.unwrap().as_deref(), Some("two"));
    }

    #[test]
    fn remote_paths_cannot_escape_the_connection_root() {
        for path in [
            "/absolute",
            "\\absolute",
            ".",
            "..",
            "a/../b",
            "a/./b",
            "a//b",
            "%2e%2e/file",
            "%252e%252e/file",
            "a%2fb",
            "a\0b",
        ] {
            assert!(validate_remote_path(path).is_err(), "{path:?} should be rejected");
        }
        for path in ["", "folder", "folder/file.txt", "folder/"] {
            assert!(validate_remote_path(path).is_ok(), "{path:?} should be accepted");
        }
    }

    #[tokio::test]
    #[ignore = "requires deploy/file-manager FTP service"]
    async fn ftp_connection_lifecycle_contract() {
        let storage = storage("ftp-contract").await;
        save_connection(&storage, ftp_request("ftp-contract", SecretUpdate::Set("dbx-password".to_string())))
            .await
            .unwrap();

        test_connection(
            &storage,
            TestFileConnectionRequest {
                id: Some("ftp-contract".to_string()),
                config: ftp_request("ftp-contract", SecretUpdate::Keep).config,
                secrets: FileSecretUpdates::default(),
            },
        )
        .await
        .unwrap();

        let mut edited = ftp_request("ftp-contract", SecretUpdate::Keep);
        edited.name = "Edited FTP".to_string();
        save_connection(&storage, edited).await.unwrap();
        assert_eq!(list_connections(&storage).await.unwrap()[0].name, "Edited FTP");
        delete_connection(&storage, "ftp-contract").await.unwrap();
        assert!(list_connections(&storage).await.unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires deploy/file-manager FTP service"]
    async fn ftp_browse_contract() {
        let storage = storage("ftp-browse-contract").await;
        save_connection(&storage, ftp_request("ftp-browse", SecretUpdate::Set("dbx-password".to_string())))
            .await
            .unwrap();
        let operator = operator_for_connection(&storage, "ftp-browse").await.unwrap();
        let directory = format!("browse-{}/", uuid::Uuid::new_v4());
        let file = format!("{directory}fixture.txt");
        operator.create_dir(&directory).await.unwrap();
        operator.write(&file, b"browse fixture".to_vec()).await.unwrap();

        let listed = list_path(&storage, "ftp-browse", directory.trim_end_matches('/')).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path, file);
        let stat = stat_path(&storage, "ftp-browse", &file).await.unwrap();
        assert_eq!(stat.kind, crate::file_manager::models::FileEntryKind::File);
        assert_eq!(stat.size, b"browse fixture".len() as u64);

        operator.delete(&file).await.unwrap();
        operator.delete(&directory).await.unwrap();
    }
}
