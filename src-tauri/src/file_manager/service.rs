use std::time::Duration;

use dbx_core::storage::Storage;
use opendal::{EntryMode, Metadata, Operator};
use percent_encoding::percent_decode_str;

use super::adapter::{build_operator, map_operation_error, resolve_secrets};
use super::models::{
    FileCapabilities, FileConnection, FileEntry, FileEntryKind, FileManagerError, FileSecretStatus,
    SaveFileConnectionRequest, StoredFileConnection, TestFileConnectionRequest,
};

const CONNECTION_TEST_TIMEOUT_SECS: u64 = 15;
const FILE_OPERATION_TIMEOUT_SECS: u64 = 60;

pub async fn list_connections(storage: &Storage) -> Result<Vec<FileConnection>, FileManagerError> {
    let values = storage
        .load_file_connections()
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to load file connections"))?;
    let mut connections = Vec::with_capacity(values.len());
    for value in values {
        let stored: StoredFileConnection = serde_json::from_value(value)
            .map_err(|_| FileManagerError::new("storage", "A saved file connection is invalid"))?;
        let keys = storage
            .file_connection_secret_keys(&stored.id)
            .await
            .map_err(|_| FileManagerError::new("storage", "Failed to load file connection credential status"))?;
        connections.push(public_connection(stored, FileSecretStatus::from_keys(&keys)));
    }
    Ok(connections)
}

pub async fn save_connection(
    storage: &Storage,
    request: SaveFileConnectionRequest,
) -> Result<FileConnection, FileManagerError> {
    validate_identity(&request.id, &request.name)?;
    let validation_request = TestFileConnectionRequest {
        id: Some(request.id.clone()),
        config: request.config.clone(),
        secrets: request.secrets.clone(),
    };
    let resolved = resolve_secrets(storage, &validation_request).await?;
    build_operator(&request.config, &resolved)?;
    let updates = request.secrets.persistence_updates()?;
    let stored = StoredFileConnection { id: request.id, name: request.name, config: request.config };
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

pub async fn delete_connection(storage: &Storage, id: &str) -> Result<(), FileManagerError> {
    if id.trim().is_empty() {
        return Err(FileManagerError::configuration("A file connection ID is required"));
    }
    let deleted = storage
        .delete_file_connection(id)
        .await
        .map_err(|_| FileManagerError::new("storage", "Failed to delete the file connection"))?;
    if deleted {
        Ok(())
    } else {
        Err(FileManagerError::new("not_found", "The file connection does not exist"))
    }
}

pub async fn test_connection(storage: &Storage, request: TestFileConnectionRequest) -> Result<(), FileManagerError> {
    let secrets = resolve_secrets(storage, &request).await?;
    let operator = build_operator(&request.config, &secrets)?;
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

pub async fn stat_path(storage: &Storage, connection_id: &str, path: &str) -> Result<FileEntry, FileManagerError> {
    let path = validate_remote_path(path)?;
    let operator = operator_for_connection(storage, connection_id).await?;
    let metadata = with_operation_timeout(operator.stat(&path)).await?;
    Ok(entry_from_metadata(&path, &metadata))
}

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

pub(crate) async fn operator_for_connection(
    storage: &Storage,
    connection_id: &str,
) -> Result<Operator, FileManagerError> {
    let stored = stored_connection(storage, connection_id).await?;
    let request = TestFileConnectionRequest { id: Some(stored.id), config: stored.config, secrets: Default::default() };
    let secrets = resolve_secrets(storage, &request).await?;
    build_operator(&request.config, &secrets)
}

async fn stored_connection(storage: &Storage, connection_id: &str) -> Result<StoredFileConnection, FileManagerError> {
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

async fn with_operation_timeout<T>(
    operation: impl std::future::Future<Output = opendal::Result<T>>,
) -> Result<T, FileManagerError> {
    match tokio::time::timeout(Duration::from_secs(FILE_OPERATION_TIMEOUT_SECS), operation).await {
        Err(_) => Err(FileManagerError::new("timeout", "The remote file operation timed out")),
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(map_operation_error(error)),
    }
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
        FileConnectionConfig, FileSecretUpdates, SaveFileConnectionRequest, SecretUpdate, SftpAuthentication,
        TestFileConnectionRequest,
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
