use std::time::Duration;

use dbx_core::storage::Storage;

use super::adapter::{build_operator, map_operation_error, resolve_secrets};
use super::models::{
    FileCapabilities, FileConnection, FileManagerError, FileSecretStatus, SaveFileConnectionRequest,
    StoredFileConnection, TestFileConnectionRequest,
};

const CONNECTION_TEST_TIMEOUT_SECS: u64 = 15;

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

    use super::{delete_connection, list_connections, save_connection, test_connection};
    use crate::file_manager::models::{
        FileConnectionConfig, FileSecretUpdates, SaveFileConnectionRequest, SecretUpdate, TestFileConnectionRequest,
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
    async fn deleting_a_file_connection_removes_its_secrets_only() {
        let storage = storage("delete").await;
        save_connection(&storage, ftp_request("ftp-1", SecretUpdate::Set("one".to_string()))).await.unwrap();
        save_connection(&storage, ftp_request("ftp-2", SecretUpdate::Set("two".to_string()))).await.unwrap();

        delete_connection(&storage, "ftp-1").await.unwrap();
        assert_eq!(storage.get_file_connection_secret("ftp-1", "password").await.unwrap(), None);
        assert_eq!(storage.get_file_connection_secret("ftp-2", "password").await.unwrap().as_deref(), Some("two"));
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
}
