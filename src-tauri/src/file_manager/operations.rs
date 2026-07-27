use dbx_core::storage::Storage;
use opendal::{ErrorKind, Operator};
use std::future::Future;
use std::time::Duration;
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

use super::adapter::map_operation_error;
use super::models::{FileManagerError, FileRemoteOperationRequest};
use super::service::{ensure_writable_connection, operator_for_connection};
use super::transfer::{
    copy_with_fixed_buffer, non_root_remote_path, transfer_timeout, FileTransferState, TRANSFER_TIMEOUT_SECS,
};

pub async fn copy(
    storage: &Storage,
    state: &FileTransferState,
    request: FileRemoteOperationRequest,
) -> Result<(), FileManagerError> {
    ensure_writable_connection(storage, &request.connection_id).await?;
    let source = non_root_remote_path(&request.source_path)?;
    let destination = destination_path(&source, &request.destination_path)?;
    let operator = operator_for_connection(storage, &request.connection_id).await?;
    let _permits = state.acquire(&request.connection_id).await?;
    transfer_timeout(copy_with_operator(&operator, &source, &destination, request.replace)).await
}

pub async fn rename(
    storage: &Storage,
    state: &FileTransferState,
    request: FileRemoteOperationRequest,
) -> Result<(), FileManagerError> {
    ensure_writable_connection(storage, &request.connection_id).await?;
    let source = non_root_remote_path(&request.source_path)?;
    let destination = destination_path(&source, &request.destination_path)?;
    let operator = operator_for_connection(storage, &request.connection_id).await?;
    let _permits = state.acquire(&request.connection_id).await?;
    if operator.info().full_capability().rename {
        transfer_timeout(async {
            ensure_source_file(&operator, &source).await?;
            ensure_destination(&operator, &destination, request.replace).await?;
            operator.rename(&source, &destination).await.map_err(map_operation_error)
        })
        .await
    } else {
        fallback_rename_with_delete(
            &operator,
            &source,
            &destination,
            request.replace,
            Duration::from_secs(TRANSFER_TIMEOUT_SECS),
            || operator.delete(&source),
        )
        .await
    }
}

pub(crate) async fn fallback_rename_with_delete<F, Fut>(
    operator: &Operator,
    source: &str,
    destination: &str,
    replace: bool,
    timeout: Duration,
    delete_source: F,
) -> Result<(), FileManagerError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), opendal::Error>>,
{
    let mut copied = false;
    let result = tokio::time::timeout(timeout, async {
        copy_with_operator(operator, source, destination, replace).await?;
        copied = true;
        delete_source().await.map_err(|_| partial_rename_error(source, destination))
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) if copied => Err(partial_rename_error(source, destination)),
        Err(_) => Err(FileManagerError::new("timeout", "The file transfer timed out")),
    }
}

async fn copy_with_operator(
    operator: &Operator,
    source: &str,
    destination: &str,
    replace: bool,
) -> Result<(), FileManagerError> {
    ensure_source_file(operator, source).await?;
    ensure_destination(operator, destination, replace).await?;
    let capability = operator.info().full_capability();
    if capability.copy {
        let result = if !replace && capability.copy_with_if_not_exists {
            operator.copy_with(source, destination).if_not_exists(true).await
        } else {
            operator.copy(source, destination).await
        };
        result.map(|_| ()).map_err(map_operation_error)
    } else {
        stream_copy(operator, source, destination, replace).await
    }
}

async fn stream_copy(
    operator: &Operator,
    source: &str,
    destination: &str,
    replace: bool,
) -> Result<(), FileManagerError> {
    let reader = operator
        .reader(source)
        .await
        .map_err(map_operation_error)?
        .into_futures_async_read(..)
        .await
        .map_err(map_operation_error)?
        .compat();
    let capability = operator.info().full_capability();
    let writer = if !replace && capability.write_with_if_not_exists {
        operator.writer_with(destination).if_not_exists(true).await
    } else {
        operator.writer(destination).await
    }
    .map_err(map_operation_error)?
    .into_futures_async_write()
    .compat_write();
    copy_with_fixed_buffer(reader, writer)
        .await
        .map(|_| ())
        .map_err(|_| FileManagerError::new("transfer", "The remote file copy did not complete"))
}

async fn ensure_source_file(operator: &Operator, source: &str) -> Result<(), FileManagerError> {
    let metadata = match operator.stat(source).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => match operator.stat(&format!("{source}/")).await {
            Ok(metadata) => metadata,
            Err(_) => return Err(map_operation_error(error)),
        },
        Err(error) => return Err(map_operation_error(error)),
    };
    if metadata.is_file() {
        Ok(())
    } else {
        Err(FileManagerError::new("unsupported", "Only files can be copied or renamed in this version"))
    }
}

async fn ensure_destination(operator: &Operator, destination: &str, replace: bool) -> Result<(), FileManagerError> {
    if !operator.exists(destination).await.map_err(map_operation_error)? {
        return Ok(());
    }
    if !replace {
        return Err(FileManagerError::new("already_exists", "The remote destination already exists"));
    }
    let metadata = operator.stat(destination).await.map_err(map_operation_error)?;
    if metadata.is_dir() {
        Err(FileManagerError::new("unsupported", "A file operation cannot replace a directory"))
    } else {
        Ok(())
    }
}

fn destination_path(source: &str, destination: &str) -> Result<String, FileManagerError> {
    let destination = non_root_remote_path(destination)?;
    if source == destination {
        Err(FileManagerError::configuration("The source and destination paths must be different"))
    } else {
        Ok(destination)
    }
}

fn partial_rename_error(source: &str, destination: &str) -> FileManagerError {
    let mut error = FileManagerError::new(
        "partial_success",
        "The destination was created, but the source file could not be deleted",
    );
    error.recovery =
        Some(format!("The source remains at '{source}'. Verify '{destination}', then delete the source manually."));
    error
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dbx_core::storage::Storage;
    use opendal::{services, Error, ErrorKind, Operator};
    use serde_json::json;
    use uuid::Uuid;

    use super::{copy, copy_with_operator, fallback_rename_with_delete, rename};
    use crate::file_manager::models::{
        FileConnectionConfig, FileManagerError, FileRemoteOperationRequest, FileSecretUpdates, FileTransferRequest,
        SaveFileConnectionRequest, SecretUpdate,
    };
    use crate::file_manager::service::save_connection;
    use crate::file_manager::transfer::{delete, download, upload, FileTransferState};

    #[test]
    fn operation_request_structurally_allows_only_one_connection() {
        let value = json!({
            "connectionId": "source",
            "destinationConnectionId": "other",
            "sourcePath": "source.txt",
            "destinationPath": "destination.txt"
        });
        assert!(serde_json::from_value::<FileRemoteOperationRequest>(value).is_err());
    }

    #[tokio::test]
    async fn stream_fallback_rejects_overwrite_and_directories() {
        let operator = Operator::new(services::Memory::default()).unwrap().finish();
        assert!(!operator.info().full_capability().copy);
        operator.write("source.txt", "first").await.unwrap();

        copy_with_operator(&operator, "source.txt", "destination.txt", false).await.unwrap();
        assert_eq!(operator.read("destination.txt").await.unwrap().to_vec(), b"first");
        assert_eq!(
            copy_with_operator(&operator, "source.txt", "destination.txt", false).await.unwrap_err().code,
            "already_exists"
        );

        operator.write("source.txt", "replacement").await.unwrap();
        copy_with_operator(&operator, "source.txt", "destination.txt", true).await.unwrap();
        assert_eq!(operator.read("destination.txt").await.unwrap().to_vec(), b"replacement");
        operator.create_dir("folder/").await.unwrap();
        assert_eq!(
            copy_with_operator(&operator, "folder", "folder-copy", false).await.unwrap_err().code,
            "unsupported"
        );
    }

    #[tokio::test]
    async fn fallback_rename_reports_delete_failure_and_timeout_as_partial_success() {
        let operator = Operator::new(services::Memory::default()).unwrap().finish();
        operator.write("delete-fails.txt", "first").await.unwrap();
        let error = fallback_rename_with_delete(
            &operator,
            "delete-fails.txt",
            "copied-before-delete-failure.txt",
            false,
            Duration::from_secs(1),
            || async { Err(Error::new(ErrorKind::PermissionDenied, "injected delete failure")) },
        )
        .await
        .unwrap_err();
        assert_partial_success(&error, "delete-fails.txt", "copied-before-delete-failure.txt");
        assert!(operator.exists("delete-fails.txt").await.unwrap());
        assert!(operator.exists("copied-before-delete-failure.txt").await.unwrap());

        operator.write("delete-times-out.txt", "second").await.unwrap();
        let error = fallback_rename_with_delete(
            &operator,
            "delete-times-out.txt",
            "copied-before-delete-timeout.txt",
            false,
            Duration::from_millis(20),
            || async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            },
        )
        .await
        .unwrap_err();
        assert_partial_success(&error, "delete-times-out.txt", "copied-before-delete-timeout.txt");
        assert!(operator.exists("delete-times-out.txt").await.unwrap());
        assert!(operator.exists("copied-before-delete-timeout.txt").await.unwrap());
    }

    fn assert_partial_success(error: &FileManagerError, source: &str, destination: &str) {
        assert_eq!(error.code, "partial_success");
        assert!(error.message.contains("destination was created"));
        assert!(error
            .recovery
            .as_deref()
            .is_some_and(|recovery| recovery.contains(source) && recovery.contains(destination)));
    }

    #[tokio::test]
    #[ignore = "requires deploy/file-manager FTP service"]
    async fn ftp_copy_rename_fallback_contract() {
        let suffix = Uuid::new_v4();
        let database = std::env::temp_dir().join(format!("dbx-file-operations-{suffix}.db"));
        let local_source = std::env::temp_dir().join(format!("dbx-operation-source-{suffix}.txt"));
        let local_download = std::env::temp_dir().join(format!("dbx-operation-download-{suffix}.txt"));
        let storage = Storage::open(&database).await.unwrap();
        save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "ftp-operations".to_string(),
                name: "FTP Operations".to_string(),
                config: FileConnectionConfig::Ftp {
                    endpoint: "127.0.0.1".to_string(),
                    port: 2121,
                    root: "/ftp/dbx/".to_string(),
                    username: "dbx".to_string(),
                },
                secrets: FileSecretUpdates {
                    password: SecretUpdate::Set("dbx-password".to_string()),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
        tokio::fs::write(&local_source, b"copy and rename fallback").await.unwrap();
        let state = FileTransferState::default();
        let directory = format!("operations-{suffix}");
        let source = format!("{directory}/source.txt");
        let copied = format!("{directory}/copied.txt");
        let renamed = format!("{directory}/renamed.txt");
        upload(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "ftp-operations".to_string(),
                remote_path: source.clone(),
                local_path: local_source.to_string_lossy().to_string(),
                replace: false,
            },
        )
        .await
        .unwrap();

        let copy_request = FileRemoteOperationRequest {
            connection_id: "ftp-operations".to_string(),
            source_path: source.clone(),
            destination_path: copied.clone(),
            replace: false,
        };
        copy(&storage, &state, copy_request.clone()).await.unwrap();
        assert_eq!(copy(&storage, &state, copy_request).await.unwrap_err().code, "already_exists");
        copy(
            &storage,
            &state,
            FileRemoteOperationRequest {
                connection_id: "ftp-operations".to_string(),
                source_path: source.clone(),
                destination_path: copied.clone(),
                replace: true,
            },
        )
        .await
        .unwrap();

        rename(
            &storage,
            &state,
            FileRemoteOperationRequest {
                connection_id: "ftp-operations".to_string(),
                source_path: copied.clone(),
                destination_path: renamed.clone(),
                replace: false,
            },
        )
        .await
        .unwrap();
        download(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "ftp-operations".to_string(),
                remote_path: renamed.clone(),
                local_path: local_download.to_string_lossy().to_string(),
                replace: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(&local_download).await.unwrap(), b"copy and rename fallback");
        assert_eq!(
            download(
                &storage,
                &state,
                FileTransferRequest {
                    connection_id: "ftp-operations".to_string(),
                    remote_path: copied,
                    local_path: local_download.to_string_lossy().to_string(),
                    replace: true,
                },
            )
            .await
            .unwrap_err()
            .code,
            "not_found"
        );
        assert_eq!(
            copy(
                &storage,
                &state,
                FileRemoteOperationRequest {
                    connection_id: "ftp-operations".to_string(),
                    source_path: directory.clone(),
                    destination_path: format!("{directory}-copy"),
                    replace: false,
                },
            )
            .await
            .unwrap_err()
            .code,
            "unsupported"
        );

        delete(&storage, &state, "ftp-operations", &source).await.unwrap();
        delete(&storage, &state, "ftp-operations", &renamed).await.unwrap();
        delete(&storage, &state, "ftp-operations", &directory).await.unwrap();
        let _ = tokio::fs::remove_file(local_source).await;
        let _ = tokio::fs::remove_file(local_download).await;
        let _ = tokio::fs::remove_file(database).await;
    }
}
