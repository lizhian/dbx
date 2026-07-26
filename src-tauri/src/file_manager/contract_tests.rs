#[cfg(unix)]
mod unix {
    use dbx_core::storage::Storage;
    use uuid::Uuid;

    use super::super::models::{
        FileConnectionConfig, FileEntryKind, FileRemoteOperationRequest, FileSecretUpdates, FileTransferRequest,
        SaveFileConnectionRequest, SecretUpdate, SftpAuthentication, TestFileConnectionRequest,
    };
    use super::super::operations;
    use super::super::service;
    use super::super::transfer::{self, FileTransferState};

    #[tokio::test]
    #[ignore = "requires deploy/file-manager SFTP service and generated private key"]
    async fn sftp_private_key_seven_operation_contract() {
        let suffix = Uuid::new_v4();
        let database = std::env::temp_dir().join(format!("dbx-sftp-contract-{suffix}.db"));
        let local_source = std::env::temp_dir().join(format!("dbx-sftp-source-{suffix}.txt"));
        let local_download = std::env::temp_dir().join(format!("dbx-sftp-download-{suffix}.txt"));
        let private_key = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../deploy/file-manager/runtime/sftp/id_ed25519")
            .canonicalize()
            .expect("run deploy/file-manager/setup.sh before the SFTP contract");
        let storage = Storage::open(&database).await.unwrap();
        let config = FileConnectionConfig::Sftp {
            endpoint: "127.0.0.1".to_string(),
            port: 2222,
            root: "/config".to_string(),
            username: "dbx".to_string(),
            authentication: SftpAuthentication::PrivateKey,
        };
        let saved = service::save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "sftp-contract".to_string(),
                name: "SFTP Contract".to_string(),
                config: config.clone(),
                secrets: FileSecretUpdates {
                    private_key: SecretUpdate::Set(private_key.to_string_lossy().to_string()),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
        assert!(saved.secret_status.private_key);
        assert!(saved.capabilities.native_copy);
        assert!(saved.capabilities.native_rename);
        assert!(saved.capabilities.atomic_rename);
        service::test_connection(
            &storage,
            TestFileConnectionRequest {
                id: Some("sftp-contract".to_string()),
                config,
                secrets: FileSecretUpdates::default(),
            },
        )
        .await
        .unwrap();
        let edited = service::save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "sftp-contract".to_string(),
                name: "Edited SFTP Contract".to_string(),
                config: FileConnectionConfig::Sftp {
                    endpoint: "127.0.0.1".to_string(),
                    port: 2222,
                    root: "/config".to_string(),
                    username: "dbx".to_string(),
                    authentication: SftpAuthentication::PrivateKey,
                },
                secrets: FileSecretUpdates::default(),
            },
        )
        .await
        .unwrap();
        assert_eq!(edited.name, "Edited SFTP Contract");
        assert!(edited.secret_status.private_key);

        let operator = service::operator_for_connection(&storage, "sftp-contract").await.unwrap();
        assert!(operator.info().full_capability().copy);
        assert!(operator.info().full_capability().rename);
        let directory = format!("dbx-sftp-{suffix}");
        operator.create_dir(&format!("{directory}/")).await.unwrap();
        let source = format!("{directory}/source.txt");
        let copied = format!("{directory}/copied.txt");
        let renamed = format!("{directory}/renamed.txt");
        tokio::fs::write(&local_source, b"SFTP seven-operation contract").await.unwrap();
        let state = FileTransferState::default();

        transfer::upload(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "sftp-contract".to_string(),
                remote_path: source.clone(),
                local_path: local_source.to_string_lossy().to_string(),
                replace: false,
            },
        )
        .await
        .unwrap();
        let stat = service::stat_path(&storage, "sftp-contract", &source).await.unwrap();
        assert_eq!(stat.kind, FileEntryKind::File);
        assert_eq!(stat.size, b"SFTP seven-operation contract".len() as u64);
        assert!(service::list_path(&storage, "sftp-contract", &directory)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.path == source));

        operations::copy(
            &storage,
            &state,
            FileRemoteOperationRequest {
                connection_id: "sftp-contract".to_string(),
                source_path: source.clone(),
                destination_path: copied.clone(),
                replace: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(service::stat_path(&storage, "sftp-contract", &source).await.unwrap().kind, FileEntryKind::File);
        operations::rename(
            &storage,
            &state,
            FileRemoteOperationRequest {
                connection_id: "sftp-contract".to_string(),
                source_path: copied.clone(),
                destination_path: renamed.clone(),
                replace: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(service::stat_path(&storage, "sftp-contract", &copied).await.unwrap_err().code, "not_found");
        transfer::download(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "sftp-contract".to_string(),
                remote_path: renamed.clone(),
                local_path: local_download.to_string_lossy().to_string(),
                replace: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(&local_download).await.unwrap(), b"SFTP seven-operation contract");

        transfer::delete(&storage, &state, "sftp-contract", &source).await.unwrap();
        transfer::delete(&storage, &state, "sftp-contract", &renamed).await.unwrap();
        transfer::delete(&storage, &state, "sftp-contract", &directory).await.unwrap();
        service::delete_connection(&storage, "sftp-contract").await.unwrap();
        assert!(service::list_connections(&storage).await.unwrap().is_empty());
        let _ = tokio::fs::remove_file(local_source).await;
        let _ = tokio::fs::remove_file(local_download).await;
        let _ = tokio::fs::remove_file(database).await;
    }
}

mod s3 {
    use std::time::Duration;

    use dbx_core::storage::Storage;
    use opendal::{Error, ErrorKind};
    use uuid::Uuid;

    use super::super::models::{
        FileConnectionConfig, FileEntryKind, FileRemoteOperationRequest, FileSecretUpdates, FileTransferRequest,
        SaveFileConnectionRequest, SecretUpdate, TestFileConnectionRequest,
    };
    use super::super::operations;
    use super::super::service;
    use super::super::transfer::{self, FileTransferState};

    #[tokio::test]
    #[ignore = "requires deploy/file-manager MinIO service"]
    async fn s3_path_style_seven_operation_contract() {
        let suffix = Uuid::new_v4();
        let database = std::env::temp_dir().join(format!("dbx-s3-contract-{suffix}.db"));
        let local_source = std::env::temp_dir().join(format!("dbx-s3-source-{suffix}.txt"));
        let local_download = std::env::temp_dir().join(format!("dbx-s3-download-{suffix}.txt"));
        let storage = Storage::open(&database).await.unwrap();
        let config = FileConnectionConfig::S3 {
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "us-east-1".to_string(),
            bucket: "dbx".to_string(),
            root: "/root/".to_string(),
            path_style: true,
        };
        let saved = service::save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "s3-contract".to_string(),
                name: "S3 Contract".to_string(),
                config: config.clone(),
                secrets: FileSecretUpdates {
                    access_key: SecretUpdate::Set("dbx-access-key".to_string()),
                    secret_key: SecretUpdate::Set("dbx-secret-key".to_string()),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
        assert!(matches!(saved.config, FileConnectionConfig::S3 { path_style: true, .. }));
        assert!(saved.capabilities.native_copy);
        assert!(!saved.capabilities.native_rename);
        assert!(!saved.capabilities.atomic_rename);
        service::test_connection(
            &storage,
            TestFileConnectionRequest {
                id: Some("s3-contract".to_string()),
                config: config.clone(),
                secrets: FileSecretUpdates::default(),
            },
        )
        .await
        .unwrap();
        let edited = service::save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "s3-contract".to_string(),
                name: "Edited S3 Contract".to_string(),
                config,
                secrets: FileSecretUpdates::default(),
            },
        )
        .await
        .unwrap();
        assert_eq!(edited.name, "Edited S3 Contract");
        assert!(edited.secret_status.access_key && edited.secret_status.secret_key);

        let operator = service::operator_for_connection(&storage, "s3-contract").await.unwrap();
        assert!(operator.info().full_capability().copy);
        assert!(!operator.info().full_capability().rename);
        let directory = format!("dbx-s3-{suffix}");
        let source = format!("{directory}/source.txt");
        let copied = format!("{directory}/copied.txt");
        let renamed = format!("{directory}/renamed.txt");
        let partial = format!("{directory}/partial.txt");
        tokio::fs::write(&local_source, b"S3 seven-operation contract").await.unwrap();
        let state = FileTransferState::default();
        transfer::upload(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "s3-contract".to_string(),
                remote_path: source.clone(),
                local_path: local_source.to_string_lossy().to_string(),
                replace: false,
            },
        )
        .await
        .unwrap();
        let stat = service::stat_path(&storage, "s3-contract", &source).await.unwrap();
        assert_eq!(stat.kind, FileEntryKind::File);
        assert_eq!(stat.size, b"S3 seven-operation contract".len() as u64);
        assert!(service::list_path(&storage, "s3-contract", &directory)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.path == source));

        let copy_request = FileRemoteOperationRequest {
            connection_id: "s3-contract".to_string(),
            source_path: source.clone(),
            destination_path: copied.clone(),
            replace: false,
        };
        operations::copy(&storage, &state, copy_request.clone()).await.unwrap();
        assert_eq!(operations::copy(&storage, &state, copy_request).await.unwrap_err().code, "already_exists");
        operations::copy(
            &storage,
            &state,
            FileRemoteOperationRequest {
                connection_id: "s3-contract".to_string(),
                source_path: source.clone(),
                destination_path: copied.clone(),
                replace: true,
            },
        )
        .await
        .unwrap();
        operations::rename(
            &storage,
            &state,
            FileRemoteOperationRequest {
                connection_id: "s3-contract".to_string(),
                source_path: copied.clone(),
                destination_path: renamed.clone(),
                replace: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(service::stat_path(&storage, "s3-contract", &copied).await.unwrap_err().code, "not_found");

        let error = operations::fallback_rename_with_delete(
            &operator,
            &source,
            &partial,
            false,
            Duration::from_secs(5),
            || async { Err(Error::new(ErrorKind::PermissionDenied, "injected S3 delete failure")) },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "partial_success");
        assert!(error
            .recovery
            .as_deref()
            .is_some_and(|recovery| { recovery.contains(&source) && recovery.contains(&partial) }));
        assert_eq!(service::stat_path(&storage, "s3-contract", &source).await.unwrap().kind, FileEntryKind::File);
        assert_eq!(service::stat_path(&storage, "s3-contract", &partial).await.unwrap().kind, FileEntryKind::File);

        transfer::download(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "s3-contract".to_string(),
                remote_path: renamed.clone(),
                local_path: local_download.to_string_lossy().to_string(),
                replace: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(&local_download).await.unwrap(), b"S3 seven-operation contract");

        transfer::delete(&storage, &state, "s3-contract", &source).await.unwrap();
        transfer::delete(&storage, &state, "s3-contract", &renamed).await.unwrap();
        transfer::delete(&storage, &state, "s3-contract", &partial).await.unwrap();
        service::delete_connection(&storage, "s3-contract").await.unwrap();
        assert!(service::list_connections(&storage).await.unwrap().is_empty());
        let _ = tokio::fs::remove_file(local_source).await;
        let _ = tokio::fs::remove_file(local_download).await;
        let _ = tokio::fs::remove_file(database).await;
    }
}

mod webdav {
    use dbx_core::storage::Storage;
    use uuid::Uuid;

    use super::super::models::{
        FileConnectionConfig, FileEntryKind, FileRemoteOperationRequest, FileSecretUpdates, FileTransferRequest,
        SaveFileConnectionRequest, SecretUpdate, TestFileConnectionRequest, WebdavAuthentication,
    };
    use super::super::operations;
    use super::super::service;
    use super::super::transfer::{self, FileTransferState};

    #[tokio::test]
    #[ignore = "requires deploy/file-manager WebDAV service"]
    async fn webdav_basic_seven_operation_and_path_boundary_contract() {
        let suffix = Uuid::new_v4();
        let database = std::env::temp_dir().join(format!("dbx-webdav-contract-{suffix}.db"));
        let local_source = std::env::temp_dir().join(format!("dbx-webdav-source-{suffix}.txt"));
        let local_download = std::env::temp_dir().join(format!("dbx-webdav-download-{suffix}.txt"));
        let storage = Storage::open(&database).await.unwrap();
        let config = FileConnectionConfig::Webdav {
            endpoint: "http://127.0.0.1:8080".to_string(),
            root: "/".to_string(),
            authentication: WebdavAuthentication::Basic { username: "dbx".to_string() },
        };
        let saved = service::save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "webdav-contract".to_string(),
                name: "WebDAV Contract".to_string(),
                config: config.clone(),
                secrets: FileSecretUpdates {
                    password: SecretUpdate::Set("dbx-password".to_string()),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
        assert!(saved.capabilities.native_copy);
        assert!(saved.capabilities.native_rename);
        assert!(saved.capabilities.atomic_rename);
        service::test_connection(
            &storage,
            TestFileConnectionRequest {
                id: Some("webdav-contract".to_string()),
                config: config.clone(),
                secrets: FileSecretUpdates::default(),
            },
        )
        .await
        .unwrap();
        let edited = service::save_connection(
            &storage,
            SaveFileConnectionRequest {
                id: "webdav-contract".to_string(),
                name: "Edited WebDAV Contract".to_string(),
                config,
                secrets: FileSecretUpdates::default(),
            },
        )
        .await
        .unwrap();
        assert_eq!(edited.name, "Edited WebDAV Contract");
        assert!(edited.secret_status.password);

        for invalid_path in ["../escape", "%2e%2e/escape", "directory%2ffile"] {
            assert_eq!(
                service::stat_path(&storage, "webdav-contract", invalid_path).await.unwrap_err().code,
                "configuration"
            );
        }

        let operator = service::operator_for_connection(&storage, "webdav-contract").await.unwrap();
        assert!(operator.info().full_capability().copy);
        assert!(operator.info().full_capability().rename);
        let directory = format!("dbx-webdav-{suffix}");
        let source = format!("{directory}/source.txt");
        let copied = format!("{directory}/copied.txt");
        let renamed = format!("{directory}/renamed.txt");
        tokio::fs::write(&local_source, b"WebDAV seven-operation contract").await.unwrap();
        let state = FileTransferState::default();
        transfer::upload(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "webdav-contract".to_string(),
                remote_path: source.clone(),
                local_path: local_source.to_string_lossy().to_string(),
                replace: false,
            },
        )
        .await
        .unwrap();
        let stat = service::stat_path(&storage, "webdav-contract", &source).await.unwrap();
        assert_eq!(stat.kind, FileEntryKind::File);
        assert_eq!(stat.size, b"WebDAV seven-operation contract".len() as u64);
        assert!(service::list_path(&storage, "webdav-contract", &directory)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.path == source));

        let copy_request = FileRemoteOperationRequest {
            connection_id: "webdav-contract".to_string(),
            source_path: source.clone(),
            destination_path: copied.clone(),
            replace: false,
        };
        operations::copy(&storage, &state, copy_request.clone()).await.unwrap();
        assert_eq!(operations::copy(&storage, &state, copy_request).await.unwrap_err().code, "already_exists");
        operations::rename(
            &storage,
            &state,
            FileRemoteOperationRequest {
                connection_id: "webdav-contract".to_string(),
                source_path: copied.clone(),
                destination_path: renamed.clone(),
                replace: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(service::stat_path(&storage, "webdav-contract", &copied).await.unwrap_err().code, "not_found");

        transfer::download(
            &storage,
            &state,
            FileTransferRequest {
                connection_id: "webdav-contract".to_string(),
                remote_path: renamed.clone(),
                local_path: local_download.to_string_lossy().to_string(),
                replace: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(&local_download).await.unwrap(), b"WebDAV seven-operation contract");

        transfer::delete(&storage, &state, "webdav-contract", &source).await.unwrap();
        transfer::delete(&storage, &state, "webdav-contract", &renamed).await.unwrap();
        service::delete_connection(&storage, "webdav-contract").await.unwrap();
        assert!(service::list_connections(&storage).await.unwrap().is_empty());
        let _ = tokio::fs::remove_file(local_source).await;
        let _ = tokio::fs::remove_file(local_download).await;
        let _ = tokio::fs::remove_file(database).await;
    }
}
