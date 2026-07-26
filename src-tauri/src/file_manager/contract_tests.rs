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
