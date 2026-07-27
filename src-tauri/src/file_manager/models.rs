use std::fmt;

use serde::{Deserialize, Serialize};

pub use dbx_core::file_connection_config::{
    FileConnectionConfig, FileSecretStatus, FileSecretUpdate as SecretUpdate, FileSecretUpdates, HdfsConfig,
    SftpAuthentication, WebdavAuthentication,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileCopyMode {
    Native,
    StreamRelay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileRenameMode {
    Native,
    CopyDelete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileCapabilities {
    pub read: bool,
    pub write: bool,
    pub stat: bool,
    pub list: bool,
    pub delete: bool,
    pub copy: bool,
    pub rename: bool,
    pub native_copy: bool,
    pub native_rename: bool,
    pub atomic_rename: bool,
    pub atomic_no_clobber: bool,
    pub copy_mode: FileCopyMode,
    pub rename_mode: FileRenameMode,
}

impl FileCapabilities {
    pub fn for_config(config: &FileConnectionConfig) -> Self {
        Self::for_config_on_platform(config, cfg!(unix))
    }

    fn for_config_on_platform(config: &FileConnectionConfig, unix: bool) -> Self {
        if matches!(config, FileConnectionConfig::Sftp { .. }) && !unix {
            return Self {
                read: false,
                write: false,
                stat: false,
                list: false,
                delete: false,
                copy: false,
                rename: false,
                native_copy: false,
                native_rename: false,
                atomic_rename: false,
                atomic_no_clobber: false,
                copy_mode: FileCopyMode::StreamRelay,
                rename_mode: FileRenameMode::CopyDelete,
            };
        }
        let (native_copy, native_rename, atomic_rename) = match config {
            FileConnectionConfig::Ftp { .. } => (false, false, false),
            FileConnectionConfig::Sftp { .. } => (true, true, true),
            FileConnectionConfig::S3 { .. } => (true, false, false),
            FileConnectionConfig::Webdav { .. } => (true, true, true),
            FileConnectionConfig::Hdfs { config: HdfsConfig::Webhdfs { .. } } => (false, false, false),
            FileConnectionConfig::Hdfs { config: HdfsConfig::Native { .. } } => (false, true, true),
        };
        Self {
            read: true,
            write: true,
            stat: true,
            list: true,
            delete: true,
            copy: true,
            rename: true,
            native_copy,
            native_rename,
            atomic_rename,
            atomic_no_clobber: false,
            copy_mode: if native_copy { FileCopyMode::Native } else { FileCopyMode::StreamRelay },
            rename_mode: if native_rename { FileRenameMode::Native } else { FileRenameMode::CopyDelete },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileConnection {
    pub id: String,
    pub name: String,
    pub config: FileConnectionConfig,
    pub capabilities: FileCapabilities,
    pub secret_status: FileSecretStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredFileConnection {
    pub id: String,
    pub name: String,
    pub config: FileConnectionConfig,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveFileConnectionRequest {
    pub id: String,
    pub name: String,
    pub config: FileConnectionConfig,
    #[serde(default)]
    pub secrets: FileSecretUpdates,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestFileConnectionRequest {
    pub id: Option<String>,
    pub config: FileConnectionConfig,
    #[serde(default)]
    pub secrets: FileSecretUpdates,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileEntryKind {
    File,
    Directory,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub path: String,
    pub name: String,
    pub kind: FileEntryKind,
    pub size: u64,
    pub modified_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileTransferRequest {
    pub connection_id: String,
    pub remote_path: String,
    pub local_path: String,
    #[serde(default)]
    pub replace: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileTransferProgress {
    pub bytes_transferred: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileRemoteOperationRequest {
    pub connection_id: String,
    pub source_path: String,
    pub destination_path: String,
    #[serde(default)]
    pub replace: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileManagerError {
    pub code: &'static str,
    pub message: String,
    pub recovery: Option<String>,
}

impl FileManagerError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), recovery: None }
    }

    pub fn configuration(message: impl Into<String>) -> Self {
        Self::new("configuration", message)
    }
}

impl fmt::Display for FileManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for FileManagerError {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{FileCapabilities, FileConnectionConfig, SftpAuthentication};

    fn sftp_config(authentication: SftpAuthentication) -> FileConnectionConfig {
        FileConnectionConfig::Sftp {
            endpoint: "127.0.0.1".to_string(),
            port: 2222,
            root: "/config".to_string(),
            username: "dbx".to_string(),
            authentication,
        }
    }

    #[test]
    fn sftp_password_authentication_is_not_part_of_the_discriminator() {
        let value = json!({
            "protocol": "sftp",
            "endpoint": "127.0.0.1",
            "port": 2222,
            "root": "/config",
            "username": "dbx",
            "authentication": { "method": "password" }
        });
        assert!(serde_json::from_value::<FileConnectionConfig>(value).is_err());
    }

    #[test]
    fn sftp_capabilities_are_native_on_unix_and_disabled_elsewhere() {
        let config = sftp_config(SftpAuthentication::PrivateKey);
        let unix = FileCapabilities::for_config_on_platform(&config, true);
        assert!(unix.read && unix.write && unix.copy && unix.rename);
        assert!(unix.native_copy && unix.native_rename && unix.atomic_rename);

        let windows = FileCapabilities::for_config_on_platform(&config, false);
        assert!(!windows.read && !windows.write && !windows.copy && !windows.rename);
        assert!(!windows.native_copy && !windows.native_rename && !windows.atomic_rename);
    }
}
