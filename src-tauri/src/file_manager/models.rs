use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum SftpAuthentication {
    SshConfig,
    SshAgent,
    PrivateKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum WebdavAuthentication {
    Basic { username: String },
    Bearer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "implementation", rename_all = "snake_case")]
pub enum HdfsConfig {
    Webhdfs {
        endpoint: String,
        root: String,
        #[serde(default)]
        simple_user: String,
        #[serde(default)]
        use_delegation_token: bool,
    },
    Native {
        name_node_uri: String,
        root: String,
        #[serde(default)]
        hadoop_config_directory: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum FileConnectionConfig {
    Ftp {
        endpoint: String,
        port: u16,
        root: String,
        username: String,
    },
    Sftp {
        endpoint: String,
        port: u16,
        root: String,
        username: String,
        authentication: SftpAuthentication,
    },
    S3 {
        endpoint: String,
        region: String,
        bucket: String,
        root: String,
        #[serde(default = "default_true")]
        path_style: bool,
    },
    Webdav {
        endpoint: String,
        root: String,
        authentication: WebdavAuthentication,
    },
    Hdfs {
        #[serde(flatten)]
        config: HdfsConfig,
    },
}

fn default_true() -> bool {
    true
}

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSecretStatus {
    pub password: bool,
    pub private_key: bool,
    pub access_key: bool,
    pub secret_key: bool,
    pub session_token: bool,
    pub bearer_token: bool,
    pub delegation_token: bool,
}

impl FileSecretStatus {
    pub fn from_keys(keys: &[String]) -> Self {
        let contains = |key: &str| keys.iter().any(|candidate| candidate == key);
        Self {
            password: contains("password"),
            private_key: contains("private_key"),
            access_key: contains("access_key"),
            secret_key: contains("secret_key"),
            session_token: contains("session_token"),
            bearer_token: contains("bearer_token"),
            delegation_token: contains("delegation_token"),
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

#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(tag = "action", content = "value", rename_all = "snake_case")]
pub enum SecretUpdate {
    #[default]
    Keep,
    Set(String),
    Clear,
}

impl fmt::Debug for SecretUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Keep => formatter.write_str("Keep"),
            Self::Set(_) => formatter.write_str("Set([REDACTED])"),
            Self::Clear => formatter.write_str("Clear"),
        }
    }
}

#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSecretUpdates {
    #[serde(default)]
    pub password: SecretUpdate,
    #[serde(default)]
    pub private_key: SecretUpdate,
    #[serde(default)]
    pub access_key: SecretUpdate,
    #[serde(default)]
    pub secret_key: SecretUpdate,
    #[serde(default)]
    pub session_token: SecretUpdate,
    #[serde(default)]
    pub bearer_token: SecretUpdate,
    #[serde(default)]
    pub delegation_token: SecretUpdate,
}

impl fmt::Debug for FileSecretUpdates {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileSecretUpdates")
            .field("password", &self.password)
            .field("private_key", &self.private_key)
            .field("access_key", &self.access_key)
            .field("secret_key", &self.secret_key)
            .field("session_token", &self.session_token)
            .field("bearer_token", &self.bearer_token)
            .field("delegation_token", &self.delegation_token)
            .finish()
    }
}

impl FileSecretUpdates {
    pub fn persistence_updates(&self) -> Result<Vec<(String, Option<String>)>, FileManagerError> {
        let fields = [
            ("password", &self.password),
            ("private_key", &self.private_key),
            ("access_key", &self.access_key),
            ("secret_key", &self.secret_key),
            ("session_token", &self.session_token),
            ("bearer_token", &self.bearer_token),
            ("delegation_token", &self.delegation_token),
        ];
        let mut updates = Vec::new();
        for (key, update) in fields {
            match update {
                SecretUpdate::Keep => {}
                SecretUpdate::Set(value) if value.is_empty() => {
                    return Err(FileManagerError::configuration(
                        "A secret value cannot be empty; use clear explicitly",
                    ));
                }
                SecretUpdate::Set(value) => updates.push((key.to_string(), Some(value.clone()))),
                SecretUpdate::Clear => updates.push((key.to_string(), None)),
            }
        }
        Ok(updates)
    }
}

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
