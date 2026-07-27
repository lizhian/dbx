use std::fmt;

use serde::{Deserialize, Serialize};

pub const FILE_SECRET_PREFIX: &str = "file.";
pub const FILE_SECRET_KEYS: [&str; 7] =
    ["password", "private_key", "access_key", "secret_key", "session_token", "bearer_token", "delegation_token"];

#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(tag = "action", content = "value", rename_all = "snake_case")]
pub enum FileSecretUpdate {
    #[default]
    Keep,
    Set(String),
    Clear,
}

impl fmt::Debug for FileSecretUpdate {
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
    pub password: FileSecretUpdate,
    #[serde(default)]
    pub private_key: FileSecretUpdate,
    #[serde(default)]
    pub access_key: FileSecretUpdate,
    #[serde(default)]
    pub secret_key: FileSecretUpdate,
    #[serde(default)]
    pub session_token: FileSecretUpdate,
    #[serde(default)]
    pub bearer_token: FileSecretUpdate,
    #[serde(default)]
    pub delegation_token: FileSecretUpdate,
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
    pub fn entries(&self) -> [(&'static str, &FileSecretUpdate); 7] {
        [
            ("password", &self.password),
            ("private_key", &self.private_key),
            ("access_key", &self.access_key),
            ("secret_key", &self.secret_key),
            ("session_token", &self.session_token),
            ("bearer_token", &self.bearer_token),
            ("delegation_token", &self.delegation_token),
        ]
    }

    pub fn persistence_updates(&self) -> Result<Vec<(String, Option<String>)>, String> {
        let mut updates = Vec::new();
        for (key, update) in self.entries() {
            match update {
                FileSecretUpdate::Keep => {}
                FileSecretUpdate::Set(value) if value.is_empty() => {
                    return Err("A secret value cannot be empty; use clear explicitly".to_string());
                }
                FileSecretUpdate::Set(value) => updates.push((key.to_string(), Some(value.clone()))),
                FileSecretUpdate::Clear => updates.push((key.to_string(), None)),
            }
        }
        Ok(updates)
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
        let contains = |key: &str| {
            keys.iter().any(|candidate| {
                candidate == key || candidate.strip_prefix(FILE_SECRET_PREFIX).is_some_and(|value| value == key)
            })
        };
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

impl FileConnectionConfig {
    pub fn driver_profile(&self) -> &'static str {
        match self {
            Self::Ftp { .. } => "ftp",
            Self::Sftp { .. } => "sftp",
            Self::S3 { .. } => "s3",
            Self::Webdav { .. } => "webdav",
            Self::Hdfs { config: HdfsConfig::Webhdfs { .. } } => "webhdfs",
            Self::Hdfs { config: HdfsConfig::Native { .. } } => "hdfs-native",
        }
    }

    pub fn driver_label(&self) -> &'static str {
        match self.driver_profile() {
            "ftp" => "FTP",
            "sftp" => "SFTP",
            "s3" => "S3",
            "webdav" => "WebDAV",
            "webhdfs" => "WebHDFS",
            "hdfs-native" => "HDFS Native",
            _ => unreachable!("all file profiles are covered"),
        }
    }

    pub fn endpoint(&self) -> &str {
        match self {
            Self::Ftp { endpoint, .. }
            | Self::Sftp { endpoint, .. }
            | Self::S3 { endpoint, .. }
            | Self::Webdav { endpoint, .. }
            | Self::Hdfs { config: HdfsConfig::Webhdfs { endpoint, .. } } => endpoint,
            Self::Hdfs { config: HdfsConfig::Native { name_node_uri, .. } } => name_node_uri,
        }
    }

    pub fn username(&self) -> &str {
        match self {
            Self::Ftp { username, .. } | Self::Sftp { username, .. } => username,
            Self::Webdav { authentication: WebdavAuthentication::Basic { username }, .. } => username,
            Self::Hdfs { config: HdfsConfig::Webhdfs { simple_user, .. } } => simple_user,
            Self::S3 { .. }
            | Self::Webdav { authentication: WebdavAuthentication::Bearer, .. }
            | Self::Hdfs { config: HdfsConfig::Native { .. } } => "",
        }
    }

    pub fn projected_host_port_ssl(&self) -> (String, u16, bool) {
        let default_port = match self {
            Self::Ftp { port, .. } | Self::Sftp { port, .. } => *port,
            Self::S3 { .. } => 443,
            Self::Webdav { .. } | Self::Hdfs { config: HdfsConfig::Webhdfs { .. } } => 80,
            Self::Hdfs { config: HdfsConfig::Native { .. } } => 8020,
        };
        let endpoint = self.endpoint().trim();
        if let Ok(url) = reqwest::Url::parse(endpoint) {
            let ssl = matches!(url.scheme(), "https" | "ftps");
            let port = url.port_or_known_default().unwrap_or(default_port);
            return (url.host_str().unwrap_or("").to_string(), port, ssl);
        }
        (endpoint.to_string(), default_port, false)
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{FileConnectionConfig, HdfsConfig};

    #[test]
    fn file_profiles_share_one_typed_discriminator() {
        let config = FileConnectionConfig::Hdfs {
            config: HdfsConfig::Native {
                name_node_uri: "hdfs://namenode:19000".to_string(),
                root: "/".to_string(),
                hadoop_config_directory: String::new(),
            },
        };
        assert_eq!(config.driver_profile(), "hdfs-native");
        assert_eq!(config.projected_host_port_ssl(), ("namenode".to_string(), 19000, false));
        assert_eq!(serde_json::to_value(config).unwrap()["protocol"], "hdfs");
    }
}
