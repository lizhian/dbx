use serde::{Deserialize, Serialize};

pub const FILE_SECRET_PREFIX: &str = "file.";
pub const FILE_SECRET_KEYS: [&str; 7] =
    ["password", "private_key", "access_key", "secret_key", "session_token", "bearer_token", "delegation_token"];

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
