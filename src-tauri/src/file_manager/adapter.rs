use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use dbx_core::storage::Storage;
use opendal::{services, ErrorKind, Operator};

use super::models::{
    FileConnectionConfig, FileManagerError, HdfsConfig, SecretUpdate, SftpAuthentication, TestFileConnectionRequest,
    WebdavAuthentication,
};

#[derive(Clone, Default)]
pub struct ResolvedSecrets {
    values: HashMap<&'static str, String>,
}

impl ResolvedSecrets {
    pub fn get(&self, key: &'static str) -> &str {
        self.values.get(key).map(String::as_str).unwrap_or_default()
    }

    pub(crate) fn update_fingerprint(&self, hasher: &mut sha2::Sha256) {
        use sha2::Digest;

        for key in dbx_core::file_connection_config::FILE_SECRET_KEYS {
            hasher.update((key.len() as u64).to_le_bytes());
            hasher.update(key.as_bytes());
            let value = self.get(key).as_bytes();
            hasher.update((value.len() as u64).to_le_bytes());
            hasher.update(value);
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeEndpoint {
    pub host: String,
    pub port: u16,
}

pub async fn resolve_secrets(
    storage: &Storage,
    request: &TestFileConnectionRequest,
) -> Result<ResolvedSecrets, FileManagerError> {
    let fields = [
        ("password", &request.secrets.password),
        ("private_key", &request.secrets.private_key),
        ("access_key", &request.secrets.access_key),
        ("secret_key", &request.secrets.secret_key),
        ("session_token", &request.secrets.session_token),
        ("bearer_token", &request.secrets.bearer_token),
        ("delegation_token", &request.secrets.delegation_token),
    ];
    let mut values = HashMap::new();
    for (key, update) in fields {
        let value = match update {
            SecretUpdate::Set(value) if !value.is_empty() => Some(value.clone()),
            SecretUpdate::Set(_) => {
                return Err(FileManagerError::configuration(
                    "A secret value cannot be empty; leave it unchanged or clear it explicitly",
                ));
            }
            SecretUpdate::Clear => None,
            SecretUpdate::Keep => match request.id.as_deref() {
                Some(id) => storage
                    .get_secret(id, &format!("file.{key}"))
                    .await
                    .map_err(|_| FileManagerError::new("storage", "Failed to read saved credentials"))?
                    .or(storage
                        .get_file_connection_secret(id, key)
                        .await
                        .map_err(|_| FileManagerError::new("storage", "Failed to read saved credentials"))?),
                None => None,
            },
        };
        if let Some(value) = value {
            values.insert(key, value);
        }
    }
    Ok(ResolvedSecrets { values })
}

pub fn build_operator(
    config: &FileConnectionConfig,
    secrets: &ResolvedSecrets,
    runtime_endpoint: Option<&RuntimeEndpoint>,
) -> Result<Operator, FileManagerError> {
    match config {
        FileConnectionConfig::Ftp { endpoint, port, root, username } => {
            validate_required("FTP endpoint", endpoint)?;
            let endpoint = runtime_endpoint
                .map(|runtime| endpoint_with_port(&runtime.host, "ftp", runtime.port))
                .unwrap_or_else(|| endpoint_with_port(endpoint, "ftp", *port));
            Operator::new(
                services::Ftp::default()
                    .endpoint(&endpoint)
                    .root(root)
                    .user(username)
                    .password(secrets.get("password")),
            )
            .map(|builder| builder.finish())
            .map_err(map_build_error)
        }
        FileConnectionConfig::Sftp { endpoint, port, root, username, authentication } => {
            #[cfg(not(unix))]
            {
                let _ = (endpoint, port, root, username, authentication, secrets);
                Err(FileManagerError::new("unsupported", "SFTP file connections are supported on macOS and Linux only"))
            }
            #[cfg(unix)]
            {
                validate_required("SFTP endpoint", endpoint)?;
                let endpoint = runtime_endpoint
                    .map(|runtime| endpoint_with_port(&runtime.host, "ssh", runtime.port))
                    .unwrap_or_else(|| endpoint_with_port(endpoint, "ssh", *port));
                let mut builder =
                    services::Sftp::default().endpoint(&endpoint).root(root).user(username).known_hosts_strategy("Add");
                if matches!(authentication, SftpAuthentication::PrivateKey) {
                    let key = secrets.get("private_key");
                    validate_required("SFTP private key", key)?;
                    builder = builder.key(key);
                }
                Operator::new(builder).map(|builder| builder.finish()).map_err(map_build_error)
            }
        }
        FileConnectionConfig::S3 { endpoint, region, bucket, root, path_style } => {
            reject_runtime_endpoint(runtime_endpoint, "S3")?;
            validate_required("S3 endpoint", endpoint)?;
            validate_required("S3 region", region)?;
            validate_required("S3 bucket", bucket)?;
            validate_required("S3 access key", secrets.get("access_key"))?;
            validate_required("S3 secret key", secrets.get("secret_key"))?;
            let mut builder = services::S3::default()
                .endpoint(endpoint)
                .region(region)
                .bucket(bucket)
                .root(root)
                .access_key_id(secrets.get("access_key"))
                .secret_access_key(secrets.get("secret_key"))
                .disable_config_load()
                .disable_ec2_metadata();
            if !secrets.get("session_token").is_empty() {
                builder = builder.session_token(secrets.get("session_token"));
            }
            if !path_style {
                builder = builder.enable_virtual_host_style();
            }
            Operator::new(builder).map(|builder| builder.finish()).map_err(map_build_error)
        }
        FileConnectionConfig::Webdav { endpoint, root, authentication } => {
            reject_runtime_endpoint(runtime_endpoint, "WebDAV")?;
            validate_required("WebDAV endpoint", endpoint)?;
            let mut builder = services::Webdav::default().endpoint(endpoint).root(root);
            match authentication {
                WebdavAuthentication::Basic { username } => {
                    validate_required("WebDAV username", username)?;
                    validate_required("WebDAV password", secrets.get("password"))?;
                    builder = builder.username(username).password(secrets.get("password"));
                }
                WebdavAuthentication::Bearer => {
                    validate_required("WebDAV bearer token", secrets.get("bearer_token"))?;
                    builder = builder.token(secrets.get("bearer_token"));
                }
            }
            Operator::new(builder).map(|builder| builder.finish()).map_err(map_build_error)
        }
        FileConnectionConfig::Hdfs { config } => match config {
            HdfsConfig::Webhdfs { endpoint, root, simple_user, use_delegation_token } => {
                reject_runtime_endpoint(runtime_endpoint, "WebHDFS")?;
                validate_required("WebHDFS endpoint", endpoint)?;
                let mut builder = services::Webhdfs::default().endpoint(endpoint).root(root);
                if *use_delegation_token {
                    validate_required("WebHDFS delegation token", secrets.get("delegation_token"))?;
                    builder = builder.delegation(secrets.get("delegation_token"));
                } else {
                    validate_required("WebHDFS simple user", simple_user)?;
                    builder = builder.user_name(simple_user);
                }
                Operator::new(builder).map(|builder| builder.finish()).map_err(map_build_error)
            }
            HdfsConfig::Native { name_node_uri, root, hadoop_config_directory } => {
                reject_runtime_endpoint(runtime_endpoint, "HDFS Native")?;
                validate_hdfs_name_node_uri(name_node_uri)?;
                let options = load_hadoop_config_options(hadoop_config_directory)?;
                let builder = services::HdfsNative::default().name_node(name_node_uri).root(root).options(options);
                Operator::new(builder).map(|builder| builder.finish()).map_err(map_build_error)
            }
        },
    }
}

fn reject_runtime_endpoint(runtime_endpoint: Option<&RuntimeEndpoint>, protocol: &str) -> Result<(), FileManagerError> {
    if runtime_endpoint.is_some() {
        Err(FileManagerError::new("unsupported", format!("{protocol} does not support SSH or proxy transport layers")))
    } else {
        Ok(())
    }
}

fn validate_hdfs_name_node_uri(value: &str) -> Result<(), FileManagerError> {
    let authority = value.trim().strip_prefix("hdfs://").map(|value| value.trim_end_matches('/')).filter(|value| {
        !value.is_empty()
            && !value.contains('/')
            && !value.chars().any(char::is_whitespace)
            && value.split(',').all(|node| !node.is_empty())
    });
    if authority.is_some() {
        Ok(())
    } else {
        Err(FileManagerError::configuration("The HDFS NameNode URI must use the hdfs:// scheme"))
    }
}

const MAX_HADOOP_CONFIG_FILE_SIZE: u64 = 1024 * 1024;

fn load_hadoop_config_options(directory: &str) -> Result<HashMap<String, String>, FileManagerError> {
    validate_required("Hadoop config directory", directory)?;
    let directory = Path::new(directory);
    if !directory.is_absolute() {
        return Err(FileManagerError::configuration("The Hadoop config directory must be an absolute path"));
    }
    if !directory
        .metadata()
        .map_err(|_| FileManagerError::configuration("The Hadoop config directory is not accessible"))?
        .is_dir()
    {
        return Err(FileManagerError::configuration("The Hadoop config path must be a directory"));
    }

    let mut options = HashMap::new();
    let mut loaded = false;
    for file_name in ["core-site.xml", "hdfs-site.xml"] {
        let path = directory.join(file_name);
        let metadata = match path.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(FileManagerError::configuration("A Hadoop config file is not accessible")),
        };
        if !metadata.is_file() || metadata.len() > MAX_HADOOP_CONFIG_FILE_SIZE {
            return Err(FileManagerError::configuration("A Hadoop config file is invalid or too large"));
        }
        let file = std::fs::File::open(&path)
            .map_err(|_| FileManagerError::configuration("A Hadoop config file is not accessible"))?;
        let mut bytes = Vec::with_capacity((metadata.len().min(MAX_HADOOP_CONFIG_FILE_SIZE) + 1) as usize);
        file.take(MAX_HADOOP_CONFIG_FILE_SIZE + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| FileManagerError::configuration("A Hadoop config file is not accessible"))?;
        if bytes.len() as u64 > MAX_HADOOP_CONFIG_FILE_SIZE {
            return Err(FileManagerError::configuration("A Hadoop config file is invalid or too large"));
        }
        let content = String::from_utf8(bytes)
            .map_err(|_| FileManagerError::configuration("A Hadoop config file is not valid UTF-8"))?;
        let document = roxmltree::Document::parse(&content)
            .map_err(|_| FileManagerError::configuration("A Hadoop config file contains invalid XML"))?;
        let configuration = document
            .root_element()
            .has_tag_name("configuration")
            .then(|| document.root_element())
            .ok_or_else(|| FileManagerError::configuration("A Hadoop config file has an invalid root element"))?;
        for property in configuration.children().filter(|node| node.has_tag_name("property")) {
            let name = property
                .children()
                .find(|node| node.has_tag_name("name"))
                .and_then(|node| node.text())
                .map(str::trim)
                .filter(|name| !name.is_empty());
            let value =
                property.children().find(|node| node.has_tag_name("value")).and_then(|node| node.text()).map(str::trim);
            if let (Some(name), Some(value)) = (name, value) {
                options.insert(name.to_string(), value.to_string());
            }
        }
        loaded = true;
    }
    if !loaded {
        return Err(FileManagerError::configuration(
            "The Hadoop config directory must contain core-site.xml or hdfs-site.xml",
        ));
    }
    Ok(options)
}

fn validate_required(label: &str, value: &str) -> Result<(), FileManagerError> {
    if value.trim().is_empty() {
        Err(FileManagerError::configuration(format!("{label} is required")))
    } else {
        Ok(())
    }
}

fn endpoint_with_port(endpoint: &str, scheme: &str, port: u16) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    let endpoint = if endpoint.contains("://") { endpoint.to_string() } else { format!("{scheme}://{endpoint}") };
    let authority = endpoint.split_once("://").map(|(_, authority)| authority).unwrap_or(&endpoint);
    if authority.rsplit_once(':').is_some_and(|(_, value)| value.parse::<u16>().is_ok()) {
        endpoint
    } else {
        format!("{endpoint}:{port}")
    }
}

fn map_build_error(error: opendal::Error) -> FileManagerError {
    match error.kind() {
        ErrorKind::ConfigInvalid => FileManagerError::configuration("The file connection configuration is invalid"),
        ErrorKind::Unsupported => FileManagerError::new("unsupported", "This operation is not supported"),
        _ => FileManagerError::new("backend", "Failed to initialize the file connection"),
    }
}

pub fn map_operation_error(error: opendal::Error) -> FileManagerError {
    match error.kind() {
        ErrorKind::ConfigInvalid => FileManagerError::configuration("The file connection configuration is invalid"),
        ErrorKind::NotFound => FileManagerError::new("not_found", "The requested file or directory does not exist"),
        ErrorKind::PermissionDenied => FileManagerError::new("permission_denied", "Permission was denied"),
        ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch => {
            FileManagerError::new("already_exists", "The destination already exists")
        }
        ErrorKind::Unsupported => FileManagerError::new("unsupported", "This operation is not supported"),
        ErrorKind::RateLimited => FileManagerError::new("rate_limited", "The remote service rate limit was reached"),
        _ => FileManagerError::new("backend", "The remote file operation failed"),
    }
}

#[cfg(test)]
mod tests {
    use crate::file_manager::models::FileConnectionConfig;
    #[cfg(unix)]
    use crate::file_manager::models::SftpAuthentication;

    use super::{
        build_operator, endpoint_with_port, load_hadoop_config_options, validate_hdfs_name_node_uri, ResolvedSecrets,
        RuntimeEndpoint,
    };

    #[test]
    fn endpoint_port_is_added_once() {
        assert_eq!(endpoint_with_port("127.0.0.1", "ftp", 2121), "ftp://127.0.0.1:2121");
        assert_eq!(endpoint_with_port("ftp://127.0.0.1", "ftp", 2121), "ftp://127.0.0.1:2121");
        assert_eq!(endpoint_with_port("ftp://127.0.0.1:2021", "ftp", 2121), "ftp://127.0.0.1:2021");
    }

    #[test]
    fn hadoop_config_directory_is_absolute_bounded_and_structured() {
        let directory = std::env::temp_dir().join(format!("dbx-hadoop-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("hdfs-site.xml"),
            r#"<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>true</value>
  </property>
</configuration>"#,
        )
        .unwrap();
        let options = load_hadoop_config_options(directory.to_str().unwrap()).unwrap();
        assert_eq!(options.get("dfs.client.use.datanode.hostname").map(String::as_str), Some("true"));
        assert_eq!(load_hadoop_config_options("relative/config").unwrap_err().code, "configuration");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn hdfs_name_node_uri_requires_an_hdfs_authority() {
        for value in ["hdfs://127.0.0.1:19000", "hdfs://nameservice", "hdfs://node-1:9000,node-2:9000/"] {
            assert!(validate_hdfs_name_node_uri(value).is_ok());
        }
        for value in ["", "http://127.0.0.1:19000", "hdfs://", "hdfs://node/path", "hdfs://node one"] {
            assert!(validate_hdfs_name_node_uri(value).is_err());
        }
    }

    #[test]
    fn runtime_endpoint_override_is_limited_to_tcp_safe_protocols() {
        let runtime = RuntimeEndpoint { host: "127.0.0.1".to_string(), port: 32123 };
        let secrets = ResolvedSecrets::default();
        let ftp = FileConnectionConfig::Ftp {
            endpoint: "ftp.example.com".to_string(),
            port: 21,
            root: "/".to_string(),
            username: "dbx".to_string(),
        };
        assert!(build_operator(&ftp, &secrets, Some(&runtime)).is_ok());

        let s3 = FileConnectionConfig::S3 {
            endpoint: "https://s3.example.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "dbx".to_string(),
            root: "/".to_string(),
            path_style: true,
        };
        let error = build_operator(&s3, &secrets, Some(&runtime)).unwrap_err();
        assert_eq!(error.code, "unsupported");
        assert!(error.message.contains("S3"));
    }

    #[cfg(unix)]
    #[test]
    fn sftp_config_and_agent_use_system_openssh_while_private_key_requires_a_secret_path() {
        let config = |authentication| FileConnectionConfig::Sftp {
            endpoint: "127.0.0.1".to_string(),
            port: 2222,
            root: "/config".to_string(),
            username: "dbx".to_string(),
            authentication,
        };
        let secrets = ResolvedSecrets::default();
        assert!(build_operator(&config(SftpAuthentication::SshConfig), &secrets, None).is_ok());
        assert!(build_operator(&config(SftpAuthentication::SshAgent), &secrets, None).is_ok());
        assert_eq!(
            build_operator(&config(SftpAuthentication::PrivateKey), &secrets, None).unwrap_err().code,
            "configuration"
        );
    }
}
