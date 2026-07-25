use std::sync::Arc;
use std::time::Duration;

use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};

use super::file_manager::{
    failed_stage, passed_stage, skipped_stage, ConnectionTestStage, FileConnectionCapabilities,
    FileConnectionTestResult, ResolvedFileSecrets,
};
use super::file_manager_paths::RemotePath;

const SFTP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SftpAuthentication {
    SshConfig,
    Agent,
    PrivateKey,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SftpConnectionConfig {
    pub endpoint: String,
    pub root: String,
    #[serde(default)]
    pub username: String,
    pub authentication: SftpAuthentication,
}

pub(super) fn capabilities() -> FileConnectionCapabilities {
    let supported = cfg!(any(target_os = "macos", target_os = "linux"));
    FileConnectionCapabilities {
        read: supported,
        write: supported,
        stat: supported,
        list: supported,
        create_directory: supported,
        delete: supported,
        copy: supported,
        rename: supported,
        server_side_copy: false,
        atomic_rename: supported,
        atomic_no_clobber: false,
    }
}

pub(super) fn normalize_root(root: &str) -> Result<String, String> {
    let decoded = percent_encoding::percent_decode_str(root.trim())
        .decode_utf8()
        .map_err(|_| "SFTP root contains invalid percent-encoded UTF-8".to_string())?;
    if !decoded.starts_with('/') {
        return Err("SFTP root must be an absolute path beginning with '/'".to_string());
    }
    if decoded.contains('\0') || decoded.contains('\\') {
        return Err("SFTP root contains an invalid character".to_string());
    }
    let mut normalized = Vec::new();
    for segment in decoded.split('/').filter(|segment| !segment.is_empty()) {
        if matches!(segment, "." | "..") {
            return Err("SFTP root cannot contain '.' or '..' path segments".to_string());
        }
        normalized.push(segment);
    }
    Ok(if normalized.is_empty() { "/".to_string() } else { format!("/{}", normalized.join("/")) })
}

pub(super) fn endpoint_host_port(endpoint: &str) -> Result<Option<(String, u16)>, String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err("SFTP endpoint is required".to_string());
    }
    if (endpoint.starts_with('-')
        || endpoint.contains('\0')
        || endpoint.contains(char::is_whitespace)
        || endpoint.contains(['/', '\\']))
        && !endpoint.starts_with("ssh://")
    {
        return Err("SFTP endpoint must be a safe SSH host alias or ssh://host[:port] URL".to_string());
    }
    if endpoint.starts_with("ssh://") {
        let url = url::Url::parse(endpoint).map_err(|_| "SFTP endpoint must be a valid ssh:// URL".to_string())?;
        if url.scheme() != "ssh" {
            return Err("SFTP endpoint must use ssh://".to_string());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("SFTP credentials must not be embedded in the endpoint".to_string());
        }
        if url.path() != "" && url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
            return Err("SFTP endpoint must not contain a path, query, or fragment; use the root field".to_string());
        }
        let host = url.host_str().ok_or_else(|| "SFTP endpoint host is required".to_string())?;
        return Ok(Some((host.to_string(), url.port().unwrap_or(22))));
    }
    if !endpoint.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')) {
        return Err("SFTP SSH host alias contains an unsupported character".to_string());
    }
    Ok(None)
}

pub(super) fn validate_config(
    config: &SftpConnectionConfig,
    creating: bool,
    secrets: &ResolvedFileSecrets,
) -> Result<(), String> {
    if !cfg!(any(target_os = "macos", target_os = "linux")) {
        return Err("Unsupported: SFTP is available only on macOS and Linux in v1".to_string());
    }
    endpoint_host_port(&config.endpoint)?;
    normalize_root(&config.root)?;
    if config.username.contains('\0') || config.username.contains(char::is_whitespace) {
        return Err("SFTP username contains an invalid character".to_string());
    }
    match config.authentication {
        SftpAuthentication::SshConfig | SftpAuthentication::Agent => {
            if secrets.sftp_private_key.is_some() || secrets.sftp_private_key_passphrase.is_some() {
                return Err("SSH config and agent authentication cannot include private-key secrets".to_string());
            }
        }
        SftpAuthentication::PrivateKey => {
            if creating && secrets.sftp_private_key.is_none() {
                return Err("SFTP private-key authentication requires inline private-key material".to_string());
            }
            if secrets.sftp_private_key.as_deref() == Some("") {
                return Err("SFTP private-key material cannot be empty".to_string());
            }
            if secrets.sftp_private_key_passphrase.as_deref() == Some("") {
                return Err("SFTP private-key passphrase cannot be empty; omit it for an unencrypted key".to_string());
            }
            if let Some(key) = secrets.sftp_private_key.as_deref() {
                validate_private_key(key, secrets.sftp_private_key_passphrase.as_deref())?;
            }
        }
    }
    Ok(())
}

pub(super) fn secret_scope(config: &SftpConnectionConfig) -> Result<String, String> {
    endpoint_host_port(&config.endpoint)?;
    Ok(format!(
        "sftp\n{}\n{}\n{}\n{:?}",
        config.endpoint.to_ascii_lowercase(),
        config.root,
        config.username,
        config.authentication
    ))
}

pub(super) fn classify_message(message: impl AsRef<str>) -> String {
    let message = redact_temporary_key_paths(message.as_ref());
    let message = message.as_str();
    let lower = message.to_ascii_lowercase();
    let kind = if lower.contains("host key verification failed")
        || lower.contains("remote host identification has changed")
        || lower.contains("known_hosts")
    {
        "SftpHostKey"
    } else if lower.contains("permission denied (publickey")
        || lower.contains("no identities")
        || lower.contains("agent refused")
        || lower.contains("incorrect passphrase")
        || lower.contains("could not read key")
    {
        "SftpAuthentication"
    } else if lower.contains("permission denied") || lower.contains("failure: permission") {
        "SftpPermission"
    } else if lower.contains("timed out") || lower.contains("timeout") {
        "SftpTimeout"
    } else if lower.contains("disconnected")
        || lower.contains("connection reset")
        || lower.contains("broken pipe")
        || lower.contains("connection was terminated")
        || lower.contains("connection refused")
        || lower.contains("background task failed: read/flush task failed")
    {
        "SftpDisconnected"
    } else {
        "SftpProtocol"
    };
    format!("{kind}: {message}")
}

pub(super) fn redact_temporary_key_paths(message: &str) -> String {
    const MARKER: &str = "dbx-sftp-keys-";
    let mut redacted = message.to_string();
    while let Some(marker) = redacted.find(MARKER) {
        let bytes = redacted.as_bytes();
        let start = bytes[..marker]
            .iter()
            .rposition(|byte| byte.is_ascii_whitespace() || matches!(*byte, b'"' | b'\''))
            .map_or(0, |index| index + 1);
        let end = bytes[marker..]
            .iter()
            .position(|byte| byte.is_ascii_whitespace() || matches!(*byte, b'"' | b'\''))
            .map_or(redacted.len(), |index| marker + index);
        redacted.replace_range(start..end, "[SFTP_KEY_MATERIAL]");
    }
    redacted
}

pub(super) fn classify_error(error: opendal::Error) -> String {
    if error.kind() == ErrorKind::PermissionDenied {
        return format!("SftpPermission: {}", redact_temporary_key_paths(&error.to_string()));
    }
    let mut diagnostics = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(error) = source {
        diagnostics.push_str(": ");
        diagnostics.push_str(&error.to_string());
        source = error.source();
    }
    classify_message(diagnostics)
}

fn classify_root_stat_error(error: opendal::Error) -> String {
    match error.kind() {
        ErrorKind::NotFound => "SftpRoot: configured root does not exist".to_string(),
        ErrorKind::NotADirectory => "SftpRoot: configured root is not a directory".to_string(),
        _ => classify_error(error),
    }
}

fn connection_error_stage(error: &str) -> &'static str {
    if error.starts_with("SftpHostKey:") {
        "host_key"
    } else if error.starts_with("SftpPermission:") || error.starts_with("SftpRoot:") {
        "root"
    } else {
        "authentication"
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
mod supported {
    use std::fs::{self, DirBuilder, File, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::{Path, PathBuf};

    use opendal::services::Sftp;
    use openssh::{KnownHosts, SessionBuilder};
    use openssh_sftp_client::{error::SftpErrorKind, Sftp as GuardSftp, SftpOptions};
    use russh::keys::decode_secret_key;
    use russh::keys::ssh_key::LineEnding;
    use uuid::Uuid;
    use zeroize::Zeroizing;

    use super::*;

    const KEY_DIR_PREFIX: &str = "dbx-sftp-keys-";
    const KEY_FILE_PREFIX: &str = "dbx-sftp-key-";

    pub(crate) struct SftpKeyMaterial {
        path: PathBuf,
        _file: File,
    }

    pub(crate) struct SftpPathGuard {
        endpoint: String,
        root: PathBuf,
        username: String,
        key: Option<Arc<SftpKeyMaterial>>,
        client: tokio::sync::Mutex<Option<GuardSftp>>,
    }

    type SftpOperatorBuild = (Operator, Option<Arc<SftpKeyMaterial>>, Arc<SftpPathGuard>);

    impl SftpPathGuard {
        fn new(config: &SftpConnectionConfig, key: Option<Arc<SftpKeyMaterial>>) -> Self {
            Self {
                endpoint: config.endpoint.clone(),
                root: PathBuf::from(&config.root),
                username: config.username.clone(),
                key,
                client: tokio::sync::Mutex::new(None),
            }
        }

        async fn connect(&self) -> Result<GuardSftp, String> {
            let mut session = SessionBuilder::default();
            session.known_hosts_check(KnownHosts::Strict);
            if !self.username.is_empty() {
                session.user(self.username.clone());
            }
            if let Some(key) = &self.key {
                session.keyfile(&key.path);
            }
            let session = session.connect(self.endpoint.clone()).await.map_err(classify_guard_error)?;
            GuardSftp::from_session(session, SftpOptions::default()).await.map_err(classify_guard_error)
        }

        pub(crate) fn require_existing(
            self: &Arc<Self>,
            relative_path: String,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'static>> {
            let guard = Arc::clone(self);
            Box::pin(async move { guard.validate(relative_path, false).await })
        }

        pub(crate) fn require_destination(
            self: &Arc<Self>,
            relative_path: String,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'static>> {
            let guard = Arc::clone(self);
            Box::pin(async move { guard.validate(relative_path, true).await })
        }

        async fn validate(&self, relative_path: String, allow_missing_leaf: bool) -> Result<(), String> {
            let relative = if relative_path.is_empty() {
                String::new()
            } else {
                RemotePath::parse(&relative_path)?.as_str().to_string()
            };
            let mut client = self.client.lock().await;
            if client.is_none() {
                *client = Some(self.connect().await?);
            }
            let sftp = client.as_mut().expect("SFTP containment client initialized");
            let result = validate_containment(sftp, self.root.clone(), relative, allow_missing_leaf).await;
            if result
                .as_ref()
                .is_err_and(|error| error.starts_with("SftpDisconnected:") || error.starts_with("SftpProtocol:"))
            {
                *client = None;
            }
            result
        }
    }

    async fn validate_containment(
        sftp: &mut GuardSftp,
        configured_root: PathBuf,
        relative_path: String,
        allow_missing_leaf: bool,
    ) -> Result<(), String> {
        let mut fs = sftp.fs();
        let canonical_root = fs.canonicalize(configured_root.clone()).await.map_err(classify_root_guard_error)?;
        let root_metadata = fs.metadata(canonical_root.clone()).await.map_err(classify_root_guard_error)?;
        if !root_metadata.file_type().is_some_and(|kind| kind.is_dir()) {
            return Err("SftpRoot: configured root is not a directory".to_string());
        }

        let mut current = configured_root;
        let components: Vec<String> =
            relative_path.split('/').filter(|segment| !segment.is_empty()).map(ToString::to_string).collect();
        for (index, segment) in components.iter().enumerate() {
            current.push(segment);
            match fs.symlink_metadata(current.clone()).await {
                Ok(_) => {
                    let canonical = fs.canonicalize(current.clone()).await.map_err(|error| {
                        format!(
                            "SftpPathEscape: remote symlink could not be resolved safely: {}",
                            redact_temporary_key_paths(&error.to_string())
                        )
                    })?;
                    ensure_within_root(&canonical_root, &canonical)?;
                }
                Err(error)
                    if guard_error_is_not_found(&error) && allow_missing_leaf && index + 1 == components.len() =>
                {
                    // SFTP v3 cannot atomically bind the later OpenDAL operation to
                    // this checked parent. Serializing dbx mutations narrows, but
                    // cannot eliminate, a server-side symlink replacement race.
                    return Ok(());
                }
                Err(error) if guard_error_is_not_found(&error) && index + 1 == components.len() => {
                    return Err(classify_guard_error(error));
                }
                Err(error) => return Err(classify_guard_error(error)),
            }
        }

        let canonical_target = fs.canonicalize(current).await.map_err(classify_guard_error)?;
        ensure_within_root(&canonical_root, &canonical_target)
    }

    fn ensure_within_root(canonical_root: &Path, canonical_path: &Path) -> Result<(), String> {
        if canonical_path == canonical_root || canonical_path.starts_with(canonical_root) {
            Ok(())
        } else {
            Err("SftpPathEscape: remote path resolves outside the configured root".to_string())
        }
    }

    fn guard_error_is_not_found(error: &openssh_sftp_client::Error) -> bool {
        matches!(error, openssh_sftp_client::Error::SftpError(SftpErrorKind::NoSuchFile, _))
    }

    fn classify_root_guard_error(error: openssh_sftp_client::Error) -> String {
        if guard_error_is_not_found(&error) {
            "SftpRoot: configured root does not exist".to_string()
        } else {
            classify_guard_error(error)
        }
    }

    fn classify_guard_error(error: impl std::error::Error) -> String {
        let mut diagnostics = error.to_string();
        let mut source = error.source();
        while let Some(error) = source {
            diagnostics.push_str(": ");
            diagnostics.push_str(&error.to_string());
            source = error.source();
        }
        classify_message(diagnostics)
    }

    impl SftpKeyMaterial {
        fn path_string(&self) -> Result<String, String> {
            self.path
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| "SFTP temporary key path is not valid UTF-8".to_string())
        }
    }

    impl Drop for SftpKeyMaterial {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
            if let Some(directory) = self.path.parent() {
                let _ = fs::remove_dir(directory);
            }
        }
    }

    pub(crate) fn cleanup_crash_residue() {
        let Ok(dir) = key_directory(false) else {
            return;
        };
        let Ok(entries) = fs::read_dir(&dir) else {
            return;
        };
        let current_pid = std::process::id();
        let uid = unsafe { libc::geteuid() };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(pid) = key_file_pid(name) else {
                continue;
            };
            if pid == current_pid || process_is_alive(pid) {
                continue;
            }
            let path = entry.path();
            let Ok(metadata) = fs::symlink_metadata(&path) else {
                continue;
            };
            if metadata.file_type().is_file()
                && metadata.uid() == uid
                && metadata.mode() & 0o777 == 0o600
                && metadata.nlink() == 1
            {
                let _ = fs::remove_file(path);
            }
        }
        let _ = fs::remove_dir(dir);
    }

    fn key_file_pid(name: &str) -> Option<u32> {
        let suffix = name.strip_prefix(KEY_FILE_PREFIX)?;
        let (pid, rest) = suffix.split_once('-')?;
        let uuid_text = rest.strip_suffix(".key")?;
        let uuid = Uuid::parse_str(uuid_text).ok()?;
        if uuid.to_string() != uuid_text {
            return None;
        }
        pid.parse().ok()
    }

    fn process_is_alive(pid: u32) -> bool {
        let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    fn key_directory(create: bool) -> Result<PathBuf, String> {
        let uid = unsafe { libc::geteuid() };
        let path = std::env::temp_dir().join(format!("{KEY_DIR_PREFIX}{uid}"));
        if create {
            match DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err("Could not create the protected SFTP key directory".to_string()),
            }
        }
        let metadata = fs::symlink_metadata(&path)
            .map_err(|_| "Could not inspect the protected SFTP key directory".to_string())?;
        if !metadata.file_type().is_dir() || metadata.uid() != uid || metadata.mode() & 0o777 != 0o700 {
            return Err("The protected SFTP key directory has unsafe ownership or permissions".to_string());
        }
        Ok(path)
    }

    fn materialize_private_key(material: &str, passphrase: Option<&str>) -> Result<Arc<SftpKeyMaterial>, String> {
        let key = decode_secret_key(material, passphrase)
            .map_err(|_| "SftpAuthentication: private-key material or passphrase is invalid".to_string())?;
        let encoded = key
            .to_openssh(LineEnding::LF)
            .map_err(|_| "SftpAuthentication: private key could not be prepared".to_string())?;
        let encoded = Zeroizing::new(encoded.to_string());
        cleanup_crash_residue();
        let directory = key_directory(true)?;
        let path = directory.join(format!("{KEY_FILE_PREFIX}{}-{}.key", std::process::id(), Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .map_err(|_| "Could not create protected SFTP key material".to_string())?;
        if let Err(error) = file.write_all(encoded.as_bytes()).and_then(|_| file.sync_all()) {
            let _ = fs::remove_file(&path);
            return Err(format!("Could not persist protected SFTP key material: {error}"));
        }
        if fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).is_err() {
            let _ = fs::remove_file(&path);
            return Err("Could not protect SFTP key material permissions".to_string());
        }
        Ok(Arc::new(SftpKeyMaterial { path, _file: file }))
    }

    pub(crate) fn build_operator(
        config: &SftpConnectionConfig,
        secrets: &ResolvedFileSecrets,
    ) -> Result<SftpOperatorBuild, String> {
        validate_config(config, false, secrets)?;
        let key = match config.authentication {
            SftpAuthentication::PrivateKey => {
                let material = secrets
                    .sftp_private_key
                    .as_deref()
                    .ok_or_else(|| "SftpAuthentication: stored private-key material is missing".to_string())?;
                Some(materialize_private_key(material, secrets.sftp_private_key_passphrase.as_deref())?)
            }
            SftpAuthentication::SshConfig | SftpAuthentication::Agent => None,
        };
        let mut builder = Sftp::default().endpoint(&config.endpoint).root("/").known_hosts_strategy("strict");
        if !config.username.is_empty() {
            builder = builder.user(&config.username);
        }
        if let Some(key) = &key {
            builder = builder.key(&key.path_string()?);
        }
        let operator = Operator::new(builder).map_err(classify_error)?.finish();
        let path_guard = Arc::new(SftpPathGuard::new(config, key.clone()));
        Ok((operator, key, path_guard))
    }

    pub(crate) async fn test_connection(
        config: &SftpConnectionConfig,
        secrets: &ResolvedFileSecrets,
    ) -> FileConnectionTestResult {
        let mut stages = Vec::new();
        if let Err(error) = validate_config(config, false, secrets) {
            stages.push(failed_stage("configuration", error));
            stages.extend(["dns", "tcp", "host_key", "authentication", "root"].into_iter().map(skipped_stage));
            return FileConnectionTestResult { success: false, stages };
        }
        stages.push(passed_stage("configuration"));
        match endpoint_host_port(&config.endpoint) {
            Ok(Some((host, port))) => {
                let addresses =
                    match tokio::time::timeout(SFTP_CONNECTION_TIMEOUT, tokio::net::lookup_host((host.as_str(), port)))
                        .await
                    {
                        Ok(Ok(mut addresses)) => {
                            if addresses.next().is_some() {
                                stages.push(passed_stage("dns"));
                                true
                            } else {
                                stages.push(ConnectionTestStage {
                                    stage: "dns",
                                    status: "skipped",
                                    message: Some(
                                        "Direct DNS returned no addresses; resolution is delegated to OpenSSH"
                                            .to_string(),
                                    ),
                                });
                                false
                            }
                        }
                        Ok(Err(error)) => {
                            stages.push(ConnectionTestStage {
                                stage: "dns",
                                status: "skipped",
                                message: Some(format!(
                                    "Direct DNS diagnostic failed ({error}); resolution is delegated to OpenSSH"
                                )),
                            });
                            false
                        }
                        Err(_) => {
                            stages.push(ConnectionTestStage {
                                stage: "dns",
                                status: "skipped",
                                message: Some(
                                    "Direct DNS diagnostic timed out; resolution is delegated to OpenSSH".to_string(),
                                ),
                            });
                            false
                        }
                    };
                if addresses {
                    match tokio::time::timeout(
                        SFTP_CONNECTION_TIMEOUT,
                        tokio::net::TcpStream::connect((host.as_str(), port)),
                    )
                    .await
                    {
                        Ok(Ok(_)) => stages.push(passed_stage("tcp")),
                        Ok(Err(error)) => stages.push(ConnectionTestStage {
                            stage: "tcp",
                            status: "skipped",
                            message: Some(format!(
                                "Direct TCP diagnostic failed ({error}); routing is delegated to OpenSSH"
                            )),
                        }),
                        Err(_) => stages.push(ConnectionTestStage {
                            stage: "tcp",
                            status: "skipped",
                            message: Some(
                                "Direct TCP diagnostic timed out; routing is delegated to OpenSSH".to_string(),
                            ),
                        }),
                    }
                } else {
                    stages.push(ConnectionTestStage {
                        stage: "tcp",
                        status: "skipped",
                        message: Some("Direct TCP diagnostic skipped; routing is delegated to OpenSSH".to_string()),
                    });
                }
            }
            Ok(None) => {
                stages.push(ConnectionTestStage {
                    stage: "dns",
                    status: "skipped",
                    message: Some("Resolution delegated to the default SSH configuration".to_string()),
                });
                stages.push(ConnectionTestStage {
                    stage: "tcp",
                    status: "skipped",
                    message: Some("TCP routing delegated to the default SSH configuration".to_string()),
                });
            }
            Err(error) => {
                stages.push(failed_stage("configuration", error));
                stages.extend(["dns", "tcp", "host_key", "authentication", "root"].into_iter().map(skipped_stage));
                return FileConnectionTestResult { success: false, stages };
            }
        }
        let (operator, _key, path_guard) = match build_operator(config, secrets) {
            Ok(value) => value,
            Err(error) => {
                let stage = connection_error_stage(&error);
                stages.push(failed_stage(stage, error));
                stages.extend(stages_after(stage).into_iter().map(skipped_stage));
                return FileConnectionTestResult { success: false, stages };
            }
        };
        match tokio::time::timeout(SFTP_CONNECTION_TIMEOUT, path_guard.require_existing(String::new())).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return failed_connection_test(stages, error),
            Err(_) => return failed_connection_test(stages, "SftpTimeout: SSH root validation timed out".to_string()),
        }
        let root = configured_root(config);
        match tokio::time::timeout(SFTP_CONNECTION_TIMEOUT, operator.stat(&root)).await {
            Ok(Ok(metadata)) if metadata.mode().is_dir() => {
                stages.push(passed_stage("host_key"));
                stages.push(passed_stage("authentication"));
                stages.push(passed_stage("root"));
                FileConnectionTestResult { success: true, stages }
            }
            Ok(Ok(_)) => {
                stages.push(passed_stage("host_key"));
                stages.push(passed_stage("authentication"));
                stages.push(failed_stage("root", "SftpRoot: configured root is not a directory".to_string()));
                FileConnectionTestResult { success: false, stages }
            }
            Ok(Err(error)) => {
                let error = classify_root_stat_error(error);
                failed_connection_test(stages, error)
            }
            Err(_) => failed_connection_test(stages, "SftpTimeout: SSH authentication timed out".to_string()),
        }
    }

    fn configured_root(config: &SftpConnectionConfig) -> String {
        let root = config.root.trim_matches('/');
        if root.is_empty() {
            "/".to_string()
        } else {
            format!("{root}/")
        }
    }

    fn stages_after(stage: &str) -> Vec<&'static str> {
        match stage {
            "host_key" => vec!["authentication", "root"],
            "authentication" => vec!["root"],
            _ => Vec::new(),
        }
    }

    fn failed_connection_test(mut stages: Vec<ConnectionTestStage>, error: String) -> FileConnectionTestResult {
        let stage = connection_error_stage(&error);
        if stage != "host_key" {
            stages.push(passed_stage("host_key"));
        }
        if stage == "root" {
            stages.push(passed_stage("authentication"));
        }
        stages.push(failed_stage(stage, error));
        stages.extend(stages_after(stage).into_iter().map(skipped_stage));
        FileConnectionTestResult { success: false, stages }
    }

    pub(crate) async fn create_directory(
        config: &SftpConnectionConfig,
        path: &RemotePath,
        secrets: &ResolvedFileSecrets,
    ) -> Result<(), String> {
        let (operator, _key, path_guard) = build_operator(config, secrets)?;
        path_guard.require_destination(path.as_str().to_string()).await?;
        let path = configured_entry(config, path.as_str(), true);
        match operator.stat(&path).await {
            Ok(_) => Err("SftpConflict: destination already exists".to_string()),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                operator.create_dir(&path).await.map_err(classify_error)
            }
            Err(error) => Err(classify_error(error)),
        }
    }

    pub(crate) async fn delete_entry(
        config: &SftpConnectionConfig,
        path: &RemotePath,
        expected_kind: Option<&str>,
        secrets: &ResolvedFileSecrets,
    ) -> Result<super::super::file_manager::FileMutationResult, String> {
        let (operator, _key, path_guard) = build_operator(config, secrets)?;
        path_guard.require_existing(path.as_str().to_string()).await?;
        let file_path = configured_entry(config, path.as_str(), false);
        let directory_path = configured_entry(config, path.as_str(), true);
        let metadata = match operator.stat(&file_path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                operator.stat(&directory_path).await.map_err(classify_error)?
            }
            Err(error) => return Err(classify_error(error)),
        };
        let actual_path = if metadata.mode().is_dir() { directory_path } else { file_path };
        let actual_kind = if metadata.mode().is_dir() { "directory" } else { "file" };
        if expected_kind.is_some_and(|expected| expected != actual_kind) {
            return Err("SftpConflict: remote entry kind changed before delete".to_string());
        }
        if metadata.mode().is_dir() {
            let mut lister = operator.lister(&actual_path).await.map_err(classify_error)?;
            use futures::StreamExt;
            let directory = actual_path.trim_end_matches('/');
            while let Some(entry) = lister.next().await.transpose().map_err(classify_error)? {
                let listed = entry.path().trim_end_matches('/');
                if listed != directory {
                    return Err("Unsupported: non-empty directory deletion is not available in v1".to_string());
                }
            }
        }
        operator.delete(&actual_path).await.map_err(classify_error)?;
        Ok(super::super::file_manager::FileMutationResult {
            outcome: super::super::file_manager::FileMutationOutcome::Completed,
        })
    }

    fn configured_entry(config: &SftpConnectionConfig, path: &str, directory: bool) -> String {
        let root = config.root.trim_matches('/');
        let path = path.trim_matches('/');
        let joined = match (root.is_empty(), path.is_empty()) {
            (true, true) => String::new(),
            (true, false) => path.to_string(),
            (false, true) => root.to_string(),
            (false, false) => format!("{root}/{path}"),
        };
        if directory {
            if joined.is_empty() {
                "/".to_string()
            } else {
                format!("{joined}/")
            }
        } else {
            joined
        }
    }

    pub(crate) fn validate_private_key(material: &str, passphrase: Option<&str>) -> Result<(), String> {
        decode_secret_key(material, passphrase)
            .map(|_| ())
            .map_err(|_| "SftpAuthentication: private-key material or passphrase is invalid".to_string())
    }

    #[cfg(test)]
    mod tests {
        use std::convert::Infallible;
        use std::os::unix::fs::MetadataExt;

        use russh::keys::ssh_key::{
            private::{Ed25519Keypair, KeypairData},
            rand_core::{TryCryptoRng, TryRng},
            PrivateKey,
        };

        use super::*;

        struct TestRng(u64);

        impl TryRng for TestRng {
            type Error = Infallible;

            fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
                Ok(self.try_next_u64()? as u32)
            }

            fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                Ok(self.0)
            }

            fn try_fill_bytes(&mut self, destination: &mut [u8]) -> Result<(), Self::Error> {
                for chunk in destination.chunks_mut(8) {
                    let bytes = self.try_next_u64()?.to_le_bytes();
                    chunk.copy_from_slice(&bytes[..chunk.len()]);
                }
                Ok(())
            }
        }

        impl TryCryptoRng for TestRng {}

        #[test]
        fn residue_filename_parser_requires_canonical_uuid_and_exact_shape() {
            let pid = std::process::id();
            let uuid = Uuid::new_v4();
            assert_eq!(key_file_pid(&format!("{KEY_FILE_PREFIX}{pid}-{uuid}.key")), Some(pid));
            assert_eq!(key_file_pid(&format!("{KEY_FILE_PREFIX}{pid}-{}.key", uuid.to_string().to_uppercase())), None);
            assert_eq!(key_file_pid(&format!("{KEY_FILE_PREFIX}{pid}-{uuid}.key.extra")), None);
            assert_eq!(key_file_pid(&format!("{KEY_FILE_PREFIX}{pid}-not-a-uuid.key")), None);
            assert_eq!(key_file_pid(&format!("unrelated-{pid}-{uuid}.key")), None);
        }

        #[test]
        fn encrypted_key_is_decrypted_into_protected_ephemeral_material() {
            let keypair = Ed25519Keypair::from_seed(&[7_u8; 32]);
            let private_key = PrivateKey::new(KeypairData::Ed25519(keypair), "dbx-test").unwrap();
            let encrypted = private_key.encrypt(&mut TestRng(0x5eed), "correct-passphrase").unwrap();
            let encoded = encrypted.to_openssh(LineEnding::LF).unwrap();

            assert!(validate_private_key(encoded.as_str(), Some("correct-passphrase")).is_ok());
            assert!(validate_private_key(encoded.as_str(), Some("wrong-passphrase")).is_err());

            let material = materialize_private_key(encoded.as_str(), Some("correct-passphrase")).unwrap();
            let path = material.path.clone();
            let directory = path.parent().unwrap().to_path_buf();
            let metadata = fs::symlink_metadata(&path).unwrap();
            assert!(metadata.file_type().is_file());
            assert_eq!(metadata.mode() & 0o777, 0o600);
            assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
            assert_eq!(metadata.nlink(), 1);
            let persisted = fs::read_to_string(&path).unwrap();
            assert!(persisted.contains("BEGIN OPENSSH PRIVATE KEY"));
            assert!(!persisted.contains("correct-passphrase"));

            drop(material);
            assert!(!path.exists());
            assert!(!directory.exists());
        }

        #[test]
        fn configured_entry_joins_root_and_path_as_distinct_segments() {
            let mut config = super::super::SftpConnectionConfig {
                endpoint: "example".to_string(),
                root: "/".to_string(),
                username: "dbx".to_string(),
                authentication: super::super::SftpAuthentication::Agent,
            };
            assert_eq!(configured_entry(&config, "child.txt", false), "child.txt");
            assert_eq!(configured_entry(&config, "nested", true), "nested/");
            assert_eq!(configured_entry(&config, "", true), "/");

            config.root = "/srv/dbx".to_string();
            assert_eq!(configured_entry(&config, "child.txt", false), "srv/dbx/child.txt");
            assert_eq!(configured_entry(&config, "nested/child", true), "srv/dbx/nested/child/");
            assert_eq!(configured_entry(&config, "", true), "srv/dbx/");
        }
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod supported {
    use super::*;

    pub(crate) struct SftpKeyMaterial;
    pub(crate) struct SftpPathGuard;
    type SftpOperatorBuild = (Operator, Option<Arc<SftpKeyMaterial>>, Arc<SftpPathGuard>);

    impl SftpPathGuard {
        pub(crate) fn require_existing(
            self: &Arc<Self>,
            _relative_path: String,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'static>> {
            Box::pin(async { Err("Unsupported: SFTP is available only on macOS and Linux in v1".to_string()) })
        }

        pub(crate) fn require_destination(
            self: &Arc<Self>,
            _relative_path: String,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'static>> {
            Box::pin(async { Err("Unsupported: SFTP is available only on macOS and Linux in v1".to_string()) })
        }
    }

    pub(crate) fn cleanup_crash_residue() {}

    pub(crate) fn build_operator(
        _config: &SftpConnectionConfig,
        _secrets: &ResolvedFileSecrets,
    ) -> Result<SftpOperatorBuild, String> {
        Err("Unsupported: SFTP is available only on macOS and Linux in v1".to_string())
    }

    pub(crate) async fn test_connection(
        _config: &SftpConnectionConfig,
        _secrets: &ResolvedFileSecrets,
    ) -> FileConnectionTestResult {
        FileConnectionTestResult {
            success: false,
            stages: std::iter::once(failed_stage(
                "configuration",
                "Unsupported: SFTP is available only on macOS and Linux in v1".to_string(),
            ))
            .chain(["dns", "tcp", "host_key", "authentication", "root"].into_iter().map(skipped_stage))
            .collect(),
        }
    }

    pub(crate) async fn create_directory(
        _config: &SftpConnectionConfig,
        _path: &RemotePath,
        _secrets: &ResolvedFileSecrets,
    ) -> Result<(), String> {
        Err("Unsupported: SFTP is available only on macOS and Linux in v1".to_string())
    }

    pub(crate) async fn delete_entry(
        _config: &SftpConnectionConfig,
        _path: &RemotePath,
        _expected_kind: Option<&str>,
        _secrets: &ResolvedFileSecrets,
    ) -> Result<super::super::file_manager::FileMutationResult, String> {
        Err("Unsupported: SFTP is available only on macOS and Linux in v1".to_string())
    }

    pub(crate) fn validate_private_key(_material: &str, _passphrase: Option<&str>) -> Result<(), String> {
        Err("Unsupported: SFTP is available only on macOS and Linux in v1".to_string())
    }
}

pub(super) use supported::{
    build_operator, cleanup_crash_residue, create_directory, delete_entry, test_connection, validate_private_key,
    SftpKeyMaterial, SftpPathGuard,
};

#[cfg(test)]
#[allow(dead_code)]
type SftpPathGuardFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'static>>;

#[cfg(test)]
#[allow(dead_code)]
fn assert_sftp_path_guard_api(guard: &Arc<SftpPathGuard>) -> (SftpPathGuardFuture, SftpPathGuardFuture) {
    (guard.require_existing(String::new()), guard.require_destination(String::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(authentication: SftpAuthentication) -> SftpConnectionConfig {
        SftpConnectionConfig {
            endpoint: "ssh://example.test:2222".to_string(),
            root: "/srv/dbx".to_string(),
            username: "dbx".to_string(),
            authentication,
        }
    }

    #[test]
    fn endpoint_accepts_alias_or_ssh_url_without_credentials_or_options() {
        assert_eq!(endpoint_host_port("prod_alias-1").unwrap(), None);
        assert_eq!(endpoint_host_port("ssh://example.test:2222").unwrap(), Some(("example.test".to_string(), 2222)));
        for endpoint in [
            "-oProxyCommand=bad",
            "alias value",
            "user@example.test",
            "ssh://user@example.test",
            "ssh://example.test/path",
            "ssh://example.test?option=value",
            "ssh://example.test#fragment",
        ] {
            assert!(endpoint_host_port(endpoint).is_err(), "{endpoint} must be rejected");
        }
    }

    #[test]
    fn root_is_absolute_and_cannot_escape_the_configured_scope() {
        assert_eq!(normalize_root("/").unwrap(), "/");
        assert_eq!(normalize_root("//srv///dbx/").unwrap(), "/srv/dbx");
        for root in ["relative", "/srv/../etc", "/srv/%2E%2E/etc", "/srv\\dbx", "/srv/%00dbx"] {
            assert!(normalize_root(root).is_err(), "{root} must be rejected");
        }
    }

    #[test]
    fn auth_modes_reject_private_secrets_outside_private_key_mode() {
        let secrets =
            ResolvedFileSecrets { sftp_private_key: Some("secret".to_string()), ..ResolvedFileSecrets::default() };
        assert!(validate_config(&config(SftpAuthentication::SshConfig), true, &secrets).is_err());
        assert!(validate_config(&config(SftpAuthentication::Agent), true, &secrets).is_err());

        let empty = ResolvedFileSecrets::default();
        assert!(validate_config(&config(SftpAuthentication::PrivateKey), true, &empty).is_err());
    }

    #[test]
    fn operational_errors_are_classified_and_temporary_paths_are_redacted() {
        assert!(classify_message("Host key verification failed").starts_with("SftpHostKey:"));
        assert!(classify_message("Permission denied (publickey)").starts_with("SftpAuthentication:"));
        assert!(classify_message("Permission denied").starts_with("SftpPermission:"));
        assert!(classify_message("operation timed out").starts_with("SftpTimeout:"));
        assert!(classify_message("connection reset by peer").starts_with("SftpDisconnected:"));
        assert!(classify_message("Background task failed: read/flush task failed").starts_with("SftpDisconnected:"));
        assert_eq!(
            redact_temporary_key_paths(
                "could not read /tmp/dbx-sftp-keys-501/dbx-sftp-key-12-00000000-0000-0000-0000-000000000000.key"
            ),
            "could not read [SFTP_KEY_MATERIAL]"
        );
    }

    #[test]
    fn opendal_source_chain_preserves_ssh_failure_classification() {
        let host_key = opendal::Error::new(ErrorKind::Unexpected, "ssh error")
            .set_source(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Host key verification failed"));
        assert!(classify_error(host_key).starts_with("SftpHostKey:"));

        let authentication = opendal::Error::new(ErrorKind::Unexpected, "ssh error").set_source(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "dbx: Permission denied (publickey)",
        ));
        assert!(classify_error(authentication).starts_with("SftpAuthentication:"));
    }

    #[test]
    fn root_not_a_directory_is_classified_at_the_root_stage() {
        let error = classify_root_stat_error(opendal::Error::new(
            ErrorKind::NotADirectory,
            "a configured root component is not a directory",
        ));
        assert_eq!(error, "SftpRoot: configured root is not a directory");
        assert_eq!(connection_error_stage(&error), "root");
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    #[tokio::test]
    async fn sftp_windows_unsupported_contract() {
        let guard = Arc::new(SftpPathGuard);
        let (existing, destination) = assert_sftp_path_guard_api(&guard);
        for result in [existing.await, destination.await] {
            assert_eq!(result.unwrap_err(), "Unsupported: SFTP is available only on macOS and Linux in v1");
        }
    }
}
