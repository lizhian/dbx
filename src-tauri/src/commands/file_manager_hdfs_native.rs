use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use hdfs_native::{Client, ClientBuilder, HdfsError};
use opendal::services::HdfsNative;
use opendal::{EntryMode, ErrorKind, Operator};
use quick_xml::events::Event;
use quick_xml::Reader;
use serde::{Deserialize, Serialize};
use tokio::net::{lookup_host, TcpStream};
use url::Url;
use uuid::Uuid;

use super::file_manager::{
    failed_stage, passed_stage, skipped_stage, ConnectionTestStage, FileConnectionCapabilities,
    FileConnectionTestResult, FileMutationOutcome, FileMutationResult,
};
use super::file_manager_paths::RemotePath;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const CONFIG_FILE_LIMIT: u64 = 1024 * 1024;
const SIMPLE_USER_ENVIRONMENT: &str = "HADOOP_USER_NAME";
const PROBE_CONTENT: &[u8] = b"dbx-hdfs-native-datanode-probe-v1";

const ALLOWED_OPTIONS: &[&str] = &[
    "dfs.client.use.datanode.hostname",
    "dfs.client.block.write.replace-datanode-on-failure.best-effort",
    "dfs.client.block.write.replace-datanode-on-failure.enable",
    "dfs.client.block.write.replace-datanode-on-failure.policy",
    "dfs.user.home.dir.prefix",
    "hadoop.security.authentication",
];

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HdfsNativeAuthenticationEnvironment {
    pub user_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HdfsNativeConnectionConfig {
    pub name_node_uri: String,
    pub root: String,
    #[serde(default)]
    pub options: BTreeMap<String, String>,
    pub hadoop_config_directory: Option<String>,
    pub authentication_environment: Option<HdfsNativeAuthenticationEnvironment>,
}

#[derive(Clone)]
pub(super) struct HdfsNativeAdapter {
    client: Client,
    root: String,
}

#[derive(Debug)]
pub(super) struct HdfsNativeMutationError {
    pub message: String,
    pub outcome_unknown: bool,
}

pub(super) fn capabilities() -> FileConnectionCapabilities {
    FileConnectionCapabilities {
        read: true,
        write: true,
        stat: true,
        list: true,
        create_directory: true,
        delete: true,
        copy: true,
        rename: true,
        server_side_copy: false,
        atomic_rename: true,
        atomic_no_clobber: false,
    }
}

pub(super) fn normalize_config(config: &mut HdfsNativeConnectionConfig) -> Result<(), String> {
    config.name_node_uri = normalize_name_node_uri(&config.name_node_uri)?;
    config.root = normalize_root(&config.root)?;
    if let Some(authentication_environment) = &mut config.authentication_environment {
        authentication_environment.user_name = authentication_environment.user_name.trim().to_string();
    }
    config.options =
        config.options.iter().map(|(key, value)| (key.trim().to_string(), value.trim().to_string())).collect();
    if let Some(directory) = config.hadoop_config_directory.as_deref() {
        let canonical = validate_config_directory(directory)?;
        config.hadoop_config_directory = Some(canonical.to_string_lossy().into_owned());
    }
    validate_config(config)
}

pub(super) fn validate_config(config: &HdfsNativeConnectionConfig) -> Result<(), String> {
    validate_static_config(config)?;
    if std::env::var_os("HADOOP_CONF_DIR").is_some() || std::env::var_os("HADOOP_HOME").is_some() {
        return Err(
            "HDFS Native refuses ambient HADOOP_CONF_DIR/HADOOP_HOME; remove them and use hadoopConfigDirectory so DBX can enforce the option allowlist"
                .to_string(),
        );
    }
    match (&config.authentication_environment, std::env::var(SIMPLE_USER_ENVIRONMENT)) {
        (Some(_), Ok(value)) if !value.trim().is_empty() => Ok(()),
        (Some(_), Ok(_)) => Err(format!("{SIMPLE_USER_ENVIRONMENT} cannot be empty")),
        (Some(_), Err(_)) => Err(format!(
            "{SIMPLE_USER_ENVIRONMENT} is not set; HDFS Native never mutates process authentication environment"
        )),
        (None, Ok(_)) => Err(format!(
            "HDFS Native refuses ambient {SIMPLE_USER_ENVIRONMENT} unless authenticationEnvironment explicitly references it"
        )),
        (None, Err(_)) => Ok(()),
    }
}

fn validate_static_config(config: &HdfsNativeConnectionConfig) -> Result<(), String> {
    normalize_name_node_uri(&config.name_node_uri)?;
    normalize_root(&config.root)?;
    if let Some(authentication_environment) = &config.authentication_environment {
        if authentication_environment.user_name != SIMPLE_USER_ENVIRONMENT {
            return Err(format!(
                "HDFS Native simple authentication supports only the {SIMPLE_USER_ENVIRONMENT} environment reference"
            ));
        }
    }
    resolved_options(config)?;
    Ok(())
}

pub(super) fn build_operator(
    config: &HdfsNativeConnectionConfig,
) -> Result<(Operator, Arc<HdfsNativeAdapter>), String> {
    validate_config(config)?;
    let options = resolved_options(config)?;
    let builder = HdfsNative::default().name_node(&config.name_node_uri).root(&config.root).options(options.clone());
    let operator =
        Operator::new(builder).map(|builder| builder.finish()).map_err(|error| classify_opendal_error(&error))?;
    let adapter = Arc::new(build_adapter(config, options)?);
    Ok((operator, adapter))
}

pub(super) fn build_direct_adapter(config: &HdfsNativeConnectionConfig) -> Result<Arc<HdfsNativeAdapter>, String> {
    validate_config(config)?;
    Ok(Arc::new(build_adapter(config, resolved_options(config)?)?))
}

pub(super) async fn test_connection(config: &HdfsNativeConnectionConfig) -> FileConnectionTestResult {
    let mut stages = Vec::new();
    if let Err(error) = validate_config(config) {
        stages.push(failed_stage("configuration", error));
        append_skipped_stages(&mut stages, &["dns", "tcp", "namenode_rpc", "root", "datanode_write", "datanode_read"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("configuration"));

    let (host, port) = match name_node_host_port(&config.name_node_uri) {
        Ok(value) => value,
        Err(error) => {
            stages.push(failed_stage("dns", error));
            append_skipped_stages(&mut stages, &["tcp", "namenode_rpc", "root", "datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    let mut addresses = match tokio::time::timeout(CONNECTION_TIMEOUT, lookup_host((host.as_str(), port))).await {
        Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
        Ok(Err(error)) => {
            stages.push(failed_stage("dns", format!("HdfsNativeDns: {error}")));
            append_skipped_stages(&mut stages, &["tcp", "namenode_rpc", "root", "datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Err(_) => {
            stages.push(failed_stage("dns", "HdfsNativeTimeout: NameNode DNS lookup timed out".to_string()));
            append_skipped_stages(&mut stages, &["tcp", "namenode_rpc", "root", "datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    if addresses.is_empty() {
        stages.push(failed_stage("dns", "HdfsNativeDns: no NameNode addresses were resolved".to_string()));
        append_skipped_stages(&mut stages, &["tcp", "namenode_rpc", "root", "datanode_write", "datanode_read"]);
        return FileConnectionTestResult { success: false, stages };
    }
    addresses.sort_unstable();
    addresses.dedup();
    addresses.truncate(16);
    stages.push(passed_stage("dns"));

    if let Err(error) = connect_any(&addresses).await {
        stages.push(failed_stage("tcp", error));
        append_skipped_stages(&mut stages, &["namenode_rpc", "root", "datanode_write", "datanode_read"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("tcp"));

    let (operator, adapter) = match build_operator(config) {
        Ok(built) => built,
        Err(error) => {
            stages.push(failed_stage("namenode_rpc", error));
            append_skipped_stages(&mut stages, &["root", "datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    match tokio::time::timeout(CONNECTION_TIMEOUT, adapter.client.get_file_info("/")).await {
        Ok(Ok(_)) => stages.push(passed_stage("namenode_rpc")),
        Ok(Err(error)) => {
            stages.push(failed_stage("namenode_rpc", classify_hdfs_error(error)));
            append_skipped_stages(&mut stages, &["root", "datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Err(_) => {
            stages
                .push(failed_stage("namenode_rpc", "HdfsNativeTimeout: NameNode RPC validation timed out".to_string()));
            append_skipped_stages(&mut stages, &["root", "datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
    }

    match tokio::time::timeout(CONNECTION_TIMEOUT, operator.stat("/")).await {
        Ok(Ok(status)) if status.mode() == EntryMode::DIR => stages.push(passed_stage("root")),
        Ok(Ok(_)) => {
            stages.push(failed_stage("root", "HdfsNativeRoot: configured root is not a directory".to_string()));
            append_skipped_stages(&mut stages, &["datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Ok(Err(error)) => {
            let message = if error.kind() == ErrorKind::NotFound {
                "HdfsNativeRoot: configured root does not exist".to_string()
            } else {
                classify_opendal_error(&error)
            };
            stages.push(failed_stage("root", message));
            append_skipped_stages(&mut stages, &["datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Err(_) => {
            stages.push(failed_stage("root", "HdfsNativeTimeout: configured root validation timed out".to_string()));
            append_skipped_stages(&mut stages, &["datanode_write", "datanode_read"]);
            return FileConnectionTestResult { success: false, stages };
        }
    }

    let probe_relative = format!(".dbx-connection-test-{}", Uuid::new_v4());
    let probe_absolute = adapter.absolute_path(&probe_relative);
    let write_result =
        tokio::time::timeout(CONNECTION_TIMEOUT, operator.write(&probe_relative, PROBE_CONTENT.to_vec())).await;
    match write_result {
        Ok(Ok(_)) => stages.push(passed_stage("datanode_write")),
        Ok(Err(error)) => {
            let cleanup = cleanup_probe(&adapter, &probe_absolute).await;
            let message = match cleanup {
                Ok(()) => classify_opendal_error(&error),
                Err(cleanup) => format!(
                    "HdfsNativePartial: DataNode write failed and probe '{}' cleanup was not confirmed: {cleanup}",
                    probe_relative
                ),
            };
            stages.push(failed_stage("datanode_write", message));
            stages.push(skipped_stage("datanode_read"));
            return FileConnectionTestResult { success: false, stages };
        }
        Err(_) => {
            let cleanup = cleanup_probe(&adapter, &probe_absolute).await;
            let message = match cleanup {
                Ok(()) => {
                    "HdfsNativeTimeout: DataNode write validation timed out; probe cleanup was confirmed".to_string()
                }
                Err(cleanup) => format!(
                    "HdfsNativePartial: DataNode write timed out and probe '{}' cleanup was not confirmed: {cleanup}",
                    probe_relative
                ),
            };
            stages.push(failed_stage("datanode_write", message));
            stages.push(skipped_stage("datanode_read"));
            return FileConnectionTestResult { success: false, stages };
        }
    }

    let read_result = tokio::time::timeout(CONNECTION_TIMEOUT, operator.read(&probe_relative)).await;
    let cleanup_result = cleanup_probe(&adapter, &probe_absolute).await;
    match (read_result, cleanup_result) {
        (Ok(Ok(content)), Ok(())) if content.to_vec() == PROBE_CONTENT => {
            stages.push(passed_stage("datanode_read"));
            FileConnectionTestResult { success: true, stages }
        }
        (Ok(Ok(content)), Ok(())) => {
            stages.push(failed_stage(
                "datanode_read",
                format!(
                    "HdfsNativeDataNode: DataNode read returned {} bytes but probe content did not match",
                    content.len()
                ),
            ));
            FileConnectionTestResult { success: false, stages }
        }
        (Ok(Ok(_)), Err(cleanup)) => {
            stages.push(failed_stage(
                "datanode_read",
                format!(
                    "HdfsNativePartial: DataNode read passed but probe '{}' cleanup was not confirmed: {cleanup}",
                    probe_relative
                ),
            ));
            FileConnectionTestResult { success: false, stages }
        }
        (Ok(Err(error)), Ok(())) => {
            stages.push(failed_stage("datanode_read", classify_opendal_error(&error)));
            FileConnectionTestResult { success: false, stages }
        }
        (Ok(Err(_)), Err(cleanup)) => {
            stages.push(failed_stage(
                "datanode_read",
                format!(
                    "HdfsNativePartial: DataNode read failed and probe '{}' cleanup was not confirmed: {cleanup}",
                    probe_relative
                ),
            ));
            FileConnectionTestResult { success: false, stages }
        }
        (Err(_), Ok(())) => {
            stages.push(failed_stage(
                "datanode_read",
                "HdfsNativeTimeout: DataNode read validation timed out".to_string(),
            ));
            FileConnectionTestResult { success: false, stages }
        }
        (Err(_), Err(cleanup)) => {
            stages.push(failed_stage(
                "datanode_read",
                format!(
                    "HdfsNativePartial: DataNode read timed out and probe '{}' cleanup was not confirmed: {cleanup}",
                    probe_relative
                ),
            ));
            FileConnectionTestResult { success: false, stages }
        }
    }
}

pub(super) async fn delete_entry(
    adapter: &HdfsNativeAdapter,
    path: &RemotePath,
    expected_kind: Option<&str>,
) -> Result<FileMutationResult, String> {
    let absolute = adapter.absolute_path(path.as_str());
    let status = adapter.client.get_file_info(&absolute).await.map_err(classify_hdfs_error)?;
    let observed = if status.isdir { "directory" } else { "file" };
    if expected_kind.is_some_and(|expected| expected != observed) {
        return Err(format!("HdfsNativeConflict: expected {expected_kind:?}, but the current entry is a {observed}"));
    }
    if status.isdir {
        let entries = adapter.client.list_status(&absolute, false).await.map_err(classify_hdfs_error)?;
        if !entries.is_empty() {
            return Err("Unsupported: non-empty directory deletion is not available in v1".to_string());
        }
        wait_at_test_delete_after_empty_check().await;
    }
    match adapter.client.delete(&absolute, false).await {
        Ok(true) => Ok(FileMutationResult { outcome: FileMutationOutcome::Completed }),
        Ok(false) => Err("HdfsNativeNotFound: entry no longer exists".to_string()),
        Err(error) => Err(classify_hdfs_error(error)),
    }
}

#[cfg(test)]
pub(super) struct DeleteBarrier {
    pub(super) reached: Arc<tokio::sync::Notify>,
    pub(super) release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static TEST_DELETE_AFTER_EMPTY_CHECK_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<DeleteBarrier>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
#[allow(dead_code)]
pub(super) fn install_test_delete_after_empty_check_barrier() -> DeleteBarrier {
    let barrier =
        DeleteBarrier { reached: Arc::new(tokio::sync::Notify::new()), release: Arc::new(tokio::sync::Notify::new()) };
    *TEST_DELETE_AFTER_EMPTY_CHECK_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner()) =
        Some(DeleteBarrier { reached: barrier.reached.clone(), release: barrier.release.clone() });
    barrier
}

#[cfg(test)]
async fn wait_at_test_delete_after_empty_check() {
    let barrier = TEST_DELETE_AFTER_EMPTY_CHECK_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take();
    if let Some(barrier) = barrier {
        barrier.reached.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(not(test))]
async fn wait_at_test_delete_after_empty_check() {}

impl HdfsNativeAdapter {
    pub(super) async fn delete_owned_file_if_exists(&self, path: &str) -> Result<(), String> {
        let absolute = self.absolute_path(path);
        let status = match self.client.get_file_info(&absolute).await {
            Ok(status) => status,
            Err(HdfsError::FileNotFound(_)) => return Ok(()),
            Err(error) => return Err(classify_hdfs_error(error)),
        };
        if status.isdir {
            return Err("HdfsNativeConflict: operation-owned partial is not a file".to_string());
        }
        match self.client.delete(&absolute, false).await {
            Ok(true) => Ok(()),
            Ok(false) => Ok(()),
            Err(error) => Err(classify_hdfs_error(error)),
        }
    }

    pub(super) async fn rename(
        &self,
        source: &str,
        destination: &str,
        replace: bool,
    ) -> Result<(), HdfsNativeMutationError> {
        let source = self.absolute_path(source);
        let destination = self.absolute_path(destination);
        self.client.rename(&source, &destination, replace).await.map_err(|error| HdfsNativeMutationError {
            outcome_unknown: mutation_outcome_unknown(&error),
            message: classify_hdfs_error(error),
        })
    }

    fn absolute_path(&self, relative: &str) -> String {
        let relative = relative.trim_matches('/');
        if relative.is_empty() {
            self.root.clone()
        } else if self.root == "/" {
            format!("/{relative}")
        } else {
            format!("{}/{relative}", self.root.trim_end_matches('/'))
        }
    }
}

fn build_adapter(
    config: &HdfsNativeConnectionConfig,
    options: HashMap<String, String>,
) -> Result<HdfsNativeAdapter, String> {
    let client = ClientBuilder::new()
        .with_url(&config.name_node_uri)
        .with_config(options)
        .build()
        .map_err(classify_hdfs_error)?;
    Ok(HdfsNativeAdapter { client, root: config.root.clone() })
}

fn resolved_options(config: &HdfsNativeConnectionConfig) -> Result<HashMap<String, String>, String> {
    let mut options = HashMap::new();
    if let Some(directory) = config.hadoop_config_directory.as_deref() {
        let directory = validate_config_directory(directory)?;
        for file in ["core-site.xml", "hdfs-site.xml"] {
            let path = directory.join(file);
            if !path.exists() {
                continue;
            }
            for (key, value) in read_hadoop_config(&path)? {
                reject_unsupported_cluster_mode(&key, &value)?;
                if ALLOWED_OPTIONS.contains(&key.as_str()) {
                    validate_option(&key, &value)?;
                    options.insert(key, value);
                }
            }
        }
    }
    for (key, value) in &config.options {
        if !ALLOWED_OPTIONS.contains(&key.as_str()) {
            return Err(format!("HDFS Native option '{key}' is not allowlisted"));
        }
        reject_unsupported_cluster_mode(key, value)?;
        validate_option(key, value)?;
        options.insert(key.clone(), value.clone());
    }
    options.insert("hadoop.security.authentication".to_string(), "simple".to_string());
    Ok(options)
}

fn validate_option(key: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 1024 || value.contains('\0') || value.contains('\r') || value.contains('\n') {
        return Err(format!("HDFS Native option '{key}' has an invalid value"));
    }
    match key {
        "hadoop.security.authentication" if !value.eq_ignore_ascii_case("simple") => {
            Err("Unsupported: HDFS Native Kerberos authentication is not certified in v1".to_string())
        }
        "dfs.client.use.datanode.hostname"
        | "dfs.client.block.write.replace-datanode-on-failure.enable"
        | "dfs.client.block.write.replace-datanode-on-failure.best-effort"
            if !matches!(value, "true" | "false") =>
        {
            Err(format!("HDFS Native option '{key}' must be true or false"))
        }
        "dfs.client.block.write.replace-datanode-on-failure.policy"
            if !matches!(value, "NEVER" | "DEFAULT" | "ALWAYS") =>
        {
            Err(format!("HDFS Native option '{key}' must be NEVER, DEFAULT, or ALWAYS"))
        }
        "dfs.user.home.dir.prefix" => normalize_root(value).map(|_| ()).map_err(|_| {
            "HDFS Native option 'dfs.user.home.dir.prefix' must be an absolute safe HDFS path".to_string()
        }),
        _ => Ok(()),
    }
}

fn reject_unsupported_cluster_mode(key: &str, value: &str) -> Result<(), String> {
    if key.starts_with("dfs.ha.")
        || key.starts_with("dfs.client.failover.")
        || key.starts_with("dfs.namenode.rpc-address.")
        || key.starts_with("fs.viewfs.")
        || (key == "fs.defaultFS" && (value.contains(',') || value.starts_with("viewfs:")))
    {
        return Err("Unsupported: HDFS Native HA, ViewFS, and multiple NameNodes are not certified in v1".to_string());
    }
    if key == "hadoop.security.authentication" && !value.eq_ignore_ascii_case("simple") {
        return Err("Unsupported: HDFS Native Kerberos authentication is not certified in v1".to_string());
    }
    Ok(())
}

fn validate_config_directory(directory: &str) -> Result<PathBuf, String> {
    let path = Path::new(directory.trim());
    if !path.is_absolute() {
        return Err("HDFS Native Hadoop config directory must be absolute".to_string());
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| "HDFS Native Hadoop config directory does not exist or is inaccessible".to_string())?;
    if !canonical.is_dir() {
        return Err("HDFS Native Hadoop config directory is not a directory".to_string());
    }
    Ok(canonical)
}

fn read_hadoop_config(path: &Path) -> Result<Vec<(String, String)>, String> {
    let metadata = fs::metadata(path).map_err(|_| "HDFS Native Hadoop config file is inaccessible".to_string())?;
    if !metadata.is_file() || metadata.len() > CONFIG_FILE_LIMIT {
        return Err("HDFS Native Hadoop config file must be a regular file no larger than 1 MiB".to_string());
    }
    let content =
        fs::read_to_string(path).map_err(|_| "HDFS Native Hadoop config file must be readable UTF-8".to_string())?;
    let mut reader = Reader::from_str(&content);
    reader.config_mut().trim_text(true);
    let mut properties = Vec::new();
    let mut in_property = false;
    let mut current_field: Option<&'static str> = None;
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) if event.name().as_ref() == b"property" => {
                in_property = true;
                key = None;
                value = None;
            }
            Ok(Event::Start(event)) if in_property && event.name().as_ref() == b"name" => {
                current_field = Some("name");
            }
            Ok(Event::Start(event)) if in_property && event.name().as_ref() == b"value" => {
                current_field = Some("value");
            }
            Ok(Event::Text(text)) if in_property => {
                let text = text
                    .decode()
                    .map_err(|_| "HDFS Native Hadoop config XML contains invalid text".to_string())?
                    .trim()
                    .to_string();
                match current_field {
                    Some("name") => key = Some(text),
                    Some("value") => value = Some(text),
                    _ => {}
                }
            }
            Ok(Event::End(event)) if event.name().as_ref() == b"name" || event.name().as_ref() == b"value" => {
                current_field = None;
            }
            Ok(Event::End(event)) if event.name().as_ref() == b"property" => {
                in_property = false;
                current_field = None;
                if let (Some(key), Some(value)) = (key.take(), value.take()) {
                    properties.push((key, value));
                }
            }
            Ok(Event::DocType(_)) => return Err("HDFS Native Hadoop config XML must not contain a DTD".to_string()),
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => return Err("HDFS Native Hadoop config XML is invalid".to_string()),
        }
    }
    Ok(properties)
}

fn normalize_name_node_uri(value: &str) -> Result<String, String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.contains(',') {
        return Err("Unsupported: HDFS Native requires exactly one NameNode in v1".to_string());
    }
    let url = Url::parse(trimmed).map_err(|_| "HDFS Native NameNode URI must be a valid hdfs:// URL".to_string())?;
    if url.scheme() != "hdfs" {
        return Err("HDFS Native NameNode URI must use hdfs://".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("HDFS Native credentials must not be embedded in the NameNode URI".to_string());
    }
    if url.host_str().is_none() || url.port().is_none() {
        return Err("HDFS Native NameNode URI must contain one host and explicit RPC port".to_string());
    }
    if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
        return Err(
            "HDFS Native NameNode URI must not contain a path, query, or fragment; use the root field".to_string()
        );
    }
    Ok(trimmed.to_string())
}

pub(super) fn normalize_root(root: &str) -> Result<String, String> {
    let decoded = percent_encoding::percent_decode_str(root.trim())
        .decode_utf8()
        .map_err(|_| "HDFS Native root contains invalid percent-encoded UTF-8".to_string())?;
    if !decoded.starts_with('/') {
        return Err("HDFS Native root must be an absolute path beginning with '/'".to_string());
    }
    if decoded.contains('\0') || decoded.contains('\\') {
        return Err("HDFS Native root contains an invalid character".to_string());
    }
    let mut segments = Vec::new();
    for segment in decoded.split('/').filter(|segment| !segment.is_empty()) {
        if matches!(segment, "." | "..") {
            return Err("HDFS Native root cannot contain '.' or '..' path segments".to_string());
        }
        segments.push(segment);
    }
    Ok(if segments.is_empty() { "/".to_string() } else { format!("/{}", segments.join("/")) })
}

fn name_node_host_port(uri: &str) -> Result<(String, u16), String> {
    let url = Url::parse(uri).map_err(|_| "HdfsNativeDns: invalid NameNode URI".to_string())?;
    let host = url.host_str().ok_or_else(|| "HdfsNativeDns: NameNode host is missing".to_string())?;
    let port = url.port().ok_or_else(|| "HdfsNativeDns: NameNode RPC port is missing".to_string())?;
    Ok((host.to_string(), port))
}

async fn connect_any(addresses: &[std::net::SocketAddr]) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + CONNECTION_TIMEOUT;
    let mut last_error = None;
    for address in addresses {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            last_error = Some("connection attempts timed out".to_string());
            break;
        }
        match tokio::time::timeout(remaining, TcpStream::connect(address)).await {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_) => last_error = Some("connection attempt timed out".to_string()),
        }
    }
    Err(format!(
        "HdfsNativeTcp: NameNode TCP connection failed: {}",
        last_error.unwrap_or_else(|| "no addresses".to_string())
    ))
}

async fn cleanup_probe(adapter: &HdfsNativeAdapter, absolute_path: &str) -> Result<(), String> {
    match tokio::time::timeout(CONNECTION_TIMEOUT, adapter.client.delete(absolute_path, false)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(classify_hdfs_error(error)),
        Err(_) => Err("HdfsNativeTimeout: connection-test probe cleanup timed out".to_string()),
    }
}

pub(super) fn classify_opendal_error(error: &opendal::Error) -> String {
    match error.kind() {
        ErrorKind::NotFound => "HdfsNativeNotFound: remote path does not exist".to_string(),
        ErrorKind::AlreadyExists => "HdfsNativeConflict: remote destination already exists".to_string(),
        ErrorKind::PermissionDenied => "HdfsNativePermission: permission denied".to_string(),
        ErrorKind::IsADirectory | ErrorKind::NotADirectory => {
            "HdfsNativePathType: remote path has the wrong type".to_string()
        }
        ErrorKind::ConfigInvalid => "HdfsNativeConfiguration: native client configuration is invalid".to_string(),
        _ => {
            let mut descriptions = Vec::new();
            let mut source = std::error::Error::source(error);
            while let Some(current) = source {
                descriptions.push(current.to_string().to_ascii_lowercase());
                source = current.source();
            }
            let description = descriptions.join(" ");
            if description.contains("accesscontrolexception") || description.contains("permission denied") {
                "HdfsNativePermission: permission denied".to_string()
            } else if description.contains("sasl")
                || description.contains("gssapi")
                || description.contains("authentication")
            {
                "HdfsNativeAuthentication: authentication failed; only simple auth is supported in v1".to_string()
            } else if description.contains("timed out") || description.contains("timeout") {
                "HdfsNativeTimeout: operation timed out".to_string()
            } else if description.contains("connection refused")
                || description.contains("connection reset")
                || description.contains("broken pipe")
                || description.contains("unexpected eof")
            {
                "HdfsNativeDisconnected: HDFS connection was interrupted".to_string()
            } else if description.contains("datanode")
                || description.contains("data transfer")
                || description.contains("checksum")
            {
                "HdfsNativeDataNode: DataNode operation failed".to_string()
            } else if description.contains("rpc") || description.contains("namenode") {
                "HdfsNativeRpc: NameNode RPC failed".to_string()
            } else {
                "HdfsNativeProtocol: native HDFS operation failed".to_string()
            }
        }
    }
}

pub(super) fn classify_hdfs_error(error: HdfsError) -> String {
    match &error {
        HdfsError::AlreadyExists(_) => "HdfsNativeConflict: remote destination already exists".to_string(),
        HdfsError::FileNotFound(_) | HdfsError::BlocksNotFound(_) => {
            "HdfsNativeNotFound: remote path does not exist".to_string()
        }
        HdfsError::IsADirectoryError(_) => "HdfsNativePathType: remote path is a directory".to_string(),
        HdfsError::InvalidPath(_) | HdfsError::InvalidArgument(_) | HdfsError::UrlParseError(_) => {
            "HdfsNativeConfiguration: native client configuration or path is invalid".to_string()
        }
        HdfsError::RPCError(class, _) | HdfsError::FatalRPCError(class, _)
            if class.contains("AccessControlException") =>
        {
            "HdfsNativePermission: permission denied".to_string()
        }
        HdfsError::IOError(_) => "HdfsNativeDisconnected: HDFS connection was interrupted".to_string(),
        HdfsError::DataTransferError(_) | HdfsError::ChecksumError => {
            "HdfsNativeDataNode: DataNode operation failed".to_string()
        }
        HdfsError::SASLError(_) | HdfsError::NoSASLMechanism | HdfsError::GSSAPIError(_, _, _) => {
            "HdfsNativeAuthentication: authentication failed; only simple auth is supported in v1".to_string()
        }
        HdfsError::RPCError(_, _) | HdfsError::FatalRPCError(_, _) => "HdfsNativeRpc: NameNode RPC failed".to_string(),
        _ => "HdfsNativeProtocol: native HDFS operation failed".to_string(),
    }
}

fn mutation_outcome_unknown(error: &HdfsError) -> bool {
    matches!(
        error,
        HdfsError::IOError(_)
            | HdfsError::DataTransferError(_)
            | HdfsError::ChecksumError
            | HdfsError::OperationFailed(_)
            | HdfsError::FatalRPCError(_, _)
            | HdfsError::InvalidRPCResponse(_)
    )
}

fn append_skipped_stages(stages: &mut Vec<ConnectionTestStage>, remaining: &[&'static str]) {
    stages.extend(remaining.iter().copied().map(skipped_stage));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_config() -> HdfsNativeConnectionConfig {
        HdfsNativeConnectionConfig {
            name_node_uri: std::env::var("DBX_TEST_HDFS_NATIVE_NAMENODE").unwrap(),
            root: std::env::var("DBX_TEST_HDFS_NATIVE_ROOT").unwrap(),
            options: BTreeMap::from([("dfs.client.use.datanode.hostname".to_string(), "true".to_string())]),
            hadoop_config_directory: Some(std::env::var("DBX_TEST_HDFS_NATIVE_HADOOP_CONFIG_DIR").unwrap()),
            authentication_environment: Some(HdfsNativeAuthenticationEnvironment {
                user_name: std::env::var("DBX_TEST_HDFS_NATIVE_AUTHENTICATION_ENVIRONMENT").unwrap(),
            }),
        }
    }

    fn config() -> HdfsNativeConnectionConfig {
        HdfsNativeConnectionConfig {
            name_node_uri: "hdfs://127.0.0.1:9000".to_string(),
            root: "/dbx".to_string(),
            options: BTreeMap::new(),
            hadoop_config_directory: None,
            authentication_environment: Some(HdfsNativeAuthenticationEnvironment {
                user_name: SIMPLE_USER_ENVIRONMENT.to_string(),
            }),
        }
    }

    #[test]
    fn rejects_ha_kerberos_and_non_native_auth_environment_indirection() {
        let mut value = config();
        value.name_node_uri = "hdfs://nn1:9000,nn2:9000".to_string();
        assert!(validate_static_config(&value).unwrap_err().contains("exactly one NameNode"));

        let mut value = config();
        value.options.insert("hadoop.security.authentication".to_string(), "kerberos".to_string());
        assert!(validate_static_config(&value).unwrap_err().contains("Kerberos"));

        let mut value = config();
        value.authentication_environment.as_mut().unwrap().user_name = "DBX_HDFS_USER".to_string();
        assert!(validate_static_config(&value).unwrap_err().contains(SIMPLE_USER_ENVIRONMENT));
    }

    #[test]
    fn reads_only_allowlisted_config_and_rejects_ha_metadata() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("core-site.xml"),
            r#"<configuration>
                <property><name>fs.defaultFS</name><value>hdfs://ignored:9000</value></property>
                <property><name>hadoop.security.authentication</name><value>simple</value></property>
                <property><name>dfs.user.home.dir.prefix</name><value>/users</value></property>
            </configuration>"#,
        )
        .unwrap();
        let mut value = config();
        value.hadoop_config_directory = Some(dir.path().to_string_lossy().into_owned());
        value.options.insert("dfs.user.home.dir.prefix".to_string(), "/explicit-users".to_string());
        value.options.insert("dfs.client.use.datanode.hostname".to_string(), "true".to_string());
        let options = resolved_options(&value).unwrap();
        assert_eq!(options.get("dfs.user.home.dir.prefix").map(String::as_str), Some("/explicit-users"));
        assert_eq!(options.get("dfs.client.use.datanode.hostname").map(String::as_str), Some("true"));
        assert!(!options.contains_key("fs.defaultFS"));

        fs::write(
            dir.path().join("hdfs-site.xml"),
            r#"<configuration>
                <property><name>dfs.ha.namenodes.cluster</name><value>nn1,nn2</value></property>
            </configuration>"#,
        )
        .unwrap();
        assert!(resolved_options(&value).unwrap_err().contains("multiple NameNodes"));
    }

    #[test]
    fn normalize_root_and_name_node_reject_escape_and_embedded_credentials() {
        assert_eq!(normalize_root(" /a/b/ ").unwrap(), "/a/b");
        assert!(normalize_root("/a/../b").is_err());
        assert!(normalize_root("/a%2F..%2Fb").is_err());
        assert!(normalize_name_node_uri("hdfs://user:secret@nn:9000").is_err());
        assert!(normalize_name_node_uri("hdfs://nn:9000/path").is_err());
        assert_eq!(normalize_name_node_uri("hdfs://nn:9000/").unwrap(), "hdfs://nn:9000");
    }

    #[test]
    fn dependency_contract_is_native_and_has_no_jni_surface() {
        let cargo = include_str!("../../Cargo.toml");
        assert!(cargo.contains("\"services-hdfs-native\""));
        assert!(!cargo.contains("\"services-hdfs\""));
        assert!(cargo.contains("hdfs-native = \"=0.13.5\""));
    }

    #[test]
    fn opendal_error_classification_never_echoes_hdfs_identity_or_paths() {
        let error =
            opendal::Error::new(ErrorKind::Unexpected, "native backend failed").set_source(std::io::Error::other(
                "org.apache.hadoop.security.AccessControlException: user=contract-secret path=/private/config",
            ));
        let message = classify_opendal_error(&error);
        assert_eq!(message, "HdfsNativePermission: permission denied");
        assert!(!message.contains("contract-secret"));
        assert!(!message.contains("/private/config"));
    }

    #[test]
    #[ignore = "run in an isolated subprocess with ambient Hadoop configuration variables"]
    fn ambient_hdfs_native_config_contract() {
        let error = validate_config(&config()).expect_err("ambient Hadoop configuration must be rejected");
        assert!(error.contains("refuses ambient HADOOP_CONF_DIR/HADOOP_HOME"), "{error}");
        for value in ["HADOOP_CONF_DIR", "HADOOP_HOME"]
            .into_iter()
            .filter_map(|name| std::env::var(name).ok())
            .filter(|value| !value.is_empty())
        {
            assert!(!error.contains(&value));
        }
    }

    #[tokio::test]
    #[ignore = "requires the fixed Hadoop and fault-proxy contract environment"]
    async fn fixed_hdfs_native_service_smoke_contract() {
        let config = fixed_config();
        let result = test_connection(&config).await;
        assert!(
            result.success,
            "connection stages: {:?}",
            result.stages.iter().map(|stage| (&stage.stage, &stage.status, &stage.message)).collect::<Vec<_>>()
        );
        assert_eq!(
            result.stages.iter().map(|stage| stage.stage).collect::<Vec<_>>(),
            ["configuration", "dns", "tcp", "namenode_rpc", "root", "datanode_write", "datanode_read",]
        );

        let (operator, adapter) = build_operator(&config).unwrap();
        let fixture = operator.stat("fixture.txt").await.unwrap();
        assert!(fixture.mode().is_file());
        let entries = operator.list("/").await.unwrap();
        assert!(entries.len() >= 208, "expected pagination fixture, got {}", entries.len());

        let prefix = format!("fixed-service-{}", Uuid::new_v4());
        let directory = format!("{prefix}/");
        let file = format!("{prefix}/roundtrip.bin");
        operator.create_dir(&directory).await.unwrap();
        operator.write(&file, b"native-roundtrip".to_vec()).await.unwrap();
        assert_eq!(operator.read(&file).await.unwrap().to_vec(), b"native-roundtrip");
        delete_entry(&adapter, &RemotePath::parse(&file).unwrap(), Some("file")).await.unwrap();
        delete_entry(&adapter, &RemotePath::parse(prefix.as_str()).unwrap(), Some("directory")).await.unwrap();

        assert!(operator.stat("denied/").await.unwrap().mode().is_dir());
        let denied_errors =
            [operator.list("denied/").await.unwrap_err(), operator.read("denied/secret.txt").await.unwrap_err()];
        let contract_user = std::env::var("DBX_TEST_HDFS_NATIVE_CONTRACT_USER").unwrap();
        for error in denied_errors {
            let classified = classify_opendal_error(&error);
            assert!(classified.starts_with("HdfsNativePermission:"), "{classified}");
            for secret in [contract_user.as_str(), "denied", "secret.txt", "token"] {
                assert!(!classified.to_ascii_lowercase().contains(&secret.to_ascii_lowercase()), "{classified}");
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires the fixed Hadoop and fault-proxy contract environment"]
    async fn fixed_hdfs_native_transfer_smoke_contract() {
        let config = fixed_config();
        let (operator, adapter) = build_operator(&config).unwrap();
        let prefix = format!("fixed-transfer-{}", Uuid::new_v4());
        let source = format!("{prefix}-source.bin");
        let copied_partial = format!("{prefix}-copy.part");
        let copied = format!("{prefix}-copy.bin");
        let renamed = format!("{prefix}-renamed.bin");
        let replacement = format!("{prefix}-replacement.bin");

        let chunk = vec![0x5a; 4 * 1024 * 1024];
        let mut writer = operator.writer(&source).await.unwrap();
        for _ in 0..8 {
            writer.write(chunk.clone()).await.unwrap();
        }
        writer.close().await.unwrap();
        assert_eq!(operator.stat(&source).await.unwrap().content_length(), 32 * 1024 * 1024);

        let source_bytes = operator.read(&source).await.unwrap();
        let mut copy_writer = operator.writer(&copied_partial).await.unwrap();
        for part in source_bytes.to_vec().chunks(4 * 1024 * 1024) {
            copy_writer.write(part.to_vec()).await.unwrap();
        }
        copy_writer.close().await.unwrap();
        adapter.rename(&copied_partial, &copied, false).await.unwrap();
        assert_eq!(operator.stat(&copied).await.unwrap().content_length(), 32 * 1024 * 1024);

        adapter.rename(&source, &renamed, false).await.unwrap();
        operator.write(&replacement, b"old".to_vec()).await.unwrap();
        let no_clobber = adapter.rename(&renamed, &replacement, false).await.unwrap_err();
        assert!(!no_clobber.outcome_unknown);
        adapter.rename(&renamed, &replacement, true).await.unwrap();
        assert_eq!(operator.stat(&replacement).await.unwrap().content_length(), 32 * 1024 * 1024);

        for path in [&copied, &replacement] {
            adapter.delete_owned_file_if_exists(path).await.unwrap();
        }
    }
}
