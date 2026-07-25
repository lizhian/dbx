use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use opendal::raw::oio::Write as _;
use opendal::raw::{Access, OpWrite};
use opendal::services::S3;
use opendal::{Buffer, EntryMode, ErrorKind, Metadata, Operator};
use serde::{Deserialize, Serialize};
use tokio::net::lookup_host;
use url::Url;

use super::file_manager::{
    ConnectionTestStage, FileConnectionCapabilities, FileConnectionTestResult, FileMutationOutcome, FileMutationResult,
    ResolvedFileSecrets,
};
use super::file_manager_paths::RemotePath;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct S3ConnectionConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub root: String,
    #[serde(default)]
    pub virtual_host_style: bool,
    #[serde(default)]
    pub anonymous: bool,
}

pub(super) fn validate_config(
    config: &S3ConnectionConfig,
    is_new: bool,
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    session_token: Option<&str>,
) -> Result<(), String> {
    endpoint_host_port(&config.endpoint)?;
    if config.region.trim().is_empty() {
        return Err("S3 region is required".to_string());
    }
    if config.bucket.trim().is_empty()
        || config.bucket.contains('/')
        || config.bucket.contains('\\')
        || config.bucket.chars().any(char::is_whitespace)
    {
        return Err("S3 bucket must be a non-empty bucket name".to_string());
    }
    normalize_root(&config.root)?;
    if access_key_id.is_some() != secret_access_key.is_some() {
        return Err("S3 access key ID and secret access key must be provided together".to_string());
    }
    if session_token.is_some() && access_key_id.is_none() {
        return Err("S3 session token requires an access key ID and secret access key".to_string());
    }
    if config.anonymous && (access_key_id.is_some() || secret_access_key.is_some()) {
        return Err("Anonymous S3 connections cannot include credentials".to_string());
    }
    if !config.anonymous && is_new && access_key_id.is_none() {
        return Err("S3 credentials are required unless anonymous access is explicitly enabled".to_string());
    }
    Ok(())
}

pub(super) fn validate_credentials(secrets: &ResolvedFileSecrets) -> Result<(), String> {
    if secrets.access_key_id.is_some() != secrets.secret_access_key.is_some() {
        return Err("S3 access key ID and secret access key must be provided together".to_string());
    }
    if secrets.session_token.is_some() && secrets.access_key_id.is_none() {
        return Err("S3 session token requires an access key ID and secret access key".to_string());
    }
    Ok(())
}

pub(super) fn normalize_root(root: &str) -> Result<String, String> {
    let root = root.trim();
    if root.contains('\0') || root.contains('\\') {
        return Err("S3 root contains an invalid character".to_string());
    }
    let mut normalized = Vec::new();
    for segment in root.trim_matches('/').split('/').filter(|segment| !segment.is_empty()) {
        if matches!(segment, "." | "..") {
            return Err("S3 root cannot contain '.' or '..' path segments".to_string());
        }
        normalized.push(segment);
    }
    Ok(if normalized.is_empty() { "/".to_string() } else { format!("/{}/", normalized.join("/")) })
}

pub(super) fn endpoint_host_port(endpoint: &str) -> Result<(String, u16), String> {
    let url = Url::parse(endpoint).map_err(|_| "S3 endpoint must be a valid http:// or https:// URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("S3 endpoint must use http:// or https://".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("Credentials must not be embedded in the S3 endpoint".to_string());
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err("S3 endpoint must not contain a path, query, or fragment; use the root field".to_string());
    }
    let host = url.host_str().ok_or_else(|| "S3 endpoint host is required".to_string())?;
    let port = url.port_or_known_default().ok_or_else(|| "S3 endpoint port is required".to_string())?;
    Ok((host.to_string(), port))
}

pub(super) fn build_operator(config: &S3ConnectionConfig, secrets: &ResolvedFileSecrets) -> Result<Operator, String> {
    validate_credentials(secrets)?;
    let mut builder = S3::default()
        .endpoint(&config.endpoint)
        .region(&config.region)
        .bucket(&config.bucket)
        .root(&normalize_root(&config.root)?)
        .disable_config_load()
        .disable_ec2_metadata();
    if config.anonymous {
        builder = builder.skip_signature();
    } else if let (Some(access_key_id), Some(secret_access_key)) =
        (secrets.access_key_id.as_deref(), secrets.secret_access_key.as_deref())
    {
        builder = builder.access_key_id(access_key_id).secret_access_key(secret_access_key);
        if let Some(session_token) = secrets.session_token.as_deref() {
            builder = builder.session_token(session_token);
        }
    } else {
        return Err("S3 credentials are required unless anonymous access is explicitly enabled".to_string());
    }
    if config.virtual_host_style {
        builder = builder.enable_virtual_host_style();
    }
    Operator::new(builder).map(|builder| builder.finish()).map_err(|error| redact(error.to_string(), secrets))
}

pub(super) async fn test_connection(
    config: &S3ConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> FileConnectionTestResult {
    let mut stages = Vec::with_capacity(6);
    if let Err(error) = validate_credentials(secrets) {
        stages.push(failed_stage("configuration", error));
        append_skipped(&mut stages, &["dns", "tcp", "authentication", "bucket", "root"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("configuration"));

    let (host, port) = match endpoint_host_port(&config.endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            stages[0] = failed_stage("configuration", error);
            append_skipped(&mut stages, &["dns", "tcp", "authentication", "bucket", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    let addresses = match resolve_addresses(&host, port).await {
        Ok(addresses) if !addresses.is_empty() => addresses,
        Ok(_) => {
            stages.push(failed_stage("dns", "No addresses returned".to_string()));
            append_skipped(&mut stages, &["tcp", "authentication", "bucket", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Err(error) => {
            stages.push(failed_stage("dns", error));
            append_skipped(&mut stages, &["tcp", "authentication", "bucket", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    stages.push(passed_stage("dns"));

    let mut tcp_error = "No S3 endpoint address accepted a TCP connection".to_string();
    let mut tcp_connected = false;
    for address in addresses {
        match tokio::time::timeout(CONNECTION_TIMEOUT, tokio::net::TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                drop(stream);
                tcp_connected = true;
                break;
            }
            Ok(Err(error)) => tcp_error = error.to_string(),
            Err(_) => tcp_error = "TCP connection timed out".to_string(),
        }
    }
    if !tcp_connected {
        stages.push(failed_stage("tcp", tcp_error));
        append_skipped(&mut stages, &["authentication", "bucket", "root"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("tcp"));

    let bucket_config = S3ConnectionConfig { root: "/".to_string(), ..config.clone() };
    let bucket_operator = match build_operator(&bucket_config, secrets) {
        Ok(operator) => operator,
        Err(error) => {
            stages.push(failed_stage("authentication", error));
            append_skipped(&mut stages, &["bucket", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    match probe_root(&bucket_operator).await {
        Ok(()) => {
            stages.push(passed_stage("authentication"));
            stages.push(passed_stage("bucket"));
        }
        Err(error) if error.kind() == ErrorKind::PermissionDenied => {
            stages.push(failed_stage("authentication", redact(error.to_string(), secrets)));
            append_skipped(&mut stages, &["bucket", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            stages.push(passed_stage("authentication"));
            stages.push(failed_stage("bucket", redact(error.to_string(), secrets)));
            stages.push(skipped_stage("root"));
            return FileConnectionTestResult { success: false, stages };
        }
        Err(error) => {
            stages.push(passed_stage("authentication"));
            stages.push(failed_stage("bucket", redact(error.to_string(), secrets)));
            stages.push(skipped_stage("root"));
            return FileConnectionTestResult { success: false, stages };
        }
    }

    match build_operator(config, secrets) {
        Ok(operator) => match probe_root(&operator).await {
            Ok(()) => {
                stages.push(passed_stage("root"));
                FileConnectionTestResult { success: true, stages }
            }
            Err(error) => {
                stages.push(failed_stage("root", redact(error.to_string(), secrets)));
                FileConnectionTestResult { success: false, stages }
            }
        },
        Err(error) => {
            stages.push(failed_stage("root", error));
            FileConnectionTestResult { success: false, stages }
        }
    }
}

pub(super) async fn write_object_exact(
    operator: &Operator,
    path: &str,
    body: Buffer,
    if_not_exists: bool,
    secrets: &ResolvedFileSecrets,
) -> Result<Metadata, String> {
    let (_, mut writer) = operator
        .inner()
        .write(path, OpWrite::new().with_if_not_exists(if_not_exists))
        .await
        .map_err(|error| redact(format!("Opening exact S3 object writer failed: {error}"), secrets))?;
    if !body.is_empty() {
        if let Err(error) = writer.write(body).await {
            let abort = writer.abort().await;
            return Err(redact(
                match abort {
                    Ok(()) => format!("Writing exact S3 object failed: {error}"),
                    Err(abort) => format!("Writing exact S3 object failed: {error}; abort failed: {abort}"),
                },
                secrets,
            ));
        }
    }
    match writer.close().await {
        Ok(metadata) => Ok(metadata),
        Err(error) => {
            let abort = writer.abort().await;
            Err(redact(
                match abort {
                    Ok(()) => format!("Closing exact S3 object writer failed: {error}"),
                    Err(abort) => format!("Closing exact S3 object writer failed: {error}; abort failed: {abort}"),
                },
                secrets,
            ))
        }
    }
}

pub(super) async fn delete_entry(
    config: &S3ConnectionConfig,
    path: &RemotePath,
    expected_kind: Option<&str>,
    secrets: &ResolvedFileSecrets,
) -> Result<FileMutationResult, String> {
    let operator = build_operator(config, secrets)?;
    let object_path = path.as_str();
    if expected_kind != Some("directory") {
        match operator.stat(object_path).await {
            Ok(metadata) if metadata.mode().is_file() => {
                delete_current(&operator, object_path, Some(&metadata), secrets).await?;
                return Ok(FileMutationResult { outcome: FileMutationOutcome::Completed });
            }
            Ok(_) => return Err("S3 returned an unsupported object type".to_string()),
            Err(error) if error.kind() == ErrorKind::NotFound && expected_kind == Some("file") => {
                return Err("S3 object no longer exists".to_string());
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(redact(error.to_string(), secrets)),
        }
    }

    let marker = format!("{}/", object_path.trim_end_matches('/'));
    match operator.stat(object_path).await {
        Ok(metadata) if metadata.mode().is_file() => {
            return Err("S3 directory emptiness cannot be proven while a same-name object exists; no data was deleted"
                .to_string());
        }
        Ok(_) => return Err("S3 returned an unsupported same-name object type".to_string()),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(redact(error.to_string(), secrets)),
    }
    let parent = marker
        .trim_end_matches('/')
        .rsplit_once('/')
        .map(|(parent, _)| format!("{parent}/"))
        .unwrap_or_else(|| "/".to_string());
    let mut children = operator
        .lister_with(&parent)
        .recursive(true)
        .start_after(&marker)
        .await
        .map_err(|error| redact(error.to_string(), secrets))?;
    while let Some(entry) = children.next().await {
        let entry = entry.map_err(|error| redact(error.to_string(), secrets))?;
        if entry.path() != marker && entry.path().starts_with(&marker) {
            return Err("S3 directory marker is not empty; recursive delete is unsupported".to_string());
        }
        if !entry.path().starts_with(&marker) {
            break;
        }
    }
    let mut entries =
        operator.lister_with(&marker).recursive(true).await.map_err(|error| redact(error.to_string(), secrets))?;
    let mut marker_metadata = None;
    while let Some(entry) = entries.next().await {
        let entry = entry.map_err(|error| redact(error.to_string(), secrets))?;
        if entry.path() == marker {
            marker_metadata = Some(entry.metadata().clone());
        } else {
            return Err("S3 directory marker is not empty; recursive delete is unsupported".to_string());
        }
    }
    let marker_metadata = match marker_metadata {
        Some(metadata) if metadata.content_length() == 0 => metadata,
        Some(_) => return Err("S3 directory marker contains data and cannot be deleted safely".to_string()),
        None => return Ok(FileMutationResult { outcome: FileMutationOutcome::NoOp }),
    };
    delete_current_marker(&operator, &marker, &marker_metadata, secrets).await?;
    Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
}

async fn delete_current_marker(
    operator: &Operator,
    marker: &str,
    expected: &Metadata,
    secrets: &ResolvedFileSecrets,
) -> Result<(), String> {
    let entries =
        operator.list_with(marker).recursive(true).await.map_err(|error| redact(error.to_string(), secrets))?;
    let current = entries
        .iter()
        .find(|entry| entry.path() == marker)
        .map(|entry| entry.metadata())
        .ok_or_else(|| "S3 directory marker changed before deletion; no delete marker was written".to_string())?;
    if current.content_length() != expected.content_length()
        || current.etag() != expected.etag()
        || expected.version().is_some_and(|version| current.version() != Some(version))
    {
        return Err("S3 directory marker changed before deletion; no delete marker was written".to_string());
    }
    operator.delete(marker).await.map_err(|error| redact(error.to_string(), secrets))?;
    let remaining =
        operator.list_with(marker).recursive(true).await.map_err(|error| redact(error.to_string(), secrets))?;
    if remaining.iter().any(|entry| entry.path() == marker) {
        return Err("S3 marker delete did not hide the current path; an older version may still be visible".to_string());
    }
    Ok(())
}

pub(super) async fn delete_current(
    operator: &Operator,
    path: &str,
    expected: Option<&Metadata>,
    secrets: &ResolvedFileSecrets,
) -> Result<(), String> {
    if let Some(expected) = expected {
        let current = operator.stat(path).await.map_err(|error| redact(error.to_string(), secrets))?;
        if current.content_length() != expected.content_length()
            || current.etag() != expected.etag()
            || expected.version().is_some_and(|version| current.version() != Some(version))
        {
            return Err("S3 object changed before deletion; no delete marker was written".to_string());
        }
    }
    operator.delete(path).await.map_err(|error| redact(error.to_string(), secrets))?;
    match operator.stat(path).await {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Ok(_) => Err("S3 delete did not hide the current path; an older version may still be visible".to_string()),
        Err(error) => Err(redact(format!("S3 delete verification failed: {error}"), secrets)),
    }
}

pub(super) async fn file_size_if_exists(
    operator: &Operator,
    path: &str,
    secrets: &ResolvedFileSecrets,
) -> Result<Option<usize>, String> {
    match operator.stat(path).await {
        Ok(metadata) if metadata.mode().is_file() => usize::try_from(metadata.content_length())
            .map(Some)
            .map_err(|_| "S3 object size is not representable by this runtime".to_string()),
        Ok(_) => Err("S3 upload reconciliation found a directory where a file was expected".to_string()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(redact(error.to_string(), secrets)),
    }
}

pub(super) async fn stat_directory_or_virtual(operator: &Operator, path: &str) -> Result<Metadata, opendal::Error> {
    match operator.stat(path).await {
        Ok(metadata) => Ok(metadata),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let mut lister = operator.lister_with(path).recursive(true).limit(1).await?;
            match lister.next().await.transpose()? {
                Some(entry) if entry.path().starts_with(path) && entry.path() != path => {
                    Ok(Metadata::new(EntryMode::DIR))
                }
                _ => Err(error),
            }
        }
        Err(error) => Err(error),
    }
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
        server_side_copy: true,
        atomic_rename: false,
        atomic_no_clobber: false,
    }
}

async fn probe_root(operator: &Operator) -> Result<(), opendal::Error> {
    tokio::time::timeout(CONNECTION_TIMEOUT, async {
        let mut lister = operator.lister_with("/").limit(1).await?;
        let _ = lister.next().await.transpose()?;
        Ok(())
    })
    .await
    .map_err(|_| opendal::Error::new(ErrorKind::Unexpected, "S3 list probe timed out"))?
}

async fn resolve_addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    tokio::time::timeout(CONNECTION_TIMEOUT, lookup_host((host, port)))
        .await
        .map_err(|_| "DNS lookup timed out".to_string())?
        .map(|addresses| addresses.collect())
        .map_err(|error| error.to_string())
}

fn redact(mut message: String, secrets: &ResolvedFileSecrets) -> String {
    for secret in
        [secrets.access_key_id.as_deref(), secrets.secret_access_key.as_deref(), secrets.session_token.as_deref()]
            .into_iter()
            .flatten()
            .filter(|value| !value.is_empty())
    {
        message = message.replace(secret, "[REDACTED]");
        let percent_encoded =
            percent_encoding::utf8_percent_encode(secret, percent_encoding::NON_ALPHANUMERIC).to_string();
        message = message.replace(&percent_encoded, "[REDACTED]");
        let form_encoded = url::form_urlencoded::byte_serialize(secret.as_bytes()).collect::<String>();
        message = message.replace(&form_encoded, "[REDACTED]");
    }
    message
}

fn passed_stage(stage: &'static str) -> ConnectionTestStage {
    ConnectionTestStage { stage, status: "passed", message: None }
}

fn failed_stage(stage: &'static str, message: String) -> ConnectionTestStage {
    ConnectionTestStage { stage, status: "failed", message: Some(message) }
}

fn skipped_stage(stage: &'static str) -> ConnectionTestStage {
    ConnectionTestStage { stage, status: "skipped", message: None }
}

fn append_skipped(stages: &mut Vec<ConnectionTestStage>, remaining: &[&'static str]) {
    stages.extend(remaining.iter().map(|stage| skipped_stage(stage)));
}
