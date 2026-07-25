use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use opendal::services::Webdav;
use opendal::{ErrorKind, Operator};
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use tokio::net::lookup_host;
use tokio_util::io::ReaderStream;
use url::Url;

use super::file_manager::{
    ConnectionTestStage, FileConnectionCapabilities, FileConnectionTestResult, FileMutationOutcome, FileMutationResult,
    ResolvedFileSecrets,
};
use super::file_manager_paths::RemotePath;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SAFE_DELETE_LOCK_SECONDS: u64 = 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WebdavMutationErrorKind {
    FailedBeforeMutation,
    DefinitiveHttpRejected,
    DispatchOutcomeUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WebdavMutationStage {
    Configuration,
    Connect,
    Dispatch,
}

#[derive(Debug)]
pub(super) struct WebdavMutationError {
    pub kind: WebdavMutationErrorKind,
    pub stage: WebdavMutationStage,
    pub http_status: Option<u16>,
    pub message: String,
}

impl WebdavMutationError {
    pub(super) fn is_outcome_unknown(&self) -> bool {
        match self.kind {
            WebdavMutationErrorKind::FailedBeforeMutation => {
                debug_assert!(matches!(self.stage, WebdavMutationStage::Configuration | WebdavMutationStage::Connect));
                debug_assert_eq!(self.http_status, None);
                false
            }
            WebdavMutationErrorKind::DefinitiveHttpRejected => {
                debug_assert_eq!(self.stage, WebdavMutationStage::Dispatch);
                debug_assert!(self.http_status.is_some());
                false
            }
            WebdavMutationErrorKind::DispatchOutcomeUnknown => {
                debug_assert_eq!(self.stage, WebdavMutationStage::Dispatch);
                debug_assert_eq!(self.http_status, None);
                true
            }
        }
    }

    pub(super) fn definitive(message: String) -> Self {
        Self {
            kind: WebdavMutationErrorKind::FailedBeforeMutation,
            stage: WebdavMutationStage::Configuration,
            http_status: None,
            message,
        }
    }

    fn rejected(status: u16, message: String) -> Self {
        Self {
            kind: WebdavMutationErrorKind::DefinitiveHttpRejected,
            stage: WebdavMutationStage::Dispatch,
            http_status: Some(status),
            message,
        }
    }

    fn connect_failure(message: String) -> Self {
        Self {
            kind: WebdavMutationErrorKind::FailedBeforeMutation,
            stage: WebdavMutationStage::Connect,
            http_status: None,
            message,
        }
    }

    pub(super) fn unknown(message: String) -> Self {
        Self {
            kind: WebdavMutationErrorKind::DispatchOutcomeUnknown,
            stage: WebdavMutationStage::Dispatch,
            http_status: None,
            message,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WebdavAuthentication {
    None,
    Basic,
    Bearer,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebdavConnectionConfig {
    pub endpoint: String,
    pub root: String,
    pub authentication: WebdavAuthentication,
    #[serde(default)]
    pub username: String,
}

pub(super) fn validate_config(
    config: &WebdavConnectionConfig,
    is_new: bool,
    password: Option<&str>,
    token: Option<&str>,
) -> Result<(), String> {
    endpoint_host_port(&config.endpoint)?;
    normalize_root(&config.root)?;
    if config.username.contains(['\r', '\n', '\0']) {
        return Err("WebDAV username contains an invalid character".to_string());
    }
    match config.authentication {
        WebdavAuthentication::None => {
            if !config.username.trim().is_empty() || password.is_some() || token.is_some() {
                return Err("Anonymous WebDAV connections cannot include authentication fields".to_string());
            }
        }
        WebdavAuthentication::Basic => {
            if config.username.trim().is_empty() {
                return Err("WebDAV Basic authentication requires a username".to_string());
            }
            if token.is_some() {
                return Err("WebDAV Basic authentication cannot include a bearer token".to_string());
            }
            if is_new && password.is_none_or(str::is_empty) {
                return Err("WebDAV Basic authentication requires a password".to_string());
            }
        }
        WebdavAuthentication::Bearer => {
            if !config.username.trim().is_empty() || password.is_some() {
                return Err("WebDAV bearer authentication cannot include Basic credentials".to_string());
            }
            if is_new && token.is_none_or(str::is_empty) {
                return Err("WebDAV bearer authentication requires a token".to_string());
            }
        }
    }
    Ok(())
}

pub(super) fn validate_resolved_credentials(
    config: &WebdavConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<(), String> {
    validate_config(config, false, secrets.password.as_deref(), secrets.webdav_token.as_deref())?;
    match config.authentication {
        WebdavAuthentication::None => Ok(()),
        WebdavAuthentication::Basic if secrets.password.as_deref().is_some_and(|value| !value.is_empty()) => Ok(()),
        WebdavAuthentication::Basic => Err("Saved WebDAV password is unavailable".to_string()),
        WebdavAuthentication::Bearer if secrets.webdav_token.as_deref().is_some_and(|value| !value.is_empty()) => {
            Ok(())
        }
        WebdavAuthentication::Bearer => Err("Saved WebDAV bearer token is unavailable".to_string()),
    }
}

pub(super) fn normalize_root(root: &str) -> Result<String, String> {
    let decoded = percent_encoding::percent_decode_str(root.trim())
        .decode_utf8()
        .map_err(|_| "WebDAV root contains invalid percent-encoded UTF-8".to_string())?;
    if !decoded.starts_with('/') {
        return Err("WebDAV root must be an absolute path beginning with '/'".to_string());
    }
    if decoded.contains('\0') || decoded.contains('\\') {
        return Err("WebDAV root contains an invalid character".to_string());
    }
    let mut segments = Vec::new();
    for segment in decoded.split('/').filter(|segment| !segment.is_empty()) {
        if matches!(segment, "." | "..") {
            return Err("WebDAV root cannot contain '.' or '..' path segments".to_string());
        }
        segments.push(segment);
    }
    Ok(if segments.is_empty() { "/".to_string() } else { format!("/{}/", segments.join("/")) })
}

pub(super) fn normalize_endpoint(endpoint: &str) -> Result<String, String> {
    let endpoint = endpoint.trim();
    endpoint_host_port(endpoint)?;
    Ok(endpoint.trim_end_matches('/').to_string())
}

pub(super) fn endpoint_host_port(endpoint: &str) -> Result<(String, u16), String> {
    let url =
        Url::parse(endpoint).map_err(|_| "WebDAV endpoint must be a valid http:// or https:// URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("WebDAV endpoint must use http:// or https://".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("Credentials must not be embedded in the WebDAV endpoint".to_string());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("WebDAV endpoint must not contain a query or fragment".to_string());
    }
    let host = url.host_str().ok_or_else(|| "WebDAV endpoint host is required".to_string())?;
    let port = url.port_or_known_default().ok_or_else(|| "WebDAV endpoint port is required".to_string())?;
    Ok((host.to_string(), port))
}

pub(super) fn build_operator(
    config: &WebdavConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<Operator, String> {
    validate_resolved_credentials(config, secrets)?;
    let mut builder = Webdav::default().endpoint(&config.endpoint).root(&normalize_root(&config.root)?);
    match config.authentication {
        WebdavAuthentication::None => {}
        WebdavAuthentication::Basic => {
            builder = builder.username(config.username.trim()).password(
                secrets.password.as_deref().ok_or_else(|| "Saved WebDAV password is unavailable".to_string())?,
            );
        }
        WebdavAuthentication::Bearer => {
            builder = builder.token(
                secrets
                    .webdav_token
                    .as_deref()
                    .ok_or_else(|| "Saved WebDAV bearer token is unavailable".to_string())?,
            );
        }
    }
    Operator::new(builder).map(|builder| builder.finish()).map_err(|error| redact(error.to_string(), secrets))
}

pub(super) async fn test_connection(
    config: &WebdavConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> FileConnectionTestResult {
    let mut stages = Vec::with_capacity(5);
    if let Err(error) = validate_resolved_credentials(config, secrets) {
        stages.push(failed_stage("configuration", error));
        append_skipped(&mut stages, &["dns", "tcp", "authentication", "root"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("configuration"));
    let (host, port) = match endpoint_host_port(&config.endpoint) {
        Ok(value) => value,
        Err(error) => {
            stages[0] = failed_stage("configuration", error);
            append_skipped(&mut stages, &["dns", "tcp", "authentication", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    let addresses = match resolve_addresses(&host, port).await {
        Ok(addresses) if !addresses.is_empty() => addresses,
        Ok(_) => {
            stages.push(failed_stage("dns", "No addresses returned".to_string()));
            append_skipped(&mut stages, &["tcp", "authentication", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Err(error) => {
            stages.push(failed_stage("dns", error));
            append_skipped(&mut stages, &["tcp", "authentication", "root"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    stages.push(passed_stage("dns"));
    if let Err(error) = connect_any(&addresses).await {
        stages.push(failed_stage("tcp", error));
        append_skipped(&mut stages, &["authentication", "root"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("tcp"));

    let authentication_config = WebdavConnectionConfig { root: "/".to_string(), ..config.clone() };
    let authentication_operator = match build_operator(&authentication_config, secrets) {
        Ok(operator) => operator,
        Err(error) => {
            stages.push(failed_stage("authentication", error));
            stages.push(skipped_stage("root"));
            return FileConnectionTestResult { success: false, stages };
        }
    };
    match probe(&authentication_operator).await {
        Ok(()) => stages.push(passed_stage("authentication")),
        Err(error) => {
            stages.push(failed_stage("authentication", redact(error.to_string(), secrets)));
            stages.push(skipped_stage("root"));
            return FileConnectionTestResult { success: false, stages };
        }
    }
    match build_operator(config, secrets) {
        Ok(operator) => match probe(&operator).await {
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

pub(super) async fn delete_entry(
    config: &WebdavConnectionConfig,
    operator: &Operator,
    path: &RemotePath,
    expected_kind: Option<&str>,
    secrets: &ResolvedFileSecrets,
) -> Result<FileMutationResult, String> {
    let file_path = path.as_str();
    let directory_path = format!("{}/", file_path.trim_end_matches('/'));
    if expected_kind == Some("directory") {
        match operator.stat(&directory_path).await {
            Ok(metadata) if metadata.mode().is_dir() => {
                delete_empty_collection_locked(config, &directory_path, secrets).await?;
                return Ok(FileMutationResult { outcome: FileMutationOutcome::Completed });
            }
            Ok(_) => return Err("WebDAV resource kind changed; no data was deleted".to_string()),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err("WebDAV resource no longer exists".to_string());
            }
            Err(error) => return Err(redact(error.to_string(), secrets)),
        }
    }
    let (actual_path, is_directory) = match operator.stat(file_path).await {
        Ok(metadata) if metadata.mode().is_file() => (file_path.to_string(), false),
        Ok(metadata) if metadata.mode().is_dir() => (directory_path.clone(), true),
        Ok(_) => return Err("WebDAV returned an unsupported resource type".to_string()),
        Err(error) if error.kind() == ErrorKind::NotFound => match operator.stat(&directory_path).await {
            Ok(metadata) if metadata.mode().is_dir() => (directory_path.clone(), true),
            Ok(_) => return Err("WebDAV returned an unsupported resource type".to_string()),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err("WebDAV resource no longer exists".to_string());
            }
            Err(error) => return Err(redact(error.to_string(), secrets)),
        },
        Err(error) => return Err(redact(error.to_string(), secrets)),
    };
    if expected_kind == Some("file") && is_directory || expected_kind == Some("directory") && !is_directory {
        return Err("WebDAV resource kind changed; no data was deleted".to_string());
    }
    if is_directory {
        delete_empty_collection_locked(config, &actual_path, secrets).await?;
        return Ok(FileMutationResult { outcome: FileMutationOutcome::Completed });
    }
    operator.delete(&actual_path).await.map_err(|error| redact(error.to_string(), secrets))?;
    Ok(FileMutationResult { outcome: FileMutationOutcome::Completed })
}

pub(super) async fn put_file(
    config: &WebdavConnectionConfig,
    path: &str,
    file: tokio::fs::File,
    size: u64,
    progress: std::sync::Arc<dyn Fn(u64) + Send + Sync>,
    secrets: &ResolvedFileSecrets,
    dispatch_started: Arc<AtomicBool>,
) -> Result<(), WebdavMutationError> {
    let client = webdav_client().map_err(WebdavMutationError::definitive)?;
    let url = resource_url(config, path).map_err(WebdavMutationError::definitive)?;
    let mut transferred = 0_u64;
    let body = reqwest::Body::wrap_stream(ReaderStream::new(file).map_ok(move |chunk| {
        transferred = transferred.saturating_add(chunk.len() as u64);
        progress(transferred);
        chunk
    }));
    let request = authorize(
        client.put(url).header(CONTENT_LENGTH, size).header(CONTENT_TYPE, "application/octet-stream").body(body),
        config,
        secrets,
    );
    dispatch_started.store(true, Ordering::Release);
    let response = request.send().await.map_err(|error| classify_send_error("PUT", error, secrets))?;
    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status().as_u16();
        Err(WebdavMutationError::rejected(
            status,
            redact(format!("WebDAV PUT was rejected with HTTP {status}"), secrets),
        ))
    }
}

pub(super) async fn copy_file(
    config: &WebdavConnectionConfig,
    source: &str,
    destination: &str,
    secrets: &ResolvedFileSecrets,
    dispatch_started: Arc<AtomicBool>,
) -> Result<(), WebdavMutationError> {
    dispatch_file_mutation(config, source, destination, "COPY", secrets, dispatch_started).await
}

pub(super) async fn move_file(
    config: &WebdavConnectionConfig,
    source: &str,
    destination: &str,
    secrets: &ResolvedFileSecrets,
    dispatch_started: Arc<AtomicBool>,
) -> Result<(), WebdavMutationError> {
    dispatch_file_mutation(config, source, destination, "MOVE", secrets, dispatch_started).await
}

async fn dispatch_file_mutation(
    config: &WebdavConnectionConfig,
    source: &str,
    destination: &str,
    method: &'static str,
    secrets: &ResolvedFileSecrets,
    dispatch_started: Arc<AtomicBool>,
) -> Result<(), WebdavMutationError> {
    let client = webdav_client().map_err(WebdavMutationError::definitive)?;
    let source_url = resource_url(config, source).map_err(WebdavMutationError::definitive)?;
    let destination_url = resource_url(config, destination).map_err(WebdavMutationError::definitive)?;
    let method_value = reqwest::Method::from_bytes(method.as_bytes()).expect("COPY and MOVE are valid WebDAV methods");
    let request = authorize(
        client
            .request(method_value, source_url)
            .header("Destination", destination_url.as_str())
            .header("Overwrite", "T"),
        config,
        secrets,
    );
    dispatch_started.store(true, Ordering::Release);
    let response = request.send().await.map_err(|error| classify_send_error(method, error, secrets))?;
    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status().as_u16();
        Err(WebdavMutationError::rejected(
            status,
            redact(format!("WebDAV {method} was rejected with HTTP {status}"), secrets),
        ))
    }
}

fn classify_send_error(
    method: &'static str,
    error: reqwest::Error,
    secrets: &ResolvedFileSecrets,
) -> WebdavMutationError {
    let message = redact(format!("WebDAV {method} response was not observed: {error}"), secrets);
    if error.is_connect() {
        WebdavMutationError::connect_failure(format!(
            "WebDAV {method} could not connect before the request was sent: {message}"
        ))
    } else {
        WebdavMutationError::unknown(message)
    }
}

async fn delete_empty_collection_locked(
    config: &WebdavConnectionConfig,
    path: &str,
    secrets: &ResolvedFileSecrets,
) -> Result<(), String> {
    let client = webdav_client()?;
    let url = resource_url(config, path)?;
    let lock_body = r#"<?xml version="1.0" encoding="utf-8" ?>
<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype><D:owner><D:href>dbx-file-manager</D:href></D:owner></D:lockinfo>"#;
    let lock = authorize(
        client
            .request(reqwest::Method::from_bytes(b"LOCK").expect("LOCK is a valid method"), url.clone())
            .header("Depth", "infinity")
            .header("Timeout", "Second-30")
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .body(lock_body),
        config,
        secrets,
    )
    .send()
    .await
    .map_err(|error| redact(error.to_string(), secrets))?;
    if !lock.status().is_success() {
        return Err(format!(
            "Safe WebDAV directory deletion requires an exclusive depth-infinity lock; LOCK failed with HTTP {}",
            lock.status().as_u16()
        ));
    }
    let token = lock
        .headers()
        .get("Lock-Token")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .ok_or_else(|| "WebDAV LOCK succeeded without a Lock-Token; directory was not deleted".to_string())?;
    let lock_document = match lock.bytes().await {
        Ok(body) => body,
        Err(error) => {
            unlock_collection(&client, url.clone(), &token, config, secrets).await;
            return Err(redact(
                format!("WebDAV LOCK response body could not be read; directory was not deleted: {error}"),
                secrets,
            ));
        }
    };
    if let Err(error) = granted_lock_timeout_seconds(&lock_document) {
        unlock_collection(&client, url.clone(), &token, config, secrets).await;
        return Err(error);
    }

    enum LockedDeleteResult {
        Completed,
        DefinitiveFailure(String),
        OutcomeUnknown(String),
    }

    let operation = async {
        let propfind = authorize(
            client
                .request(reqwest::Method::from_bytes(b"PROPFIND").expect("PROPFIND is a valid method"), url.clone())
                .header("Depth", "1")
                .header(CONTENT_TYPE, "application/xml")
                .body(r#"<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/></D:prop></D:propfind>"#),
            config,
            secrets,
        )
        .send()
        .await
        .map_err(|error| LockedDeleteResult::DefinitiveFailure(redact(error.to_string(), secrets)))?;
        if propfind.status().as_u16() != 207 {
            return Err(LockedDeleteResult::DefinitiveFailure(format!(
                "Locked WebDAV emptiness check failed with HTTP {}",
                propfind.status().as_u16()
            )));
        }
        let body = propfind
            .bytes()
            .await
            .map_err(|error| LockedDeleteResult::DefinitiveFailure(redact(error.to_string(), secrets)))?;
        let count = multistatus_response_count(&body).map_err(LockedDeleteResult::DefinitiveFailure)?;
        if count != 1 {
            return Err(LockedDeleteResult::DefinitiveFailure(
                "WebDAV directory is not empty; recursive delete is unsupported".to_string(),
            ));
        }
        let delete = authorize(
            client.delete(url.clone()).header("If", format!("<{url}> ({token})")),
            config,
            secrets,
        )
        .send()
        .await;
        match delete {
            Ok(response) if response.status().is_success() || response.status().as_u16() == 404 => {
                Ok(LockedDeleteResult::Completed)
            }
            Ok(response) => {
                match collection_absent(&client, &url, config, secrets).await {
                    Ok(true) => Ok(LockedDeleteResult::Completed),
                    Ok(false) | Err(_) => Err(LockedDeleteResult::DefinitiveFailure(format!(
                        "Locked WebDAV directory DELETE was rejected with HTTP {}",
                        response.status().as_u16()
                    ))),
                }
            }
            Err(error) => {
                match collection_absent(&client, &url, config, secrets).await {
                    Ok(true) => Ok(LockedDeleteResult::Completed),
                    Ok(false) => Err(LockedDeleteResult::OutcomeUnknown(redact(
                        format!("Locked WebDAV directory DELETE response was lost: {error}"),
                        secrets,
                    ))),
                    Err(reconcile_error) => Err(LockedDeleteResult::OutcomeUnknown(redact(
                        format!(
                            "Locked WebDAV directory DELETE response was lost: {error}; reconciliation failed: {reconcile_error}"
                        ),
                        secrets,
                    ))),
                }
            }
        }
    }
    .await;

    let result = match operation {
        Ok(result) => result,
        Err(result) => result,
    };
    if !matches!(result, LockedDeleteResult::OutcomeUnknown(_)) {
        unlock_collection(&client, url, &token, config, secrets).await;
    }
    match result {
        LockedDeleteResult::Completed => Ok(()),
        LockedDeleteResult::DefinitiveFailure(error) | LockedDeleteResult::OutcomeUnknown(error) => Err(error),
    }
}

async fn unlock_collection(
    client: &reqwest::Client,
    url: Url,
    token: &str,
    config: &WebdavConnectionConfig,
    secrets: &ResolvedFileSecrets,
) {
    let _ = authorize(
        client
            .request(reqwest::Method::from_bytes(b"UNLOCK").expect("UNLOCK is a valid method"), url)
            .header("Lock-Token", token),
        config,
        secrets,
    )
    .send()
    .await;
}

fn granted_lock_timeout_seconds(body: &[u8]) -> Result<u64, String> {
    let mut reader = Reader::from_reader(body);
    let mut in_timeout = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) if start.local_name().as_ref() == b"timeout" => {
                in_timeout = true;
            }
            Ok(Event::Text(text)) if in_timeout => {
                let value = text
                    .decode()
                    .map_err(|_| "WebDAV LOCK returned an invalid timeout; directory was not deleted".to_string())?;
                let value = value.trim();
                let Some(seconds) = value
                    .get(0.."Second-".len())
                    .filter(|prefix| prefix.eq_ignore_ascii_case("Second-"))
                    .and_then(|_| value.get("Second-".len()..))
                    .and_then(|seconds| seconds.parse::<u64>().ok())
                else {
                    return Err(
                        "Safe WebDAV directory deletion requires an explicitly finite LOCK timeout; directory was not deleted"
                            .to_string(),
                    );
                };
                if seconds == 0 || seconds > MAX_SAFE_DELETE_LOCK_SECONDS {
                    return Err(format!(
                        "Safe WebDAV directory deletion requires a LOCK timeout between 1 and {MAX_SAFE_DELETE_LOCK_SECONDS} seconds; server granted {seconds} seconds"
                    ));
                }
                return Ok(seconds);
            }
            Ok(Event::End(end)) if end.local_name().as_ref() == b"timeout" => {
                return Err(
                    "Safe WebDAV directory deletion requires an explicitly finite LOCK timeout; directory was not deleted"
                        .to_string(),
                );
            }
            Ok(Event::Eof) => {
                return Err(
                    "Safe WebDAV directory deletion requires an explicitly finite LOCK timeout; directory was not deleted"
                        .to_string(),
                );
            }
            Ok(_) => {}
            Err(_) => {
                return Err("WebDAV returned an invalid LOCK document; directory was not deleted".to_string());
            }
        }
    }
}

async fn collection_absent(
    client: &reqwest::Client,
    url: &Url,
    config: &WebdavConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<bool, String> {
    let response = authorize(
        client
            .request(reqwest::Method::from_bytes(b"PROPFIND").expect("PROPFIND is a valid method"), url.clone())
            .header("Depth", "0")
            .header(CONTENT_TYPE, "application/xml")
            .body(r#"<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/></D:prop></D:propfind>"#),
        config,
        secrets,
    )
    .send()
    .await
    .map_err(|error| redact(error.to_string(), secrets))?;
    Ok(response.status().as_u16() == 404)
}

fn multistatus_response_count(body: &[u8]) -> Result<usize, String> {
    let mut reader = Reader::from_reader(body);
    let mut count = 0;
    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) if start.local_name().as_ref() == b"response" => count += 1,
            Ok(Event::Eof) => return Ok(count),
            Ok(_) => {}
            Err(_) => {
                return Err("WebDAV returned an invalid multistatus document; directory was not deleted".to_string())
            }
        }
    }
}

fn webdav_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .use_rustls_tls()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| error.to_string())
}

fn authorize(
    request: reqwest::RequestBuilder,
    config: &WebdavConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> reqwest::RequestBuilder {
    match config.authentication {
        WebdavAuthentication::None => request,
        WebdavAuthentication::Basic => {
            request.basic_auth(config.username.trim(), secrets.password.as_deref().map(ToString::to_string))
        }
        WebdavAuthentication::Bearer => {
            request.header(AUTHORIZATION, format!("Bearer {}", secrets.webdav_token.as_deref().unwrap_or_default()))
        }
    }
}

fn resource_url(config: &WebdavConnectionConfig, path: &str) -> Result<Url, String> {
    let mut url = Url::parse(&config.endpoint).map_err(|_| "Stored WebDAV endpoint is invalid".to_string())?;
    let mut segments =
        url.path_segments_mut().map_err(|_| "WebDAV endpoint cannot be used as a hierarchical URL".to_string())?;
    segments.pop_if_empty();
    for segment in config.root.trim_matches('/').split('/').filter(|segment| !segment.is_empty()) {
        segments.push(segment);
    }
    for segment in path.trim_matches('/').split('/').filter(|segment| !segment.is_empty()) {
        segments.push(segment);
    }
    if path.ends_with('/') {
        segments.push("");
    }
    drop(segments);
    Ok(url)
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

async fn probe(operator: &Operator) -> Result<(), opendal::Error> {
    let mut lister = operator.lister("/").await?;
    if let Some(entry) = lister.next().await {
        entry?;
    }
    Ok(())
}

async fn resolve_addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    tokio::time::timeout(CONNECTION_TIMEOUT, lookup_host((host, port)))
        .await
        .map_err(|_| "DNS lookup timed out".to_string())?
        .map(|addresses| addresses.collect())
        .map_err(|error| error.to_string())
}

async fn connect_any(addresses: &[SocketAddr]) -> Result<(), String> {
    let mut last_error = "No WebDAV endpoint address accepted a TCP connection".to_string();
    for address in addresses {
        match tokio::time::timeout(CONNECTION_TIMEOUT, tokio::net::TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                drop(stream);
                return Ok(());
            }
            Ok(Err(error)) => last_error = error.to_string(),
            Err(_) => last_error = "TCP connection timed out".to_string(),
        }
    }
    Err(last_error)
}

fn redact(message: String, secrets: &ResolvedFileSecrets) -> String {
    secrets.redactor().redact(message).as_str().to_string()
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

fn append_skipped(stages: &mut Vec<ConnectionTestStage>, names: &[&'static str]) {
    stages.extend(names.iter().copied().map(skipped_stage));
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};
    use std::sync::Arc;

    use super::*;

    fn basic_config(endpoint: String) -> WebdavConnectionConfig {
        WebdavConnectionConfig {
            endpoint,
            root: "/tenant/root/".to_string(),
            authentication: WebdavAuthentication::Basic,
            username: "dbx".to_string(),
        }
    }

    fn basic_secrets(password: String) -> ResolvedFileSecrets {
        ResolvedFileSecrets {
            password: Some(dbx_core::file_secrets::FileSecret::new(password).unwrap()),
            ..ResolvedFileSecrets::default()
        }
    }

    #[test]
    fn webdav_configuration_rejects_mixed_and_empty_authentication() {
        let endpoint = "https://dav.example.test/base".to_string();
        let mut config = basic_config(endpoint);
        assert!(validate_config(&config, true, Some(""), None).unwrap_err().contains("password"));
        assert!(validate_config(&config, true, Some("password"), Some("token")).unwrap_err().contains("token"));
        config.authentication = WebdavAuthentication::Bearer;
        config.username.clear();
        assert!(validate_config(&config, true, None, Some("")).unwrap_err().contains("token"));
        assert!(validate_config(&config, true, Some("password"), Some("token")).unwrap_err().contains("Basic"));
        config.authentication = WebdavAuthentication::None;
        assert!(validate_config(&config, true, None, None).is_ok());
    }

    #[test]
    fn webdav_endpoint_and_root_normalization_preserve_service_base() {
        assert_eq!(
            normalize_endpoint(" https://dav.example.test/service/ ").unwrap(),
            "https://dav.example.test/service"
        );
        assert_eq!(normalize_root("/tenant//root/").unwrap(), "/tenant/root/");
        assert!(normalize_root("/tenant/%2e%2e/root").is_err());
        assert!(endpoint_host_port("https://user:secret@dav.example.test/").is_err());
        assert!(endpoint_host_port("https://dav.example.test/?token=secret").is_err());
    }

    #[test]
    fn webdav_delete_lock_requires_an_explicit_short_finite_lease() {
        assert_eq!(
            granted_lock_timeout_seconds(
                br#"<D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:timeout>Second-30</D:timeout></D:activelock></D:lockdiscovery></D:prop>"#
            )
            .unwrap(),
            30
        );
        for body in [
            br#"<D:prop xmlns:D="DAV:"><D:timeout>Infinite</D:timeout></D:prop>"#.as_slice(),
            br#"<D:prop xmlns:D="DAV:"><D:timeout>Second-31</D:timeout></D:prop>"#.as_slice(),
            br#"<D:prop xmlns:D="DAV:"><D:lockdiscovery/></D:prop>"#.as_slice(),
        ] {
            assert!(granted_lock_timeout_seconds(body).is_err());
        }
    }

    #[tokio::test]
    async fn webdav_connect_failures_are_classified_before_mutation() {
        let port = portpicker::pick_unused_port().expect("an unused local port");
        let config = basic_config(format!("http://127.0.0.1:{port}"));
        let secrets = basic_secrets("password".to_string());
        let put_started = Arc::new(AtomicBool::new(false));
        let put = put_file(
            &config,
            "connect-failure-put.bin",
            tokio::fs::File::from_std(tempfile_with_content(b"put")),
            3,
            Arc::new(|_| {}),
            &secrets,
            put_started.clone(),
        )
        .await
        .unwrap_err();
        assert!(put_started.load(Ordering::Acquire));
        assert_eq!(put.kind, WebdavMutationErrorKind::FailedBeforeMutation);
        assert_eq!(put.stage, WebdavMutationStage::Connect);
        assert_eq!(put.http_status, None);

        for error in [
            copy_file(&config, "source.bin", "copy.bin", &secrets, Arc::new(AtomicBool::new(false))).await.unwrap_err(),
            move_file(&config, "source.bin", "move.bin", &secrets, Arc::new(AtomicBool::new(false))).await.unwrap_err(),
        ] {
            assert_eq!(error.kind, WebdavMutationErrorKind::FailedBeforeMutation);
            assert_eq!(error.stage, WebdavMutationStage::Connect);
            assert!(!error.is_outcome_unknown());
        }
    }

    #[tokio::test]
    #[ignore = "requires tests/webdav-contract.sh"]
    async fn fixed_webdav_service_contract() {
        let endpoint = std::env::var("DBX_TEST_WEBDAV_ENDPOINT").expect("DBX_TEST_WEBDAV_ENDPOINT");
        let username = std::env::var("DBX_TEST_WEBDAV_USERNAME").expect("DBX_TEST_WEBDAV_USERNAME");
        let password = std::env::var("DBX_TEST_WEBDAV_PASSWORD").expect("DBX_TEST_WEBDAV_PASSWORD");
        let config = WebdavConnectionConfig { username, ..basic_config(endpoint) };
        let secrets = basic_secrets(password.clone());

        let result = test_connection(&config, &secrets).await;
        assert!(result.success, "{:?}", result.stages.iter().map(|stage| &stage.message).collect::<Vec<_>>());
        assert_eq!(result.stages.len(), 5);
        let bad = test_connection(&config, &basic_secrets("wrong-password".to_string())).await;
        assert!(
            !bad.success,
            "{:?}",
            bad.stages.iter().map(|stage| (stage.stage, stage.status, &stage.message)).collect::<Vec<_>>()
        );
        assert_eq!(
            bad.stages.iter().find(|stage| stage.status == "failed").map(|stage| stage.stage),
            Some("authentication")
        );

        let operator = build_operator(&config, &secrets).unwrap();
        let fixture = operator.read("fixture.txt").await.unwrap().to_vec();
        assert_eq!(fixture, b"fixture");
        let metadata = operator.stat("fixture.txt").await.unwrap();
        assert!(metadata.mode().is_file());

        operator.create_dir("created/").await.unwrap();
        operator.write("created/file.txt", "created").await.unwrap();
        let listed = operator.list("created/").await.unwrap();
        assert!(listed.iter().any(|entry| entry.path() == "created/file.txt"));

        let mut file = tempfile::tempfile().unwrap();
        let payload = vec![b'x'; 9 * 1024 * 1024 + 17];
        file.write_all(&payload).unwrap();
        file.rewind().unwrap();
        put_file(
            &config,
            "streaming.bin",
            tokio::fs::File::from_std(file),
            payload.len() as u64,
            Arc::new(|_| {}),
            &secrets,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        assert_eq!(operator.stat("streaming.bin").await.unwrap().content_length(), payload.len() as u64);

        copy_file(&config, "fixture.txt", "copied.txt", &secrets, Arc::new(AtomicBool::new(false))).await.unwrap();
        move_file(&config, "copied.txt", "moved.txt", &secrets, Arc::new(AtomicBool::new(false))).await.unwrap();
        assert!(!operator.exists("copied.txt").await.unwrap());
        assert_eq!(operator.read("moved.txt").await.unwrap().to_vec(), b"fixture");

        operator.write("replace.txt", "old").await.unwrap();
        copy_file(&config, "fixture.txt", "replace.txt", &secrets, Arc::new(AtomicBool::new(false))).await.unwrap();
        assert_eq!(operator.read("replace.txt").await.unwrap().to_vec(), b"fixture");
        operator.write("move-replace.txt", "old").await.unwrap();
        copy_file(&config, "fixture.txt", "move-source.txt", &secrets, Arc::new(AtomicBool::new(false))).await.unwrap();
        move_file(&config, "move-source.txt", "move-replace.txt", &secrets, Arc::new(AtomicBool::new(false)))
            .await
            .unwrap();
        assert_eq!(operator.read("move-replace.txt").await.unwrap().to_vec(), b"fixture");

        let response_loss =
            copy_file(&config, "fixture.txt", "response-loss-copy.txt", &secrets, Arc::new(AtomicBool::new(false)))
                .await
                .unwrap_err();
        assert_eq!(response_loss.kind, WebdavMutationErrorKind::DispatchOutcomeUnknown);
        assert_eq!(response_loss.stage, WebdavMutationStage::Dispatch);
        assert_eq!(response_loss.http_status, None);
        assert_eq!(operator.read("response-loss-copy.txt").await.unwrap().to_vec(), b"fixture");

        let anonymous = WebdavConnectionConfig {
            authentication: WebdavAuthentication::None,
            username: String::new(),
            ..config.clone()
        };
        put_file(
            &anonymous,
            "auth-anonymous.txt",
            tokio::fs::File::from_std(tempfile_with_content(b"anonymous")),
            9,
            Arc::new(|_| {}),
            &ResolvedFileSecrets::default(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let bearer = WebdavConnectionConfig {
            authentication: WebdavAuthentication::Bearer,
            username: String::new(),
            ..config.clone()
        };
        let bearer_secrets = ResolvedFileSecrets {
            webdav_token: Some(dbx_core::file_secrets::FileSecret::new("dbx-bearer-token".to_string()).unwrap()),
            ..ResolvedFileSecrets::default()
        };
        put_file(
            &bearer,
            "auth-bearer.txt",
            tokio::fs::File::from_std(tempfile_with_content(b"bearer")),
            6,
            Arc::new(|_| {}),
            &bearer_secrets,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();

        operator.create_dir("empty-delete/").await.unwrap();
        delete_entry(&config, &operator, &RemotePath::parse("empty-delete").unwrap(), Some("directory"), &secrets)
            .await
            .unwrap();
        assert!(!operator.exists("empty-delete/").await.unwrap());

        operator.create_dir("concurrent-delete/").await.unwrap();
        delete_entry(&config, &operator, &RemotePath::parse("concurrent-delete").unwrap(), Some("directory"), &secrets)
            .await
            .unwrap();
        assert!(!operator.exists("concurrent-delete/").await.unwrap());

        operator.create_dir("unsafe-timeout-delete/").await.unwrap();
        let unsafe_timeout = delete_entry(
            &config,
            &operator,
            &RemotePath::parse("unsafe-timeout-delete").unwrap(),
            Some("directory"),
            &secrets,
        )
        .await
        .unwrap_err();
        assert!(unsafe_timeout.contains("finite LOCK timeout"), "{unsafe_timeout}");
        assert!(operator.exists("unsafe-timeout-delete/").await.unwrap());

        operator.create_dir("response-loss-delete/").await.unwrap();
        let delete_unknown = delete_entry(
            &config,
            &operator,
            &RemotePath::parse("response-loss-delete").unwrap(),
            Some("directory"),
            &secrets,
        )
        .await
        .unwrap_err();
        assert!(delete_unknown.contains("response was lost"), "{delete_unknown}");
        tokio::time::sleep(Duration::from_millis(900)).await;

        operator.create_dir("nonempty-delete/").await.unwrap();
        operator.write("nonempty-delete/child.txt", "keep").await.unwrap();
        let error = delete_entry(
            &config,
            &operator,
            &RemotePath::parse("nonempty-delete").unwrap(),
            Some("directory"),
            &secrets,
        )
        .await
        .unwrap_err();
        assert!(error.contains("not empty"));
        assert_eq!(operator.read("nonempty-delete/child.txt").await.unwrap().to_vec(), b"keep");

        let permission = operator.write("denied/new.txt", "must-not-write").await.unwrap_err();
        assert!(matches!(permission.kind(), ErrorKind::PermissionDenied | ErrorKind::Unexpected));
        assert!(!operator.exists("denied/new.txt").await.unwrap());

        let timed_out = tokio::time::timeout(
            Duration::from_millis(500),
            copy_file(&config, "fixture.txt", "timeout-copy.txt", &secrets, Arc::new(AtomicBool::new(false))),
        )
        .await;
        assert!(timed_out.is_err());
    }

    fn tempfile_with_content(content: &[u8]) -> std::fs::File {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(content).unwrap();
        file.rewind().unwrap();
        file
    }
}
