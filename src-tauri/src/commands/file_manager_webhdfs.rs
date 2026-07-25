use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{stream, StreamExt};
use opendal::layers::HttpClientLayer;
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use opendal::services::Webhdfs;
use opendal::{Buffer, Error, ErrorKind, Operator};
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Client, Response, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use url::Host;
use uuid::Uuid;

use super::file_manager::{
    ConnectionTestStage, FileConnectionCapabilities, FileConnectionTestResult, ResolvedFileSecrets,
};
use super::file_manager_paths::RemotePath;

const CONTROL_BODY_LIMIT: usize = 64 * 1024;
const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_CONTROL_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_IDLE_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_CHUNK_SIZE_MIB: u32 = 4;
const MAX_CHUNK_SIZE_MIB: u32 = 16;
const OWNED_CLEANUP_INITIAL_DELAY: Duration = Duration::from_millis(500);
const OWNED_CLEANUP_POLL_DELAY: Duration = Duration::from_millis(250);
const OWNED_CLEANUP_ATTEMPTS: usize = 10;
#[cfg(test)]
static TEST_OPEN_REQUEST_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WebhdfsAuthentication {
    Simple,
    Delegation,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebhdfsWriteOptions {
    pub permission: Option<String>,
    pub replication: Option<u16>,
    pub block_size: Option<u64>,
    pub buffer_size: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebhdfsConnectionConfig {
    pub endpoint: String,
    pub root: String,
    pub authentication: WebhdfsAuthentication,
    #[serde(default)]
    pub user_name: String,
    #[serde(default)]
    pub disable_list_batch: bool,
    #[serde(default)]
    pub allowed_data_node_origins: Vec<String>,
    #[serde(default)]
    pub data_node_hostname_mapping: BTreeMap<String, String>,
    pub tls_ca_certificate_path: Option<String>,
    pub proxy_url: Option<String>,
    pub proxy_bypass: Option<String>,
    #[serde(default)]
    pub allow_tls_downgrade: bool,
    #[serde(default = "default_connect_timeout_seconds")]
    pub connect_timeout_seconds: u64,
    #[serde(default = "default_control_timeout_seconds")]
    pub control_timeout_seconds: u64,
    #[serde(default = "default_idle_timeout_seconds")]
    pub idle_timeout_seconds: u64,
    #[serde(default = "default_chunk_size_mib")]
    pub chunk_size_mib: u32,
    #[serde(default)]
    pub write_options: WebhdfsWriteOptions,
}

#[derive(Debug)]
pub(super) struct WebhdfsMutationError {
    pub message: String,
    outcome_unknown: bool,
}

impl WebhdfsMutationError {
    fn definitive(message: String) -> Self {
        Self { message, outcome_unknown: false }
    }

    fn uncertain(message: String) -> Self {
        Self { message, outcome_unknown: true }
    }

    pub(super) fn is_outcome_unknown(&self) -> bool {
        self.outcome_unknown
    }
}

#[derive(Clone)]
struct RequestPolicy {
    name_node_origin: String,
    allowed_data_node_origins: BTreeSet<String>,
    user_name: Option<String>,
    delegation: Option<String>,
    allow_tls_downgrade: bool,
    endpoint_is_https: bool,
}

#[derive(Clone)]
struct ValidatingHttpFetch {
    client: reqwest13::Client,
    policy: RequestPolicy,
}

impl HttpFetch for ValidatingHttpFetch {
    async fn fetch(&self, request: http::Request<Buffer>) -> opendal::Result<http::Response<HttpBody>> {
        let raw = request.uri().to_string();
        let url = Url::parse(&raw).map_err(|error| {
            Error::new(ErrorKind::Unexpected, "WebHDFS request URL is invalid")
                .with_operation("webhdfs_request_policy")
                .set_source(error)
        })?;
        #[cfg(test)]
        if url.query_pairs().any(|(key, value)| key == "op" && value == "OPEN") {
            TEST_OPEN_REQUEST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        self.policy.validate_url(&url).map_err(|message| {
            Error::new(ErrorKind::PermissionDenied, "WebHDFS request target was rejected")
                .with_operation("webhdfs_request_policy")
                .with_context("reason", message)
        })?;
        let redirect_request = clone_http_request(&request).map_err(|message| {
            Error::new(ErrorKind::Unexpected, "WebHDFS request could not be prepared")
                .with_operation("webhdfs_request_policy")
                .with_context("reason", message)
        })?;
        let response = <reqwest13::Client as HttpFetch>::fetch(&self.client, request).await?;
        if response.status() != http::StatusCode::TEMPORARY_REDIRECT {
            return Ok(response);
        }
        if canonical_origin(&url).as_deref() != Ok(self.policy.name_node_origin.as_str()) {
            return Err(Error::new(ErrorKind::PermissionDenied, "WebHDFS redirect source was rejected")
                .with_operation("webhdfs_request_policy")
                .with_context("reason", "only the configured NameNode may redirect to a DataNode"));
        }
        let location = exactly_one_location(response.headers()).map_err(|message| {
            Error::new(ErrorKind::Unexpected, "WebHDFS redirect Location is invalid")
                .with_operation("webhdfs_request_policy")
                .with_context("reason", message)
        })?;
        let data_node_url = Url::parse(location).map_err(|error| {
            Error::new(ErrorKind::Unexpected, "WebHDFS redirect Location must be an absolute URL")
                .with_operation("webhdfs_request_policy")
                .set_source(error)
        })?;
        self.policy.validate_data_node_url(&data_node_url).map_err(|message| {
            Error::new(ErrorKind::PermissionDenied, "WebHDFS DataNode redirect was rejected")
                .with_operation("webhdfs_request_policy")
                .with_context("reason", message)
        })?;
        let redirected = replace_request_uri(redirect_request, data_node_url.as_str()).map_err(|message| {
            Error::new(ErrorKind::Unexpected, "WebHDFS DataNode request URL is invalid")
                .with_operation("webhdfs_request_policy")
                .with_context("reason", message)
        })?;
        let response = <reqwest13::Client as HttpFetch>::fetch(&self.client, redirected).await?;
        if response.status() == http::StatusCode::TEMPORARY_REDIRECT {
            return Err(Error::new(ErrorKind::PermissionDenied, "WebHDFS DataNode redirect was rejected")
                .with_operation("webhdfs_request_policy")
                .with_context("reason", "WebHDFS permits exactly one NameNode-to-DataNode redirect"));
        }
        Ok(response)
    }
}

#[cfg(test)]
pub(super) fn reset_test_open_request_count() {
    TEST_OPEN_REQUEST_COUNT.store(0, Ordering::SeqCst);
}

#[cfg(test)]
pub(super) fn test_open_request_count() -> usize {
    TEST_OPEN_REQUEST_COUNT.load(Ordering::SeqCst)
}

fn clone_http_request(request: &http::Request<Buffer>) -> Result<http::Request<Buffer>, String> {
    let mut clone = http::Request::builder()
        .method(request.method().clone())
        .uri(request.uri().clone())
        .version(request.version())
        .body(request.body().clone())
        .map_err(|error| format!("Clone WebHDFS request: {error}"))?;
    *clone.headers_mut() = request.headers().clone();
    Ok(clone)
}

fn replace_request_uri(mut request: http::Request<Buffer>, location: &str) -> Result<http::Request<Buffer>, String> {
    *request.uri_mut() = location.parse().map_err(|error| format!("Parse WebHDFS DataNode request URI: {error}"))?;
    Ok(request)
}

fn exactly_one_location(headers: &http::HeaderMap) -> Result<&str, String> {
    let mut locations = headers.get_all(http::header::LOCATION).iter();
    let location = locations.next().ok_or_else(|| "WebHDFS 307 response did not include Location".to_string())?;
    if locations.next().is_some() {
        return Err("WebHDFS 307 response included multiple Location headers".to_string());
    }
    location.to_str().map_err(|_| "WebHDFS redirect Location is not valid ASCII".to_string())
}

pub(super) struct WebhdfsDirectAdapter {
    config: WebhdfsConnectionConfig,
    client: Client,
    policy: RequestPolicy,
    user_name: Option<String>,
    delegation: Option<String>,
}

#[derive(Debug)]
pub(super) struct WebhdfsStreamingWriter {
    sender: Option<mpsc::Sender<Result<Bytes, std::io::Error>>>,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
    expected_size: u64,
    sent: u64,
}

impl WebhdfsStreamingWriter {
    pub(super) async fn write(&mut self, bytes: Bytes) -> Result<(), String> {
        let next = self
            .sent
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| "WebHDFS streamed byte count overflowed".to_string())?;
        if next > self.expected_size {
            return Err(format!("WebHDFS stream exceeded declared Content-Length {}", self.expected_size));
        }
        self.sender
            .as_ref()
            .ok_or_else(|| "WebHDFS stream is already closed".to_string())?
            .send(Ok(bytes))
            .await
            .map_err(|_| "WebHDFS DataNode stopped accepting the request body".to_string())?;
        self.sent = next;
        Ok(())
    }

    pub(super) async fn close(&mut self) -> Result<(), String> {
        if self.sent != self.expected_size {
            let mismatch =
                format!("WebHDFS stream length mismatch: declared {}, supplied {}", self.expected_size, self.sent);
            self.abort_and_wait().await.map_err(|error| format!("{mismatch}; {error}"))?;
            return Err(mismatch);
        }
        self.sender.take();
        let result = self.task.as_mut().expect("WebHDFS writer task is present until close").await;
        self.task.take();
        result.map_err(|error| format!("WebHDFS DataNode write task failed: {error}"))?
    }

    pub(super) async fn abort_and_wait(&mut self) -> Result<(), String> {
        self.sender.take();
        if let Some(task) = self.task.as_mut() {
            task.abort();
            let result = match task.await {
                Ok(result) => result,
                Err(error) if error.is_cancelled() => Ok(()),
                Err(error) => Err(format!("WebHDFS DataNode write task failed while aborting: {error}")),
            };
            self.task.take();
            result
        } else {
            Ok(())
        }
    }
}

impl Drop for WebhdfsStreamingWriter {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
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
        server_side_copy: false,
        atomic_rename: true,
        atomic_no_clobber: false,
    }
}

pub(super) fn normalize_config(config: &mut WebhdfsConnectionConfig) -> Result<(), String> {
    config.endpoint = normalize_endpoint(&config.endpoint)?;
    config.root = normalize_root(&config.root)?;
    config.user_name = config.user_name.trim().to_string();
    config.allowed_data_node_origins = parse_allowed_origins(&config.allowed_data_node_origins)?.into_iter().collect();
    config.data_node_hostname_mapping = normalize_hostname_mapping(&config.data_node_hostname_mapping)?;
    config.tls_ca_certificate_path = normalize_optional(config.tls_ca_certificate_path.take());
    config.proxy_url = normalize_optional(config.proxy_url.take());
    config.proxy_bypass = normalize_optional(config.proxy_bypass.take());
    normalize_write_options(&mut config.write_options);
    validate_config(config, false, None)
}

pub(super) fn validate_config(
    config: &WebhdfsConnectionConfig,
    is_new: bool,
    delegation: Option<&str>,
) -> Result<(), String> {
    let endpoint = endpoint_url(&config.endpoint)?;
    normalize_root(&config.root)?;
    match config.authentication {
        WebhdfsAuthentication::Simple => {
            if config.user_name.trim().is_empty() {
                return Err("WebHDFS simple authentication requires a user name".to_string());
            }
            if delegation.is_some() {
                return Err("WebHDFS simple authentication cannot include a delegation token".to_string());
            }
        }
        WebhdfsAuthentication::Delegation => {
            if !config.user_name.trim().is_empty() {
                return Err("WebHDFS delegation authentication cannot include a simple user name".to_string());
            }
            if is_new && delegation.is_none_or(str::is_empty) {
                return Err("WebHDFS delegation authentication requires a delegation token".to_string());
            }
        }
    }
    parse_allowed_origins(&config.allowed_data_node_origins)?;
    normalize_hostname_mapping(&config.data_node_hostname_mapping)?;
    if config.allowed_data_node_origins.is_empty() {
        return Err("WebHDFS requires at least one allowlisted DataNode origin".to_string());
    }
    if endpoint.scheme() == "https" && config.allow_tls_downgrade {
        return Err("WebHDFS HTTPS connections cannot allow an HTTP DataNode downgrade".to_string());
    }
    if let Some(path) = config.tls_ca_certificate_path.as_deref() {
        if path.trim().is_empty() || !Path::new(path).is_absolute() {
            return Err("WebHDFS TLS CA certificate path must be absolute".to_string());
        }
    }
    if let Some(proxy) = config.proxy_url.as_deref() {
        validate_proxy_url(proxy)?;
    }
    validate_duration("connectTimeoutSeconds", config.connect_timeout_seconds)?;
    validate_duration("controlTimeoutSeconds", config.control_timeout_seconds)?;
    validate_duration("idleTimeoutSeconds", config.idle_timeout_seconds)?;
    if !(1..=MAX_CHUNK_SIZE_MIB).contains(&config.chunk_size_mib) {
        return Err(format!("WebHDFS chunkSizeMib must be between 1 and {MAX_CHUNK_SIZE_MIB}"));
    }
    validate_write_options(&config.write_options)?;
    Ok(())
}

pub(super) fn build_operator(
    config: &WebhdfsConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<Operator, String> {
    let (policy, user_name, delegation) = build_policy_context(config, secrets)?;
    let mut builder = Webhdfs::default().endpoint(&config.endpoint).root(&config.root);
    if config.disable_list_batch {
        builder = builder.disable_list_batch();
    }
    if let Some(user_name) = &user_name {
        builder = builder.user_name(user_name);
    }
    if let Some(delegation) = &delegation {
        builder = builder.delegation(delegation);
    }
    let client =
        HttpClient::with(ValidatingHttpFetch { client: build_opendal_http_client(config, policy.clone())?, policy });
    Operator::new(builder)
        .map(|operator| operator.layer(HttpClientLayer::new(client)).finish())
        .map_err(|error| redact(error.to_string(), secrets))
}

pub(super) fn build_direct_adapter(
    config: &WebhdfsConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<WebhdfsDirectAdapter, String> {
    let (client, policy, user_name, delegation) = build_http_context(config, secrets)?;
    Ok(WebhdfsDirectAdapter { config: config.clone(), client, policy, user_name, delegation })
}

impl WebhdfsDirectAdapter {
    pub(super) async fn open_streaming_write(
        &self,
        relative_path: &str,
        size: u64,
        dispatch_started: Arc<AtomicBool>,
    ) -> Result<WebhdfsStreamingWriter, String> {
        let path = RemotePath::parse(relative_path)?;
        let mut url = self.operation_url(path.as_str(), "CREATE")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("overwrite", "false");
            append_write_options(&mut query, &self.config.write_options);
        }
        let location = self.redirect_location(self.client.put(url), "CREATE").await?;
        let (sender, receiver) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
        let body =
            stream::unfold(receiver, |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) });
        let client = self.client.clone();
        let timeout = Duration::from_secs(self.config.idle_timeout_seconds);
        let task = tokio::spawn(async move {
            dispatch_started.store(true, Ordering::Release);
            let response = client
                .put(location)
                .header(CONTENT_LENGTH, size)
                .body(reqwest::Body::wrap_stream(body))
                .send()
                .await
                .map_err(|error| format!("WebHDFS DataNode PUT failed: {error}"))?;
            if matches!(response.status(), StatusCode::CREATED | StatusCode::OK) {
                Ok(())
            } else {
                let status = response.status();
                let body = read_text_bounded(response, CONTROL_BODY_LIMIT, timeout)
                    .await
                    .unwrap_or_else(|error| format!("<unreadable error body: {error}>"));
                Err(format!("WebHDFS DataNode PUT returned {status}: {body}"))
            }
        });
        Ok(WebhdfsStreamingWriter { sender: Some(sender), task: Some(task), expected_size: size, sent: 0 })
    }

    pub(super) async fn rename(
        &self,
        source: &str,
        destination: &str,
        dispatch_started: Arc<AtomicBool>,
    ) -> Result<(), WebhdfsMutationError> {
        let source = RemotePath::parse(source).map_err(WebhdfsMutationError::definitive)?;
        let destination = RemotePath::parse(destination).map_err(WebhdfsMutationError::definitive)?;
        let mut url = self.operation_url(source.as_str(), "RENAME").map_err(WebhdfsMutationError::definitive)?;
        let rooted_destination =
            rooted_absolute_path(&self.config.root, destination.as_str()).map_err(WebhdfsMutationError::definitive)?;
        url.query_pairs_mut().append_pair("destination", &rooted_destination);
        dispatch_started.store(true, Ordering::Release);
        let response = self.send_control(self.client.put(url)).await.map_err(WebhdfsMutationError::uncertain)?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = self
                .read_error_body(response)
                .await
                .unwrap_or_else(|error| format!("<unreadable error body: {error}>"));
            return Err(WebhdfsMutationError::definitive(format!("WebHDFS RENAME returned {status}: {body}")));
        }
        let response: BooleanResp =
            read_json_bounded(response, CONTROL_BODY_LIMIT, self.idle_timeout()).await.map_err(|error| {
                WebhdfsMutationError::uncertain(format!("WebHDFS RENAME response could not be verified: {error}"))
            })?;
        if !response.boolean {
            return Err(WebhdfsMutationError::definitive("WebHDFS RENAME returned BooleanResp=false".to_string()));
        }
        Ok(())
    }

    pub(super) async fn delete_owned_file_if_exists(&self, path: &str) -> Result<(), String> {
        let cleanup = async {
            // A cancelled request body can race HDFS pipeline teardown: an
            // immediate DELETE may observe no inode just before it appears.
            tokio::time::sleep(OWNED_CLEANUP_INITIAL_DELAY).await;
            let mut last_error = None;
            for _ in 0..OWNED_CLEANUP_ATTEMPTS {
                if let Err(error) = self.delete_non_recursive_raw(path, true).await {
                    last_error = Some(error);
                }
                tokio::time::sleep(OWNED_CLEANUP_POLL_DELAY).await;
                match self.entry_exists(path).await {
                    Ok(false) => {
                        tokio::time::sleep(OWNED_CLEANUP_POLL_DELAY).await;
                        match self.entry_exists(path).await {
                            Ok(false) => return Ok(()),
                            Ok(true) => {}
                            Err(error) => last_error = Some(error),
                        }
                    }
                    Ok(true) => {}
                    Err(error) => last_error = Some(error),
                }
            }
            let detail = last_error.map(|error| format!("; last cleanup error: {error}")).unwrap_or_default();
            Err(format!("WebHDFS operation-owned partial still exists after cleanup retries: {path}{detail}"))
        };
        tokio::time::timeout(Duration::from_secs(self.config.control_timeout_seconds), cleanup)
            .await
            .map_err(|_| "WebHDFS operation-owned partial cleanup timed out".to_string())?
    }

    pub(super) async fn delete_entry(&self, path: &str) -> Result<(), String> {
        if self.delete_non_recursive_raw(path, false).await? {
            Ok(())
        } else {
            Err("WebHDFS DELETE returned BooleanResp=false".to_string())
        }
    }

    async fn delete_non_recursive_raw(&self, path: &str, absent_is_success: bool) -> Result<bool, String> {
        let path = RemotePath::parse(path)?;
        let mut url = self.operation_url(path.as_str(), "DELETE")?;
        url.query_pairs_mut().append_pair("recursive", "false");
        let response = self.send_control(self.client.delete(url)).await?;
        if response.status() == StatusCode::NOT_FOUND && absent_is_success {
            return Ok(false);
        }
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = self.read_error_body(response).await.unwrap_or_default();
            return Err(format!("WebHDFS DELETE returned {status}: {body}"));
        }
        let response: BooleanResp = read_json_bounded(response, CONTROL_BODY_LIMIT, self.idle_timeout()).await?;
        Ok(response.boolean)
    }

    async fn entry_exists(&self, path: &str) -> Result<bool, String> {
        let path = RemotePath::parse(path)?;
        let url = self.operation_url(path.as_str(), "GETFILESTATUS")?;
        let response = self.send_control(self.client.get(url)).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = self.read_error_body(response).await.unwrap_or_default();
            return Err(format!("WebHDFS GETFILESTATUS returned {status}: {body}"));
        }
        let response: FileStatusResp = read_json_bounded(response, CONTROL_BODY_LIMIT, self.idle_timeout()).await?;
        if response.file_status.is_object() {
            Ok(true)
        } else {
            Err("WebHDFS GETFILESTATUS response omitted FileStatus".to_string())
        }
    }

    async fn redirect_location(&self, request: reqwest::RequestBuilder, operation: &str) -> Result<Url, String> {
        let response = self.send_control(request).await?;
        if response.status() != StatusCode::TEMPORARY_REDIRECT {
            let status = response.status();
            let body = self
                .read_error_body(response)
                .await
                .unwrap_or_else(|error| format!("<unreadable error body: {error}>"));
            return Err(format!("WebHDFS {operation} expected 307, got {status}: {body}"));
        }
        let raw = exactly_one_location(response.headers())
            .map_err(|error| format!("WebHDFS {operation} redirect: {error}"))?;
        let location = Url::parse(raw).map_err(|_| format!("WebHDFS {operation} Location is not an absolute URL"))?;
        self.policy.validate_data_node_url(&location)?;
        Ok(location)
    }

    fn operation_url(&self, relative_path: &str, operation: &str) -> Result<Url, String> {
        operation_url(
            &self.config.endpoint,
            &self.config.root,
            relative_path,
            operation,
            self.user_name.as_deref(),
            self.delegation.as_deref(),
        )
    }

    async fn send_control(&self, request: reqwest::RequestBuilder) -> Result<Response, String> {
        tokio::time::timeout(Duration::from_secs(self.config.control_timeout_seconds), request.send())
            .await
            .map_err(|_| format!("WebHDFS control request exceeded {} seconds", self.config.control_timeout_seconds))?
            .map_err(|error| format!("WebHDFS control request failed: {error}"))
    }

    async fn read_error_body(&self, response: Response) -> Result<String, String> {
        read_text_bounded(response, CONTROL_BODY_LIMIT, self.idle_timeout()).await
    }

    fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.config.idle_timeout_seconds)
    }
}

pub(super) async fn test_connection(
    config: &WebhdfsConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> FileConnectionTestResult {
    let mut stages = Vec::with_capacity(6);
    if let Err(error) = validate_resolved_credentials(config, secrets) {
        stages.push(failed_stage("configuration", error));
        append_skipped(&mut stages, &["dns", "tcp", "namenode", "root", "datanode"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("configuration"));
    let endpoint = match endpoint_url(&config.endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            stages[0] = failed_stage("configuration", error);
            append_skipped(&mut stages, &["dns", "tcp", "namenode", "root", "datanode"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    let host = endpoint.host_str().expect("validated WebHDFS endpoint host");
    let port = endpoint.port_or_known_default().expect("validated WebHDFS endpoint port");
    let addresses = match tokio::time::timeout(
        Duration::from_secs(config.connect_timeout_seconds),
        tokio::net::lookup_host((host, port)),
    )
    .await
    {
        Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
        Ok(Err(error)) => {
            stages.push(failed_stage("dns", error.to_string()));
            append_skipped(&mut stages, &["tcp", "namenode", "root", "datanode"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Err(_) => {
            stages.push(failed_stage("dns", "NameNode DNS lookup timed out".to_string()));
            append_skipped(&mut stages, &["tcp", "namenode", "root", "datanode"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    stages.push(passed_stage("dns"));
    let tcp_result =
        connect_name_node_addresses_with(
            addresses,
            Duration::from_secs(config.connect_timeout_seconds),
            |address| async move {
                tokio::net::TcpStream::connect(address).await.map(drop).map_err(|error| error.to_string())
            },
        )
        .await;
    if let Err(error) = tcp_result {
        stages.push(failed_stage("tcp", error));
        append_skipped(&mut stages, &["namenode", "root", "datanode"]);
        return FileConnectionTestResult { success: false, stages };
    }
    stages.push(passed_stage("tcp"));

    let operator = match build_operator(config, secrets) {
        Ok(operator) => operator,
        Err(error) => {
            stages.push(failed_stage("namenode", error));
            append_skipped(&mut stages, &["root", "datanode"]);
            return FileConnectionTestResult { success: false, stages };
        }
    };
    let root_result =
        tokio::time::timeout(Duration::from_secs(config.control_timeout_seconds), operator.stat("/")).await;
    match root_result {
        Ok(Ok(metadata)) if metadata.mode().is_dir() => {
            stages.push(passed_stage("namenode"));
            stages.push(passed_stage("root"));
        }
        Ok(Ok(_)) => {
            stages.push(passed_stage("namenode"));
            stages.push(failed_stage("root", "Configured WebHDFS root is not a directory".to_string()));
            stages.push(skipped_stage("datanode"));
            return FileConnectionTestResult { success: false, stages };
        }
        Ok(Err(error)) => {
            stages.push(failed_stage("namenode", redact(error.to_string(), secrets)));
            append_skipped(&mut stages, &["root", "datanode"]);
            return FileConnectionTestResult { success: false, stages };
        }
        Err(_) => {
            stages.push(failed_stage("namenode", "NameNode request timed out".to_string()));
            append_skipped(&mut stages, &["root", "datanode"]);
            return FileConnectionTestResult { success: false, stages };
        }
    }

    let adapter = match build_direct_adapter(config, secrets) {
        Ok(adapter) => adapter,
        Err(error) => {
            stages.push(failed_stage("datanode", error));
            return FileConnectionTestResult { success: false, stages };
        }
    };
    let probe = format!(".dbx-connection-probe-{}", Uuid::new_v4());
    let dispatched = Arc::new(AtomicBool::new(false));
    let control_timeout = Duration::from_secs(config.control_timeout_seconds);
    let idle_timeout = Duration::from_secs(config.idle_timeout_seconds);
    let probe_result = async {
        let mut writer = tokio::time::timeout(control_timeout, adapter.open_streaming_write(&probe, 1, dispatched))
            .await
            .map_err(|_| ("WebHDFS DataNode probe CREATE timed out".to_string(), true))?
            .map_err(|error| (error, true))?;
        let write_result = tokio::time::timeout(idle_timeout, writer.write(Bytes::from_static(b"x")))
            .await
            .map_err(|_| "WebHDFS DataNode probe write stalled".to_string())
            .and_then(|result| result);
        if let Err(error) = write_result {
            return Err(abort_probe_writer(&mut writer, idle_timeout, error).await);
        }
        let close_result = tokio::time::timeout(idle_timeout, writer.close())
            .await
            .map_err(|_| "WebHDFS DataNode probe close stalled".to_string())
            .and_then(|result| result);
        if let Err(error) = close_result {
            return Err(abort_probe_writer(&mut writer, idle_timeout, error).await);
        }
        let read = tokio::time::timeout(control_timeout, operator.read(&probe))
            .await
            .map_err(|_| ("WebHDFS DataNode probe read timed out".to_string(), true))?
            .map_err(|error| (redact(error.to_string(), secrets), true))?;
        if read.to_bytes().as_ref() != b"x" {
            return Err(("WebHDFS DataNode read probe returned unexpected content".to_string(), true));
        }
        Ok(())
    }
    .await;
    let cleanup_safe = probe_result.as_ref().err().is_none_or(|(_, cleanup_safe)| *cleanup_safe);
    let cleanup = if cleanup_safe {
        adapter.delete_owned_file_if_exists(&probe).await
    } else {
        Err("probe writer termination was not confirmed; operation-owned probe was preserved".to_string())
    };
    let probe_result = probe_result.map_err(|(error, _)| error);
    match (probe_result, cleanup) {
        (Ok(()), Ok(())) => {
            stages.push(passed_stage("datanode"));
            FileConnectionTestResult { success: true, stages }
        }
        (Err(error), cleanup) => {
            let cleanup = cleanup
                .err()
                .map(|value| format!("; probe cleanup failed: {}", redact(value, secrets)))
                .unwrap_or_default();
            stages.push(failed_stage("datanode", format!("{}{cleanup}", redact(error, secrets))));
            FileConnectionTestResult { success: false, stages }
        }
        (Ok(()), Err(error)) => {
            stages.push(failed_stage("datanode", format!("DataNode probe cleanup failed: {}", redact(error, secrets))));
            FileConnectionTestResult { success: false, stages }
        }
    }
}

async fn abort_probe_writer(
    writer: &mut WebhdfsStreamingWriter,
    timeout: Duration,
    mut error: String,
) -> (String, bool) {
    match tokio::time::timeout(timeout, writer.abort_and_wait()).await {
        Ok(Ok(())) => (error, true),
        Ok(Err(abort_error)) => {
            error.push_str(&format!("; probe writer abort failed: {abort_error}"));
            (error, true)
        }
        Err(_) => {
            error.push_str("; probe writer abort timed out");
            (error, false)
        }
    }
}

async fn connect_name_node_addresses_with<F, Fut>(
    addresses: Vec<SocketAddr>,
    timeout: Duration,
    mut connect: F,
) -> Result<(), String>
where
    F: FnMut(SocketAddr) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    tokio::time::timeout(timeout, async move {
        let mut last_error = "No NameNode address accepted a TCP connection".to_string();
        for address in addresses {
            match connect(address).await {
                Ok(()) => return Ok(()),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    })
    .await
    .map_err(|_| "NameNode TCP connection timed out".to_string())?
}

fn build_http_context(
    config: &WebhdfsConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<(Client, RequestPolicy, Option<String>, Option<String>), String> {
    let (policy, user_name, delegation) = build_policy_context(config, secrets)?;
    let mut builder = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds));
    if let Some(path) = config.tls_ca_certificate_path.as_deref() {
        let pem = std::fs::read(path).map_err(|error| format!("Read WebHDFS TLS CA certificate {path}: {error}"))?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|error| format!("Parse WebHDFS TLS CA bundle: {error}"))?;
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
    }
    if let Some(proxy) = config.proxy_url.as_deref() {
        let mut configured = reqwest::Proxy::all(proxy).map_err(|error| format!("Configure WebHDFS proxy: {error}"))?;
        if let Some(bypass) = config.proxy_bypass.as_deref() {
            configured = configured.no_proxy(reqwest::NoProxy::from_string(bypass));
        }
        builder = builder.proxy(configured);
    } else {
        builder = builder.no_proxy();
    }
    for (hostname, address) in normalize_hostname_mapping(&config.data_node_hostname_mapping)? {
        builder = builder.resolve(&hostname, parse_mapping_address(&address)?);
    }
    let client = builder.build().map_err(|error| format!("Build WebHDFS HTTP client: {error}"))?;
    Ok((client, policy, user_name, delegation))
}

fn build_policy_context(
    config: &WebhdfsConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<(RequestPolicy, Option<String>, Option<String>), String> {
    validate_resolved_credentials(config, secrets)?;
    let endpoint = endpoint_url(&config.endpoint)?;
    let user_name =
        (config.authentication == WebhdfsAuthentication::Simple).then(|| config.user_name.trim().to_string());
    let delegation = (config.authentication == WebhdfsAuthentication::Delegation)
        .then(|| {
            secrets
                .webhdfs_delegation_token
                .clone()
                .ok_or_else(|| "Saved WebHDFS delegation token is unavailable".to_string())
        })
        .transpose()?;
    let policy = RequestPolicy {
        name_node_origin: canonical_origin(&endpoint)?,
        allowed_data_node_origins: parse_allowed_origins(&config.allowed_data_node_origins)?,
        user_name: user_name.clone(),
        delegation: delegation.clone(),
        allow_tls_downgrade: config.allow_tls_downgrade,
        endpoint_is_https: endpoint.scheme() == "https",
    };
    Ok((policy, user_name, delegation))
}

fn build_opendal_http_client(
    config: &WebhdfsConnectionConfig,
    _policy: RequestPolicy,
) -> Result<reqwest13::Client, String> {
    let mut builder = reqwest13::Client::builder()
        .redirect(reqwest13::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds));
    if let Some(path) = config.tls_ca_certificate_path.as_deref() {
        let pem = std::fs::read(path).map_err(|error| format!("Read WebHDFS TLS CA certificate {path}: {error}"))?;
        let certificates = reqwest13::Certificate::from_pem_bundle(&pem)
            .map_err(|error| format!("Parse WebHDFS TLS CA bundle: {error}"))?;
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
    }
    if let Some(proxy) = config.proxy_url.as_deref() {
        let mut configured =
            reqwest13::Proxy::all(proxy).map_err(|error| format!("Configure WebHDFS proxy: {error}"))?;
        if let Some(bypass) = config.proxy_bypass.as_deref() {
            configured = configured.no_proxy(reqwest13::NoProxy::from_string(bypass));
        }
        builder = builder.proxy(configured);
    } else {
        builder = builder.no_proxy();
    }
    for (hostname, address) in normalize_hostname_mapping(&config.data_node_hostname_mapping)? {
        builder = builder.resolve(&hostname, parse_mapping_address(&address)?);
    }
    builder.build().map_err(|error| format!("Build OpenDAL WebHDFS HTTP client: {error}"))
}

fn validate_resolved_credentials(
    config: &WebhdfsConnectionConfig,
    secrets: &ResolvedFileSecrets,
) -> Result<(), String> {
    validate_config(config, false, secrets.webhdfs_delegation_token.as_deref())?;
    if config.authentication == WebhdfsAuthentication::Delegation
        && secrets.webhdfs_delegation_token.as_deref().is_none_or(str::is_empty)
    {
        return Err("Saved WebHDFS delegation token is unavailable".to_string());
    }
    Ok(())
}

impl RequestPolicy {
    fn validate_url(&self, url: &Url) -> Result<(), String> {
        let origin = canonical_origin(url)?;
        if origin == self.name_node_origin {
            return self.validate_auth_query(url);
        }
        self.validate_data_node_url(url)
    }

    fn validate_data_node_url(&self, url: &Url) -> Result<(), String> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("DataNode redirect uses unsupported scheme {}", url.scheme()));
        }
        if self.endpoint_is_https && url.scheme() == "http" && !self.allow_tls_downgrade {
            return Err("DataNode redirect attempted HTTPS-to-HTTP downgrade".to_string());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("DataNode redirect must not contain URL userinfo".to_string());
        }
        if url.fragment().is_some() {
            return Err("DataNode redirect must not contain a fragment".to_string());
        }
        let origin = canonical_origin(url)?;
        if !self.allowed_data_node_origins.contains(&origin) {
            return Err(format!("DataNode redirect origin {origin} is not allowlisted"));
        }
        self.validate_auth_query(url)
    }

    fn validate_auth_query(&self, url: &Url) -> Result<(), String> {
        let pairs: Vec<_> = url.query_pairs().into_owned().collect();
        let user_values: Vec<_> = pairs.iter().filter(|(key, _)| key == "user.name").map(|(_, value)| value).collect();
        let delegation_values: Vec<_> =
            pairs.iter().filter(|(key, _)| key == "delegation").map(|(_, value)| value).collect();
        match (&self.user_name, &self.delegation) {
            (Some(expected), None)
                if user_values.len() == 1
                    && user_values.first().is_some_and(|value| value.as_str() == expected)
                    && delegation_values.is_empty() =>
            {
                Ok(())
            }
            (None, Some(expected))
                if delegation_values.len() == 1
                    && delegation_values.first().is_some_and(|value| value.as_str() == expected)
                    && user_values.is_empty() =>
            {
                Ok(())
            }
            _ => {
                Err("WebHDFS request did not preserve exactly one configured authentication query parameter"
                    .to_string())
            }
        }
    }
}

fn operation_url(
    endpoint: &str,
    root: &str,
    relative_path: &str,
    operation: &str,
    user_name: Option<&str>,
    delegation: Option<&str>,
) -> Result<Url, String> {
    let path = RemotePath::parse(relative_path)?;
    let mut url = endpoint_url(endpoint)?;
    {
        let mut segments = url.path_segments_mut().map_err(|_| "WebHDFS endpoint cannot be a base URL".to_string())?;
        segments.clear();
        segments.push("webhdfs");
        segments.push("v1");
        for segment in root.trim_matches('/').split('/').filter(|segment| !segment.is_empty()) {
            segments.push(segment);
        }
        for segment in path.as_str().split('/') {
            segments.push(segment);
        }
    }
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("op", operation);
        if let Some(user_name) = user_name {
            query.append_pair("user.name", user_name);
        }
        if let Some(delegation) = delegation {
            query.append_pair("delegation", delegation);
        }
    }
    Ok(url)
}

fn rooted_absolute_path(root: &str, relative_path: &str) -> Result<String, String> {
    RemotePath::parse(relative_path)?;
    let root = normalize_root(root)?;
    Ok(if root == "/" {
        format!("/{relative_path}")
    } else {
        format!("{}/{relative_path}", root.trim_end_matches('/'))
    })
}

fn append_write_options<T: url::form_urlencoded::Target>(
    query: &mut url::form_urlencoded::Serializer<'_, T>,
    options: &WebhdfsWriteOptions,
) {
    if let Some(permission) = &options.permission {
        query.append_pair("permission", permission);
    }
    if let Some(replication) = options.replication {
        query.append_pair("replication", &replication.to_string());
    }
    if let Some(block_size) = options.block_size {
        query.append_pair("blocksize", &block_size.to_string());
    }
    if let Some(buffer_size) = options.buffer_size {
        query.append_pair("buffersize", &buffer_size.to_string());
    }
}

fn normalize_write_options(options: &mut WebhdfsWriteOptions) {
    options.permission = normalize_optional(options.permission.take());
}

fn validate_write_options(options: &WebhdfsWriteOptions) -> Result<(), String> {
    if let Some(permission) = options.permission.as_deref() {
        if permission.len() != 3 || !permission.bytes().all(|byte| matches!(byte, b'0'..=b'7')) {
            return Err("WebHDFS write permission must be a three-digit octal mode".to_string());
        }
    }
    if options.replication == Some(0) {
        return Err("WebHDFS write replication must be greater than zero".to_string());
    }
    if options.block_size == Some(0) {
        return Err("WebHDFS write blockSize must be greater than zero".to_string());
    }
    if options.buffer_size == Some(0) {
        return Err("WebHDFS write bufferSize must be greater than zero".to_string());
    }
    Ok(())
}

fn endpoint_url(endpoint: &str) -> Result<Url, String> {
    let url = Url::parse(endpoint.trim())
        .map_err(|_| "WebHDFS endpoint must be a valid http:// or https:// URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("WebHDFS endpoint must use http:// or https://".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("Credentials must not be embedded in the WebHDFS endpoint".to_string());
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(
            "WebHDFS endpoint must not contain a path, query, or fragment; HttpFS is a separate deployment mode"
                .to_string(),
        );
    }
    if url.host_str().is_none() || url.port_or_known_default().is_none() {
        return Err("WebHDFS endpoint host and effective port are required".to_string());
    }
    Ok(url)
}

fn normalize_endpoint(endpoint: &str) -> Result<String, String> {
    let mut url = endpoint_url(endpoint)?;
    url.set_path("");
    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn normalize_root(root: &str) -> Result<String, String> {
    let root = root.trim();
    if !root.starts_with('/') {
        return Err("WebHDFS root must be an absolute path beginning with '/'".to_string());
    }
    if root.contains('\0') || root.contains('\\') {
        return Err("WebHDFS root contains an invalid character".to_string());
    }
    let mut segments = Vec::new();
    for segment in root.split('/').filter(|segment| !segment.is_empty()) {
        if matches!(segment, "." | "..") {
            return Err("WebHDFS root cannot contain '.' or '..' path segments".to_string());
        }
        segments.push(segment);
    }
    Ok(if segments.is_empty() { "/".to_string() } else { format!("/{}", segments.join("/")) })
}

fn parse_allowed_origins(origins: &[String]) -> Result<BTreeSet<String>, String> {
    origins
        .iter()
        .map(|origin| {
            let url = Url::parse(origin.trim()).map_err(|_| format!("Invalid WebHDFS DataNode origin: {origin}"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.path() != "/"
                || url.query().is_some()
                || url.fragment().is_some()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(format!("DataNode origin must contain only scheme, host, and port: {origin}"));
            }
            canonical_origin(&url)
        })
        .collect()
}

fn canonical_origin(url: &Url) -> Result<String, String> {
    let host = url.host_str().ok_or_else(|| "URL host is required".to_string())?;
    let port = url.port_or_known_default().ok_or_else(|| "URL effective port is required".to_string())?;
    Ok(format!("{}://{}:{port}", url.scheme(), format_host(host)))
}

fn format_host(host: &str) -> String {
    match Host::parse(host) {
        Ok(Host::Ipv6(value)) => format!("[{value}]"),
        _ => host.to_ascii_lowercase(),
    }
}

fn normalize_hostname_mapping(mapping: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>, String> {
    mapping
        .iter()
        .map(|(hostname, address)| {
            let hostname = hostname.trim().to_ascii_lowercase();
            if hostname.is_empty()
                || hostname.contains('/')
                || hostname.contains(':')
                || hostname.chars().any(char::is_whitespace)
            {
                return Err("WebHDFS DataNode hostname mapping contains an invalid hostname".to_string());
            }
            parse_mapping_address(address)?;
            Ok((hostname, address.trim().to_string()))
        })
        .collect()
}

fn parse_mapping_address(address: &str) -> Result<SocketAddr, String> {
    let address = address.trim();
    address
        .parse::<SocketAddr>()
        .or_else(|_| address.parse::<IpAddr>().map(|ip| SocketAddr::new(ip, 0)))
        .map_err(|_| format!("Invalid WebHDFS DataNode hostname mapping address: {address}"))
}

fn validate_proxy_url(proxy: &str) -> Result<(), String> {
    let url = Url::parse(proxy).map_err(|_| "WebHDFS proxy URL is invalid".to_string())?;
    if !matches!(url.scheme(), "http" | "https" | "socks5") {
        return Err("WebHDFS proxy must use http://, https://, or socks5://".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("WebHDFS proxy credentials cannot be embedded in configuration".to_string());
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err("WebHDFS proxy URL must contain only scheme, host, and port".to_string());
    }
    Ok(())
}

fn validate_duration(field: &str, value: u64) -> Result<(), String> {
    if !(1..=3600).contains(&value) {
        return Err(format!("WebHDFS {field} must be between 1 and 3600"));
    }
    Ok(())
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        (!value.is_empty()).then_some(value)
    })
}

fn default_connect_timeout_seconds() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_SECONDS
}

fn default_control_timeout_seconds() -> u64 {
    DEFAULT_CONTROL_TIMEOUT_SECONDS
}

fn default_idle_timeout_seconds() -> u64 {
    DEFAULT_IDLE_TIMEOUT_SECONDS
}

fn default_chunk_size_mib() -> u32 {
    DEFAULT_CHUNK_SIZE_MIB
}

#[derive(Deserialize)]
struct BooleanResp {
    boolean: bool,
}

#[derive(Deserialize)]
struct FileStatusResp {
    #[serde(rename = "FileStatus")]
    file_status: serde_json::Value,
}

async fn read_body_bounded(response: Response, max_bytes: usize, idle_timeout: Duration) -> Result<Bytes, String> {
    if response.content_length().is_some_and(|length| length > max_bytes as u64) {
        return Err(format!("WebHDFS control response exceeds {max_bytes} bytes"));
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    loop {
        let next = tokio::time::timeout(idle_timeout, stream.next())
            .await
            .map_err(|_| "WebHDFS response body stalled".to_string())?;
        let Some(next) = next else {
            break;
        };
        let chunk = next.map_err(|error| format!("Read WebHDFS response body: {error}"))?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(format!("WebHDFS control response exceeds {max_bytes} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

async fn read_json_bounded<T: DeserializeOwned>(
    response: Response,
    max_bytes: usize,
    idle_timeout: Duration,
) -> Result<T, String> {
    let body = read_body_bounded(response, max_bytes, idle_timeout).await?;
    serde_json::from_slice(&body).map_err(|error| format!("Parse WebHDFS JSON response: {error}"))
}

async fn read_text_bounded(response: Response, max_bytes: usize, idle_timeout: Duration) -> Result<String, String> {
    let body = read_body_bounded(response, max_bytes, idle_timeout).await?;
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn redact(message: String, secrets: &ResolvedFileSecrets) -> String {
    let mut message = message;
    if let Some(token) = secrets.webhdfs_delegation_token.as_deref().filter(|token| !token.is_empty()) {
        message = message.replace(token, "[REDACTED]");
        let percent_encoded =
            percent_encoding::utf8_percent_encode(token, percent_encoding::NON_ALPHANUMERIC).to_string();
        message = message.replace(&percent_encoded, "[REDACTED]");
        let form_encoded = url::form_urlencoded::byte_serialize(token.as_bytes()).collect::<String>();
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

fn append_skipped(stages: &mut Vec<ConnectionTestStage>, names: &[&'static str]) {
    stages.extend(names.iter().copied().map(skipped_stage));
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn config() -> WebhdfsConnectionConfig {
        WebhdfsConnectionConfig {
            endpoint: "https://namenode.test:9871".to_string(),
            root: "/tenant/dbx".to_string(),
            authentication: WebhdfsAuthentication::Delegation,
            user_name: String::new(),
            disable_list_batch: false,
            allowed_data_node_origins: vec!["https://datanode.test:9865".to_string()],
            data_node_hostname_mapping: BTreeMap::new(),
            tls_ca_certificate_path: None,
            proxy_url: None,
            proxy_bypass: None,
            allow_tls_downgrade: false,
            connect_timeout_seconds: 10,
            control_timeout_seconds: 30,
            idle_timeout_seconds: 30,
            chunk_size_mib: 4,
            write_options: WebhdfsWriteOptions::default(),
        }
    }

    fn policy() -> RequestPolicy {
        RequestPolicy {
            name_node_origin: "https://namenode.test:9871".to_string(),
            allowed_data_node_origins: BTreeSet::from(["https://datanode.test:9865".to_string()]),
            user_name: None,
            delegation: Some("secret".to_string()),
            allow_tls_downgrade: false,
            endpoint_is_https: true,
        }
    }

    fn local_config(address: SocketAddr) -> WebhdfsConnectionConfig {
        WebhdfsConnectionConfig {
            endpoint: format!("http://{address}"),
            root: "/tenant/dbx".to_string(),
            authentication: WebhdfsAuthentication::Simple,
            user_name: "hadoop".to_string(),
            disable_list_batch: false,
            allowed_data_node_origins: vec![format!("http://{address}")],
            data_node_hostname_mapping: BTreeMap::new(),
            tls_ca_certificate_path: None,
            proxy_url: None,
            proxy_bypass: None,
            allow_tls_downgrade: false,
            connect_timeout_seconds: 1,
            control_timeout_seconds: 1,
            idle_timeout_seconds: 1,
            chunk_size_mib: 4,
            write_options: WebhdfsWriteOptions::default(),
        }
    }

    async fn serve_once(response: &'static [u8]) -> (SocketAddr, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 16 * 1024];
            let count = socket.read(&mut request).await.unwrap();
            socket.write_all(response).await.unwrap();
            String::from_utf8_lossy(&request[..count]).into_owned()
        });
        (address, task)
    }

    async fn serve_sequence(responses: Vec<&'static [u8]>) -> (SocketAddr, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 16 * 1024];
                let count = socket.read(&mut request).await.unwrap();
                socket.write_all(response).await.unwrap();
                requests.push(String::from_utf8_lossy(&request[..count]).into_owned());
            }
            requests
        });
        (address, task)
    }

    #[test]
    fn encodes_each_path_segment_and_destination_query() {
        let url = operation_url(
            "https://namenode.test:9871",
            "/tenant/dbx",
            "space here/literal%2Fslash",
            "RENAME",
            None,
            Some("token"),
        )
        .unwrap();
        assert_eq!(url.path(), "/webhdfs/v1/tenant/dbx/space%20here/literal%252Fslash");
        assert_eq!(url.query_pairs().filter(|(key, _)| key == "delegation").count(), 1);
    }

    #[test]
    fn rejects_untrusted_datanode_and_auth_confusion() {
        let policy = policy();
        assert!(policy
            .validate_data_node_url(&Url::parse("https://attacker.test:9865/file?delegation=secret").unwrap())
            .unwrap_err()
            .contains("not allowlisted"));
        assert!(policy
            .validate_data_node_url(
                &Url::parse("https://datanode.test:9865/file?delegation=secret&user.name=hadoop").unwrap()
            )
            .is_err());
        assert!(policy
            .validate_data_node_url(
                &Url::parse("https://datanode.test:9865/file?delegation=secret&delegation=secret").unwrap()
            )
            .is_err());
    }

    #[test]
    fn validates_auth_tls_proxy_mapping_and_write_options() {
        let mut value = config();
        assert!(validate_config(&value, false, Some("secret")).is_ok());
        value.allow_tls_downgrade = true;
        assert!(validate_config(&value, false, Some("secret")).unwrap_err().contains("downgrade"));
        value.allow_tls_downgrade = false;
        value.proxy_url = Some("http://user:password@proxy.test:8080".to_string());
        assert!(validate_config(&value, false, Some("secret")).unwrap_err().contains("credentials"));
        value.proxy_url = None;
        value.write_options.permission = Some("888".to_string());
        assert!(validate_config(&value, false, Some("secret")).unwrap_err().contains("octal"));
    }

    #[test]
    fn requires_exactly_one_auth_mode() {
        let mut value = config();
        value.user_name = "hadoop".to_string();
        assert!(validate_config(&value, false, Some("secret")).is_err());
        value.authentication = WebhdfsAuthentication::Simple;
        assert!(validate_config(&value, false, Some("secret")).is_err());
        assert!(validate_config(&value, false, None).is_ok());
    }

    #[test]
    fn normalizes_root_endpoint_origins_and_mapping() {
        let mut value = config();
        value.endpoint = " https://namenode.test:9871/ ".to_string();
        value.root = "/tenant//dbx/".to_string();
        value.allowed_data_node_origins =
            vec!["https://DATANODE.test:9865/".to_string(), "https://datanode.test:9865".to_string()];
        value.data_node_hostname_mapping.insert(" DATANODE.TEST ".to_string(), "127.0.0.1:9865".to_string());
        normalize_config(&mut value).unwrap();
        assert_eq!(value.endpoint, "https://namenode.test:9871");
        assert_eq!(value.root, "/tenant/dbx");
        assert_eq!(value.allowed_data_node_origins, vec!["https://datanode.test:9865"]);
        assert_eq!(value.data_node_hostname_mapping.get("datanode.test").map(String::as_str), Some("127.0.0.1:9865"));
    }

    #[tokio::test]
    async fn rename_false_is_definitive_and_paths_are_encoded() {
        let (address, server) = serve_once(b"HTTP/1.1 200 OK\r\nContent-Length: 17\r\n\r\n{\"boolean\":false}").await;
        let adapter = build_direct_adapter(&local_config(address), &ResolvedFileSecrets::default()).unwrap();
        let error = adapter
            .rename("source 100%+#?&=.txt", "destination 100%+#?&=.txt", Arc::new(AtomicBool::new(false)))
            .await
            .unwrap_err();
        assert!(!error.is_outcome_unknown());
        assert!(error.message.contains("BooleanResp=false"));
        let request = server.await.unwrap();
        assert!(request.contains("/source%20100%25+%23%3F&=.txt?"));
        assert!(request.contains("destination=%2Ftenant%2Fdbx%2Fdestination+100%25%2B%23%3F%26%3D.txt"));
    }

    #[tokio::test]
    async fn ordinary_delete_does_not_treat_delete_false_as_absent() {
        let (address, server) = serve_once(b"HTTP/1.1 200 OK\r\nContent-Length: 17\r\n\r\n{\"boolean\":false}").await;
        let adapter = build_direct_adapter(&local_config(address), &ResolvedFileSecrets::default()).unwrap();
        let error = adapter.delete_entry("partial").await.unwrap_err();
        assert!(error.contains("BooleanResp=false"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn owned_cleanup_reconciles_delete_false_until_absence_is_stable() {
        let (address, server) = serve_sequence(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 17\r\n\r\n{\"boolean\":false}",
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let mut config = local_config(address);
        config.control_timeout_seconds = 2;
        let adapter = build_direct_adapter(&config, &ResolvedFileSecrets::default()).unwrap();
        adapter.delete_owned_file_if_exists("partial").await.unwrap();
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("DELETE "));
        assert!(requests[1].starts_with("GET "));
        assert!(requests[2].starts_with("GET "));
    }

    #[tokio::test]
    async fn owned_cleanup_accepts_stable_absence_after_delete_permission_error() {
        let (address, server) = serve_sequence(vec![
            b"HTTP/1.1 403 Forbidden\r\nContent-Length: 20\r\n\r\npermission denied!!!",
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let mut config = local_config(address);
        config.control_timeout_seconds = 2;
        let adapter = build_direct_adapter(&config, &ResolvedFileSecrets::default()).unwrap();
        adapter.delete_owned_file_if_exists("partial").await.unwrap();
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("DELETE "));
        assert!(requests[1].starts_with("GET "));
        assert!(requests[2].starts_with("GET "));
    }

    #[tokio::test]
    async fn opendal_fetch_validates_and_follows_one_datanode_redirect() {
        let (data_node, data_node_server) = serve_once(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata").await;
        let location = format!("http://{data_node}/file?user.name=hadoop");
        let name_node_response =
            format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n");
        let name_node_response: &'static [u8] = Box::leak(name_node_response.into_bytes().into_boxed_slice());
        let (name_node, name_node_server) = serve_once(name_node_response).await;
        let mut config = local_config(name_node);
        config.allowed_data_node_origins = vec![format!("http://{data_node}")];
        let (policy, _, _) = build_policy_context(&config, &ResolvedFileSecrets::default()).unwrap();
        let fetch = ValidatingHttpFetch { client: build_opendal_http_client(&config, policy.clone()).unwrap(), policy };
        let request = http::Request::builder()
            .uri(format!("http://{name_node}/webhdfs/v1/file?op=OPEN&user.name=hadoop"))
            .body(Buffer::new())
            .unwrap();
        let response = fetch.fetch(request).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(name_node_server.await.unwrap().starts_with("GET "));
        assert!(data_node_server.await.unwrap().starts_with("GET /file?user.name=hadoop "));
    }

    #[tokio::test]
    async fn opendal_fetch_rejects_untrusted_redirect_before_contact() {
        let capture = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let capture_address = capture.local_addr().unwrap();
        let response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{capture_address}/file?user.name=hadoop\r\nContent-Length: 0\r\n\r\n"
        );
        let response: &'static [u8] = Box::leak(response.into_bytes().into_boxed_slice());
        let (name_node, server) = serve_once(response).await;
        let mut config = local_config(name_node);
        config.allowed_data_node_origins = vec!["http://127.0.0.1:1".to_string()];
        let (policy, _, _) = build_policy_context(&config, &ResolvedFileSecrets::default()).unwrap();
        let fetch = ValidatingHttpFetch { client: build_opendal_http_client(&config, policy.clone()).unwrap(), policy };
        let request = http::Request::builder()
            .uri(format!("http://{name_node}/webhdfs/v1/file?op=OPEN&user.name=hadoop"))
            .body(Buffer::new())
            .unwrap();
        let error = match fetch.fetch(request).await {
            Ok(_) => panic!("untrusted redirect unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("not allowlisted"));
        server.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(100), capture.accept()).await.is_err());
    }

    #[tokio::test]
    async fn tcp_stage_uses_multiple_addresses_with_one_overall_timeout() {
        let first: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let second: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_connect = calls.clone();
        connect_name_node_addresses_with(vec![first, second], Duration::from_secs(1), move |address| {
            let calls = calls_for_connect.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                if address == first {
                    Err("first failed".to_string())
                } else {
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        let error = connect_name_node_addresses_with(vec![first, second], Duration::from_millis(20), |_| async {
            std::future::pending::<Result<(), String>>().await
        })
        .await
        .unwrap_err();
        assert_eq!(error, "NameNode TCP connection timed out");
    }

    #[tokio::test]
    async fn cancelled_close_retains_task_for_abort_and_wait() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_by_task = dropped.clone();
        let task = tokio::spawn(async move {
            let _probe = DropProbe(dropped_by_task);
            std::future::pending::<Result<(), String>>().await
        });
        tokio::task::yield_now().await;
        let mut writer = WebhdfsStreamingWriter { sender: None, task: Some(task), expected_size: 0, sent: 0 };
        assert!(tokio::time::timeout(Duration::from_millis(20), writer.close()).await.is_err());
        assert!(writer.task.is_some());
        writer.abort_and_wait().await.unwrap();
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn rename_body_stall_is_bounded_and_uncertain() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 17\r\n\r\n").await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let adapter = build_direct_adapter(&local_config(address), &ResolvedFileSecrets::default()).unwrap();
        let error = adapter.rename("source", "destination", Arc::new(AtomicBool::new(false))).await.unwrap_err();
        assert!(error.is_outcome_unknown());
        assert!(error.message.contains("stalled"));
        server.abort();
    }

    #[tokio::test]
    async fn untrusted_create_location_is_rejected_before_datanode_contact() {
        let capture = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let capture_address = capture.local_addr().unwrap();
        let response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{capture_address}/file?user.name=hadoop\r\nContent-Length: 0\r\n\r\n"
        );
        let response: &'static [u8] = Box::leak(response.into_bytes().into_boxed_slice());
        let (name_node, server) = serve_once(response).await;
        let mut config = local_config(name_node);
        config.allowed_data_node_origins = vec!["http://127.0.0.1:1".to_string()];
        let adapter = build_direct_adapter(&config, &ResolvedFileSecrets::default()).unwrap();
        let error = adapter.open_streaming_write("target", 1, Arc::new(AtomicBool::new(false))).await.unwrap_err();
        assert!(error.contains("not allowlisted"));
        server.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(100), capture.accept()).await.is_err());
    }

    #[tokio::test]
    async fn duplicate_create_locations_are_rejected_before_datanode_contact() {
        let capture = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let capture_address = capture.local_addr().unwrap();
        let response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{capture_address}/first?user.name=hadoop\r\nLocation: http://{capture_address}/second?user.name=hadoop\r\nContent-Length: 0\r\n\r\n"
        );
        let response: &'static [u8] = Box::leak(response.into_bytes().into_boxed_slice());
        let (name_node, server) = serve_once(response).await;
        let mut config = local_config(name_node);
        config.allowed_data_node_origins = vec![format!("http://{capture_address}")];
        let adapter = build_direct_adapter(&config, &ResolvedFileSecrets::default()).unwrap();
        let error = adapter.open_streaming_write("target", 1, Arc::new(AtomicBool::new(false))).await.unwrap_err();
        assert!(error.contains("multiple Location"));
        server.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(100), capture.accept()).await.is_err());
    }

    #[tokio::test]
    #[ignore = "run through tests/webhdfs-contract.sh with the fixed Hadoop service"]
    async fn fixed_webhdfs_service_contract() {
        use crate::commands::file_manager::{
            save_file_connection, test_file_connection, FileConnectionConfig, FileConnectionInput,
            FileConnectionSecrets, FileManagerRuntime, HdfsConnectionConfig,
        };
        use dbx_core::connection::AppState;
        use futures::TryStreamExt;
        use tauri::Manager;

        let endpoint = std::env::var("DBX_TEST_WEBHDFS_ENDPOINT").unwrap();
        let root = std::env::var("DBX_TEST_WEBHDFS_ROOT").unwrap();
        let data_node_origin = std::env::var("DBX_TEST_WEBHDFS_DATANODE_ORIGIN").unwrap();
        let config = WebhdfsConnectionConfig {
            endpoint,
            root,
            authentication: WebhdfsAuthentication::Simple,
            user_name: std::env::var("DBX_TEST_WEBHDFS_USER").unwrap_or_else(|_| "hadoop".to_string()),
            disable_list_batch: false,
            allowed_data_node_origins: vec![data_node_origin],
            data_node_hostname_mapping: BTreeMap::new(),
            tls_ca_certificate_path: None,
            proxy_url: None,
            proxy_bypass: None,
            allow_tls_downgrade: false,
            connect_timeout_seconds: 10,
            control_timeout_seconds: 30,
            idle_timeout_seconds: 30,
            chunk_size_mib: 4,
            write_options: WebhdfsWriteOptions::default(),
        };
        let directory = tempfile::tempdir().unwrap();
        let storage = dbx_core::storage::Storage::open(&directory.path().join("dbx.sqlite")).await.unwrap();
        let state = Arc::new(AppState::new(storage));
        let app = tauri::test::mock_builder()
            .manage(state.clone())
            .manage(FileManagerRuntime::default())
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let input = FileConnectionInput {
            id: None,
            expected_revision: None,
            name: "WebHDFS service contract".to_string(),
            config: FileConnectionConfig::Hdfs(HdfsConnectionConfig::Webhdfs(config.clone())),
            secrets: Some(FileConnectionSecrets {
                clear_webhdfs_credentials: Some(true),
                ..FileConnectionSecrets::default()
            }),
        };
        let tested =
            test_file_connection(app.state::<Arc<AppState>>(), app.state::<FileManagerRuntime>(), input.clone())
                .await
                .unwrap();
        assert!(tested.success, "{:?}", tested.stages);
        let saved =
            save_file_connection(app.state::<Arc<AppState>>(), app.state::<FileManagerRuntime>(), input).await.unwrap();
        assert!(!saved.has_credentials);
        let stored = state.storage.load_file_connection(&saved.id).await.unwrap().unwrap();
        assert!(!stored.config_json.contains("delegation"));

        let secrets = ResolvedFileSecrets::default();
        let operator = build_operator(&config, &secrets).unwrap();
        let adapter = build_direct_adapter(&config, &secrets).unwrap();
        let source = "special/source 100%+#?&=.bin";
        let destination = "special/destination 100%+#?&=.bin";
        operator.create_dir("special/").await.unwrap();
        let total = 9 * 1024 * 1024 + 17_u64;
        let mut writer = adapter.open_streaming_write(source, total, Arc::new(AtomicBool::new(false))).await.unwrap();
        let chunk = Bytes::from(vec![0x5a; 4 * 1024 * 1024]);
        let mut remaining = total;
        while remaining > 0 {
            let count = remaining.min(chunk.len() as u64) as usize;
            writer.write(chunk.slice(..count)).await.unwrap();
            remaining -= count as u64;
        }
        writer.close().await.unwrap();
        assert_eq!(operator.stat(source).await.unwrap().content_length(), total);

        let mut source_stream = operator.reader_with(source).await.unwrap().into_stream(..).await.unwrap();
        let mut copied =
            adapter.open_streaming_write(destination, total, Arc::new(AtomicBool::new(false))).await.unwrap();
        let mut copied_bytes = 0_u64;
        while let Some(buffer) = source_stream.try_next().await.unwrap() {
            assert!(buffer.len() <= 4 * 1024 * 1024);
            copied_bytes += buffer.len() as u64;
            copied.write(buffer.to_bytes()).await.unwrap();
        }
        assert_eq!(copied_bytes, total);
        copied.close().await.unwrap();
        let renamed = "special/renamed 100%+#?&=.bin";
        adapter.rename(destination, renamed, Arc::new(AtomicBool::new(false))).await.unwrap();
        assert!(!operator.exists(destination).await.unwrap());
        assert_eq!(operator.stat(renamed).await.unwrap().content_length(), total);
        operator.delete(source).await.unwrap();
        operator.delete(renamed).await.unwrap();
        operator.delete("special/").await.unwrap();
    }
}
