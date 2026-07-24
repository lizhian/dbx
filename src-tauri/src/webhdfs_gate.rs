use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use opendal::{services::Webhdfs, Operator};
use reqwest::{header::LOCATION, redirect::Policy, Body, Client, Response, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, io,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const OPEN_DAL_VERSION: &str = "0.57.0";

#[derive(Clone, Debug)]
pub struct GateConfig {
    pub endpoint: Url,
    pub root: String,
    pub atomic_write_dir: String,
    pub user_name: Option<String>,
    pub delegation: Option<String>,
    pub allowed_datanode_origins: BTreeSet<String>,
    pub dns_overrides: BTreeMap<String, SocketAddr>,
    pub proxy: Option<Url>,
    pub proxy_bypass: Option<String>,
    pub ca_pem: Option<Vec<u8>>,
    pub allow_tls_downgrade: bool,
    pub chunk_bytes: usize,
    pub fault_after_bytes: Option<u64>,
    pub idle_timeout: Duration,
}

#[derive(Debug, Serialize)]
pub struct GateRun {
    pub candidate: &'static str,
    pub operation: &'static str,
    pub bytes: u64,
    pub chunk_bytes: usize,
    pub elapsed_ms: u128,
    pub throughput_mib_s: f64,
    pub cleanup: Option<String>,
}

impl GateConfig {
    pub fn from_env() -> Result<Self> {
        let endpoint = env::var("WEBHDFS_GATE_ENDPOINT")
            .context("WEBHDFS_GATE_ENDPOINT is required")?
            .parse::<Url>()
            .context("WEBHDFS_GATE_ENDPOINT must be an absolute HTTP(S) URL")?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            bail!("WEBHDFS_GATE_ENDPOINT must use http or https");
        }

        let root = env::var("WEBHDFS_GATE_ROOT").unwrap_or_else(|_| "/dbx-webhdfs-gate".into());
        let atomic_write_dir = env::var("WEBHDFS_GATE_ATOMIC_WRITE_DIR").unwrap_or_else(|_| ".dbx-blocks/".into());
        let user_name = nonempty_env("WEBHDFS_GATE_USER_NAME");
        let delegation = nonempty_env("WEBHDFS_GATE_DELEGATION");
        if user_name.is_some() && delegation.is_some() {
            bail!("set only one of WEBHDFS_GATE_USER_NAME and WEBHDFS_GATE_DELEGATION");
        }

        let allowed_datanode_origins =
            parse_allowed_origins(&env::var("WEBHDFS_GATE_ALLOWED_DATANODE_ORIGINS").unwrap_or_default())?;
        let dns_overrides = parse_dns_overrides(&env::var("WEBHDFS_GATE_DNS_OVERRIDES").unwrap_or_default())?;
        let proxy = nonempty_env("WEBHDFS_GATE_PROXY")
            .map(|value| value.parse::<Url>().context("WEBHDFS_GATE_PROXY is invalid"))
            .transpose()?;
        let proxy_bypass = nonempty_env("WEBHDFS_GATE_PROXY_BYPASS");
        let ca_pem = nonempty_env("WEBHDFS_GATE_CA_PEM")
            .map(|path| std::fs::read(&path).with_context(|| format!("read WEBHDFS_GATE_CA_PEM {path}")))
            .transpose()?;
        let allow_tls_downgrade = parse_bool_env("WEBHDFS_GATE_ALLOW_TLS_DOWNGRADE")?;
        let chunk_bytes = env::var("WEBHDFS_GATE_CHUNK_MIB")
            .ok()
            .map(|value| value.parse::<usize>())
            .transpose()
            .context("WEBHDFS_GATE_CHUNK_MIB must be an integer")?
            .unwrap_or(4)
            .checked_mul(1024 * 1024)
            .ok_or_else(|| anyhow!("WEBHDFS_GATE_CHUNK_MIB is too large"))?;
        if chunk_bytes == 0 {
            bail!("WEBHDFS_GATE_CHUNK_MIB must be greater than zero");
        }
        let fault_after_bytes = env::var("WEBHDFS_GATE_FAULT_AFTER_BYTES")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("WEBHDFS_GATE_FAULT_AFTER_BYTES must be an integer")?;
        let idle_timeout = Duration::from_secs(
            env::var("WEBHDFS_GATE_IDLE_TIMEOUT_SECONDS")
                .ok()
                .map(|value| value.parse::<u64>())
                .transpose()
                .context("WEBHDFS_GATE_IDLE_TIMEOUT_SECONDS must be an integer")?
                .unwrap_or(30),
        );
        if idle_timeout.is_zero() {
            bail!("WEBHDFS_GATE_IDLE_TIMEOUT_SECONDS must be greater than zero");
        }

        Ok(Self {
            endpoint,
            root,
            atomic_write_dir,
            user_name,
            delegation,
            allowed_datanode_origins,
            dns_overrides,
            proxy,
            proxy_bypass,
            ca_pem,
            allow_tls_downgrade,
            chunk_bytes,
            fault_after_bytes,
            idle_timeout,
        })
    }

    pub fn opendal_operator(&self) -> Result<Operator> {
        let mut builder = Webhdfs::default()
            .endpoint(self.endpoint.as_str())
            .root(&self.root)
            .atomic_write_dir(&self.atomic_write_dir);
        if let Some(user_name) = &self.user_name {
            builder = builder.user_name(user_name);
        }
        if let Some(delegation) = &self.delegation {
            builder = builder.delegation(delegation);
        }
        Ok(Operator::new(builder)?.finish())
    }

    fn client(&self) -> Result<Client> {
        let mut builder = Client::builder().redirect(Policy::none()).connect_timeout(Duration::from_secs(10));
        if let Some(pem) = &self.ca_pem {
            for certificate in reqwest::Certificate::from_pem_bundle(pem).context("parse WEBHDFS_GATE_CA_PEM")? {
                builder = builder.add_root_certificate(certificate);
            }
        }
        if let Some(proxy) = &self.proxy {
            let mut configured_proxy = reqwest::Proxy::all(proxy.as_str())?;
            if let Some(bypass) = &self.proxy_bypass {
                configured_proxy = configured_proxy.no_proxy(reqwest::NoProxy::from_string(bypass));
            }
            builder = builder.proxy(configured_proxy);
        } else {
            builder = builder.no_proxy();
        }
        for (host, address) in &self.dns_overrides {
            builder = builder.resolve(host, *address);
        }
        builder.build().context("build WebHDFS HTTP client")
    }

    fn webhdfs_url(&self, path: &str, operation: &str) -> Result<Url> {
        validate_remote_path(path)?;
        let root = self.root.trim_matches('/');
        let path = path.trim_start_matches('/');
        let combined = if root.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            root.to_string()
        } else {
            format!("{root}/{path}")
        };
        let mut url = self.endpoint.join(&format!(
            "webhdfs/v1/{}",
            percent_encoding::utf8_percent_encode(&combined, percent_encoding::NON_ALPHANUMERIC)
                .to_string()
                .replace("%2F", "/")
        ))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("op", operation);
            if let Some(user_name) = &self.user_name {
                query.append_pair("user.name", user_name);
            }
            if let Some(delegation) = &self.delegation {
                query.append_pair("delegation", delegation);
            }
        }
        Ok(url)
    }

    fn rooted_absolute_path(&self, path: &str) -> Result<String> {
        validate_remote_path(path)?;
        let root = self.root.trim_matches('/');
        Ok(if root.is_empty() { format!("/{path}") } else { format!("/{root}/{path}") })
    }
}

pub async fn candidate_a_write(config: &GateConfig, path: &str, size: u64) -> Result<GateRun> {
    let op = config.opendal_operator()?;
    op.create_dir(&config.atomic_write_dir).await?;
    let started = Instant::now();
    write_generated_opendal(&op, path, size, config.chunk_bytes).await?;
    let metadata = op.stat(path).await?;
    if metadata.content_length() != size {
        bail!("candidate A length mismatch: expected {size}, got {}", metadata.content_length());
    }
    Ok(run_result("atomic-concat", "write", size, config.chunk_bytes, started, None))
}

pub async fn candidate_a_copy(config: &GateConfig, source: &str, destination: &str) -> Result<GateRun> {
    validate_distinct_paths(source, destination)?;
    let op = config.opendal_operator()?;
    op.create_dir(&config.atomic_write_dir).await?;
    let size = op.stat(source).await?.content_length();
    let started = Instant::now();
    let reader = op.reader_with(source).chunk(config.chunk_bytes).await?;
    let mut input = reader.into_stream(..).await?;
    let mut output = op.writer_with(destination).chunk(config.chunk_bytes).await?;
    while let Some(buffer) = input.try_next().await? {
        output.write(buffer).await?;
    }
    output.close().await?;
    let copied = op.stat(destination).await?.content_length();
    if copied != size {
        bail!("candidate A copy length mismatch: expected {size}, got {copied}");
    }
    Ok(run_result("atomic-concat", "fallback-copy", size, config.chunk_bytes, started, None))
}

pub async fn candidate_a_abort(config: &GateConfig, path: &str) -> Result<()> {
    let op = config.opendal_operator()?;
    op.create_dir(&config.atomic_write_dir).await?;
    let before = list_paths(&op, &config.atomic_write_dir).await?;
    let mut writer = op.writer_with(path).chunk(config.chunk_bytes).await?;
    for _ in 0..3 {
        writer.write(Bytes::from(vec![0x5a; config.chunk_bytes])).await?;
    }
    writer.write(Bytes::from_static(b"force-block-flush")).await?;
    writer.abort().await.context("OpenDAL writer abort failed")?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let after = list_paths(&op, &config.atomic_write_dir).await?;
    let leaked: Vec<_> = after.difference(&before).cloned().collect();
    if !leaked.is_empty() {
        bail!("OpenDAL abort leaked temporary blocks: {}", leaked.join(", "));
    }
    if op.exists(path).await? {
        bail!("OpenDAL abort left destination path {path}");
    }
    Ok(())
}

pub async fn candidate_b_write(config: &GateConfig, path: &str, size: u64) -> Result<GateRun> {
    let client = config.client()?;
    let target = prepare_operation_target(config, &client, path).await?;
    let location = initiate_redirect(config, &client, &target.temp_path, "CREATE", reqwest::Method::PUT).await?;
    let progress = Arc::new(AtomicU64::new(0));
    let stream = generated_stream(size, config.chunk_bytes, config.fault_after_bytes, progress.clone());
    let started = Instant::now();
    let response = send_with_progress_watchdog(
        client.put(location).header(reqwest::header::CONTENT_LENGTH, size).body(Body::wrap_stream(stream)),
        progress,
        config.idle_timeout,
    )
    .await;
    finish_data_write(config, &client, &target.temp_path, response).await?;
    if let Err(error) = verify_length(config, &client, &target.temp_path, size).await {
        let cleanup = cleanup_owned_partial(config, &client, &target.temp_path).await;
        bail!("failed_before_commit: temp verification failed: {error}; owned_temp_cleanup: {cleanup:?}");
    }
    commit_operation_target(config, &client, &target).await?;
    if let Err(error) = verify_length(config, &client, path, size).await {
        bail!("committed_unverified: {path}; length verification failed: {error}");
    }
    if let Err(error) = verify_pattern_samples(config, &client, path, size, 0x5a).await {
        bail!("committed_unverified: {path}; content verification failed: {error}");
    }
    Ok(run_result("streaming-put", "write", size, config.chunk_bytes, started, None))
}

pub async fn candidate_b_copy(config: &GateConfig, source: &str, destination: &str) -> Result<GateRun> {
    validate_distinct_paths(source, destination)?;
    let client = config.client()?;
    let source_length = file_length(config, &client, source).await?.ok_or_else(|| anyhow!("source does not exist"))?;
    let source_checksum = file_checksum(config, &client, source).await;
    let source_location = initiate_redirect(config, &client, source, "OPEN", reqwest::Method::GET).await?;
    let source_response = send_control(client.get(source_location)).await?;
    if !source_response.status().is_success() {
        bail!("WebHDFS DataNode OPEN failed with {}", source_response.status());
    }
    let size =
        source_response.content_length().ok_or_else(|| anyhow!("DataNode OPEN did not return Content-Length"))?;
    if size != source_length {
        bail!("OPEN Content-Length {size} differs from GETFILESTATUS {source_length}");
    }
    let target = prepare_operation_target(config, &client, destination).await?;
    let destination_location =
        initiate_redirect(config, &client, &target.temp_path, "CREATE", reqwest::Method::PUT).await?;
    let started = Instant::now();
    let progress = Arc::new(AtomicU64::new(0));
    let observed_progress = progress.clone();
    let source_hasher = Arc::new(Mutex::new(Sha256::new()));
    let observed_hasher = source_hasher.clone();
    let source_stream = source_response.bytes_stream().inspect_ok(move |bytes| {
        observed_progress.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        observed_hasher.lock().expect("source hash mutex poisoned").update(bytes);
    });
    let response = send_with_progress_watchdog(
        client
            .put(destination_location)
            .header(reqwest::header::CONTENT_LENGTH, size)
            .body(Body::wrap_stream(source_stream)),
        progress,
        config.idle_timeout,
    )
    .await;
    finish_data_write(config, &client, &target.temp_path, response).await?;
    if let Err(error) = verify_length(config, &client, &target.temp_path, size).await {
        let cleanup = cleanup_owned_partial(config, &client, &target.temp_path).await;
        bail!("failed_before_commit: temp verification failed: {error}; owned_temp_cleanup: {cleanup:?}");
    }
    commit_operation_target(config, &client, &target).await?;
    if let Err(error) = verify_length(config, &client, destination, size).await {
        bail!("committed_unverified: {destination}; length verification failed: {error}");
    }
    let destination_checksum = file_checksum(config, &client, destination).await;
    if !matches!((&source_checksum, &destination_checksum), (Ok(source), Ok(destination)) if source == destination) {
        let source_digest: [u8; 32] =
            source_hasher.lock().map_err(|_| anyhow!("source hash mutex poisoned"))?.clone().finalize().into();
        let destination_digest = file_sha256(config, &client, destination).await?;
        if source_digest != destination_digest {
            bail!("committed_unverified: {destination}; fallback copy content digest mismatch");
        }
    }
    Ok(run_result("streaming-put", "fallback-copy", size, config.chunk_bytes, started, None))
}

#[derive(Debug)]
struct OperationTarget {
    final_path: String,
    temp_path: String,
}

async fn prepare_operation_target(config: &GateConfig, client: &Client, path: &str) -> Result<OperationTarget> {
    validate_remote_path(path)?;
    if file_length(config, client, path).await?.is_some() {
        bail!("destination already exists; candidate B uses create-new semantics");
    }
    create_dir(config, client, ".dbx-streaming").await?;
    Ok(OperationTarget { final_path: path.to_string(), temp_path: format!(".dbx-streaming/{}", Uuid::new_v4()) })
}

async fn commit_operation_target(config: &GateConfig, client: &Client, target: &OperationTarget) -> Result<()> {
    if file_length(config, client, &target.final_path).await?.is_some() {
        bail!("destination appeared before commit; operation-owned partial preserved at {}", target.temp_path);
    }
    let mut url = config.webhdfs_url(&target.temp_path, "RENAME")?;
    let destination = config.rooted_absolute_path(&target.final_path)?;
    url.query_pairs_mut().append_pair("destination", &destination);
    let response = send_control(client.put(url)).await?;
    if response.status() != StatusCode::OK {
        bail!(
            "RENAME commit failed with {}; operation-owned partial preserved at {}",
            response.status(),
            target.temp_path
        );
    }
    #[derive(serde::Deserialize)]
    struct BooleanResponse {
        boolean: bool,
    }
    if !read_json_bounded::<BooleanResponse>(response, 64 * 1024, Duration::from_secs(30)).await?.boolean {
        bail!("RENAME commit returned false; operation-owned partial preserved at {}", target.temp_path);
    }
    Ok(())
}

async fn create_dir(config: &GateConfig, client: &Client, path: &str) -> Result<()> {
    let url = config.webhdfs_url(path, "MKDIRS")?;
    let response = send_control(client.put(url)).await?;
    if response.status() != StatusCode::OK {
        bail!("MKDIRS failed with {}", response.status());
    }
    Ok(())
}

async fn verify_length(config: &GateConfig, client: &Client, path: &str, expected: u64) -> Result<()> {
    let actual =
        file_length(config, client, path).await?.ok_or_else(|| anyhow!("GETFILESTATUS did not find {path}"))?;
    if actual != expected {
        bail!("remote length mismatch for {path}: expected {expected}, got {actual}");
    }
    Ok(())
}

async fn file_length(config: &GateConfig, client: &Client, path: &str) -> Result<Option<u64>> {
    let url = config.webhdfs_url(path, "GETFILESTATUS")?;
    let response = send_control(client.get(url)).await?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if response.status() != StatusCode::OK {
        bail!("GETFILESTATUS failed with {}", response.status());
    }
    let value: serde_json::Value = read_json_bounded(response, 1024 * 1024, Duration::from_secs(30)).await?;
    Ok(Some(value["FileStatus"]["length"].as_u64().ok_or_else(|| anyhow!("GETFILESTATUS omitted FileStatus.length"))?))
}

async fn file_checksum(config: &GateConfig, client: &Client, path: &str) -> Result<serde_json::Value> {
    let location = initiate_redirect(config, client, path, "GETFILECHECKSUM", reqwest::Method::GET).await?;
    let response = send_control(client.get(location)).await?;
    if response.status() != StatusCode::OK {
        bail!("GETFILECHECKSUM DataNode request failed with {}", response.status());
    }
    read_json_bounded(response, 1024 * 1024, Duration::from_secs(30)).await
}

async fn file_sha256(config: &GateConfig, client: &Client, path: &str) -> Result<[u8; 32]> {
    let location = initiate_redirect(config, client, path, "OPEN", reqwest::Method::GET).await?;
    let response = send_control(client.get(location)).await?;
    if response.status() != StatusCode::OK {
        bail!("OPEN digest verification failed with {}", response.status());
    }
    let mut stream = response.bytes_stream();
    let mut hasher = Sha256::new();
    loop {
        let next = tokio::time::timeout(config.idle_timeout, stream.try_next())
            .await
            .context("WebHDFS digest response body stalled")?
            .context("read WebHDFS digest response body")?;
        let Some(chunk) = next else {
            break;
        };
        hasher.update(chunk);
    }
    Ok(hasher.finalize().into())
}

async fn verify_pattern_samples(
    config: &GateConfig,
    client: &Client,
    path: &str,
    size: u64,
    expected: u8,
) -> Result<()> {
    if size == 0 {
        return Ok(());
    }
    let sample_size = size.min(64 * 1024);
    for offset in [0, size.saturating_sub(sample_size)] {
        let mut url = config.webhdfs_url(path, "OPEN")?;
        url.query_pairs_mut()
            .append_pair("offset", &offset.to_string())
            .append_pair("length", &sample_size.to_string());
        let location = initiate_redirect_url(config, client, url, "OPEN", reqwest::Method::GET).await?;
        let response = send_control(client.get(location)).await?;
        if response.status() != StatusCode::OK {
            bail!("sample OPEN failed with {}", response.status());
        }
        let bytes = read_body_bounded(response, sample_size as usize, config.idle_timeout).await?;
        if bytes.len() as u64 != sample_size || bytes.iter().any(|byte| *byte != expected) {
            bail!("deterministic content sample mismatch at offset {offset}");
        }
    }
    Ok(())
}

pub async fn delete_path(config: &GateConfig, path: &str) -> Result<bool> {
    let client = config.client()?;
    cleanup_partial(config, &client, path).await
}

async fn write_generated_opendal(op: &Operator, path: &str, size: u64, chunk_bytes: usize) -> Result<()> {
    let chunk = Bytes::from(vec![0x5a; chunk_bytes]);
    let mut remaining = size;
    let mut writer = op.writer_with(path).chunk(chunk_bytes).await?;
    while remaining > 0 {
        let count = remaining.min(chunk_bytes as u64) as usize;
        writer.write(chunk.slice(..count)).await?;
        remaining -= count as u64;
    }
    writer.close().await?;
    Ok(())
}

async fn initiate_redirect(
    config: &GateConfig,
    client: &Client,
    path: &str,
    operation: &str,
    method: reqwest::Method,
) -> Result<Url> {
    let url = config.webhdfs_url(path, operation)?;
    initiate_redirect_url(config, client, url, operation, method).await
}

async fn initiate_redirect_url(
    config: &GateConfig,
    client: &Client,
    url: Url,
    operation: &str,
    method: reqwest::Method,
) -> Result<Url> {
    let response = send_control(client.request(method, url)).await?;
    if response.status() != StatusCode::TEMPORARY_REDIRECT {
        let status = response.status();
        let body = read_text_bounded(response, 64 * 1024, Duration::from_secs(30))
            .await
            .unwrap_or_else(|error| format!("<unreadable error body: {error}>"));
        bail!("WebHDFS {operation} expected 307, got {status}: {body}");
    }
    let raw = response
        .headers()
        .get(LOCATION)
        .ok_or_else(|| anyhow!("WebHDFS {operation} 307 omitted Location"))?
        .to_str()
        .context("WebHDFS Location is not valid UTF-8")?;
    let location = Url::parse(raw).context("WebHDFS Location is not an absolute URL")?;
    validate_datanode_location(config, &location)?;
    Ok(location)
}

fn validate_datanode_location(config: &GateConfig, location: &Url) -> Result<()> {
    if !matches!(location.scheme(), "http" | "https") {
        bail!("DataNode redirect uses unsupported scheme {}", location.scheme());
    }
    if config.endpoint.scheme() == "https" && location.scheme() == "http" && !config.allow_tls_downgrade {
        bail!("DataNode redirect attempted HTTPS-to-HTTP downgrade");
    }
    if !location.username().is_empty() || location.password().is_some() {
        bail!("DataNode redirect must not contain URL userinfo");
    }
    if location.fragment().is_some() {
        bail!("DataNode redirect must not contain a fragment");
    }
    let origin = canonical_origin(location)?;
    if !config.allowed_datanode_origins.contains(&origin) {
        bail!("DataNode redirect origin {origin} is not allowlisted");
    }

    let pairs: Vec<_> = location.query_pairs().into_owned().collect();
    for sensitive in ["delegation", "user.name"] {
        if pairs.iter().filter(|(key, _)| key == sensitive).count() > 1 {
            bail!("DataNode redirect contains duplicate {sensitive} parameters");
        }
    }
    if let Some(expected) = &config.delegation {
        if pairs.iter().find(|(key, _)| key == "delegation").map(|(_, value)| value) != Some(expected) {
            bail!("DataNode redirect did not preserve the configured delegation token");
        }
    }
    if let Some(expected) = &config.user_name {
        if pairs.iter().find(|(key, _)| key == "user.name").map(|(_, value)| value) != Some(expected) {
            bail!("DataNode redirect did not preserve the configured user.name");
        }
    }
    Ok(())
}

async fn finish_data_write(config: &GateConfig, client: &Client, path: &str, response: Result<Response>) -> Result<()> {
    if !path.starts_with(".dbx-streaming/") {
        bail!("refusing partial cleanup for non-operation-owned path {path}");
    }
    match response {
        Ok(response) if matches!(response.status(), StatusCode::CREATED | StatusCode::OK) => Ok(()),
        Ok(response) => {
            let status = response.status();
            let cleanup = cleanup_owned_partial(config, client, path).await;
            bail!("DataNode PUT failed with {status}; partial destination cleanup: {cleanup:?}")
        }
        Err(error) => {
            let cleanup = cleanup_owned_partial(config, client, path).await;
            bail!("DataNode PUT transport failed: {error}; partial destination cleanup: {cleanup:?}")
        }
    }
}

async fn cleanup_owned_partial(config: &GateConfig, client: &Client, path: &str) -> Result<bool> {
    if !path.starts_with(".dbx-streaming/") {
        bail!("refusing owned cleanup for non-operation temporary path {path}");
    }
    // A failed request-body stream can race with DataNode pipeline teardown:
    // DELETE may observe no inode just before the partial inode is published.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut deleted = false;
    for _ in 0..10 {
        deleted |= cleanup_partial(config, client, path).await?;
        tokio::time::sleep(Duration::from_millis(250)).await;
        if file_length(config, client, path).await?.is_none() {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if file_length(config, client, path).await?.is_none() {
                return Ok(deleted);
            }
        }
    }
    bail!("operation-owned partial still exists after cleanup retries: {path}")
}

async fn cleanup_partial(config: &GateConfig, client: &Client, path: &str) -> Result<bool> {
    let mut url = config.webhdfs_url(path, "DELETE")?;
    url.query_pairs_mut().append_pair("recursive", "false");
    let response = send_control(client.delete(url)).await?;
    if response.status() != StatusCode::OK {
        bail!("DELETE cleanup failed with {}", response.status());
    }
    #[derive(serde::Deserialize)]
    struct BooleanResponse {
        boolean: bool,
    }
    Ok(read_json_bounded::<BooleanResponse>(response, 64 * 1024, Duration::from_secs(30)).await?.boolean)
}

async fn send_control(request: reqwest::RequestBuilder) -> Result<Response> {
    tokio::time::timeout(Duration::from_secs(30), request.send())
        .await
        .context("WebHDFS control request exceeded 30 seconds")?
        .context("WebHDFS control request failed")
}

async fn read_body_bounded(response: Response, max_bytes: usize, idle_timeout: Duration) -> Result<Bytes> {
    if response.content_length().is_some_and(|length| length > max_bytes as u64) {
        bail!("WebHDFS control response exceeds {max_bytes} bytes");
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    loop {
        let next = tokio::time::timeout(idle_timeout, stream.try_next())
            .await
            .context("WebHDFS response body stalled")?
            .context("read WebHDFS response body")?;
        let Some(chunk) = next else {
            break;
        };
        if body.len().saturating_add(chunk.len()) > max_bytes {
            bail!("WebHDFS control response exceeds {max_bytes} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

async fn read_json_bounded<T: DeserializeOwned>(
    response: Response,
    max_bytes: usize,
    idle_timeout: Duration,
) -> Result<T> {
    let body = read_body_bounded(response, max_bytes, idle_timeout).await?;
    serde_json::from_slice(&body).context("parse WebHDFS JSON response")
}

async fn read_text_bounded(response: Response, max_bytes: usize, idle_timeout: Duration) -> Result<String> {
    let body = read_body_bounded(response, max_bytes, idle_timeout).await?;
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn canonical_origin(url: &Url) -> Result<String> {
    let host = url.host_str().ok_or_else(|| anyhow!("URL has no host"))?;
    let port = url.port_or_known_default().ok_or_else(|| anyhow!("URL has no effective port"))?;
    Ok(format!("{}://{host}:{port}", url.scheme()))
}

fn generated_stream(
    size: u64,
    chunk_bytes: usize,
    fault_after_bytes: Option<u64>,
    progress: Arc<AtomicU64>,
) -> impl futures::Stream<Item = std::result::Result<Bytes, io::Error>> + Send + 'static {
    let chunk = Bytes::from(vec![0x5a; chunk_bytes]);
    stream::unfold((size, chunk, 0_u64, false), move |(remaining, chunk, sent, faulted)| {
        let progress = progress.clone();
        async move {
            if !faulted && fault_after_bytes.is_some_and(|limit| sent >= limit) {
                return Some((
                    Err(io::Error::new(io::ErrorKind::ConnectionAborted, "injected WebHDFS body failure")),
                    (remaining, chunk, sent, true),
                ));
            }
            if remaining == 0 {
                return None;
            }
            let count = remaining.min(chunk.len() as u64) as usize;
            progress.fetch_add(count as u64, Ordering::Relaxed);
            Some((Ok(chunk.slice(..count)), (remaining - count as u64, chunk, sent + count as u64, faulted)))
        }
    })
}

async fn send_with_progress_watchdog(
    request: reqwest::RequestBuilder,
    progress: Arc<AtomicU64>,
    idle_timeout: Duration,
) -> Result<Response> {
    let send = request.send();
    tokio::pin!(send);
    let mut last_progress = progress.load(Ordering::Relaxed);
    loop {
        tokio::select! {
            response = &mut send => return response.context("WebHDFS streaming request failed"),
            _ = tokio::time::sleep(idle_timeout) => {
                let current = progress.load(Ordering::Relaxed);
                if current == last_progress {
                    bail!("WebHDFS streaming request made no body progress for {} seconds", idle_timeout.as_secs());
                }
                last_progress = current;
            }
        }
    }
}

async fn list_paths(op: &Operator, path: &str) -> Result<BTreeSet<String>> {
    Ok(op.list(path).await?.into_iter().map(|entry| entry.path().to_string()).collect())
}

fn run_result(
    candidate: &'static str,
    operation: &'static str,
    bytes: u64,
    chunk_bytes: usize,
    started: Instant,
    cleanup: Option<String>,
) -> GateRun {
    let elapsed = started.elapsed();
    let throughput_mib_s = if elapsed.is_zero() { 0.0 } else { bytes as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64() };
    GateRun { candidate, operation, bytes, chunk_bytes, elapsed_ms: elapsed.as_millis(), throughput_mib_s, cleanup }
}

fn validate_remote_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.as_bytes().contains(&0)
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        bail!("path must be a non-empty normalized relative WebHDFS path");
    }
    Ok(())
}

fn validate_distinct_paths(source: &str, destination: &str) -> Result<()> {
    validate_remote_path(source)?;
    validate_remote_path(destination)?;
    if source == destination {
        bail!("source and destination must differ");
    }
    Ok(())
}

fn parse_dns_overrides(value: &str) -> Result<BTreeMap<String, SocketAddr>> {
    value
        .split(',')
        .filter_map(nonempty_trimmed)
        .map(|item| {
            let (host, address) = item.split_once('=').ok_or_else(|| anyhow!("DNS override must use host=ip:port"))?;
            let address = SocketAddr::from_str(address)
                .or_else(|_| IpAddr::from_str(address).map(|ip| SocketAddr::new(ip, 0)))
                .with_context(|| format!("invalid DNS override address {address}"))?;
            Ok((host.to_string(), address))
        })
        .collect()
}

fn parse_allowed_origins(value: &str) -> Result<BTreeSet<String>> {
    value
        .split(',')
        .filter_map(nonempty_trimmed)
        .map(|origin| {
            let url = Url::parse(&origin)
                .with_context(|| format!("invalid WEBHDFS_GATE_ALLOWED_DATANODE_ORIGINS entry {origin}"))?;
            if url.path() != "/"
                || url.query().is_some()
                || url.fragment().is_some()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                bail!("allowed DataNode origin must contain only scheme, host, and port: {origin}");
            }
            canonical_origin(&url)
        })
        .collect()
}

fn parse_bool_env(name: &str) -> Result<bool> {
    match env::var(name).ok().as_deref() {
        None | Some("") | Some("0") | Some("false") => Ok(false),
        Some("1") | Some("true") => Ok(true),
        Some(value) => bail!("{name} must be true/false or 1/0, got {value}"),
    }
}

fn nonempty_env(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|value| {
        let value = value.trim().to_string();
        (!value.is_empty()).then_some(value)
    })
}

fn nonempty_trimmed(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> GateConfig {
        GateConfig {
            endpoint: Url::parse("https://namenode.test:9871/").unwrap(),
            root: "/gate".into(),
            atomic_write_dir: ".tmp/".into(),
            user_name: None,
            delegation: Some("secret-token".into()),
            allowed_datanode_origins: BTreeSet::from(["https://datanode.test:443".into()]),
            dns_overrides: BTreeMap::new(),
            proxy: None,
            proxy_bypass: None,
            ca_pem: None,
            allow_tls_downgrade: false,
            chunk_bytes: 4,
            fault_after_bytes: None,
            idle_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn rejects_redirect_to_untrusted_host() {
        let error = validate_datanode_location(
            &config(),
            &Url::parse("https://attacker.test/file?delegation=secret-token").unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("not allowlisted"));
    }

    #[test]
    fn rejects_tls_downgrade_and_missing_auth() {
        let error = validate_datanode_location(
            &config(),
            &Url::parse("http://datanode.test/file?delegation=secret-token").unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("downgrade"));

        let error =
            validate_datanode_location(&config(), &Url::parse("https://datanode.test/file").unwrap()).unwrap_err();
        assert!(error.to_string().contains("delegation"));
    }

    #[test]
    fn redirect_allowlist_includes_effective_port_and_rejects_url_tricks() {
        let error = validate_datanode_location(
            &config(),
            &Url::parse("https://datanode.test:444/file?delegation=secret-token").unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("origin"));

        for raw in [
            "https://user@datanode.test/file?delegation=secret-token",
            "https://datanode.test/file?delegation=secret-token#fragment",
            "https://datanode.test/file?delegation=secret-token&delegation=secret-token",
        ] {
            assert!(validate_datanode_location(&config(), &Url::parse(raw).unwrap()).is_err(), "{raw}");
        }
    }

    #[tokio::test]
    async fn generator_never_yields_more_than_one_configured_chunk() {
        let chunks: Vec<_> = generated_stream(11, 4, None, Arc::new(AtomicU64::new(0))).try_collect().await.unwrap();
        assert_eq!(chunks.iter().map(Bytes::len).collect::<Vec<_>>(), vec![4, 4, 3]);
        assert_eq!(chunks.iter().map(Bytes::len).sum::<usize>(), 11);
    }

    #[tokio::test]
    async fn generator_can_inject_a_partial_body_failure() {
        let error =
            generated_stream(11, 4, Some(5), Arc::new(AtomicU64::new(0))).try_collect::<Vec<_>>().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    }

    #[tokio::test]
    async fn response_body_stall_after_headers_is_bounded() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n").await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let client = Client::builder().redirect(Policy::none()).build().unwrap();
        let response = send_control(client.get(format!("http://{address}/"))).await.unwrap();
        let error = read_body_bounded(response, 1, Duration::from_millis(50)).await.unwrap_err();
        assert!(error.to_string().contains("stalled"));
        server.abort();
    }

    #[test]
    fn allowed_origin_rejects_password() {
        assert!(parse_allowed_origins("https://user:password@datanode.test:443").is_err());
    }

    #[test]
    fn paths_cannot_escape_root() {
        for path in ["", "/absolute", "../escape", "a/../b", "a//b", "a\\b"] {
            assert!(validate_remote_path(path).is_err(), "{path}");
        }
        assert!(validate_remote_path("safe/path.bin").is_ok());
    }
}
