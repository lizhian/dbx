# Spec: 基于 Apache OpenDAL 的文件管理

- 状态：Conditional Go
- 关联 Issue：https://github.com/t8y2/dbx/issues/4416
- 产品范围：五种连接类型、六种协议实现
- 生产级估算：30–45 engineer-days，35–60 个文件，约 6,000–10,000 LOC（含测试）

## Problem Statement

DBX 桌面端目前可以管理数据库、消息队列和配置，但不能直接浏览和管理 FTP、SFTP、对象存储、WebDAV 或 HDFS 上的文件。用户必须切换到其他工具完成文件上传、下载、复制、重命名和删除，连接信息、操作反馈和错误处理也无法复用 DBX 的桌面体验。

这些存储协议对相同文件操作的实现能力并不一致。部分后端支持服务端复制或原子重命名，部分后端只能由客户端中继数据或使用“复制后删除”模拟重命名；部分操作可能覆盖目标，或者在复制成功、删除源文件失败时产生部分成功。如果将这些差异隐藏成完全一致的成功语义，用户可能误判操作的原子性、覆盖行为和数据安全性。

本功能需要在不污染现有数据库连接模型、不让大文件经过 Tauri JSON IPC、不过度增加 CLI/MCP 二进制依赖的前提下，为桌面端提供统一且诚实的文件管理体验。

## Solution

在 DBX 桌面端新增独立的文件管理页面。用户可以创建 FTP、SFTP、S3、WebDAV 和 HDFS 文件连接，其中 HDFS 可以选择 WebHDFS 或 HDFS Native 实现。

用户可以浏览目录、查看文件属性、创建目录、上传、下载、删除、同连接复制和重命名文件。第一版不支持跨连接复制，也不支持目录递归复制、重命名或删除。

所有大文件操作在桌面后端与存储服务之间进行有界流式传输。前端只通过 Tauri 命令传递路径、元数据和传输标识，并通过事件与状态查询获得进度、取消结果和最终状态。

产品会根据连接实现显示操作能力和限制。服务端复制、客户端中继、非原子重命名、最佳努力不覆盖和部分成功必须被明确区分，不能把模拟操作描述成原子操作。

本方案为 Conditional Go。发布前必须通过 WebHDFS 有界流式写入 Gate，完成 secrets 落盘安全决策，并确认目标平台支持矩阵。

## User Stories

1. As a DBX desktop user, I want to open a dedicated file manager, so that I can manage remote files without leaving DBX.
2. As a file manager user, I want to create an FTP connection, so that I can access an existing FTP server.
3. As a file manager user, I want to create an SFTP connection, so that I can access files through an SSH-based transport on a supported platform.
4. As a file manager user, I want to create an S3 connection, so that I can manage files in an S3-compatible bucket.
5. As a file manager user, I want to create a WebDAV connection, so that I can manage files exposed by a WebDAV server.
6. As an HDFS user, I want HDFS to appear as one product connection type, so that the connection list is not fragmented by implementation details.
7. As an HDFS user, I want to choose WebHDFS or HDFS Native while configuring a connection, so that I can match my cluster deployment.
8. As a connection owner, I want protocol-specific configuration fields, so that irrelevant settings are not shown or persisted.
9. As a connection owner, I want credentials stored separately from non-secret configuration, so that configuration serialization and export do not expose secrets.
10. As a connection owner, I want to test a connection before saving it, so that configuration, network, authentication and root-path failures are distinguishable.
11. As a connection owner, I want to edit a saved connection, so that endpoint, root and credentials can be rotated safely.
12. As a connection owner, I want deleting a connection to remove its persisted credentials immediately, so that stale secrets are not retained.
13. As a file manager user, I want to browse a remote directory, so that I can inspect its files and child directories.
14. As a file manager user, I want large directories to load in pages, so that the UI remains responsive.
15. As a file manager user, I want an expired directory cursor to return a clear refresh requirement, so that I do not unknowingly receive duplicate or inconsistent pages.
16. As a file manager user, I want to view file type, size and modification metadata, so that I can identify the correct object before acting.
17. As a file manager user, I want to create a directory, so that I can organize uploaded files.
18. As a file manager user, I want to upload a local file, so that I can place data on the selected remote connection.
19. As a file manager user, I want to download a remote file, so that I can use it locally.
20. As a file manager user, I want upload and download progress, so that I can estimate completion and detect stalls.
21. As a file manager user, I want to cancel a running transfer, so that I can stop an unwanted or stalled operation.
22. As a file manager user, I want completed and interrupted transfers to have queryable terminal states, so that missing UI events do not hide the outcome.
23. As a file manager user, I want failed downloads to leave the requested destination untouched, so that an incomplete file is not mistaken for a complete download.
24. As a file manager user, I want failed uploads to abort or report a partial destination, so that cleanup decisions are explicit.
25. As a file manager user, I want to copy a file within the same connection, so that I can duplicate remote data without downloading it manually.
26. As a file manager user, I want cross-connection copy to be unavailable in v1, so that the product does not imply an unimplemented transfer contract.
27. As a file manager user, I want to rename a file, so that I can reorganize remote data using the strongest behavior supported by the backend.
28. As a file manager user, I want copy-then-delete rename failures to report partial success, so that I know whether the destination exists and the source still needs cleanup.
29. As a file manager user, I want overwrite behavior to require an explicit choice, so that a default action does not silently replace existing data.
30. As a file manager user, I want best-effort no-clobber behavior labelled as non-atomic, so that I do not mistake a preflight check for a storage-level guarantee.
31. As a file manager user, I want to delete a file, so that I can remove obsolete remote data.
32. As a file manager user, I want non-empty directory deletion rejected in v1, so that a directory action cannot trigger an unexpected recursive deletion.
33. As an S3 user, I want virtual prefixes distinguished from directory marker objects, so that deleting an apparent directory never bulk-deletes a prefix.
34. As a user operating inside a configured root, I want paths prevented from escaping that root, so that a malformed or hostile path cannot access unintended data.
35. As a desktop user, I want local upload and download paths validated, so that relative paths, unsafe symlinks and invalid destinations are rejected.
36. As a user transferring large files, I want memory use to remain bounded, so that transfer size does not determine application memory consumption.
37. As a user with multiple transfers, I want per-connection and global concurrency limits, so that one backend cannot exhaust application or server resources.
38. As an FTP user, I want the UI to state that FTP is unencrypted, so that I can make an informed security decision.
39. As an SFTP user, I want unsupported password or platform combinations rejected during configuration, so that the product does not promise unavailable authentication.
40. As a WebHDFS user, I want connection testing to validate DataNode redirect reachability, so that a reachable NameNode does not produce misleading success.
41. As an HDFS Native user, I want connection testing to validate NameNode and DataNode connectivity, so that RPC-only success is not mistaken for usable file access.
42. As an administrator, I want file connection secrets excluded from logs, errors, IPC responses, exports and crash metadata, so that credentials are not leaked.
43. As a support engineer, I want connection-test failures classified by stage, so that configuration, DNS, TCP, authentication, redirect and root failures can be diagnosed.
44. As a release engineer, I want each protocol implementation tested against a pinned real service, so that advertised capabilities reflect external behavior.
45. As a release engineer, I want platform-specific support stated explicitly, so that unsupported Windows SFTP behavior is not shipped accidentally.
46. As a product owner, I want the WebHDFS write/copy Gate to block release when neither bounded implementation works, so that runtime degradation is not counted as meeting the requirement.

## Implementation Decisions

### Product and capability model

- The product exposes five connection types: FTP, SFTP, S3, WebDAV and HDFS.
- HDFS contains a second discriminator with `WebHDFS` and `Native` variants, producing six protocol implementations in total.
- The v1 operation set is `read`, `write`, `stat`, `list`, `delete`, `copy`, `rename` and `create_dir`.
- In product terminology, `read` and `write` are local download and upload workflows. Arbitrary file bytes are never returned through JSON IPC.
- `stat` and `list` support files and directories. `read`, `write`, `copy` and `rename` are file-only in v1.
- Directory copy and rename are unsupported.
- Directory delete first verifies that the directory is empty. Recursive delete is unsupported.
- For object storage, deleting a virtual prefix without a directory marker is a no-op. Only an empty directory marker may be deleted; prefix-wide deletion is prohibited.
- Cross-connection copy is structurally excluded by accepting one `connection_id` with source and destination paths.

### Backend operation mapping

| Implementation | Basic operations | Copy | Rename |
| --- | --- | --- | --- |
| FTP | OpenDAL | Bounded client relay | Copy then delete; non-atomic |
| SFTP | OpenDAL | Client relay | Native rename |
| S3 | OpenDAL | Server-side copy | Copy then delete; non-atomic |
| WebDAV | OpenDAL | Server-side COPY | Native MOVE |
| WebHDFS | OpenDAL subject to the write Gate | Client relay subject to the write Gate | Narrow WebHDFS REST RENAME adapter |
| HDFS Native | OpenDAL native HDFS service | Bounded client relay | Native rename |

- OpenDAL and backend service dependencies are linked only into the desktop Tauri application. They are not added to shared core binaries used by CLI or MCP.
- HDFS Native uses the pure Rust native HDFS service rather than the JNI-based HDFS service.
- The WebHDFS REST rename adapter must share authentication, TLS, proxy, hostname mapping and timeout behavior with the corresponding OpenDAL operator.

### Conflict and failure semantics

- Source and destination equality, directory operands and paths outside the configured root are rejected before execution.
- The default non-replacement policy is named `best_effort_no_clobber`.
- Capabilities must expose `atomic_no_clobber`. It is `false` wherever the backend cannot perform a conditional atomic destination creation.
- Best-effort no-clobber performs a destination preflight and an in-process path lock, but the UI and API must preserve the external TOCTOU risk.
- Replace is a distinct policy and requires explicit user confirmation.
- Strict atomic no-clobber is out of scope for v1.
- Copy-delete rename has at least these terminal outcomes: `completed`, `copied_source_delete_failed`, `failed_with_partial_destination` and `failed_before_copy`.
- A retry of source deletion after partial rename must verify source and destination fingerprints before deleting the source.
- Automatic partial cleanup is allowed only when an ETag, version or operation-unique temporary path proves ownership. Otherwise the partial object is preserved and reported.

### Connection and persistence model

- File connections use a model independent of database `DatabaseType` and database `ConnectionConfig`.
- File connection configuration and file connection secrets use independent persistence tables.
- Non-secret configuration is a tagged protocol-specific union. HDFS configuration contains a nested WebHDFS or Native union.
- Non-secret configuration never contains credentials.
- Secret fields are allowlisted per protocol instead of accepting arbitrary secret JSON.
- FTP and WebDAV may store passwords or tokens; SFTP may store inline private-key material and passphrases; S3 may store access keys, secret keys and session tokens; WebHDFS may store delegation tokens; HDFS Native stores a keytab path rather than keytab contents.
- Configuration, secrets and a monotonic configuration revision are updated in one transaction.
- Operator cache keys include connection ID and configuration revision.
- Editing configuration or secrets commits persistence first and then evicts the previous operator.
- Deleting a connection first marks it as deleting to reject new work, deletes configuration and secrets in one transaction, then cancels work and evicts runtime state.
- Already running work may finish or enter unknown/partial state after deletion. Persisted terminal records contain only redacted operation metadata.
- Plaintext SQLite secret storage is considered a release risk requiring explicit acceptance. If it is not accepted, operating-system keychain integration becomes release-blocking.
- DTOs, debug formatting, logs, errors, exports and crash metadata must redact secrets.

### Runtime state and command contracts

- A desktop-scoped file manager state owns operator caching, transfer registry, cancellation tokens, semaphores and list sessions.
- A cached operator is evicted after configuration changes, authentication changes, protocol disconnection or timeout.
- List continuation tokens are opaque UUIDs bound to connection ID, revision, path and options.
- List sessions have a five-minute idle TTL, global and per-connection limits, and LRU eviction.
- Expired cursors return `CursorExpired`; list operations never silently restart from the first page.
- The command surface includes connection CRUD and connection test, `stat`, `list`, `list_next`, `delete`, `create_dir`, `copy`, `rename`, `start_upload`, `start_download`, `get_transfer`, `list_transfers` and `cancel_transfer`.
- Upload and download commands return a `transfer_id`; they do not wait for the entire transfer or return file bytes.
- Transfer terminal state is persisted before its final event is emitted. The frontend can recover missed events through transfer queries.
- Ordinary progress events are throttled to at most 5–10 Hz per transfer and also receive a global cap. Terminal events are never throttled away.
- Transfer state persistence supports terminal-state recovery and crash cleanup only. Resumable transfers are not implied or implemented in v1.

### Streaming, cancellation and timeouts

- Uploads, downloads and fallback copies use bounded streaming with backpressure.
- The initial transfer buffer is 4 MiB and may be tuned up to 8 MiB after measurement.
- Initial limits are eight global transfers and four transfers per connection.
- Metadata operations use an independent bounded pool, initially 32 global and eight per connection.
- The initial list page size is 200 with an allowed maximum of 1,000.
- FTP relay consumes independent read and write resources; FTP transfer concurrency must be tuned below generic defaults when benchmarks require it.
- Downloads write to an operation-unique file in the destination directory using create-new semantics, then flush, sync and rename into place.
- Cancelled or failed uploads invoke backend abort when supported. Abort failures become partial outcomes.
- Execution layering is limiter, retry policy, per-attempt operation timeout or I/O progress watchdog, then backend.
- Automatic retry is limited to safe operations such as stat, list and ranged read. Side-effecting operations are not blindly retried.
- A timeout or broken protocol session evicts the cached operator.

### Path and local file safety

- Remote paths use `/`-separated lexical segments relative to the configured root.
- Absolute paths, `.`, `..`, NUL bytes and backslashes are rejected.
- Percent-decoded input is normalized before root-boundary checks.
- Local paths used for upload and download must be absolute and validated for type, parent directory, authorization and symlink behavior.
- Path validation is performed before acquiring backend resources where possible.

### HDFS decisions

- The baseline for both HDFS variants is simple authentication with a single NameNode.
- WebHDFS configuration includes endpoint, root, simple user name or delegation token, optional atomic write directory, list behavior, TLS, proxy, DataNode hostname mapping and timeouts.
- WebHDFS connection tests validate the NameNode and the host and port returned by DataNode redirects.
- HDFS Native configuration includes NameNode URI, root, allowlisted options, Hadoop configuration directory and authentication environment references.
- HDFS Native requires direct NameNode RPC connectivity and connectivity to DataNodes.
- Kerberos and HDFS HA are advertised only after dedicated configuration, ticket/GSS, platform and failover test Gates pass.
- HttpFS is treated as a separate deployment mode and is not silently equated with NameNode WebHDFS.

### WebHDFS release Gate

- OpenDAL 0.57 only advertises multi-write for WebHDFS when an atomic write directory is configured. Generic bounded multipart write cannot be assumed.
- A 2–3 engineer-day blocking Spike runs before production implementation.
- Candidate A validates an atomic write directory using temporary blocks and CONCAT, including the HDFS version requirement, same-filesystem and encryption-zone behavior, permissions, quota, abort and cleanup.
- Candidate B implements a narrow streaming WebHDFS PUT and validates CREATE-to-307 redirects, authentication query forwarding, TLS, proxy, hostname mapping, streaming request bodies, bounded RSS and partial failures.
- At least one candidate must pass for WebHDFS `write` and fallback `copy` to satisfy v1.
- Runtime capability degradation is defensive behavior and does not count as completing the user requirement.
- If both candidates fail, the v1 scope is No-Go unless the requirement is changed explicitly.

### Platform and security boundaries

- macOS and Linux are the baseline platforms for all six protocol implementations.
- Windows v1 supports FTP, S3, WebDAV, WebHDFS and HDFS Native, but not SFTP.
- OpenDAL 0.57 SFTP is Unix-only and supports SSH config, agent and private-key authentication, not a normal password field.
- Windows SFTP or password SFTP requires a separate 2–3 day blocking Spike comparing an SFTP protocol library and custom OpenDAL access implementation. Existing SSH tunnel support is not an SFTP implementation.
- FTP is unencrypted. FTPS is not included in this spec.
- If production policy requires encrypted transport, FTP must be disabled or FTPS must receive a separate implementation and acceptance scope.

### Desktop experience

- File Manager is a dedicated special page rather than a database query tab.
- The page contains file connection navigation, a paginated file list, selection and operation controls, transfer progress and terminal-status surfaces.
- Controls are enabled from reported backend capabilities rather than hard-coded protocol names.
- Partial rename, partial upload and best-effort conflict behavior use explicit user-facing states and recovery actions.
- Connection testing reports configuration, DNS, TCP, authentication, root, redirect and DataNode stages separately where applicable.

### Delivery estimate and release criteria

- The production baseline is 30–45 engineer-days, 35–60 changed or added files and approximately 6,000–10,000 LOC including tests and excluding lockfile churn.
- The estimate includes the WebHDFS Gate: 2–3 days for the Gate, 3–4 for models, persistence and secrets, 6–9 for six backend implementations and HDFS adapters, 5–7 for transfer/list/runtime state, 5–7 for UI, 7–10 for contract/fault/security/E2E/performance testing, and 2–3 for hardening and release work.
- A 20–30 day MVP can reduce platform certification, compatibility depth, 100 GiB tests, fault coverage and detailed partial-success UX, but it is not the production baseline described by this spec.
- Release remains No-Go until the WebHDFS Gate passes, secret-storage policy is accepted or remediated, and the platform matrix is verified.

## Testing Decisions

### Primary test seam

- The primary seam is the file manager command contract at the Tauri backend boundary.
- One backend-agnostic conformance suite runs the same external behaviors against FTP, SFTP, S3, WebDAV, WebHDFS and HDFS Native.
- Tests assert returned metadata, visible remote state, conflict behavior, terminal status and cleanup. They do not assert internal OpenDAL calls or private state structure.
- This seam covers connection lifecycle, stat, paginated list, create directory, upload, download, delete, copy, rename, cancellation and transfer recovery.

### Protocol and integration coverage

- Pull requests compile and run unit tests on macOS, Linux and Windows.
- Linux pull requests run the conformance suite against pinned FTP, OpenSSH SFTP, S3-compatible, WebDAV and Hadoop services.
- WebHDFS and HDFS Native have separate Hadoop test suites because their networking, authentication and operation capabilities differ.
- WebHDFS integration tests exercise real 307 DataNode redirects and REST rename behavior.
- HDFS Native integration tests exercise NameNode RPC, DataNode reads/writes and native rename.
- A target Hadoop distribution is tested before release; a service test that did not run cannot be reported as passed.

### Desktop and state coverage

- Desktop E2E covers connection CRUD, connection testing, navigation, pagination, upload/download progress, cancellation, create directory, delete, same-connection copy, rename, conflicts and partial-success recovery.
- Frontend behavior tests follow the existing store and component test style.
- Progress and cancellation handling follow the existing Tauri progress-event and cancellation patterns used by long-running desktop operations.
- Terminal events are tested for persistence-before-emission and recovery through `get_transfer` or `list_transfers`.
- Cursor tests cover TTL expiry, revision changes, path changes, refresh, deletion, limits and LRU eviction.

### Failure, security and path coverage

- Fault injection covers disconnection after a WebHDFS redirect, copy success followed by source-delete failure, abort failure, concurrent source changes, DNS failure, TLS failure, host-key failure, permission denial and local or remote disk exhaustion.
- Security tests scan SQLite data, serialized configuration, export output, IPC DTOs, logs and crash metadata for credentials.
- WebHDFS tests ensure delegation tokens and user query parameters are forwarded only to intended redirect targets.
- Path property and fuzz tests prove that encoded and decoded paths cannot escape the configured root.
- Local path tests cover relative paths, invalid parents, symlinks, existing partial files and destination replacement.
- Directory tests cover non-empty rejection, concurrent changes during empty checks, S3 virtual prefixes, marker deletion and unsupported directory copy/rename.

### Performance and release measurements

- Performance comparison uses a pure OpenDAL harness under the same host, network, chunk and concurrency settings.
- Single-transfer read, write and relayed copy target at least 90% of the harness throughput; eight-transfer aggregate throughput targets at least 85%.
- Server-side copy is measured by completion latency and client bytes transferred; client data bytes should stay below 1% of object size.
- RSS is measured with 1 GiB, 10 GiB and 100 GiB files to verify a memory plateau rather than growth with file size.
- Cancellation UI acknowledgement targets 200 ms, active I/O stop targets 2 seconds, and stalled I/O terminates within the configured progress timeout.
- Release measurements record cold start, idle RSS, executable size and installer size. An increase above 10% or 150 ms startup time, 30 MiB idle RSS, or 20% or 15 MiB compressed package size triggers dependency splitting, delayed loading or explicit review.
- Nightly tests cover real AWS S3, a real WebDAV server, the target Hadoop distribution, concurrency, cancellation, cursor pressure and fault injection.

## Out of Scope

- Cross-connection copy.
- Recursive directory copy, directory rename and recursive directory delete.
- Resumable upload, download or copy.
- Strict atomic no-clobber guarantees across all backends.
- Windows SFTP in the baseline release.
- SFTP password authentication in the baseline release.
- FTPS.
- FastDFS.
- HDFS Kerberos certification.
- HDFS HA certification and failover guarantees.
- Multiple NameNode support in the baseline.
- Treating HttpFS as interchangeable with NameNode WebHDFS.
- Storing HDFS keytab contents in DBX.
- JVM/JNI-based HDFS integration.

## Further Notes

- The capability analysis is based on OpenDAL 0.57 behavior and must be revalidated before upgrading OpenDAL.
- Server-side copy on S3 and WebDAV transfers control traffic through DBX but keeps file data on the server side.
- FTP, SFTP, WebHDFS and HDFS Native fallback copy reads and writes the full object through the desktop process and therefore consumes both download and upload bandwidth.
- Adding many transitive crates indicates dependency complexity but does not predict packaged size; release builds are the source of truth.
- Plaintext file connection secrets are a high-risk compatibility choice, not a routine implementation detail.
- The three-party design review reached consensus on Conditional Go. The implementation is not release-ready until the WebHDFS Gate, secret-storage decision and target platform matrix are closed.
