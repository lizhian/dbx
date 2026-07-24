# WebHDFS bounded write/copy release Gate

This Gate is executable evidence for `lizhian/dbx#1`; it is not a runtime
capability fallback. The release remains No-Go until one complete candidate
passes every applicable row below on the target Hadoop distribution.

## Pinned local environment

- Apache Hadoop `3.4.1` (`apache/hadoop:3.4.1`)
- Apache OpenDAL `0.57.0`, pinned exactly in `src-tauri/Cargo.toml`
- 4 MiB application chunks, one in-flight request body
- NameNode `9870`, DataNode `9864`, WebHDFS simple user `hadoop`

Run the required size matrix:

```sh
WEBHDFS_GATE_SIZES_GIB="1 10 100" ./scripts/webhdfs-gate.sh
```

For a fast harness check, explicitly reduce the matrix; this does not pass the
release Gate:

```sh
WEBHDFS_GATE_SIZES_GIB="0" ./scripts/webhdfs-gate.sh
```

Each command writes JSON, stderr, peak RSS in KiB, and wall time under
`target/webhdfs-gate`. RSS passes only when the 1/10/100 GiB results form a
plateau attributable to fixed buffers rather than input size. A skipped size is
not a pass.

## Candidates

Candidate A uses OpenDAL `atomic_write_dir`. It writes fixed-size temporary
files, calls WebHDFS `CONCAT`, and renames the concatenated file. OpenDAL 0.57
has two release-blocking behaviors: `abort_block` writes
`atomic_write_dir/UUID` but deletes `root/UUID`, and close performs
`CONCAT -> DELETE destination -> RENAME`. The first leaks blocks; the second
can lose an old destination before a failed rename. The harness keeps the
correctness probes for upgrade regression evidence, but candidate A is No-Go
for OpenDAL 0.57.

Candidate B is a narrow WebHDFS adapter. It intentionally disables automatic
redirects, requires `CREATE -> 307`, validates the DataNode origin, and sends a
streaming request body with a fixed Content-Length. It always creates an
operation-unique temporary file, verifies size/content, then commits with HDFS
rename under create-new semantics. PUT failures only delete that owned
temporary file, never the requested destination. Fallback copy relays one
DataNode `OPEN` byte stream directly into the temporary DataNode request and
compares HDFS checksums after commit.

Candidate B never copies credentials from the NameNode URL to an arbitrary
redirect. The Location must preserve exactly one configured `delegation` or
`user.name` value, match an allowlisted scheme/host/effective-port origin,
contain no userinfo or fragment, and avoid TLS downgrade unless the test
explicitly opts in. Connect, control-request and streaming body-progress
timeouts are independent. `WEBHDFS_GATE_FAULT_AFTER_BYTES` injects a partial
body failure and verifies that the final destination remains absent.

## Required matrix

| Area | Pass evidence |
| --- | --- |
| Hadoop version | Exact target distribution/version is recorded; WebHDFS two-step create behavior is verified |
| Same filesystem | temp directory and destination are in the same HDFS namespace |
| Encryption zone | temp directory and destination tested in the same zone; cross-zone failure is classified and cleaned |
| Permissions | create/concat/rename/delete tested as the production identity; denial leaves no unowned destination |
| Quota | namespace and space quota exhaustion tested during temporary block creation and finalization |
| Abort/cleanup | cancellation and injected DataNode failure leave no destination or operation-owned temporary blocks |
| Redirect | real NameNode 307 and externally reachable DataNode hostname/port |
| Authentication | simple user and, on secure target, delegation token forwarding; token is never sent to an untrusted host |
| TLS | trusted certificate succeeds; invalid certificate and HTTPS-to-HTTP downgrade fail closed |
| Proxy | production proxy route succeeds, and bypass/deny behavior for DataNode is recorded |
| Host mapping | advertised DataNode host is reachable directly or through an explicit DNS override |
| Partial failure | disconnect after 307 reports cleanup result and never reports success |
| RSS | successful write and fallback copy at 1/10/100 GiB with a stable memory plateau |

The bundled fixtures prove the unencrypted single-filesystem simple-auth path,
KMS-backed encryption-zone behavior, and equivalent mock coverage for
delegation-token forwarding, TLS, proxy, and hostname mapping. They do not
replace validation against the production Kerberos-secured cluster and its
network policy.

## Decision rule

- Go with candidate A only if its complete matrix, including abort cleanup,
  passes without patching over leaked blocks at runtime.
- Otherwise Go with candidate B only if its complete matrix passes and the
  deployment can make validated DataNode redirects reachable.
- If neither candidate completes both write and fallback copy at all three
  required sizes with bounded RSS, WebHDFS in v1 is No-Go.
- `Unsupported` at runtime, a skipped environment row, or a reduced size matrix
  does not satisfy the requirement.
