# WebHDFS Gate results: 2026-07-24

## Verdict

- Candidate A (`atomic_write_dir + CONCAT`): **No-Go with OpenDAL 0.57.0**.
- Candidate B (narrow two-step streaming PUT): **Local Go** for the pinned
  Hadoop 3.4.1 simple-auth fixture.
- Production release: **still No-Go** until the same candidate passes the
  Kerberos-secured target-cluster rows listed under "Remaining boundary".

`Local Go` is not a production release approval and must not be converted into
runtime `Unsupported` degradation.

## Environment

| Item | Value |
| --- | --- |
| Host | macOS Darwin 24.6.0, arm64 |
| Docker server | 29.4.0 |
| Hadoop | 3.4.1, `apache/hadoop:3.4.1` |
| Image digest | `sha256:69ffa97339aff768c4e6120c3fb27aa04c121402b1c8158408a5fb5be586a30e` |
| OpenDAL | exactly 0.57.0 |
| Authentication | simple `user.name=hadoop` |
| Topology | one NameNode and one DataNode, same HDFS namespace |
| Transfer chunk | 4 MiB |
| Result root | run-unique; removed after the run |

The command was:

```sh
WEBHDFS_GATE_SIZES_GIB="1 10 100" ./scripts/webhdfs-gate.sh
```

Tracked raw evidence and checksums are under
`tests/webhdfs-gate/evidence/2026-07-24/`.

## Candidate A

The 9 MiB write and fallback copy completed, proving that Hadoop 3.4.1 accepts
the OpenDAL temporary-block `CONCAT` path in the unencrypted same-filesystem
fixture.

Abort then leaked two 4 MiB files under `.dbx-blocks/UUID`. This reproduces the
OpenDAL 0.57 implementation mismatch: blocks are created under
`atomic_write_dir/UUID`, while `abort_block` deletes `root/UUID`. The close path
also performs `CONCAT`, deletes the requested destination, and only then
renames the temporary result. A failed final rename can therefore lose the old
destination. Candidate A is No-Go even though its happy-path write/copy passes.

## Candidate B

All required large write and relayed-copy cases completed:

| Operation | Size | Elapsed | Throughput | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| write | 1 GiB | 1.820 s | 562.64 MiB/s | 8,784 KiB |
| fallback copy | 1 GiB | 2.403 s | 425.98 MiB/s | 23,968 KiB |
| write | 10 GiB | 15.456 s | 662.49 MiB/s | 8,672 KiB |
| fallback copy | 10 GiB | 25.088 s | 408.15 MiB/s | 27,824 KiB |
| write | 100 GiB | 167.928 s | 609.78 MiB/s | 8,816 KiB |
| fallback copy | 100 GiB | 407.915 s | 251.03 MiB/s | 22,576 KiB |

The six large-transfer RSS samples stayed between 8,672 and 27,824 KiB. The
19,152 KiB spread is below the harness's 64 MiB plateau threshold and did not
grow with input size. Write used remote length plus deterministic first/last
content samples. Copy compared source and destination HDFS checksums.

The failure matrix also passed:

| Scenario | Observed result |
| --- | --- |
| Body failure after 4 MiB | transport failure; operation-owned temp cleanup `Ok(true)`; no final/temp leak |
| Permission denial | `GETFILESTATUS` returned 403; no write began |
| Space-quota exhaustion | DataNode PUT returned 403; operation-owned temp cleanup `Ok(true)` |
| Destination safety | write uses UUID temp + create-new rename; failure cleanup refuses non-owned paths |

An earlier full run exposed a harness defect: the 100 GiB write failed after
31 seconds because a client-wide 30-second response-read timeout incorrectly
covered an active upload. Cleanup succeeded. The fix removed that conflicting
timeout and kept three distinct controls: 10-second connect timeout, 30-second
control-request timeout, and a configurable streaming body-progress watchdog.
The successful 100 GiB rerun above is evidence for the corrected behavior.

## Encryption-zone fixture

A second Hadoop 3.4.1 fixture created two KMS-backed encryption zones. Candidate
A failed closed both within one zone and across zones because HDFS rejects
`CONCAT` for files in an encryption zone. Candidate B completed same-zone
streaming write and copy.

Encrypted HDFS files can have different HDFS block checksums despite identical
plaintext. Candidate B therefore keeps `GETFILECHECKSUM` as the fast path and,
when those checksums are unavailable or differ, compares the SHA-256 of the
source bytes relayed during copy with a bounded-memory sequential read of the
committed destination. This adds one destination read only on the fallback
path and keeps memory independent of file size.

## Secure equivalent fixture

The local secure mock passed trusted TLS, rejected an invalid TLS chain,
validated proxy routing and DataNode bypass, required explicit hostname
mapping, forwarded one delegation token only to the allowlisted DataNode
origin, and rejected an invalid token. This validates client policy and
fail-closed behavior, but it is not a real Kerberos-secured Hadoop cluster.

## Remaining boundary

The bundled fixtures do not prove these production requirements:

- Kerberos login and delegation-token acquisition/renewal against the target
  Hadoop distribution;
- production HTTPS certificates, redirect endpoints, proxy policy, DNS, and
  DataNode reachability;
- target-cluster quota and permission policies beyond the local injected cases.

Candidate B must run these rows against the target release cluster. A skipped
row is not a pass. If DataNode redirects cannot be reached through the
validated origin/DNS/proxy policy, or any secured row fails without safe
owned-temp handling, WebHDFS v1 remains No-Go.
