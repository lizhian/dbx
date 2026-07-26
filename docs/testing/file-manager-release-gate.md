# File Manager release gate

The File Manager release decision is machine-owned and fail-closed. A skipped
job, compile-only result, historical benchmark, or independently uploaded
artifact cannot produce a release `GO`.

## Trust model

- `tests/file-manager-release/canonical-policy.json` is the canonical
  requirement set. `matrix.json` must match it exactly, and the validator pins
  its semantic SHA-256 fingerprint. Removing a row, test, platform,
  implementation, operation, threshold, image digest, or protected gate is a
  structural error.
- `.github/workflows/file-manager-evidence.yml` is the only certification
  workflow. Every `passed` row in a release report must have the same
  repository, commit, run ID, run attempt, workflow, event, ref, allowlisted
  job, allowlisted command, and runner OS.
- Release evidence must come from a successful `push` run on
  `refs/heads/main` in `lizhian/dbx`. Pull-request and manual runs are useful
  audits but are not trusted release producers.
- `.github/workflows/release.yml` selects the trusted run for the exact tag
  commit and validates its downloaded report and raw artifacts before any
  build, version bump, container push, asset upload, or publication can start.

This deliberately uses one certification run. Independent workflow artifacts
are not merged, because binding results from unrelated runs would make
provenance and protected-environment review ambiguous.

## Evidence validation

Every `passed` row must reference a downloaded artifact. The validator opens
that file and recomputes SHA-256; a claim in the evidence envelope without the
raw file cannot pass. Contract artifacts are logs. Every other kind uses a
machine-readable raw-result JSON artifact, and the validator derives the
decision from that artifact rather than trusting duplicated envelope fields.
Validation is kind-specific:

- Real-service contracts require exact pinned image digests and the complete
  expected Rust test set parsed from the log. Zero-test success is rejected.
- `platform-compile-*` rows only prove compilation. They are optional and
  cannot satisfy required `platform-conformance-*` rows.
- Platform conformance requires passed raw results for every protocol and
  operation combination across read, write, stat, list, delete, copy, and
  rename.
- Performance rows derive exact byte counts, durations, peak RSS, throughput
  ratios, and cancellation thresholds from raw measurements.
- Fault, release-measurement, protected-service, desktop E2E, and release-gate
  rows derive canonical test, scenario, metric, review, or check sets from raw
  result arrays.

Unknown rows, duplicates, stale source runs, producer mismatches, missing raw
artifacts, forged hashes, incomplete test sets, and weakened policies are
structural errors.

## States

| State | Meaning | Satisfies release |
| --- | --- | --- |
| `passed` | Current trusted producer completed and all kind-specific evidence validates | Yes |
| `failed` | The test ran and failed | No |
| `pending` | The test did not run, its environment was unavailable, or no evidence was supplied | No |
| `legacy` | Historical evidence from another context | No |
| `not_applicable` | Reserved for a non-required row | No required row may use it |

The tracked `baseline.json` is audit-only. Its historical WebHDFS results
remain `legacy` and cannot satisfy another commit or run.

## Local audit

```bash
node --test scripts/file-manager-release-report.test.mjs
node scripts/file-manager-release-report.mjs validate \
  --commit "$(git rev-parse HEAD)" \
  --run-id local-audit \
  --run-attempt 1 \
  --event push \
  --ref refs/heads/main \
  --mode audit \
  --report /tmp/file-manager-release-audit.json
```

Audit mode exits successfully for a structurally valid `NO-GO` report. Release
mode exits nonzero until every required row is current `passed`.

## Protected environments

Real AWS S3, real WebDAV, target Hadoop WebHDFS, target Hadoop Native, the
WebHDFS production gate, and secrets-at-rest require protected environments.
Unavailable credentials or runners must remain `pending`; they can never be
converted to a synthetic pass.

A protected `passed` row must bind both its source-run envelope and raw result
to the canonical environment name, and both must report a successful job
conclusion. In addition, the validator requires a jobs attestation downloaded
from the GitHub Actions API and independently matches the allowlisted job name,
run ID, run attempt, commit, and successful conclusion. The release workflow
downloads this attestation itself; it does not trust one bundled with evidence.
It also downloads deployments for the exact commit and each deployment's
latest status from the GitHub Deployments API. A protected pass needs exactly
one deployment whose environment, original environment, commit, and ref match
canonical policy, whose latest status is `success`, and whose `log_url` is the
matched Actions run/job URL. Jobs API success without this independent
deployment binding remains `NO-GO`. Neither attestation is trusted from the
evidence bundle.

The current repository has no such deployments, so protected rows correctly
remain `pending`. The allowlisted protected job must run inside its canonical
GitHub environment in the single certification workflow.

The AWS nightly workflow is an independent operational diagnostic. Its
artifact is not consumed by the release gate. To satisfy `real-aws-s3` in the
future, add a protected job to the same main-branch certification workflow and
emit evidence from that certification run. The same rule applies to the other
protected service rows.

WebHDFS production RSS evidence must execute all upload and copy cases at
1/10/100 GiB. SFTP and HDFS Native smoke modes only prove fixture
reachability, and the certification workflow sets
`DBX_REQUIRE_FULL_CONTRACT=1` so they cannot be mistaken for product
contracts.

Until those protected jobs, real platform conformance, desktop E2E,
performance, fault injection, and release measurements exist in the
certification workflow, the production verdict remains `NO-GO`.
