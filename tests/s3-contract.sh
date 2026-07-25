#!/usr/bin/env bash
set -euo pipefail

minio_image="quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
mc_image="quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z@sha256:aead63c77f9db9107f1696fb08ecb0faeda23729cde94b0f663edf4fe09728e3"

container=""
proxy_pid=""
proxy_trace=""
port="${DBX_TEST_S3_PORT:-29090}"
proxy_port="${DBX_TEST_S3_PROXY_PORT:-$((port + 1))}"
bucket="dbx-s3-contract"
region="us-east-1"
root="/tenant/root/"
root_key="tenant/root/"
access_key_id="dbx-s3-access-key"
secret_access_key="dbx-s3-secret-key"
direct_endpoint="http://127.0.0.1:${port}"

if ! [[ "${port}" =~ ^[0-9]+$ ]] || ((port < 1 || port > 65535)); then
  echo "DBX_TEST_S3_PORT must be an integer between 1 and 65535" >&2
  exit 2
fi
if ! [[ "${proxy_port}" =~ ^[0-9]+$ ]] || ((proxy_port < 1 || proxy_port > 65535)); then
  echo "DBX_TEST_S3_PROXY_PORT must be an integer between 1 and 65535" >&2
  exit 2
fi

command -v docker >/dev/null
command -v curl >/dev/null
command -v node >/dev/null
docker info >/dev/null

stop_minio() {
  if [[ -z "${container}" ]]; then
    return
  fi

  docker rm -fv "${container}" >/dev/null 2>&1 || true
  container=""

  for _ in $(seq 1 50); do
    if ! curl --silent --fail --max-time 1 \
      "${direct_endpoint}/minio/health/ready" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done

  ! curl --silent --fail --max-time 1 \
    "${direct_endpoint}/minio/health/ready" >/dev/null 2>&1
}

cleanup() {
  local result=$?
  trap - EXIT

  if [[ -n "${proxy_pid}" ]]; then
    kill "${proxy_pid}" >/dev/null 2>&1 || true
    wait "${proxy_pid}" >/dev/null 2>&1 || true
    proxy_pid=""
  fi
  if [[ -n "${proxy_trace}" ]]; then
    rm -f "${proxy_trace}"
    proxy_trace=""
  fi

  if [[ "${result}" -ne 0 && -n "${container}" ]]; then
    docker inspect \
      --format 'container={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}' \
      "${container}" >&2 || true
    docker logs "${container}" >&2 || true
  fi

  stop_minio || true
  exit "${result}"
}
trap cleanup EXIT

mc_run() {
  docker run --rm -i \
    --network "container:${container}" \
    -e "MC_HOST_dbx=http://${access_key_id}:${secret_access_key}@127.0.0.1:9000" \
    "${mc_image}" \
    "$@"
}

seed_object() {
  local key="$1"
  local value="$2"
  local fixture_kind="$3"

  printf '%s' "${value}" |
    mc_run pipe \
      --attr "dbx-fixture=${fixture_kind}" \
      "dbx/${bucket}/${key}" >/dev/null
}

seed_empty_marker() {
  local key="$1"
  mc_run mb --ignore-existing "dbx/${bucket}/${key}" >/dev/null
}

# Compile before starting MinIO so a cold Rust build cannot consume the
# service readiness window.
cargo test -p dbx --lib fixed_s3_ --no-default-features --no-run

container="dbx-s3-contract-${RANDOM}"
docker run -d \
  --name "${container}" \
  -p "127.0.0.1:${port}:9000" \
  -e "MINIO_ROOT_USER=${access_key_id}" \
  -e "MINIO_ROOT_PASSWORD=${secret_access_key}" \
  "${minio_image}" \
  server /data \
  --address ":9000" \
  --console-address ":9001" >/dev/null

for _ in $(seq 1 60); do
  if curl --silent --show-error --fail --max-time 2 \
    "${direct_endpoint}/minio/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl --silent --show-error --fail --max-time 2 \
  "${direct_endpoint}/minio/health/ready" >/dev/null

mc_run mb --ignore-existing --region "${region}" "dbx/${bucket}" >/dev/null
mc_run version enable "dbx/${bucket}" >/dev/null

# S3 can contain an object "a", a directory marker "a/", and children under
# "a/" at the same time. Keep all three forms in the fixed fixture.
seed_empty_marker "${root_key}"
seed_object "${root_key}a" "file-a" "file-a"
seed_empty_marker "${root_key}a/"
seed_object "${root_key}a/child.txt" "child-a" "child"
seed_object "${root_key}virtual/child.txt" "virtual-child" "virtual-child"
seed_empty_marker "${root_key}empty/"

# This fixture is intentionally a non-empty object whose key ends in "/".
# The pinned mc rejects non-empty stdin for such keys, so grant anonymous PUT
# on only this prefix for the duration of one request, then remove the policy.
mc_run anonymous set upload "dbx/${bucket}/${root_key}nonzero/" >/dev/null
curl --silent --show-error --fail \
  --request PUT \
  --header "Content-Type: application/octet-stream" \
  --data-binary "nonzero-marker" \
  "${direct_endpoint}/${bucket}/${root_key}nonzero/" >/dev/null
mc_run anonymous set private "dbx/${bucket}/${root_key}nonzero/" >/dev/null

# These canaries are outside the configured root and must never be listed,
# copied, renamed, or deleted by a root-scoped DBX connection.
seed_object "tenant/outside-root-canary.txt" "tenant-canary" "outside-root-canary"
seed_object "outside-root-canary.txt" "bucket-canary" "outside-root-canary"

# Assert that mc created exact marker keys instead of only virtual prefixes.
mc_run stat "dbx/${bucket}/${root_key}a" >/dev/null
mc_run stat "dbx/${bucket}/${root_key}a/" >/dev/null
mc_run stat "dbx/${bucket}/${root_key}a/child.txt" >/dev/null
mc_run stat "dbx/${bucket}/${root_key}virtual/child.txt" >/dev/null
mc_run stat "dbx/${bucket}/${root_key}empty/" >/dev/null
mc_run stat "dbx/${bucket}/${root_key}nonzero/" >/dev/null
mc_run stat "dbx/${bucket}/tenant/outside-root-canary.txt" >/dev/null
mc_run stat "dbx/${bucket}/outside-root-canary.txt" >/dev/null

proxy_trace="$(mktemp "${TMPDIR:-/tmp}/dbx-s3-fault-proxy.XXXXXX")"
proxy_endpoint="http://127.0.0.1:${proxy_port}"
DBX_S3_FAULT_PROXY_PORT="${proxy_port}" \
DBX_S3_FAULT_PROXY_UPSTREAM="${direct_endpoint}" \
DBX_S3_FAULT_PROXY_TRACE="${proxy_trace}" \
  node tests/s3-fault-proxy.mjs &
proxy_pid=$!
for _ in $(seq 1 50); do
  if curl --silent --max-time 1 "${proxy_endpoint}/minio/health/ready" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${proxy_pid}" >/dev/null 2>&1; then
    wait "${proxy_pid}"
  fi
  sleep 0.1
done
curl --silent --show-error --fail --max-time 2 \
  "${proxy_endpoint}/minio/health/ready" >/dev/null

test_endpoint="${proxy_endpoint}"
DBX_TEST_S3_ENDPOINT="${test_endpoint}" \
DBX_TEST_S3_DIRECT_ENDPOINT="${direct_endpoint}" \
DBX_TEST_S3_FAULT_PROXY="1" \
DBX_TEST_S3_REGION="${region}" \
DBX_TEST_S3_BUCKET="${bucket}" \
DBX_TEST_S3_ROOT="${root}" \
DBX_TEST_S3_ACCESS_KEY_ID="${access_key_id}" \
DBX_TEST_S3_SECRET_ACCESS_KEY="${secret_access_key}" \
DBX_TEST_S3_CONTAINER="${container}" \
DBX_TEST_S3_MC_IMAGE="${mc_image}" \
DBX_TEST_S3_OUTSIDE_CANARY_KEY="tenant/outside-root-canary.txt" \
DBX_TEST_S3_BUCKET_CANARY_KEY="outside-root-canary.txt" \
  cargo test -p dbx --lib fixed_s3_ --no-default-features -- \
    --ignored --test-threads=1

node - "${proxy_trace}" <<'NODE'
const fs = require("node:fs");
const tracePath = process.argv[2];
const events = fs
  .readFileSync(tracePath, "utf8")
  .trim()
  .split("\n")
  .filter(Boolean)
  .map((line) => JSON.parse(line));
const requiredNames = new Set([
  "after_commit_response_loss",
  "copy_200_error",
  "multipart_part_200_error",
  "multipart_abort_failure",
]);
const required = events.filter(({ event }) => requiredNames.has(event));
const expected = [
  ["after_commit_response_loss", "PUT", "response-loss-copy-destination"],
  ["after_commit_response_loss", "PUT", "response-loss-rename-destination"],
  ["after_commit_response_loss", "PUT", "response-loss-upload-target"],
  ["copy_200_error", "PUT", "fault-200-error-destination"],
  ["multipart_part_200_error", "PUT", "fault-abort-error-destination"],
  ["multipart_abort_failure", "DELETE", "fault-abort-error-destination"],
];
if (
  required.length !== expected.length ||
  required.some(
    (entry, index) =>
      entry.event !== expected[index][0] ||
      entry.method !== expected[index][1] ||
      !decodeURIComponent(entry.url).includes(expected[index][2]),
  )
) {
  process.stderr.write(`Unexpected S3 fault trace:\n${JSON.stringify(events, null, 2)}\n`);
  process.exit(1);
}
NODE
