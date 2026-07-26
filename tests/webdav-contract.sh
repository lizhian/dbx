#!/usr/bin/env bash
set -euo pipefail

image="bytemark/webdav@sha256:bcabbc024c511b9c63ed3345f88573e31d84c952ee493c9acb3fe345f4f80f57"
port="${DBX_TEST_WEBDAV_PORT:-28080}"
proxy_port="${DBX_TEST_WEBDAV_PROXY_PORT:-28081}"
container="dbx-webdav-contract-${RANDOM}"
trace="$(mktemp "${TMPDIR:-/tmp}/dbx-webdav-trace.XXXXXX")"
proxy_pid=""

cleanup() {
  result=$?
  trap - EXIT
  if [[ "${result}" -ne 0 && -s "${trace}" ]]; then
    cat "${trace}" >&2
  fi
  if [[ -n "${proxy_pid}" ]]; then
    kill "${proxy_pid}" >/dev/null 2>&1 || true
    wait "${proxy_pid}" >/dev/null 2>&1 || true
  fi
  docker rm -f "${container}" >/dev/null 2>&1 || true
  rm -f "${trace}"
  exit "${result}"
}
trap cleanup EXIT

command -v docker >/dev/null
command -v curl >/dev/null
command -v node >/dev/null
docker info >/dev/null

cargo test -p dbx --lib fixed_webdav_ --no-default-features --no-run

docker run -d \
  --platform linux/amd64 \
  --name "${container}" \
  -p "127.0.0.1:${port}:80" \
  -e AUTH_TYPE=Basic \
  -e USERNAME=dbx \
  -e PASSWORD=dbx-password \
  "${image}" >/dev/null

for _ in $(seq 1 60); do
  status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    --user dbx:dbx-password --request PROPFIND --header 'Depth: 0' \
    "http://127.0.0.1:${port}/" || true)"
  [[ "${status}" == "207" ]] && break
  sleep 1
done
[[ "${status}" == "207" ]]

curl --silent --show-error --fail --user dbx:dbx-password \
  --request MKCOL "http://127.0.0.1:${port}/tenant/" >/dev/null
curl --silent --show-error --fail --user dbx:dbx-password \
  --request MKCOL "http://127.0.0.1:${port}/tenant/root/" >/dev/null
curl --silent --show-error --fail --user dbx:dbx-password \
  --upload-file <(printf 'fixture') "http://127.0.0.1:${port}/tenant/root/fixture.txt" >/dev/null
docker exec "${container}" sh -c '
  mkdir -p /var/lib/dav/data/tenant/root/denied
  chown root:root /var/lib/dav/data/tenant/root/denied
  chmod 0555 /var/lib/dav/data/tenant/root/denied
'

DBX_WEBDAV_PROXY_PORT="${proxy_port}" \
DBX_WEBDAV_PROXY_UPSTREAM="http://127.0.0.1:${port}" \
DBX_WEBDAV_PROXY_TRACE="${trace}" \
  node tests/webdav-fault-proxy.mjs &
proxy_pid=$!
for _ in $(seq 1 50); do
  if curl --silent --output /dev/null --user dbx:dbx-password \
    --request PROPFIND --header 'Depth: 0' "http://127.0.0.1:${proxy_port}/"; then
    break
  fi
  sleep 0.1
done

run_exact_contract() {
  local test_name="$1"
  local output
  output="$(mktemp "${TMPDIR:-/tmp}/dbx-webdav-contract-result.XXXXXX")"
  DBX_TEST_WEBDAV_ENDPOINT="http://127.0.0.1:${proxy_port}" \
  DBX_TEST_WEBDAV_USERNAME="dbx" \
  DBX_TEST_WEBDAV_PASSWORD="dbx-password" \
    cargo test -p dbx --lib "${test_name}" --no-default-features -- \
      --ignored --exact --test-threads=1 2>&1 | tee "${output}"
  grep -Fx "test ${test_name} ... ok" "${output}" >/dev/null
  grep -F 'test result: ok. 1 passed; 0 failed;' "${output}" >/dev/null
  rm -f "${output}"
}

run_exact_contract commands::file_manager_webdav::tests::fixed_webdav_service_contract
run_exact_contract commands::file_transfer::tests::fixed_webdav_file_transfer_worker_contract

timeout_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  --user dbx:dbx-password --request PROPFIND --header 'Depth: 0' \
  "http://127.0.0.1:${port}/tenant/root/timeout-copy.txt")"
[[ "${timeout_status}" == "404" ]]

node - "${trace}" <<'NODE'
const fs = require("node:fs");
const events = fs.readFileSync(process.argv[2], "utf8").trim().split("\n").filter(Boolean).map(JSON.parse);
for (const method of ["COPY", "MOVE"]) {
  const event = events.find((entry) => entry.method === method);
  if (!event || event.overwrite !== "T" || !event.destination || event.bodyBytes !== 0) {
    throw new Error(`Missing native ${method} contract: ${JSON.stringify(events, null, 2)}`);
  }
}
const puts = events.filter((entry) => entry.method === "PUT");
if (!puts.some((entry) => entry.bodyBytes > 8 * 1024 * 1024)) {
  throw new Error("Bounded streaming PUT fixture was not observed");
}
const concurrentWrite = events.find((entry) => entry.event === "concurrent_write");
if (!concurrentWrite?.rejectedByLock) {
  throw new Error(`Depth-infinity LOCK did not reject a concurrent child write: ${JSON.stringify(events, null, 2)}`);
}
const lateWrite = events.find((entry) => entry.event === "late_delete_concurrent_write");
if (!lateWrite?.rejectedByLock) {
  throw new Error(`DELETE response loss released its safety lock before delayed commit: ${JSON.stringify(events, null, 2)}`);
}
if (events.some((entry) => entry.method === "UNLOCK" && entry.url.includes("response-loss-delete"))) {
  throw new Error(`DELETE outcome-unknown path was explicitly unlocked: ${JSON.stringify(events, null, 2)}`);
}
const unsafeLock = events.find((entry) => entry.event === "unsafe_lock_timeout_injected");
if (!unsafeLock || unsafeLock.timeout !== "Infinite") {
  throw new Error(`Unsafe server-granted LOCK timeout was not injected: ${JSON.stringify(events, null, 2)}`);
}
if (events.some((entry) => entry.method === "DELETE" && entry.url.includes("unsafe-timeout-delete"))) {
  throw new Error(`DELETE dispatched under an unbounded LOCK lease: ${JSON.stringify(events, null, 2)}`);
}
if (!events.some((entry) => entry.method === "UNLOCK" && entry.url.includes("unsafe-timeout-delete"))) {
  throw new Error(`Unsafe LOCK lease was not released before failure: ${JSON.stringify(events, null, 2)}`);
}
for (const mode of ["anonymous", "bearer"]) {
  const event = events.find((entry) => entry.event === "auth_mode" && entry.mode === mode);
  if (!event?.valid) {
    throw new Error(`Missing valid ${mode} authentication request: ${JSON.stringify(events, null, 2)}`);
  }
}
if (!events.some((entry) => entry.authorizationScheme === "Basic")) {
  throw new Error(`Missing real Basic-authenticated request: ${JSON.stringify(events, null, 2)}`);
}
if (
  events.some(
    (entry) =>
      (entry.method === "COPY" || entry.method === "MOVE") &&
      entry.destination?.includes("worker-cancelled-before-dispatch"),
  )
) {
  throw new Error(`Cancelled mutation-lock waiter dispatched a request: ${JSON.stringify(events, null, 2)}`);
}
NODE
