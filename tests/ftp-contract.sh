#!/usr/bin/env bash
set -euo pipefail

image="delfer/alpine-ftp-server@sha256:60bb774d8408d9d4d5c74d05d1c086a34ce192c6c1a142ffac268cac0dbc6fac"
container=""
control_port="${DBX_TEST_FTP_CONTROL_PORT:-2121}"
passive_min_port="${DBX_TEST_FTP_PASSIVE_MIN_PORT:-21000}"
passive_max_port="${DBX_TEST_FTP_PASSIVE_MAX_PORT:-21010}"

# Compile before starting the service so a cold Rust build cannot consume the
# fixed image's entire readiness window.
cargo test -p dbx --lib fixed_ftp_ --no-default-features --no-run

stop_ftp() {
  if [[ -z "${container}" ]]; then
    return
  fi
  docker rm -f "${container}" >/dev/null 2>&1 || true
  container=""
  for _ in $(seq 1 50); do
    if ! nc -z 127.0.0.1 "${control_port}" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  ! nc -z 127.0.0.1 "${control_port}" >/dev/null 2>&1
}

cleanup() {
  result=$?
  if [[ "${result}" -ne 0 && -n "${container}" ]]; then
    docker inspect --format 'container={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}' "${container}" >&2 || true
    docker logs "${container}" >&2 || true
  fi
  stop_ftp || true
  exit "${result}"
}
trap cleanup EXIT

start_ftp() {
  local suffix="$1"
  stop_ftp
  container="dbx-ftp-contract-${suffix}-${RANDOM}"
  docker run -d \
    --name "${container}" \
    -e "USERS=dbx|dbx-password" \
    -e "ADDRESS=127.0.0.1" \
    -e "MIN_PORT=${passive_min_port}" \
    -e "MAX_PORT=${passive_max_port}" \
    -p "${control_port}:21" \
    -p "${passive_min_port}-${passive_max_port}:${passive_min_port}-${passive_max_port}" \
    "${image}" \
    /usr/sbin/vsftpd \
    /etc/vsftpd/vsftpd.conf \
    -obackground=NO \
    "-opasv_min_port=${passive_min_port}" \
    "-opasv_max_port=${passive_max_port}" \
    -opasv_address=127.0.0.1 >/dev/null

  # Run vsftpd in the foreground so Docker readiness cannot race the image's
  # pidproxy setup and accidentally attach the container to a session child.
  for _ in $(seq 1 30); do
    if docker exec "${container}" sh -c '
      test "$(cat /proc/1/comm)" = "tini" &&
      pidof vsftpd >/dev/null
    ' >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  docker exec "${container}" sh -c '
    test "$(cat /proc/1/comm)" = "tini" &&
    pidof vsftpd >/dev/null
  ' >/dev/null

  for _ in $(seq 1 30); do
    if nc -z 127.0.0.1 "${control_port}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  nc -z 127.0.0.1 "${control_port}"

  for _ in $(seq 1 30); do
    if curl --silent --show-error --fail --user "dbx:dbx-password" --list-only \
      "ftp://127.0.0.1:${control_port}/" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  curl --silent --show-error --fail --user "dbx:dbx-password" --list-only \
    "ftp://127.0.0.1:${control_port}/" >/dev/null

  docker exec "${container}" sh -c "
    printf 'dbx ftp fixture\n' > /ftp/dbx/fixture.txt
    dd if=/dev/zero of=/ftp/dbx/large.bin bs=1M count=256 status=none
    mkdir -p /ftp/dbx/nested
    mkdir -p /ftp/dbx/a
    printf 'literal-percent-space\n' > '/ftp/dbx/a%20b'
    printf 'actual-space\n' > '/ftp/dbx/a b'
    printf 'literal-percent-slash\n' > '/ftp/dbx/a%2Fb'
    printf 'nested-slash\n' > '/ftp/dbx/a/b'
    for index in \$(seq -w 1 205); do
      printf 'page fixture %s\n' \"\${index}\" > \"/ftp/dbx/page-\${index}.txt\"
    done
    chown -R dbx:dbx /ftp/dbx
  "
}

run_contract() {
  local test_name="$1"
  DBX_TEST_FTP_ENDPOINT="ftp://127.0.0.1:${control_port}" \
  DBX_TEST_FTP_USERNAME="dbx" \
  DBX_TEST_FTP_PASSWORD="dbx-password" \
  DBX_TEST_FTP_CONTAINER="${container}" \
    cargo test -p dbx --lib "${test_name}" --no-default-features -- --ignored --test-threads=1
}

start_ftp "read"
run_contract "fixed_ftp_service_contract"

start_ftp "download"
run_contract "fixed_ftp_download_contract"

# Each disconnect run starts from a fresh service. The Rust contract stops at
# an explicit reader-open barrier, kills this container, and only then permits
# the first read, so disconnect timing does not depend on polling file growth.
for attempt in $(seq 1 3); do
  start_ftp "disconnect-${attempt}"
  run_contract "fixed_ftp_worker_success_cancel_and_disconnect_contract"
done
