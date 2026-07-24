#!/usr/bin/env bash
set -euo pipefail

image="delfer/alpine-ftp-server@sha256:60bb774d8408d9d4d5c74d05d1c086a34ce192c6c1a142ffac268cac0dbc6fac"
container="dbx-ftp-contract-${RANDOM}"
control_port="${DBX_TEST_FTP_CONTROL_PORT:-2121}"
passive_min_port="${DBX_TEST_FTP_PASSIVE_MIN_PORT:-21000}"
passive_max_port="${DBX_TEST_FTP_PASSIVE_MAX_PORT:-21010}"

# Compile before starting the service so a cold Rust build cannot consume the
# fixed image's entire readiness window.
cargo test -p dbx --lib file_manager::tests::fixed_ftp_service_contract --no-default-features --no-run

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d \
  --name "${container}" \
  -e "USERS=dbx|dbx-password" \
  -e "ADDRESS=127.0.0.1" \
  -e "MIN_PORT=${passive_min_port}" \
  -e "MAX_PORT=${passive_max_port}" \
  -p "${control_port}:21" \
  -p "${passive_min_port}-${passive_max_port}:${passive_min_port}-${passive_max_port}" \
  "${image}" >/dev/null

for _ in $(seq 1 30); do
  if docker exec "${container}" pgrep vsftpd >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${container}" pgrep vsftpd >/dev/null
for _ in $(seq 1 30); do
  if nc -z 127.0.0.1 "${control_port}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
nc -z 127.0.0.1 "${control_port}"
docker exec "${container}" sh -c "
  printf 'dbx ftp fixture\n' > /ftp/dbx/fixture.txt
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

DBX_TEST_FTP_ENDPOINT="ftp://127.0.0.1:${control_port}" \
DBX_TEST_FTP_USERNAME="dbx" \
DBX_TEST_FTP_PASSWORD="dbx-password" \
  cargo test -p dbx --lib file_manager::tests::fixed_ftp_service_contract --no-default-features -- --ignored
