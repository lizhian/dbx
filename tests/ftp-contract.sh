#!/usr/bin/env bash
set -euo pipefail

image="delfer/alpine-ftp-server@sha256:60bb774d8408d9d4d5c74d05d1c086a34ce192c6c1a142ffac268cac0dbc6fac"
container="dbx-ftp-contract-${RANDOM}"

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
  -p 2121:21 \
  -p 21000-21010:21000-21010 \
  "${image}" >/dev/null

for _ in $(seq 1 30); do
  if docker exec "${container}" pgrep vsftpd >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${container}" pgrep vsftpd >/dev/null
for _ in $(seq 1 30); do
  if nc -z 127.0.0.1 2121 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
nc -z 127.0.0.1 2121
docker exec "${container}" sh -c "printf 'dbx ftp fixture\n' > /ftp/dbx/fixture.txt && chown dbx:dbx /ftp/dbx/fixture.txt"

DBX_TEST_FTP_ENDPOINT="ftp://127.0.0.1:2121" \
DBX_TEST_FTP_USERNAME="dbx" \
DBX_TEST_FTP_PASSWORD="dbx-password" \
  cargo test -p dbx --lib file_manager::tests::fixed_ftp_service_contract --no-default-features -- --ignored
