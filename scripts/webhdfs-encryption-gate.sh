#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/tests/webhdfs-gate/compose-encryption.yml"
binary="$repo_root/target/debug/webhdfs_gate"
result_dir="${WEBHDFS_GATE_ENCRYPTION_RESULT_DIR:-$repo_root/target/webhdfs-encryption-gate}"

cleanup() {
  docker compose -f "$compose_file" down >/dev/null 2>&1 || true
}
trap cleanup EXIT
mkdir -p "$result_dir"

docker compose -f "$repo_root/tests/webhdfs-gate/compose.yml" down >/dev/null 2>&1 || true
docker compose -f "$compose_file" up -d
ready=0
for _ in $(seq 1 180); do
  if curl -fsS 'http://127.0.0.1:9870/webhdfs/v1/?op=GETFILESTATUS&user.name=hadoop' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  docker compose -f "$compose_file" logs >"$result_dir/compose.log"
  echo "encrypted WebHDFS fixture did not become ready" >&2
  exit 1
fi

docker compose -f "$compose_file" exec -T namenode hdfs dfs -mkdir -p /zones/a /zones/b
docker compose -f "$compose_file" exec -T namenode \
  hdfs crypto -createZone -keyName dbx-gate-key -path /zones/a
docker compose -f "$compose_file" exec -T namenode \
  hdfs crypto -createZone -keyName dbx-gate-key -path /zones/b
docker compose -f "$compose_file" exec -T namenode hdfs crypto -listZones \
  | tee "$result_dir/zones.txt"

cargo build -p dbx --bin webhdfs_gate --no-default-features --features webhdfs-gate
export WEBHDFS_GATE_ENDPOINT="http://127.0.0.1:9870/"
export WEBHDFS_GATE_USER_NAME=hadoop
export WEBHDFS_GATE_ALLOWED_DATANODE_ORIGINS="http://localhost:9864"
export WEBHDFS_GATE_CHUNK_MIB=1

if WEBHDFS_GATE_ROOT=/zones/a WEBHDFS_GATE_ATOMIC_WRITE_DIR=.dbx-blocks/ \
  "$binary" write-a same-zone-a.bin 2097153 \
  >"$result_dir/a-same-zone.stdout" 2>"$result_dir/a-same-zone.stderr"; then
  echo "same-encryption-zone CONCAT unexpectedly succeeded" >&2
  exit 1
fi
WEBHDFS_GATE_ROOT=/zones/a WEBHDFS_GATE_ATOMIC_WRITE_DIR=.dbx-blocks/ \
  "$binary" write-b same-zone-b.bin 2097153 | tee "$result_dir/b-same-zone-write.json"
WEBHDFS_GATE_ROOT=/zones/a WEBHDFS_GATE_ATOMIC_WRITE_DIR=.dbx-blocks/ \
  "$binary" copy-b same-zone-b.bin same-zone-b-copy.bin | tee "$result_dir/b-same-zone-copy.json"

if WEBHDFS_GATE_ROOT=/ WEBHDFS_GATE_ATOMIC_WRITE_DIR=zones/b/.dbx-blocks/ \
  "$binary" write-a zones/a/cross-zone-a.bin 2097153 \
  >"$result_dir/a-cross-zone.stdout" 2>"$result_dir/a-cross-zone.stderr"; then
  echo "cross-encryption-zone CONCAT unexpectedly succeeded" >&2
  exit 1
fi

{
  echo "encryption_zones=pass"
  echo "same_zone_atomic_concat=fail_closed"
  echo "same_zone_streaming_write_copy=pass"
  echo "cross_zone_atomic_concat=fail_closed"
} | tee "$result_dir/verdict.txt"
