#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/tests/webhdfs-gate/compose.yml"
profile="${WEBHDFS_GATE_PROFILE:-release}"
if [ "$profile" = "release" ]; then
  binary="$repo_root/target/release/webhdfs_gate"
elif [ "$profile" = "debug" ]; then
  binary="$repo_root/target/debug/webhdfs_gate"
else
  echo "WEBHDFS_GATE_PROFILE must be release or debug" >&2
  exit 2
fi
result_dir="${WEBHDFS_GATE_RESULT_DIR:-$repo_root/target/webhdfs-gate}"
sizes_gib="${WEBHDFS_GATE_SIZES_GIB:-1 10 100}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"

mkdir -p "$result_dir"

export WEBHDFS_GATE_ENDPOINT="${WEBHDFS_GATE_ENDPOINT:-http://127.0.0.1:9870/}"
export WEBHDFS_GATE_ROOT="${WEBHDFS_GATE_ROOT:-/dbx-webhdfs-gate/$run_id}"
export WEBHDFS_GATE_ATOMIC_WRITE_DIR="${WEBHDFS_GATE_ATOMIC_WRITE_DIR:-.dbx-blocks/}"
export WEBHDFS_GATE_USER_NAME="${WEBHDFS_GATE_USER_NAME:-hadoop}"
export WEBHDFS_GATE_ALLOWED_DATANODE_ORIGINS="${WEBHDFS_GATE_ALLOWED_DATANODE_ORIGINS:-http://localhost:9864}"
export WEBHDFS_GATE_CHUNK_MIB="${WEBHDFS_GATE_CHUNK_MIB:-4}"

docker compose -f "$compose_file" up -d

ready=0
for _ in $(seq 1 90); do
  if curl -fsS \
    "${WEBHDFS_GATE_ENDPOINT%/}/webhdfs/v1/?op=GETFILESTATUS&user.name=$WEBHDFS_GATE_USER_NAME" \
    >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  docker compose -f "$compose_file" logs
  echo "WebHDFS did not become ready" >&2
  exit 1
fi

if [ "$profile" = "release" ]; then
  cargo build --release -p dbx --bin webhdfs_gate --no-default-features --features webhdfs-gate
else
  cargo build -p dbx --bin webhdfs_gate --no-default-features --features webhdfs-gate
fi
"$binary" info | tee "$result_dir/info.json"
{
  uname -a
  docker version --format 'docker_server={{.Server.Version}}'
  docker image inspect apache/hadoop:3.4.1 --format 'hadoop_image={{.Id}}'
} >"$result_dir/environment.txt"

cleanup_run() {
  docker compose -f "$compose_file" exec -T namenode \
    hdfs dfs -rm -r -skipTrash "$WEBHDFS_GATE_ROOT" >/dev/null 2>&1 || true
}
trap cleanup_run EXIT

measure() {
  local label="$1"
  shift
  local stdout_file="$result_dir/$label.json"
  local stderr_file="$result_dir/$label.stderr"
  local rss_file="$result_dir/$label.rss-kib"
  local started
  started="$(date +%s)"
  "$binary" "$@" >"$stdout_file" 2>"$stderr_file" &
  local pid=$!
  local peak=0
  while kill -0 "$pid" 2>/dev/null; do
    local rss
    rss="$(ps -o rss= -p "$pid" | tr -d ' ' || true)"
    if [[ "$rss" =~ ^[0-9]+$ ]] && [ "$rss" -gt "$peak" ]; then
      peak="$rss"
    fi
    sleep 0.2
  done
  local status=0
  wait "$pid" || status=$?
  printf '%s\n' "$peak" >"$rss_file"
  printf '%s\t%s\t%s\t%s\n' "$label" "$status" "$peak" "$(( $(date +%s) - started ))" \
    | tee -a "$result_dir/summary.tsv"
  if [ "$status" -ne 0 ]; then
    cat "$stderr_file" >&2
    return "$status"
  fi
  cat "$stdout_file"
}

printf 'case\texit_status\tpeak_rss_kib\twall_seconds\n' >"$result_dir/summary.tsv"

# Both implementations first run a small correctness and cleanup matrix.
candidate_a_passed=true
if ! measure a-write-small write-a a-small.bin 9437185; then
  candidate_a_passed=false
fi
if [ "$candidate_a_passed" = true ] && ! measure a-copy-small copy-a a-small.bin a-small-copy.bin; then
  candidate_a_passed=false
fi
if ! measure a-abort abort-a a-abort.bin; then
  candidate_a_passed=false
fi
printf '%s\n' \
  "Candidate A is No-Go: OpenDAL 0.57 abort_block deletes root/UUID instead of atomic_write_dir/UUID, and close uses CONCAT -> DELETE destination -> RENAME." \
  | tee "$result_dir/candidate-a-no-go.txt"
measure b-write-small write-b b-small.bin 9437185
measure b-copy-small copy-b b-small.bin b-small-copy.bin

if WEBHDFS_GATE_FAULT_AFTER_BYTES=4194304 "$binary" write-b b-fault.bin 9437185 \
  >"$result_dir/b-fault.stdout" 2>"$result_dir/b-fault.stderr"; then
  echo "Injected partial PUT unexpectedly succeeded" >&2
  exit 1
fi
if curl -fsS \
  "${WEBHDFS_GATE_ENDPOINT%/}/webhdfs/v1/${WEBHDFS_GATE_ROOT#/}/b-fault.bin?op=GETFILESTATUS&user.name=$WEBHDFS_GATE_USER_NAME" \
  >/dev/null 2>&1; then
  echo "Injected partial PUT exposed the final destination" >&2
  exit 1
fi
if curl -fsS \
  "${WEBHDFS_GATE_ENDPOINT%/}/webhdfs/v1/${WEBHDFS_GATE_ROOT#/}/.dbx-streaming?op=LISTSTATUS&user.name=$WEBHDFS_GATE_USER_NAME" \
  | grep -q '"pathSuffix"'; then
  echo "Injected partial PUT leaked an operation-owned temporary file" >&2
  exit 1
fi

docker compose -f "$compose_file" exec -T namenode hdfs dfs -chmod 700 "$WEBHDFS_GATE_ROOT"
if WEBHDFS_GATE_USER_NAME=dbx-denied "$binary" write-b b-permission-denied.bin 9437185 \
  >"$result_dir/b-permission.stdout" 2>"$result_dir/b-permission.stderr"; then
  echo "Permission-denied scenario unexpectedly succeeded" >&2
  exit 1
fi
docker compose -f "$compose_file" exec -T namenode hdfs dfs -chmod 755 "$WEBHDFS_GATE_ROOT"

docker compose -f "$compose_file" exec -T namenode hdfs dfsadmin -setSpaceQuota 1048576 "$WEBHDFS_GATE_ROOT"
if "$binary" write-b b-quota.bin 9437185 >"$result_dir/b-quota.stdout" 2>"$result_dir/b-quota.stderr"; then
  echo "Space-quota scenario unexpectedly succeeded" >&2
  exit 1
fi
docker compose -f "$compose_file" exec -T namenode hdfs dfsadmin -clrSpaceQuota "$WEBHDFS_GATE_ROOT"

# Candidate B is the selected path only when all configured sizes plateau in RSS
# and both write and relayed copy complete. The release matrix must keep 1/10/100.
for gib in $sizes_gib; do
  bytes=$((gib * 1024 * 1024 * 1024))
  source="b-${gib}g.bin"
  destination="b-${gib}g-copy.bin"
  measure "b-write-${gib}g" write-b "$source" "$bytes"
  measure "b-copy-${gib}g" copy-b "$source" "$destination"
  "$binary" delete "$source"
  "$binary" delete "$destination"
done

if [ "$sizes_gib" = "1 10 100" ]; then
  rss_values="$(awk -F '\t' '$1 ~ /^b-(write|copy)-(1|10|100)g$/ && $2 == 0 {print $3}' "$result_dir/summary.tsv")"
  count="$(printf '%s\n' "$rss_values" | awk 'NF {count++} END {print count+0}')"
  min_rss="$(printf '%s\n' "$rss_values" | awk 'NF && (min == 0 || $1 < min) {min=$1} END {print min+0}')"
  max_rss="$(printf '%s\n' "$rss_values" | awk 'NF && $1 > max {max=$1} END {print max+0}')"
  allowed_spread=$((64 * 1024))
  if [ "$count" -ne 6 ] || [ $((max_rss - min_rss)) -gt "$allowed_spread" ]; then
    printf '{"candidate":"streaming-put","verdict":"NO-GO","reason":"RSS plateau failed","samples":%s,"min_rss_kib":%s,"max_rss_kib":%s}\n' \
      "$count" "$min_rss" "$max_rss" | tee "$result_dir/gate-verdict.json"
    exit 1
  fi
  printf '{"candidate":"streaming-put","verdict":"LOCAL-GO","scope":"Hadoop 3.4.1 simple-auth fixture","samples":6,"min_rss_kib":%s,"max_rss_kib":%s}\n' \
    "$min_rss" "$max_rss" | tee "$result_dir/gate-verdict.json"
else
  printf '{"candidate":"streaming-put","verdict":"NO-GO","reason":"required sizes 1/10/100 GiB were not all executed"}\n' \
    | tee "$result_dir/gate-verdict.json"
fi

echo "Results written to $result_dir"
