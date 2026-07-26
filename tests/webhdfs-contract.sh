#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_root}/tests/webhdfs-gate/compose.yml"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
contract_user="dbx-webhdfs-contract"
contract_base="/dbx-webhdfs-contract/${run_id}"
contract_root="${contract_base}/root"
permission_root="${contract_base}/permission-denied"
quota_root="${contract_base}/quota"
proxy_port="${DBX_TEST_WEBHDFS_DATANODE_PROXY_PORT:-9864}"
proxy_control_port="${DBX_TEST_WEBHDFS_DATANODE_PROXY_CONTROL_PORT:-19865}"
data_node_direct_port="${DBX_TEST_WEBHDFS_DATANODE_DIRECT_PORT:-19866}"
data_node_container="dbx-webhdfs-datanode-${run_id//[^a-zA-Z0-9_.-]/-}"
workspace_base="${TMPDIR:-/tmp}"
workspace_base="${workspace_base%/}"
workspace="$(mktemp -d "${workspace_base}/dbx-webhdfs.XXXXXX")"
proxy_trace="${workspace}/datanode-proxy-trace.jsonl"
proxy_log="${workspace}/datanode-proxy.log"
proxy_pid=""

cleanup() {
  local result=$?
  trap - EXIT
  if [[ "${result}" -ne 0 ]]; then
    docker logs "${data_node_container}" >&2 2>/dev/null || true
    for log in "${proxy_log}" "${proxy_trace}"; do
      if [[ -s "${log}" ]]; then
        sed -E 's/(secret|token|password)[^[:space:]"]*/[REDACTED]/gi' "${log}" >&2 || true
      fi
    done
  fi
  if [[ -n "${proxy_pid}" ]]; then
    kill "${proxy_pid}" >/dev/null 2>&1 || true
    wait "${proxy_pid}" >/dev/null 2>&1 || true
  fi
  docker compose -f "${compose_file}" exec -T namenode \
    hdfs dfsadmin -clrSpaceQuota "${quota_root}" >/dev/null 2>&1 || true
  docker compose -f "${compose_file}" exec -T namenode \
    hdfs dfs -rm -r -skipTrash "${contract_base}" >/dev/null 2>&1 || true
  docker rm -fv "${data_node_container}" >/dev/null 2>&1 || true
  docker compose -f "${compose_file}" down --remove-orphans -v >/dev/null 2>&1 || true
  case "${workspace}" in
    "${workspace_base}"/dbx-webhdfs.*)
      find "${workspace}" -depth -delete >/dev/null 2>&1 || true
      ;;
  esac
  exit "${result}"
}
trap cleanup EXIT

for dependency in cargo curl docker node sed; do
  command -v "${dependency}" >/dev/null
done
docker info >/dev/null
docker compose -f "${compose_file}" up -d namenode
docker compose -f "${compose_file}" run -d \
  --no-deps \
  --name "${data_node_container}" \
  -p "127.0.0.1:${data_node_direct_port}:9864" \
  datanode >/dev/null

ready=0
for _ in $(seq 1 90); do
  if curl -fsS \
    "http://127.0.0.1:9870/webhdfs/v1/?op=GETFILESTATUS&user.name=hadoop" \
    >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "${ready}" -ne 1 ]]; then
  docker compose -f "${compose_file}" logs
  echo "WebHDFS did not become ready" >&2
  exit 1
fi

data_node_ready=0
for _ in $(seq 1 90); do
  report="$(docker compose -f "${compose_file}" exec -T namenode hdfs dfsadmin -report 2>/dev/null || true)"
  if grep -F "Live datanodes (1):" <<<"${report}" >/dev/null; then
    data_node_ready=1
    break
  fi
  sleep 1
done
if [[ "${data_node_ready}" -ne 1 ]]; then
  docker compose -f "${compose_file}" logs
  docker logs "${data_node_container}" >&2 2>/dev/null || true
  echo "WebHDFS DataNode did not become ready" >&2
  exit 1
fi

docker compose -f "${compose_file}" exec -T namenode bash -euc "
  hdfs dfs -mkdir -p '${contract_root}' '${permission_root}' '${quota_root}'
  hdfs dfs -chown -R '${contract_user}:supergroup' '${contract_root}' '${quota_root}'
  hdfs dfs -chmod 755 '${contract_root}' '${quota_root}'
  hdfs dfs -chown 'hadoop:supergroup' '${permission_root}'
  hdfs dfs -chmod 555 '${permission_root}'
  hdfs dfsadmin -setSpaceQuota 1048576 '${quota_root}'
"

DBX_HDFS_PROXY_PORT="${proxy_port}" \
DBX_HDFS_PROXY_CONTROL_PORT="${proxy_control_port}" \
DBX_HDFS_PROXY_UPSTREAM_PORT="${data_node_direct_port}" \
DBX_HDFS_PROXY_TRACE="${proxy_trace}" \
  node tests/hdfs-native/tcp-fault-proxy.mjs >"${proxy_log}" 2>&1 &
proxy_pid=$!

for _ in $(seq 1 100); do
  if curl --silent --show-error --fail \
    "http://127.0.0.1:${proxy_control_port}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl --silent --show-error --fail \
  "http://127.0.0.1:${proxy_control_port}/health" >/dev/null

export DBX_TEST_WEBHDFS_ENDPOINT="http://127.0.0.1:9870"
export DBX_TEST_WEBHDFS_DATANODE_ORIGIN="http://localhost:9864"
export DBX_TEST_WEBHDFS_DATANODE_MAPPING="localhost=127.0.0.1:${proxy_port}"
export DBX_TEST_WEBHDFS_ROOT="${contract_root}"
export DBX_TEST_WEBHDFS_PERMISSION_ROOT="${permission_root}"
export DBX_TEST_WEBHDFS_QUOTA_ROOT="${quota_root}"
export DBX_TEST_WEBHDFS_FAULT_CONTROL="http://127.0.0.1:${proxy_control_port}"
export DBX_TEST_WEBHDFS_FAULT_TRACE="${proxy_trace}"
export DBX_TEST_WEBHDFS_USER="${contract_user}"

run_exact_contract() {
  local test_name="$1"
  local output="${workspace}/contract-result-${test_name//::/-}.log"
  cargo test -p dbx --lib "${test_name}" -- \
    --ignored --exact --nocapture --test-threads=1 2>&1 | tee "${output}"
  grep -Fx "test ${test_name} ... ok" "${output}" >/dev/null
  grep -F 'test result: ok. 1 passed; 0 failed;' "${output}" >/dev/null
}

run_exact_contract commands::file_manager_webhdfs::tests::fixed_webhdfs_service_contract
run_exact_contract commands::file_transfer::tests::fixed_webhdfs_file_transfer_worker_contract
run_exact_contract commands::file_transfer::tests::fixed_webhdfs_permission_failure_contract
run_exact_contract commands::file_transfer::tests::fixed_webhdfs_quota_failure_contract
run_exact_contract commands::file_transfer::tests::fixed_webhdfs_datanode_disconnect_contract

rss_sizes="${DBX_TEST_WEBHDFS_RSS_SIZES_GIB:-}"
if [[ -n "${rss_sizes//[[:space:]]/}" ]]; then
  rss_sequence=""
  for gib in ${rss_sizes}; do
    if ! [[ "${gib}" =~ ^[1-9][0-9]*$ ]]; then
      echo "DBX_TEST_WEBHDFS_RSS_SIZES_GIB must contain positive integer GiB values" >&2
      exit 1
    fi
    rss_sequence="${rss_sequence:+${rss_sequence} }${gib}"
  done
  if [[ "${rss_sequence}" != "1 10 100" ]]; then
    echo "The production RSS gate requires DBX_TEST_WEBHDFS_RSS_SIZES_GIB='1 10 100'" >&2
    exit 1
  fi
  if [[ ! -x /usr/bin/time ]]; then
    echo "/usr/bin/time is required for the production RSS gate" >&2
    exit 1
  fi

  rss_local_dir="${workspace}/rss-local"
  rss_results="${workspace}/rss-results.tsv"
  cargo_json="${workspace}/cargo-test-artifacts.jsonl"
  mkdir -p "${rss_local_dir}"
  printf 'operation\tsize_gib\tsize_bytes\tpeak_rss_kib\topen_requests\n' >"${rss_results}"

  cargo test -p dbx --lib --release --no-run --message-format=json >"${cargo_json}"
  test_binary="$(
    node - "${cargo_json}" <<'NODE'
const fs = require("fs");
const artifacts = fs
  .readFileSync(process.argv[2], "utf8")
  .split(/\r?\n/)
  .filter(Boolean)
  .map((line) => JSON.parse(line))
  .filter(
    (message) =>
      message.reason === "compiler-artifact" &&
      message.profile?.test === true &&
      message.profile?.opt_level !== "0" &&
      message.profile?.debug_assertions === false &&
      message.target?.name === "dbx_lib" &&
      typeof message.executable === "string",
  )
  .map((message) => message.executable);
const unique = [...new Set(artifacts)];
if (unique.length !== 1) {
  process.stderr.write(`expected one release dbx lib test executable, found ${unique.length}\n`);
  process.exit(1);
}
process.stdout.write(unique[0]);
NODE
  )"
  if [[ ! -x "${test_binary}" ]]; then
    echo "Cargo reported a non-executable dbx lib test artifact: ${test_binary}" >&2
    exit 1
  fi

  rss_platform="$(uname -s)"
  case "${rss_platform}" in
    Darwin | Linux) ;;
    *)
      echo "The production RSS gate only supports Darwin and Linux, not ${rss_platform}" >&2
      exit 1
      ;;
  esac

  for operation in upload copy; do
    open_reference=""
    rss_min=""
    rss_max=""
    for gib in ${rss_sizes}; do
      size_bytes=$((gib * 1024 * 1024 * 1024))
      if [[ "${operation}" == "upload" ]]; then
        required_bytes=$(((size_bytes * 11 + 9) / 10 + 10 * 1024 * 1024 * 1024))
      else
        required_bytes=$(((size_bytes * 21 + 9) / 10 + 10 * 1024 * 1024 * 1024))
      fi
      available_kib="$(
        docker exec "${data_node_container}" \
          df -Pk /tmp/hadoop/data | awk 'NR == 2 { print $4 }'
      )"
      if ! [[ "${available_kib}" =~ ^[0-9]+$ ]]; then
        echo "Could not determine DataNode free space" >&2
        exit 1
      fi
      available_bytes=$((available_kib * 1024))
      if ((available_bytes < required_bytes)); then
        echo "Insufficient DataNode space for ${operation} ${gib} GiB: need ${required_bytes}, have ${available_bytes}" >&2
        exit 1
      fi

      case_root="${contract_base}/rss-${operation}-${gib}g"
      docker compose -f "${compose_file}" exec -T namenode bash -euc "
        hdfs dfs -mkdir -p '${case_root}'
        hdfs dfs -chown '${contract_user}:supergroup' '${case_root}'
        hdfs dfs -chmod 755 '${case_root}'
      "
      export DBX_TEST_WEBHDFS_ROOT="${case_root}"
      export DBX_TEST_WEBHDFS_RSS_OPERATION="${operation}"
      export DBX_TEST_WEBHDFS_RSS_SIZE_BYTES="${size_bytes}"
      export DBX_TEST_WEBHDFS_RSS_LOCAL_DIR="${rss_local_dir}"

      if [[ "${operation}" == "copy" ]]; then
        seed_log="${workspace}/rss-copy-${gib}g-seed.log"
        if ! "${test_binary}" \
          --ignored --exact commands::file_transfer::tests::fixed_webhdfs_production_rss_seed_contract \
          --nocapture >"${seed_log}" 2>&1; then
          sed -E 's/(secret|token|password)[^[:space:]"]*/[REDACTED]/gi' "${seed_log}" >&2 || true
          exit 1
        fi
      fi

      case_log="${workspace}/rss-${operation}-${gib}g.log"
      case_metrics="${workspace}/rss-${operation}-${gib}g.time"
      if [[ "${rss_platform}" == "Darwin" ]]; then
        time_args=(-l -o "${case_metrics}")
      else
        time_args=(-v -o "${case_metrics}")
      fi
      if ! /usr/bin/time "${time_args[@]}" \
        "${test_binary}" \
        --ignored --exact commands::file_transfer::tests::fixed_webhdfs_production_worker_rss_contract \
        --nocapture >"${case_log}" 2>&1; then
        sed -E 's/(secret|token|password)[^[:space:]"]*/[REDACTED]/gi' "${case_log}" >&2 || true
        sed -E 's/(secret|token|password)[^[:space:]"]*/[REDACTED]/gi' "${case_metrics}" >&2 || true
        exit 1
      fi
      marker_count="$(grep -c 'DBX_WEBHDFS_RSS ' "${case_log}" || true)"
      if [[ "${marker_count}" -ne 1 ]]; then
        echo "Expected one DBX_WEBHDFS_RSS marker for ${operation} ${gib} GiB, found ${marker_count}" >&2
        exit 1
      fi
      marker="$(grep 'DBX_WEBHDFS_RSS ' "${case_log}")"
      reported_operation="$(sed -E 's/.* operation=([^ ]+).*/\1/' <<<"${marker}")"
      reported_size="$(sed -E 's/.* size_bytes=([0-9]+).*/\1/' <<<"${marker}")"
      reported_bytes="$(sed -E 's/.* bytes_transferred=([0-9]+).*/\1/' <<<"${marker}")"
      open_requests="$(sed -E 's/.* namenode_open_requests=([0-9]+).*/\1/' <<<"${marker}")"
      if [[ "${reported_operation}" != "${operation}" || "${reported_size}" != "${size_bytes}" || \
        "${reported_bytes}" != "${size_bytes}" || ! "${open_requests}" =~ ^[0-9]+$ ]]; then
        echo "Invalid production RSS marker: ${marker}" >&2
        exit 1
      fi

      if [[ "${rss_platform}" == "Darwin" ]]; then
        peak_rss_native="$(awk '/maximum resident set size/ { print $1 }' "${case_metrics}" | tail -n 1)"
        peak_rss_kib=$(((peak_rss_native + 1023) / 1024))
      else
        peak_rss_kib="$(
          awk -F: '/Maximum resident set size/ { gsub(/[[:space:]]/, "", $2); print $2 }' \
            "${case_metrics}" | tail -n 1
        )"
      fi
      if ! [[ "${peak_rss_kib}" =~ ^[0-9]+$ ]] || ((peak_rss_kib <= 0)); then
        echo "Could not parse peak RSS for ${operation} ${gib} GiB" >&2
        exit 1
      fi
      if ((peak_rss_kib > 256 * 1024)); then
        echo "Peak RSS ${peak_rss_kib} KiB exceeds 256 MiB for ${operation} ${gib} GiB" >&2
        exit 1
      fi
      if [[ -z "${open_reference}" ]]; then
        open_reference="${open_requests}"
      elif [[ "${open_requests}" != "${open_reference}" ]]; then
        echo "OPEN request count grew across ${operation} sizes: expected ${open_reference}, got ${open_requests}" >&2
        exit 1
      fi
      if [[ -z "${rss_min}" ]] || ((peak_rss_kib < rss_min)); then
        rss_min="${peak_rss_kib}"
      fi
      if [[ -z "${rss_max}" ]] || ((peak_rss_kib > rss_max)); then
        rss_max="${peak_rss_kib}"
      fi
      printf '%s\t%s\t%s\t%s\t%s\n' \
        "${operation}" "${gib}" "${size_bytes}" "${peak_rss_kib}" "${open_requests}" | tee -a "${rss_results}"

      docker compose -f "${compose_file}" exec -T namenode \
        hdfs dfs -rm -r -skipTrash "${case_root}" >/dev/null
      if docker compose -f "${compose_file}" exec -T namenode \
        hdfs dfs -test -e "${case_root}"; then
        echo "RSS case root still exists after cleanup: ${case_root}" >&2
        exit 1
      fi
    done
    if ((rss_max - rss_min > 64 * 1024)); then
      echo "${operation} RSS spread $((rss_max - rss_min)) KiB exceeds the 64 MiB plateau threshold" >&2
      exit 1
    fi
  done

  cat "${rss_results}"
elif [[ "${DBX_REQUIRE_WEBHDFS_RSS:-0}" == "1" ]]; then
  echo "The WebHDFS production RSS gate was required but DBX_TEST_WEBHDFS_RSS_SIZES_GIB was not set" >&2
  exit 3
fi
