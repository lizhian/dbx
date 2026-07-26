#!/usr/bin/env bash
set -euo pipefail

hadoop_image="apache/hadoop:3.4.1@sha256:69ffa97339aff768c4e6120c3fb27aa04c121402b1c8158408a5fb5be586a30e"
namenode_port="${DBX_TEST_HDFS_NATIVE_NAMENODE_PORT:-29000}"
namenode_direct_port="${DBX_TEST_HDFS_NATIVE_NAMENODE_DIRECT_PORT:-29001}"
namenode_control_port="${DBX_TEST_HDFS_NATIVE_NAMENODE_CONTROL_PORT:-29002}"
datanode_port=29866
datanode_direct_port="${DBX_TEST_HDFS_NATIVE_DATANODE_DIRECT_PORT:-29869}"
datanode_control_port="${DBX_TEST_HDFS_NATIVE_DATANODE_CONTROL_PORT:-29870}"
contract_user="dbx-hdfs-contract"
contract_root="/tenant/root"
smoke_only="${DBX_TEST_HDFS_NATIVE_SMOKE_ONLY:-0}"
require_full_contract="${DBX_REQUIRE_FULL_CONTRACT:-0}"
contract_filter="${DBX_TEST_HDFS_NATIVE_CONTRACT_FILTER:-all}"
suffix="${RANDOM}-$$"
network="dbx-hdfs-native-${suffix}"
namenode="dbx-hdfs-native-nn-${suffix}"
datanode="dbx-hdfs-native-dn-${suffix}"
workspace_base="${DBX_TEST_HDFS_NATIVE_WORKSPACE_BASE:-${TMPDIR:-/tmp}}"
workspace_base="${workspace_base%/}"
workspace="$(mktemp -d "${workspace_base}/dbx-hdfs-native.XXXXXX")"
vendor_target_dir="$(pwd)/target/hdfs-native-vendor"
client_config="${workspace}/client-config"
rejected_client_config="${workspace}/rejected-client-config"
ambient_config="${workspace}/ambient-config"
ambient_home="${workspace}/ambient-home"
namenode_trace="${workspace}/namenode-trace.jsonl"
datanode_trace="${workspace}/datanode-trace.jsonl"
namenode_proxy_log="${workspace}/namenode-proxy.log"
datanode_proxy_log="${workspace}/datanode-proxy.log"
contract_output="${workspace}/contract-output.log"
namenode_proxy_pid=""
datanode_proxy_pid=""
started_namenode=0
started_datanode=0
started_network=0

validate_port() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || ((value < 1 || value > 65535)); then
    echo "${name} must be an integer between 1 and 65535" >&2
    exit 2
  fi
}

for pair in \
  "namenode_port:${namenode_port}" \
  "namenode_direct_port:${namenode_direct_port}" \
  "namenode_control_port:${namenode_control_port}" \
  "datanode_direct_port:${datanode_direct_port}" \
  "datanode_control_port:${datanode_control_port}"; do
  validate_port "${pair%%:*}" "${pair#*:}"
done

case "$(uname -s)" in
  Darwin | Linux) ;;
  *)
    echo "The Docker-backed HDFS Native service contract runs on macOS/Linux; Windows uses a native runner plus tests/hdfs-native/windows-dependency-contract.sh" >&2
    exit 2
    ;;
esac

case "${contract_filter}" in
  all)
    contract_tests=(
      commands::file_manager::tests::fixed_hdfs_native_service_contract
      commands::file_transfer::tests::fixed_hdfs_native_transfer_contract
    )
    ;;
  service)
    contract_tests=(commands::file_manager::tests::fixed_hdfs_native_service_contract)
    ;;
  transfer)
    contract_tests=(commands::file_transfer::tests::fixed_hdfs_native_transfer_contract)
    ;;
  *)
    echo "DBX_TEST_HDFS_NATIVE_CONTRACT_FILTER must be all, service, or transfer" >&2
    exit 2
    ;;
esac

cleanup() {
  local result=$?
  trap - EXIT

  if [[ "${result}" -ne 0 ]]; then
    for container in "${namenode}" "${datanode}"; do
      docker inspect \
        --format 'container={{.Name}} status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}' \
        "${container}" >&2 2>/dev/null || true
      docker logs "${container}" >&2 2>/dev/null || true
    done
    for log in "${namenode_proxy_log}" "${datanode_proxy_log}" "${namenode_trace}" "${datanode_trace}"; do
      if [[ -s "${log}" ]]; then
        sed -E 's/(secret|token|password)[^[:space:]"]*/[REDACTED]/gi' "${log}" >&2 || true
      fi
    done
  fi

  for pid in "${namenode_proxy_pid}" "${datanode_proxy_pid}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" >/dev/null 2>&1 || true
    fi
  done
  if [[ "${started_datanode}" == 1 ]]; then
    docker rm -fv "${datanode}" >/dev/null 2>&1 || true
  fi
  if [[ "${started_namenode}" == 1 ]]; then
    docker rm -fv "${namenode}" >/dev/null 2>&1 || true
  fi
  if [[ "${started_network}" == 1 ]]; then
    docker network rm "${network}" >/dev/null 2>&1 || true
  fi
  case "${workspace}" in
    "${workspace_base}"/dbx-hdfs-native.*)
      find "${workspace}" -depth -delete >/dev/null 2>&1 || true
      ;;
  esac
  exit "${result}"
}
trap cleanup EXIT

for dependency in cargo curl docker nc node rg sed tee; do
  command -v "${dependency}" >/dev/null
done
docker info >/dev/null
node --test tests/hdfs-native/tcp-fault-proxy.test.mjs
if [[ "${smoke_only}" != 1 ]]; then
  bash tests/hdfs-native/windows-dependency-contract.sh
  vendor_listed_tests="$(
    CARGO_TARGET_DIR="${vendor_target_dir}" cargo test \
      --manifest-path vendor/hdfs-native/Cargo.toml \
      -- \
      --list
  )"
  for vendor_contract in \
    pipeline_drop_tests \
    block_reader_drop_tests \
    rpc_connection_drop_tests \
    datanode_cache_contract_tests \
    connect_task_drop_tests \
    lease_renewal_drop_tests; do
    grep -F "${vendor_contract}" <<<"${vendor_listed_tests}" |
      grep -F ': test' >/dev/null
    CARGO_TARGET_DIR="${vendor_target_dir}" cargo test \
      --manifest-path vendor/hdfs-native/Cargo.toml \
      "${vendor_contract}"
  done

  # Build before starting Hadoop so compilation cannot consume readiness or
  # fault-injection windows.
  cargo test -p dbx --lib fixed_hdfs_native_ --no-default-features --no-run
  listed_tests="$(
    cargo test -p dbx --lib --no-default-features -- --list
  )"
  for required_test in "${contract_tests[@]}"; do
    grep -Fx "${required_test}: test" <<<"${listed_tests}" >/dev/null
  done
fi

mkdir -p \
  "${client_config}" \
  "${rejected_client_config}" \
  "${ambient_config}" \
  "${ambient_home}/etc/hadoop"
chmod 700 \
  "${workspace}" \
  "${client_config}" \
  "${rejected_client_config}" \
  "${ambient_config}" \
  "${ambient_home}" \
  "${ambient_home}/etc" \
  "${ambient_home}/etc/hadoop"
cp tests/hdfs-native/client-core-site.xml "${client_config}/core-site.xml"
cp tests/hdfs-native/client-hdfs-site.xml "${client_config}/hdfs-site.xml"
cp tests/hdfs-native/rejected-core-site.xml "${rejected_client_config}/core-site.xml"
cp tests/hdfs-native/rejected-hdfs-site.xml "${rejected_client_config}/hdfs-site.xml"
cp tests/hdfs-native/rejected-core-site.xml "${ambient_config}/core-site.xml"
cp tests/hdfs-native/rejected-hdfs-site.xml "${ambient_config}/hdfs-site.xml"
cp tests/hdfs-native/rejected-core-site.xml "${ambient_home}/etc/hadoop/core-site.xml"
cp tests/hdfs-native/rejected-hdfs-site.xml "${ambient_home}/etc/hadoop/hdfs-site.xml"
chmod 600 \
  "${client_config}/core-site.xml" \
  "${client_config}/hdfs-site.xml" \
  "${rejected_client_config}/core-site.xml" \
  "${rejected_client_config}/hdfs-site.xml" \
  "${ambient_config}/core-site.xml" \
  "${ambient_config}/hdfs-site.xml" \
  "${ambient_home}/etc/hadoop/core-site.xml" \
  "${ambient_home}/etc/hadoop/hdfs-site.xml"

# The host-side client cannot route to a Docker-internal DataNode IP. The
# explicit config directory must therefore select the advertised hostname,
# which resolves to the host-side DataTransfer proxy.
grep -F '<name>dfs.client.use.datanode.hostname</name>' \
  "${client_config}/hdfs-site.xml" >/dev/null
grep -A1 -F '<name>dfs.client.use.datanode.hostname</name>' \
  "${client_config}/hdfs-site.xml" |
  grep -F '<value>true</value>' >/dev/null

docker network create "${network}" >/dev/null
started_network=1

started_namenode=1
docker run -d \
  --platform linux/amd64 \
  --name "${namenode}" \
  --hostname namenode \
  --network "${network}" \
  -p "127.0.0.1:${namenode_direct_port}:9000" \
  -v "$(pwd)/tests/hdfs-native/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro" \
  -v "$(pwd)/tests/hdfs-native/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro" \
  "${hadoop_image}" \
  bash -lc '
    if [ ! -d /tmp/hadoop/name/current ]; then
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode
  ' >/dev/null

for _ in $(seq 1 120); do
  if nc -z 127.0.0.1 "${namenode_direct_port}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
nc -z 127.0.0.1 "${namenode_direct_port}" >/dev/null

started_datanode=1
docker run -d \
  --platform linux/amd64 \
  --name "${datanode}" \
  --hostname datanode \
  --network "${network}" \
  -p "127.0.0.1:${datanode_direct_port}:${datanode_port}" \
  -v "$(pwd)/tests/hdfs-native/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro" \
  -v "$(pwd)/tests/hdfs-native/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro" \
  "${hadoop_image}" \
  hdfs datanode >/dev/null

for _ in $(seq 1 120); do
  report="$(docker exec "${namenode}" hdfs dfsadmin -report 2>/dev/null || true)"
  if grep -F 'Live datanodes (1):' <<<"${report}" >/dev/null; then
    break
  fi
  sleep 1
done
grep -F 'Live datanodes (1):' <<<"${report}" >/dev/null
grep -F 'Hostname: 127.0.0.1' <<<"${report}" >/dev/null
if grep -F "Name: 127.0.0.1:${datanode_port}" <<<"${report}" >/dev/null; then
  echo "DataNode unexpectedly advertised a host-routable IP; hostname routing is not being tested" >&2
  exit 1
fi

DBX_HDFS_PROXY_PORT="${namenode_port}" \
DBX_HDFS_PROXY_CONTROL_PORT="${namenode_control_port}" \
DBX_HDFS_PROXY_UPSTREAM_PORT="${namenode_direct_port}" \
DBX_HDFS_PROXY_TRACE="${namenode_trace}" \
  node tests/hdfs-native/tcp-fault-proxy.mjs >"${namenode_proxy_log}" 2>&1 &
namenode_proxy_pid=$!

DBX_HDFS_PROXY_PORT="${datanode_port}" \
DBX_HDFS_PROXY_CONTROL_PORT="${datanode_control_port}" \
DBX_HDFS_PROXY_UPSTREAM_PORT="${datanode_direct_port}" \
DBX_HDFS_PROXY_TRACE="${datanode_trace}" \
  node tests/hdfs-native/tcp-fault-proxy.mjs >"${datanode_proxy_log}" 2>&1 &
datanode_proxy_pid=$!

for control_port in "${namenode_control_port}" "${datanode_control_port}"; do
  for _ in $(seq 1 100); do
    if curl --silent --show-error --fail \
      "http://127.0.0.1:${control_port}/health" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
  curl --silent --show-error --fail \
    "http://127.0.0.1:${control_port}/health" >/dev/null
done

docker exec -e HADOOP_USER_NAME=hadoop "${namenode}" bash -euc "
  rm -rf /tmp/dbx-hdfs-native-seed
  mkdir -p /tmp/dbx-hdfs-native-seed/nested
  printf 'hdfs native fixture\n' > /tmp/dbx-hdfs-native-seed/fixture.txt
  printf 'nested fixture\n' > /tmp/dbx-hdfs-native-seed/nested/child.txt
  for index in \$(seq -w 1 205); do
    printf 'page fixture %s\n' \"\${index}\" > /tmp/dbx-hdfs-native-seed/page-\"\${index}\".txt
  done
  hdfs dfs -mkdir -p '${contract_root}'
  hdfs dfs -put /tmp/dbx-hdfs-native-seed/* '${contract_root}/'
  hdfs dfs -mkdir '${contract_root}/empty' '${contract_root}/denied'
  printf 'permission canary\n' | hdfs dfs -put - '${contract_root}/denied/secret.txt'
  printf 'outside root canary\n' | hdfs dfs -put - '/tenant/outside-root-canary.txt'
  hdfs dfs -chown -R '${contract_user}:supergroup' '/tenant'
  hdfs dfs -chown \
    'hadoop:supergroup' \
    '${contract_root}/denied' \
    '/tenant/outside-root-canary.txt'
  hdfs dfs -chmod 000 '${contract_root}/denied'
  rm -rf /tmp/dbx-hdfs-native-seed
"

# The environment variable contains the ambient simple-auth identity. The
# product receives only its standard name and must never mutate it.
export HADOOP_USER_NAME="${contract_user}"
export DBX_TEST_HDFS_NATIVE_NAMENODE="hdfs://127.0.0.1:${namenode_port}"
export DBX_TEST_HDFS_NATIVE_DIRECT_NAMENODE="hdfs://127.0.0.1:${namenode_direct_port}"
export DBX_TEST_HDFS_NATIVE_ROOT="${contract_root}"
export DBX_TEST_HDFS_NATIVE_HADOOP_CONFIG_DIR="${client_config}"
export DBX_TEST_HDFS_NATIVE_REJECTED_CONFIG_DIR="${rejected_client_config}"
export DBX_TEST_HDFS_NATIVE_AMBIENT_CONFIG_DIR="${ambient_config}"
export DBX_TEST_HDFS_NATIVE_AMBIENT_HOME="${ambient_home}"
export DBX_TEST_HDFS_NATIVE_AUTHENTICATION_ENVIRONMENT="HADOOP_USER_NAME"
export DBX_TEST_HDFS_NATIVE_NAMENODE_CONTAINER="${namenode}"
export DBX_TEST_HDFS_NATIVE_DATANODE_CONTAINER="${datanode}"
export DBX_TEST_HDFS_NATIVE_NAMENODE_FAULT_CONTROL="http://127.0.0.1:${namenode_control_port}"
export DBX_TEST_HDFS_NATIVE_DATANODE_FAULT_CONTROL="http://127.0.0.1:${datanode_control_port}"
export DBX_TEST_HDFS_NATIVE_NAMENODE_PROXY_TRACE="${namenode_trace}"
export DBX_TEST_HDFS_NATIVE_DATANODE_PROXY_TRACE="${datanode_trace}"
export DBX_TEST_HDFS_NATIVE_CONTRACT_USER="${contract_user}"

if [[ "${smoke_only}" != 1 ]]; then
  ambient_result_output="${workspace}/ambient-contract-result.log"
  HADOOP_CONF_DIR="${ambient_config}" \
  HADOOP_HOME="${ambient_home}" \
    cargo test -p dbx --lib \
      commands::file_manager_hdfs_native::tests::ambient_hdfs_native_config_contract \
      --no-default-features -- \
      --ignored --exact --test-threads=1 2>&1 |
      tee -a "${contract_output}" "${ambient_result_output}"
  grep -Fx \
    'test commands::file_manager_hdfs_native::tests::ambient_hdfs_native_config_contract ... ok' \
    "${ambient_result_output}" >/dev/null
  grep -F 'test result: ok. 1 passed; 0 failed;' "${ambient_result_output}" >/dev/null

  contract_index=0
  for contract_test in "${contract_tests[@]}"; do
    contract_index=$((contract_index + 1))
    contract_result_output="${workspace}/product-contract-result-${contract_index}.log"
    env -u HADOOP_CONF_DIR -u HADOOP_HOME \
      cargo test -p dbx --lib "${contract_test}" --no-default-features -- \
        --ignored --exact --test-threads=1 2>&1 |
        tee -a "${contract_output}" "${contract_result_output}"
    grep -Fx "test ${contract_test} ... ok" "${contract_result_output}" >/dev/null
    grep -F 'test result: ok. 1 passed; 0 failed;' "${contract_result_output}" >/dev/null
  done

  if [[ "${contract_filter}" == all || "${contract_filter}" == transfer ]]; then
    node - "${namenode_trace}" <<'NODE'
const fs = require("node:fs");
const events = fs
  .readFileSync(process.argv[2], "utf8")
  .trim()
  .split("\n")
  .filter(Boolean)
  .map((line) => JSON.parse(line));

for (const label of ["hdfs-rename-namenode-reset-recovery", "hdfs-rename-response-loss"]) {
  const bound = events.find((event) => event.event === "bind" && event.label === label);
  const triggered = events.find((event) => event.event === "trigger" && event.label === label);
  if (
    !bound ||
    !triggered ||
    bound.pairId !== triggered.pairId ||
    triggered.boundPairId !== bound.pairId ||
    triggered.scope !== "next"
  ) {
    throw new Error(`invalid NameNode fault binding for ${label}`);
  }
  console.log(
    `HDFS Native NameNode fault ${label}: bindPair=${bound.pairId} triggerPair=${triggered.pairId} action=${triggered.action}`,
  );
}
NODE
  fi

  # Product tests must actually exercise both RPC and DataTransfer proxies.
  grep -F '"event":"open"' "${namenode_trace}" >/dev/null
  grep -F '"event":"open"' "${datanode_trace}" >/dev/null
  docker exec -e HADOOP_USER_NAME=hadoop "${namenode}" bash -euc "
    test \"\$(hdfs dfs -cat '/tenant/outside-root-canary.txt')\" = 'outside root canary'
    residue=\"\$(
      hdfs dfs -find '${contract_root}' -name '.dbx-connection-test-*' -print
      hdfs dfs -find '${contract_root}' -name '.dbx-upload-*' -print
      hdfs dfs -find '${contract_root}' -name '.dbx-copy-*' -print
      hdfs dfs -find '${contract_root}' -name '*.part' -print
    )\"
    if [ -n \"\${residue}\" ]; then
      printf 'HDFS Native contract left owned temporary paths:\n%s\n' \"\${residue}\" >&2
      exit 1
    fi
  "

  # The product must not persist or echo ambient environment values.
  if rg -F "${contract_user}" \
    "${workspace}" \
    --glob '!*-trace.jsonl' \
    --glob '!core-site.xml' \
    --glob '!hdfs-site.xml' >/dev/null 2>&1; then
    echo "Ambient HDFS identity leaked into a generated artifact" >&2
    exit 1
  fi
  if rg -i \
    'token identifier|block[_ ]?token|rpc.?sasl|sasl (message|payload|response)|delegation.?token|tokenproto|password: \[' \
    "${contract_output}" >/dev/null 2>&1; then
    echo "HDFS Native protocol credential material reached application test output" >&2
    exit 1
  fi
fi

if [[ "${smoke_only}" == 1 ]]; then
  if [[ "${require_full_contract}" == 1 ]]; then
    echo "HDFS Native fixture smoke completed, but the required product contract did not run" >&2
    exit 3
  fi
  exit 0
fi
