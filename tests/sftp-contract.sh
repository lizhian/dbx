#!/usr/bin/env bash
set -euo pipefail

# LinuxServer's immutable multi-architecture manifest for OpenSSH 10.0p1-r9.
image="lscr.io/linuxserver/openssh-server@sha256:3acac97f1b835860fbcc7fd4e9d6be3e0571f4742cdec97c1fe08ed07b8fc24c"
port="${DBX_TEST_SFTP_PORT:-22220}"
stall_port="${DBX_TEST_SFTP_STALL_PORT:-22221}"
proxy_port="${DBX_TEST_SFTP_PROXY_PORT:-22222}"
proxy_control_port="${DBX_TEST_SFTP_PROXY_CONTROL_PORT:-22223}"
smoke_only="${DBX_TEST_SFTP_SMOKE_ONLY:-0}"
require_full_contract="${DBX_REQUIRE_FULL_CONTRACT:-0}"
container=""
agent_pid=""
stall_pid=""
proxy_pid=""
contract_root="/home/dbx/files"
host_alias="dbx-sftp-contract"
username="dbx"
key_passphrase="dbx-sftp-contract-secret-passphrase"
original_home="${HOME:?HOME is required}"
original_path="${PATH:?PATH is required}"
cargo_home="${CARGO_HOME:-${original_home}/.cargo}"
rustup_home="${RUSTUP_HOME:-${original_home}/.rustup}"
workspace_base="${DBX_TEST_SFTP_WORKSPACE_BASE:-/tmp}"
[[ "${workspace_base}" == /* ]]
workspace_base="${workspace_base%/}"
workspace="$(mktemp -d "${workspace_base}/dbx-sftp.XXXXXX")"
server_config="${workspace}/server-config"
public_keys="${workspace}/public-keys"
contract_home="${workspace}/home"
ssh_dir="${contract_home}/.ssh"
wrapper_dir="${workspace}/bin"
runtime_tmp="${workspace}/runtime-tmp"
key_residue_dir="${runtime_tmp}/dbx-sftp-keys-$(id -u)"
key_residue_file="${key_residue_dir}/dbx-sftp-key-999999999-00000000-0000-4000-8000-000000000001.key"
known_hosts="${ssh_dir}/known_hosts"
mismatch_known_hosts="${ssh_dir}/known_hosts-mismatch"
ssh_config="${ssh_dir}/config"
mismatch_config="${ssh_dir}/config-mismatch"
config_key="${workspace}/config-key"
agent_key="${workspace}/agent-key"
encrypted_key="${workspace}/encrypted-key"
agent_socket="${workspace}/agent.sock"
stall_log="${workspace}/stall.log"
proxy_log="${workspace}/proxy.log"
proxy_trace="${workspace}/proxy-trace.jsonl"
ssh_trace="${workspace}/ssh-trace.log"

stop_sftp() {
  if [[ -z "${container}" ]]; then
    return
  fi
  docker rm -f "${container}" >/dev/null 2>&1 || true
  container=""
  for _ in $(seq 1 50); do
    if ! nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  ! nc -z 127.0.0.1 "${port}" >/dev/null 2>&1
}

cleanup() {
  result=$?
  trap - EXIT
  if [[ "${result}" -ne 0 && -n "${container}" ]]; then
    docker inspect \
      --format 'container={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}' \
      "${container}" >&2 || true
    docker logs "${container}" >&2 || true
  fi
  if [[ "${result}" -ne 0 && -s "${stall_log}" ]]; then
    cat "${stall_log}" >&2
  fi
  if [[ "${result}" -ne 0 && -s "${proxy_log}" ]]; then
    cat "${proxy_log}" >&2
  fi
  if [[ "${result}" -ne 0 && -s "${proxy_trace}" ]]; then
    cat "${proxy_trace}" >&2
  fi
  if [[ "${result}" -ne 0 && -s "${ssh_trace}" ]]; then
    sed -E 's#[^[:space:]"]*dbx-sftp-keys-[^[:space:]"]*#[SFTP_KEY_MATERIAL]#g' "${ssh_trace}" >&2
  fi
  stop_sftp || true
  if [[ -n "${stall_pid}" ]]; then
    kill "${stall_pid}" >/dev/null 2>&1 || true
    wait "${stall_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${proxy_pid}" ]]; then
    kill "${proxy_pid}" >/dev/null 2>&1 || true
    wait "${proxy_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${agent_pid}" ]]; then
    kill "${agent_pid}" >/dev/null 2>&1 || true
  fi
  case "${workspace}" in
    "${workspace_base}"/dbx-sftp.*)
      find "${workspace}" -depth -delete >/dev/null 2>&1 || true
      ;;
  esac
  exit "${result}"
}
trap cleanup EXIT

for dependency in cargo curl docker nc node ssh ssh-add ssh-agent ssh-keygen ssh-keyscan sftp; do
  command -v "${dependency}" >/dev/null
done
docker info >/dev/null
bash tests/sftp/windows-cfg-contract.sh

# Compile before starting the fixture so a cold Rust build does not consume the
# server readiness or fault-injection windows. Maintainers can isolate Docker,
# OpenSSH and fault-fixture diagnostics with DBX_TEST_SFTP_SMOKE_ONLY=1.
if [[ "${smoke_only}" != "1" ]]; then
  cargo test -p dbx --lib fixed_sftp_ --no-default-features --no-run
fi

mkdir -p "${server_config}" "${public_keys}" "${ssh_dir}" "${wrapper_dir}" "${runtime_tmp}"
chmod 700 \
  "${workspace}" \
  "${server_config}" \
  "${public_keys}" \
  "${contract_home}" \
  "${ssh_dir}" \
  "${wrapper_dir}" \
  "${runtime_tmp}"

ssh-keygen -q -t ed25519 -N "" -C dbx-sftp-config-contract -f "${config_key}"
ssh-keygen -q -t ed25519 -N "" -C dbx-sftp-agent-contract -f "${agent_key}"
ssh-keygen -q -t ed25519 -N "${key_passphrase}" -C dbx-sftp-inline-contract -f "${encrypted_key}"
chmod 600 "${config_key}" "${agent_key}" "${encrypted_key}"
cp "${config_key}.pub" "${public_keys}/config.pub"
cp "${agent_key}.pub" "${public_keys}/agent.pub"
cp "${encrypted_key}.pub" "${public_keys}/inline.pub"
chmod 600 "${public_keys}"/*.pub

# Prove that the inline fixture really is encrypted and the advertised
# passphrase decrypts it. The product receives the encrypted OpenSSH material,
# never a pre-decrypted substitute.
if ssh-keygen -y -P definitely-wrong -f "${encrypted_key}" >/dev/null 2>&1; then
  echo "Encrypted SFTP fixture unexpectedly accepted the wrong passphrase" >&2
  exit 1
fi
ssh-keygen -y -P "${key_passphrase}" -f "${encrypted_key}" >/dev/null

ssh-agent -a "${agent_socket}" >"${workspace}/agent.env"
agent_pid="$(awk -F '[=;]' '/^SSH_AGENT_PID=/{print $2}' "${workspace}/agent.env")"
[[ "${agent_pid}" =~ ^[0-9]+$ ]]
export SSH_AUTH_SOCK="${agent_socket}"
ssh-add "${agent_key}" >/dev/null 2>&1
[[ "$(ssh-add -l | wc -l | tr -d ' ')" == "1" ]]

real_ssh="$(command -v ssh)"
real_sftp="$(command -v sftp)"
cp tests/sftp/ssh-wrapper.sh "${wrapper_dir}/ssh"
chmod 700 "${wrapper_dir}/ssh"

DBX_SFTP_STALL_PORT="${stall_port}" node tests/sftp/stall-server.mjs >"${stall_log}" 2>&1 &
stall_pid=$!
for _ in $(seq 1 50); do
  if nc -z 127.0.0.1 "${stall_port}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
nc -z 127.0.0.1 "${stall_port}" >/dev/null 2>&1

DBX_SFTP_PROXY_PORT="${proxy_port}" \
DBX_SFTP_PROXY_CONTROL_PORT="${proxy_control_port}" \
DBX_SFTP_PROXY_UPSTREAM_HOST=127.0.0.1 \
DBX_SFTP_PROXY_UPSTREAM_PORT="${port}" \
DBX_SFTP_PROXY_TRACE="${proxy_trace}" \
  node tests/sftp/tcp-fault-proxy.mjs >"${proxy_log}" 2>&1 &
proxy_pid=$!
for _ in $(seq 1 50); do
  if curl --silent --show-error --fail \
    "http://127.0.0.1:${proxy_control_port}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl --silent --show-error --fail \
  "http://127.0.0.1:${proxy_control_port}/health" >/dev/null

start_sftp() {
  local suffix="$1"
  stop_sftp
  container="dbx-sftp-contract-${suffix}-${RANDOM}"
  docker run -d \
    --name "${container}" \
    -e "PUID=$(id -u)" \
    -e "PGID=$(id -g)" \
    -e "USER_NAME=${username}" \
    -e PASSWORD_ACCESS=false \
    -e SUDO_ACCESS=false \
    -e PUBLIC_KEY_DIR=/contract-public-keys \
    -p "127.0.0.1:${port}:2222" \
    -v "${server_config}:/config" \
    -v "${public_keys}:/contract-public-keys:ro" \
    "${image}" >/dev/null

  for _ in $(seq 1 60); do
    if nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  nc -z 127.0.0.1 "${port}" >/dev/null 2>&1

  version="$(docker exec "${container}" ssh -V 2>&1)"
  [[ "${version}" == OpenSSH_10.0p1* ]]

  docker exec "${container}" sh -euc "
    mkdir -p '${contract_root}/nested'
    mkdir -p '/home/dbx/outside'
    printf 'dbx sftp fixture\n' > '${contract_root}/fixture.txt'
    printf 'nested fixture\n' > '${contract_root}/nested/child.txt'
    printf 'outside read sentinel\n' > '/home/dbx/outside/read-secret.txt'
    printf 'outside delete sentinel\n' > '/home/dbx/outside/delete-victim.txt'
    printf 'outside copy sentinel\n' > '/home/dbx/outside/copy-source.txt'
    printf 'outside rename sentinel\n' > '/home/dbx/outside/rename-source.txt'
    printf 'regular file root sentinel\n' > '/home/dbx/root-file'
    mkdir -p '${contract_root}/a'
    printf 'literal-percent-space\n' > '${contract_root}/a%20b'
    printf 'actual-space\n' > '${contract_root}/a b'
    printf 'literal-percent-slash\n' > '${contract_root}/a%2Fb'
    printf 'nested-slash\n' > '${contract_root}/a/b'
    dd if=/dev/zero of='${contract_root}/large.bin' bs=1M count=32 status=none
    for index in \$(seq -w 1 205); do
      printf 'page fixture %s\n' \"\${index}\" > '${contract_root}/page-'\"\${index}\"'.txt'
    done
    chown -R '${username}:${username}' '${contract_root}' '/home/dbx/outside' '/home/dbx/root-file'
    ln -s '/home/dbx/outside' '${contract_root}/escape'
    ln -s '/etc' '${contract_root}/escape-etc'
    mkdir -p '${contract_root}/denied'
    chown root:root '${contract_root}/denied'
    chmod 000 '${contract_root}/denied'
  "

  scanned_hosts=""
  for _ in $(seq 1 30); do
    scanned_hosts="$(ssh-keyscan -T 2 -p "${port}" 127.0.0.1 2>/dev/null || true)"
    if [[ -n "${scanned_hosts}" ]]; then
      break
    fi
    sleep 0.2
  done
  [[ -n "${scanned_hosts}" ]]
  printf '%s\n' "${scanned_hosts}" >"${known_hosts}"
  printf '%s\n' "${scanned_hosts}" \
    | sed -E "s/^\\[127\\.0\\.0\\.1\\]:${port}/[${host_alias}]:${port}/" \
    >>"${known_hosts}"
  printf '%s\n' "${scanned_hosts}" \
    | sed -E "s/^\\[127\\.0\\.0\\.1\\]:${port}/${host_alias}/" \
    >>"${known_hosts}"
  printf '%s\n' "${scanned_hosts}" \
    | sed -E "s/^\\[127\\.0\\.0\\.1\\]:${port}/[127.0.0.1]:${proxy_port}/" \
    >>"${known_hosts}"
  chmod 600 "${known_hosts}"
}

assert_symlink_escape_integrity() {
  docker exec "${container}" sh -euc "
    test \"\$(cat '/home/dbx/outside/read-secret.txt')\" = 'outside read sentinel'
    test \"\$(cat '/home/dbx/outside/delete-victim.txt')\" = 'outside delete sentinel'
    test \"\$(cat '/home/dbx/outside/copy-source.txt')\" = 'outside copy sentinel'
    test \"\$(cat '/home/dbx/outside/rename-source.txt')\" = 'outside rename sentinel'
    test ! -e '/home/dbx/outside/write-created.txt'
    test ! -e '/home/dbx/outside/copy-created.txt'
    test ! -e '/home/dbx/outside/rename-created.txt'
    test ! -e '${contract_root}/symlink-copy-result.txt'
    test ! -e '${contract_root}/symlink-rename-result.txt'
  "
}

assert_no_temporary_sftp_keys() {
  if find "${runtime_tmp}" -name 'dbx-sftp-key-*.key' -print -quit | grep -q .; then
    echo "SFTP contract leaked temporary private-key material under ${runtime_tmp}" >&2
    find "${runtime_tmp}" -name 'dbx-sftp-key-*.key' -print >&2
    return 1
  fi
  if find "${runtime_tmp}" -type d -name 'dbx-sftp-keys-*' -print -quit | grep -q .; then
    echo "SFTP contract leaked a temporary private-key directory under ${runtime_tmp}" >&2
    find "${runtime_tmp}" -type d -name 'dbx-sftp-keys-*' -print >&2
    return 1
  fi
}

assert_transfer_fault_coverage() {
  node - "${proxy_trace}" <<'NODE'
const fs = require("node:fs");
const events = fs
  .readFileSync(process.argv[2], "utf8")
  .trim()
  .split("\n")
  .filter(Boolean)
  .map(JSON.parse);
const triggers = new Map(
  events
    .filter((event) => event.event === "trigger" && event.label)
    .map((event) => [event.label, event]),
);
for (const operation of ["download", "upload", "copy", "rename"]) {
  for (const [fault, action] of [
    ["disconnect", "reset"],
    ["timeout", "blackhole"],
  ]) {
    const label = `${operation}-${fault}`;
    const event = triggers.get(label);
    if (!event || event.action !== action) {
      throw new Error(`Missing ${label} SFTP proxy trigger: ${JSON.stringify(events, null, 2)}`);
    }
    if (
      operation === "rename" &&
      (event.scope !== "next" || event.boundPairId !== event.pairId)
    ) {
      throw new Error(`Rename fault was not bound to its next SSH pair: ${JSON.stringify(event)}`);
    }
  }
}
NODE
}

install_crash_residue_fixture() {
  mkdir -p "${key_residue_dir}"
  chmod 700 "${key_residue_dir}"
  printf '%s\n' 'dbx-sftp-crash-residue-secret' >"${key_residue_file}"
  chmod 600 "${key_residue_file}"
}

write_ssh_configs() {
  fake_host_key="${workspace}/fake-host-key"
  if [[ ! -f "${fake_host_key}" ]]; then
    ssh-keygen -q -t ed25519 -N "" -C dbx-sftp-wrong-host -f "${fake_host_key}"
  fi
  fake_key="$(awk '{print $1 " " $2}' "${fake_host_key}.pub")"
  {
    printf '[127.0.0.1]:%s %s\n' "${port}" "${fake_key}"
    printf '[%s]:%s %s\n' "${host_alias}" "${port}" "${fake_key}"
    printf '%s %s\n' "${host_alias}" "${fake_key}"
  } >"${mismatch_known_hosts}"
  chmod 600 "${mismatch_known_hosts}"

  {
    printf 'Host %s\n' "${host_alias}"
    printf '  HostName 127.0.0.1\n'
    printf '  Port %s\n' "${port}"
    printf '  User %s\n' "${username}"
    printf '  IdentityFile %s\n' "${config_key}"
    printf '  IdentitiesOnly yes\n'
    printf 'Host *\n'
    printf '  BatchMode yes\n'
    printf '  PasswordAuthentication no\n'
    printf '  KbdInteractiveAuthentication no\n'
    printf '  PreferredAuthentications publickey\n'
    printf '  StrictHostKeyChecking yes\n'
    printf '  UserKnownHostsFile %s\n' "${known_hosts}"
    printf '  GlobalKnownHostsFile /dev/null\n'
    printf '  UpdateHostKeys no\n'
    printf '  LogLevel ERROR\n'
  } >"${ssh_config}"
  sed "s|UserKnownHostsFile ${known_hosts}|UserKnownHostsFile ${mismatch_known_hosts}|" \
    "${ssh_config}" >"${mismatch_config}"
  chmod 600 "${ssh_config}" "${mismatch_config}"
}

smoke_sftp_fixture() {
  "${real_ssh}" -F "${ssh_config}" "${host_alias}" \
    "test \"\$(cat '${contract_root}/fixture.txt')\" = 'dbx sftp fixture'" </dev/null

  printf 'ls %s\nquit\n' "${contract_root}" \
    | "${real_sftp}" -F "${ssh_config}" -b - "${host_alias}" >/dev/null

  SSH_AUTH_SOCK="${agent_socket}" "${real_ssh}" -F "${ssh_config}" \
    -o IdentitiesOnly=no -o IdentityFile=none -p "${port}" \
    "${username}@127.0.0.1" true </dev/null
  SSH_AUTH_SOCK="${agent_socket}" "${real_ssh}" -F "${ssh_config}" \
    -o IdentitiesOnly=no -o IdentityFile=none -p "${proxy_port}" \
    "${username}@127.0.0.1" true </dev/null

  if "${real_ssh}" -F "${mismatch_config}" "${host_alias}" true \
    >"${workspace}/mismatch.out" 2>&1 </dev/null; then
    echo "Strict known_hosts accepted the intentionally wrong host key" >&2
    exit 1
  fi
  if ! grep -Eiq 'host key verification failed|remote host identification has changed' \
    "${workspace}/mismatch.out"; then
    cat "${workspace}/mismatch.out" >&2
    echo "Host-key mismatch fixture did not produce a recognizable OpenSSH error" >&2
    exit 1
  fi

  if printf 'ls %s/denied\nquit\n' "${contract_root}" \
    | "${real_sftp}" -F "${ssh_config}" -b - "${host_alias}" \
      >"${workspace}/denied.out" 2>&1; then
    echo "Permission-denied SFTP fixture was readable" >&2
    exit 1
  fi
  grep -Eiq 'permission denied|failure' "${workspace}/denied.out"

  if "${real_ssh}" -F /dev/null -o BatchMode=yes \
    -o StrictHostKeyChecking=no -o ConnectTimeout=1 \
    -p "${stall_port}" nobody@127.0.0.1 true \
    >"${workspace}/timeout.out" 2>&1 </dev/null; then
    echo "SSH handshake stall fixture unexpectedly authenticated" >&2
    exit 1
  fi
  grep -Eiq 'timed out|timeout' "${workspace}/timeout.out"
}

run_contract() {
  local test_name="$1"
  local module="commands::file_transfer::tests"
  local output="${workspace}/contract-result-${test_name}.log"
  if [[ "${test_name}" == "fixed_sftp_service_contract" ]]; then
    module="commands::file_manager::tests"
  fi
  local qualified="${module}::${test_name}"
  env \
    "HOME=${contract_home}" \
    "TMPDIR=${runtime_tmp}" \
    "CARGO_HOME=${cargo_home}" \
    "RUSTUP_HOME=${rustup_home}" \
    "PATH=${wrapper_dir}:${original_path}" \
    "SSH_AUTH_SOCK=${agent_socket}" \
    "DBX_TEST_SFTP_REAL_SSH=${real_ssh}" \
    "DBX_TEST_SFTP_SSH_CONFIG=${ssh_config}" \
    "DBX_TEST_SFTP_SSH_TRACE=${ssh_trace}" \
    "DBX_TEST_SFTP_MISMATCH_SSH_CONFIG=${mismatch_config}" \
    "DBX_TEST_SFTP_ENDPOINT=ssh://127.0.0.1:${port}" \
    "DBX_TEST_SFTP_HOST_ALIAS=${host_alias}" \
    "DBX_TEST_SFTP_USERNAME=${username}" \
    "DBX_TEST_SFTP_ROOT=${contract_root}" \
    "DBX_TEST_SFTP_PRIVATE_KEY_FILE=${encrypted_key}" \
    "DBX_TEST_SFTP_PRIVATE_KEY_PASSPHRASE=${key_passphrase}" \
    "DBX_TEST_SFTP_KNOWN_HOSTS=${known_hosts}" \
    "DBX_TEST_SFTP_MISMATCH_KNOWN_HOSTS=${mismatch_known_hosts}" \
    "DBX_TEST_SFTP_TIMEOUT_ENDPOINT=ssh://127.0.0.1:${stall_port}" \
    "DBX_TEST_SFTP_DISCONNECT_ENDPOINT=ssh://127.0.0.1:${proxy_port}" \
    "DBX_TEST_SFTP_DISCONNECT_CONTROL=http://127.0.0.1:${proxy_control_port}" \
    "DBX_TEST_SFTP_FAULT_ENDPOINT=ssh://127.0.0.1:${proxy_port}" \
    "DBX_TEST_SFTP_FAULT_CONTROL=http://127.0.0.1:${proxy_control_port}" \
    "DBX_TEST_SFTP_RUNTIME_TMP=${runtime_tmp}" \
    "DBX_TEST_SFTP_CRASH_RESIDUE_FILE=${key_residue_file}" \
    "DBX_TEST_SFTP_PROXY_TRACE=${proxy_trace}" \
    "DBX_TEST_SFTP_CONTAINER=${container}" \
    cargo test -p dbx --lib "${qualified}" --no-default-features -- \
      --ignored --exact --test-threads=1 2>&1 | tee "${output}"
  grep -Fx "test ${qualified} ... ok" "${output}" >/dev/null
  grep -F 'test result: ok. 1 passed; 0 failed;' "${output}" >/dev/null
}

start_sftp "service"
write_ssh_configs
smoke_sftp_fixture
if [[ "${smoke_only}" == "1" ]]; then
  if [[ "${require_full_contract}" == "1" ]]; then
    echo "SFTP fixture smoke completed, but the required product contract did not run" >&2
    exit 3
  fi
  exit 0
fi
install_crash_residue_fixture
run_contract "fixed_sftp_service_contract"
assert_symlink_escape_integrity
assert_no_temporary_sftp_keys

# Recreate the service with persisted host keys so state-machine fixtures start
# from a clean remote namespace after the destructive security cases.
start_sftp "transfer"
write_ssh_configs
smoke_sftp_fixture
run_contract "fixed_sftp_transfer_contract"
assert_symlink_escape_integrity
assert_no_temporary_sftp_keys
assert_transfer_fault_coverage
