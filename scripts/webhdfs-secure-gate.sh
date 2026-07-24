#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="$repo_root/target/debug/webhdfs_gate"
result_dir="${WEBHDFS_GATE_SECURE_RESULT_DIR:-$repo_root/target/webhdfs-secure-gate}"
fixture_dir="$(mktemp -d "${TMPDIR:-/tmp}/dbx-webhdfs-secure.XXXXXX")"
fixture_pid=""

cleanup() {
  if [ -n "$fixture_pid" ]; then
    kill "$fixture_pid" >/dev/null 2>&1 || true
    wait "$fixture_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$fixture_dir"
}
trap cleanup EXIT

mkdir -p "$result_dir"
: >"$result_dir/proxy.log"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj "/CN=DBX WebHDFS Gate CA" \
  -keyout "$fixture_dir/ca.key" -out "$fixture_dir/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -subj "/CN=namenode.gate.test" \
  -keyout "$fixture_dir/server.key" -out "$fixture_dir/server.csr" >/dev/null 2>&1
openssl x509 -req -days 1 -sha256 \
  -in "$fixture_dir/server.csr" -CA "$fixture_dir/ca.crt" -CAkey "$fixture_dir/ca.key" -CAcreateserial \
  -extfile <(printf '%s\n' \
    'subjectAltName=DNS:namenode.gate.test,DNS:datanode.gate.test' \
    'extendedKeyUsage=serverAuth' \
    'keyUsage=digitalSignature,keyEncipherment') \
  -out "$fixture_dir/server.crt" >/dev/null 2>&1

WEBHDFS_GATE_TLS_DIR="$fixture_dir" \
WEBHDFS_GATE_READY_FILE="$fixture_dir/ready" \
WEBHDFS_GATE_PROXY_LOG="$result_dir/proxy.log" \
node "$repo_root/tests/webhdfs-gate/secure-fixture.mjs" \
  >"$result_dir/fixture.stdout" 2>"$result_dir/fixture.stderr" &
fixture_pid=$!
for _ in $(seq 1 100); do
  [ -f "$fixture_dir/ready" ] && break
  sleep 0.05
done
[ -f "$fixture_dir/ready" ] || { cat "$result_dir/fixture.stderr" >&2; exit 1; }

cargo build -p dbx --bin webhdfs_gate --no-default-features --features webhdfs-gate

export WEBHDFS_GATE_ENDPOINT="https://namenode.gate.test:19443/"
export WEBHDFS_GATE_ROOT="/secure-gate"
export WEBHDFS_GATE_DELEGATION="gate-token"
unset WEBHDFS_GATE_USER_NAME || true
export WEBHDFS_GATE_ALLOWED_DATANODE_ORIGINS="https://datanode.gate.test:19444"
export WEBHDFS_GATE_DNS_OVERRIDES="namenode.gate.test=127.0.0.1:19443,datanode.gate.test=127.0.0.1:19444"
export WEBHDFS_GATE_CA_PEM="$fixture_dir/ca.crt"
export WEBHDFS_GATE_CHUNK_MIB=1

"$binary" write-b direct.bin 2097153 | tee "$result_dir/direct-write.json"
"$binary" copy-b direct.bin direct-copy.bin | tee "$result_dir/direct-copy.json"

if WEBHDFS_GATE_CA_PEM= "$binary" write-b invalid-tls.bin 1024 \
  >"$result_dir/invalid-tls.stdout" 2>"$result_dir/invalid-tls.stderr"; then
  echo "invalid TLS certificate unexpectedly succeeded" >&2
  exit 1
fi
if WEBHDFS_GATE_DNS_OVERRIDES= "$binary" write-b missing-dns.bin 1024 \
  >"$result_dir/missing-dns.stdout" 2>"$result_dir/missing-dns.stderr"; then
  echo "missing hostname mapping unexpectedly succeeded" >&2
  exit 1
fi
if WEBHDFS_GATE_DELEGATION=wrong-token "$binary" write-b invalid-token.bin 1024 \
  >"$result_dir/invalid-token.stdout" 2>"$result_dir/invalid-token.stderr"; then
  echo "invalid delegation token unexpectedly succeeded" >&2
  exit 1
fi

export WEBHDFS_GATE_PROXY="http://127.0.0.1:19445"
"$binary" write-b proxy.bin 2097153 | tee "$result_dir/proxy-write.json"
grep -q 'namenode.gate.test:19443' "$result_dir/proxy.log"
grep -q 'datanode.gate.test:19444' "$result_dir/proxy.log"

proxy_lines_before="$(wc -l <"$result_dir/proxy.log" | tr -d ' ')"
export WEBHDFS_GATE_PROXY_BYPASS="namenode.gate.test,datanode.gate.test"
"$binary" write-b bypass.bin 2097153 | tee "$result_dir/bypass-write.json"
proxy_lines_after="$(wc -l <"$result_dir/proxy.log" | tr -d ' ')"
[ "$proxy_lines_before" = "$proxy_lines_after" ] || {
  echo "proxy bypass still routed requests through proxy" >&2
  exit 1
}

{
  echo "trusted_tls=pass"
  echo "invalid_tls=fail_closed"
  echo "proxy_route=pass"
  echo "proxy_bypass=pass"
  echo "hostname_mapping=pass"
  echo "missing_hostname_mapping=fail_closed"
  echo "delegation_forwarding=pass"
  echo "invalid_delegation=fail_closed"
} | tee "$result_dir/verdict.txt"
