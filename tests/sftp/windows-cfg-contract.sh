#!/usr/bin/env bash
set -euo pipefail

windows_target="${DBX_TEST_WINDOWS_TARGET:-x86_64-pc-windows-msvc}"
tree="$(cargo tree -p dbx --target "${windows_target}" --no-default-features --edges normal)"

if grep -Eq '(^|[[:space:]])(opendal-service-sftp|openssh|openssh-sftp)([[:space:]]|$)' <<<"${tree}"; then
  echo "Windows normal dependency graph unexpectedly contains the Unix-only SFTP stack" >&2
  grep -E 'opendal-service-sftp|openssh|openssh-sftp' <<<"${tree}" >&2
  exit 1
fi

host="$(rustc -vV | awk '/^host: / { print $2 }')"
case "${host}" in
  *-pc-windows-*)
    cargo check -p dbx --target "${windows_target}" --no-default-features
    cargo test -p dbx --lib sftp_windows_unsupported_contract \
      --target "${windows_target}" \
      --no-default-features \
      -- --test-threads=1
    ;;
  *)
    echo "Windows dependency graph passed; native Windows check/test requires a Windows runner (host=${host})"
    ;;
esac
