#!/usr/bin/env bash
set -euo pipefail

# openssh-rs deliberately delegates to the system ssh client. Injecting an
# isolated config keeps the contract hermetic without reading or changing the
# developer's real ~/.ssh files.
: "${DBX_TEST_SFTP_REAL_SSH:?DBX_TEST_SFTP_REAL_SSH is required}"
: "${DBX_TEST_SFTP_SSH_CONFIG:?DBX_TEST_SFTP_SSH_CONFIG is required}"

if [[ -n "${DBX_TEST_SFTP_SSH_TRACE:-}" ]]; then
  printf -v escaped_arguments ' %q' -F "${DBX_TEST_SFTP_SSH_CONFIG}" "$@"
  printf 'ssh%s\n' "${escaped_arguments}" >>"${DBX_TEST_SFTP_SSH_TRACE}"
fi

exec "${DBX_TEST_SFTP_REAL_SSH}" -F "${DBX_TEST_SFTP_SSH_CONFIG}" "$@"
