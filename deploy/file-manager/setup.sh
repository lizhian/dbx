#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
sftp_dir="${script_dir}/runtime/sftp"

if ! command -v ssh-keygen >/dev/null 2>&1; then
  echo "Missing required command: ssh-keygen" >&2
  exit 1
fi

mkdir -p "${sftp_dir}"
chmod 700 "${script_dir}/runtime" "${sftp_dir}"

if [[ ! -f "${sftp_dir}/id_ed25519" || ! -f "${sftp_dir}/id_ed25519.pub" ]]; then
  ssh-keygen \
    -q \
    -t ed25519 \
    -N "" \
    -C "dbx-opendal-test" \
    -f "${sftp_dir}/id_ed25519"
fi

chmod 600 "${sftp_dir}/id_ed25519"
chmod 644 "${sftp_dir}/id_ed25519.pub"

echo "Generated OpenDAL SFTP key under ${sftp_dir}"
