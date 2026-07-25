#!/usr/bin/env bash
set -euo pipefail

workspace_manifest="src-tauri/Cargo.toml"
target="${DBX_HDFS_NATIVE_WINDOWS_TARGET:-x86_64-pc-windows-msvc}"

tree="$(
  cargo tree \
    --manifest-path "${workspace_manifest}" \
    --target "${target}" \
    --no-default-features \
    -e normal
)"

grep -F 'opendal-service-hdfs-native v0.57.0' <<<"${tree}" >/dev/null
if grep -E '(^|[[:space:]])(opendal-service-hdfs|hdrs|jni|j4rs)([[:space:]]|$)' <<<"${tree}" >/dev/null; then
  echo "JNI-backed HDFS dependency leaked into the Windows graph" >&2
  exit 1
fi

production_source="$(
  sed '/^#\[cfg(test)\]/,$d' \
    src-tauri/src/commands/file_manager_hdfs_native.rs
)"
if grep -E '(^|[^[:alnum:]_])(set_var|remove_var)[[:space:]]*\(' <<<"${production_source}" >/dev/null; then
  echo "HDFS Native product code mutates process-global environment" >&2
  exit 1
fi
