#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  DBX_AWS_S3_BUCKET
  DBX_AWS_S3_REGION
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
)
for variable in "${required_variables[@]}"; do
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
done

command -v aws >/dev/null
command -v jq >/dev/null

# The OIDC role needs bucket-level GetBucketVersioning, ListBucket,
# ListBucketVersions, and ListBucketMultipartUploads on the dedicated bucket.
# Its object policy needs GetObject, PutObject, DeleteObject,
# DeleteObjectVersion, AbortMultipartUpload, and ListMultipartUploadParts only
# on dbx-nightly/*. The protected environment role must actually include
# DeleteObjectVersion; a bucket lifecycle that expires versions is only the
# fallback for a tiny permission probe left behind by a failed preflight.
bucket="${DBX_AWS_S3_BUCKET}"
region="${DBX_AWS_S3_REGION}"
aws_dns_suffix="amazonaws.com"
if [[ "${region}" == cn-* ]]; then
  aws_dns_suffix="amazonaws.com.cn"
fi
endpoint="https://s3.${region}.${aws_dns_suffix}"
run_id="${DBX_AWS_S3_RUN_ID:-local-$(date -u +%Y%m%d%H%M%S)-$$}"
run_attempt="${DBX_AWS_S3_RUN_ATTEMPT:-1}"
nonce="${DBX_AWS_S3_RUN_NONCE:-${RANDOM}${RANDOM}}"

if ! [[ "${run_id}" =~ ^[A-Za-z0-9._-]+$ &&
  "${run_attempt}" =~ ^[A-Za-z0-9._-]+$ &&
  "${nonce}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "AWS contract run identifiers may contain only letters, digits, dot, underscore, and hyphen" >&2
  exit 2
fi

scope="dbx-nightly/${run_id}-${run_attempt}-${nonce}"
root_key="${scope}/root/"
root="/${root_key}"
outside_canary_key="${scope}/outside-root-canary.txt"
bucket_canary_key="${scope}/bucket-canary.txt"
fixture_file="$(mktemp)"
aws_global_args=(--region "${region}")

aws_s3api() {
  aws "${aws_global_args[@]}" s3api "$@"
}

abort_multipart_uploads() {
  local page uploads key upload_id

  for _ in $(seq 1 20); do
    if ! page="$(aws_s3api list-multipart-uploads \
      --bucket "${bucket}" \
      --prefix "${scope}" \
      --max-uploads 1000 \
      --output json)"; then
      return 1
    fi
    uploads="$(jq -r '.Uploads // [] | length' <<<"${page}")"
    if [[ "${uploads}" == "0" ]]; then
      return 0
    fi

    while IFS=$'\t' read -r key upload_id; do
      [[ -n "${key}" && -n "${upload_id}" ]] || continue
      aws_s3api abort-multipart-upload \
        --bucket "${bucket}" \
        --key "${key}" \
        --upload-id "${upload_id}" >/dev/null || return 1
    done < <(jq -r '.Uploads[]? | [.Key, .UploadId] | @tsv' <<<"${page}")
  done

  echo "multipart upload cleanup did not converge for ${scope}" >&2
  return 1
}

delete_object_versions() {
  local page objects count delete_request delete_response delete_errors

  for _ in $(seq 1 20); do
    if ! page="$(aws_s3api list-object-versions \
      --bucket "${bucket}" \
      --prefix "${scope}" \
      --max-keys 1000 \
      --output json)"; then
      return 1
    fi
    objects="$(jq -c '[((.Versions // []) + (.DeleteMarkers // []))[] | {Key, VersionId}]' <<<"${page}")"
    count="$(jq 'length' <<<"${objects}")"
    if [[ "${count}" == "0" ]]; then
      return 0
    fi

    delete_request="$(jq -cn --argjson objects "${objects}" '{Objects: $objects, Quiet: true}')"
    if ! delete_response="$(aws_s3api delete-objects \
      --bucket "${bucket}" \
      --delete "${delete_request}" \
      --output json)"; then
      return 1
    fi
    delete_errors="$(jq -r '.Errors // [] | length' <<<"${delete_response}")"
    if [[ "${delete_errors}" != "0" ]]; then
      jq -r '.Errors[]? | "failed to delete \(.Key) version \(.VersionId): \(.Code) \(.Message)"' \
        <<<"${delete_response}" >&2
      return 1
    fi
  done

  echo "object version cleanup did not converge for ${scope}" >&2
  return 1
}

cleanup() {
  local result=$?
  local cleanup_result=0
  local multipart_cleanup_result=0
  local version_cleanup_result=0
  trap - EXIT
  set +e

  abort_multipart_uploads
  multipart_cleanup_result=$?
  delete_object_versions
  version_cleanup_result=$?
  if [[ "${multipart_cleanup_result}" != "0" || "${version_cleanup_result}" != "0" ]]; then
    cleanup_result=1
  fi
  rm -f "${fixture_file}"

  if [[ "${result}" == "0" && "${cleanup_result}" != "0" ]]; then
    result="${cleanup_result}"
  fi
  exit "${result}"
}
trap cleanup EXIT

if [[ "${DBX_AWS_S3_CLEANUP_ONLY:-false}" == "true" ]]; then
  exit 0
fi

versioning_status="$(aws_s3api get-bucket-versioning \
  --bucket "${bucket}" \
  --query Status \
  --output text)"
if [[ "${versioning_status}" != "Enabled" ]]; then
  echo "DBX_AWS_S3_BUCKET must be a dedicated bucket with versioning enabled" >&2
  exit 2
fi

permission_probe_key="${scope}/permission-probe"
printf 'dbx-s3-permission-probe' >"${fixture_file}"
if ! permission_probe_response="$(aws_s3api put-object \
  --bucket "${bucket}" \
  --key "${permission_probe_key}" \
  --body "${fixture_file}" \
  --output json)"; then
  echo "AWS S3 contract preflight could not write its isolated permission probe" >&2
  exit 2
fi
permission_probe_version="$(jq -r '.VersionId // empty' <<<"${permission_probe_response}")"
if [[ -z "${permission_probe_version}" || "${permission_probe_version}" == "null" ]]; then
  echo "AWS S3 contract preflight did not receive a VersionId from the versioned test bucket" >&2
  exit 2
fi
if ! aws_s3api delete-object \
  --bucket "${bucket}" \
  --key "${permission_probe_key}" \
  --version-id "${permission_probe_version}" >/dev/null; then
  echo "AWS S3 contract preflight requires s3:DeleteObjectVersion on dbx-nightly/*" >&2
  exit 2
fi
if ! permission_probe_versions="$(aws_s3api list-object-versions \
  --bucket "${bucket}" \
  --prefix "${permission_probe_key}" \
  --output json)"; then
  echo "AWS S3 contract preflight requires version-list permission for cleanup verification" >&2
  exit 2
fi
permission_probe_residuals="$(jq \
  --arg key "${permission_probe_key}" \
  '[((.Versions // []) + (.DeleteMarkers // []))[] | select(.Key == $key)] | length' \
  <<<"${permission_probe_versions}")"
if [[ "${permission_probe_residuals}" != "0" ]]; then
  echo "AWS S3 contract preflight could not prove permanent cleanup of its permission probe" >&2
  exit 2
fi

seed_object() {
  local key="$1"
  local value="$2"
  local fixture_kind="$3"

  printf '%s' "${value}" >"${fixture_file}"
  aws_s3api put-object \
    --bucket "${bucket}" \
    --key "${key}" \
    --body "${fixture_file}" \
    --metadata "dbx-fixture=${fixture_kind}" >/dev/null
}

seed_empty_marker() {
  local key="$1"
  aws_s3api put-object \
    --bucket "${bucket}" \
    --key "${key}" >/dev/null
}

# Preserve the fixed local fixture semantics, including simultaneous "a",
# "a/", and descendants, under an isolated per-run prefix.
seed_empty_marker "${root_key}"
seed_object "${root_key}a" "file-a" "file-a"
seed_empty_marker "${root_key}a/"
seed_object "${root_key}a/child.txt" "child-a" "child"
seed_object "${root_key}virtual/child.txt" "virtual-child" "virtual-child"
seed_empty_marker "${root_key}empty/"
seed_object "${root_key}nonzero/" "nonzero-marker" "nonzero-marker"
seed_object "${outside_canary_key}" "tenant-canary" "outside-root-canary"
seed_object "${bucket_canary_key}" "bucket-canary" "outside-root-canary"

contract_env=(
  "DBX_TEST_S3_ENDPOINT=${endpoint}"
  "DBX_TEST_S3_DIRECT_ENDPOINT=${endpoint}"
  "DBX_TEST_S3_REGION=${region}"
  "DBX_TEST_S3_BUCKET=${bucket}"
  "DBX_TEST_S3_ROOT=${root}"
  "DBX_TEST_S3_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
  "DBX_TEST_S3_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
  "DBX_TEST_S3_OUTSIDE_CANARY_KEY=${outside_canary_key}"
  "DBX_TEST_S3_BUCKET_CANARY_KEY=${bucket_canary_key}"
)
if [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then
  contract_env+=("DBX_TEST_S3_SESSION_TOKEN=${AWS_SESSION_TOKEN}")
fi

env "${contract_env[@]}" \
  cargo test -p dbx --lib fixed_s3_ --no-default-features -- \
    --ignored --test-threads=1
