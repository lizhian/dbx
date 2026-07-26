#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { basename, isAbsolute, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const SHA256 = /^[a-f0-9]{64}$/;
const IMAGE_DIGEST = /@sha256:[a-f0-9]{64}$/;
const EVIDENCE_SCHEMA = JSON.parse(readFileSync(
  fileURLToPath(new URL("../tests/file-manager-release/evidence.schema.json", import.meta.url)),
  "utf8",
));
const RAW_RESULT_SCHEMA = JSON.parse(readFileSync(
  fileURLToPath(new URL("../tests/file-manager-release/raw-result.schema.json", import.meta.url)),
  "utf8",
));
const STATUSES = new Set(EVIDENCE_SCHEMA.properties.entries.items.properties.status.enum);
const CANONICAL_POLICY_PATH = fileURLToPath(
  new URL("../tests/file-manager-release/canonical-policy.json", import.meta.url),
);
const CANONICAL_POLICY = JSON.parse(readFileSync(CANONICAL_POLICY_PATH, "utf8"));
// This fingerprint makes accidental edits to both matrix files fail closed.
// Deliberate policy changes must update the policy, matrix, tests and this value.
export const CANONICAL_POLICY_SHA256 = "da238547a73675f2aa1ffd4c6fa86d898315c57d7d83b5ed938ac060d135ee3d";

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function digest(value) {
  return createHash("sha256").update(value).digest("hex");
}

function requiredString(value, name) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${name} is required`);
  }
  return value;
}

function parseArgs(argv) {
  const [command, ...tokens] = argv;
  const options = { evidence: [], serviceImage: [] };
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (!token.startsWith("--")) throw new Error(`unexpected argument: ${token}`);
    const key = token.slice(2).replaceAll("-", "_");
    const value = tokens[index + 1];
    if (value === undefined || value.startsWith("--")) throw new Error(`${token} requires a value`);
    index += 1;
    if (key === "evidence") options.evidence.push(value);
    else if (key === "service_image") options.serviceImage.push(value);
    else options[key] = value;
  }
  return { command, options };
}

function sameValues(actual, expected) {
  return JSON.stringify([...(actual ?? [])].sort()) === JSON.stringify([...(expected ?? [])].sort());
}

function runnerFor(row) {
  if (row.platform === "macos") return "macOS";
  if (row.platform === "windows") return "Windows";
  return "Linux";
}

function producerFor(row) {
  if (row.kind === "contract") {
    return {
      job: `contracts-${row.id}`,
      command: `bash tests/${row.id.slice("contract-".length)}-contract.sh`,
    };
  }
  if (row.kind === "platform-compile") {
    return {
      job: row.id,
      command: "cargo test -p dbx --lib --no-default-features --no-run",
    };
  }
  return {
    job: row.id,
    command: `node scripts/file-manager-certify.mjs --row ${row.id}`,
  };
}

function validateMatrix(matrix, canonical = CANONICAL_POLICY) {
  const errors = [];
  if (matrix?.schema_version !== 2) errors.push("matrix schema_version must be 2");
  if (stableJson(matrix) !== stableJson(canonical)) {
    errors.push("release matrix differs from canonical policy; requirements cannot be removed or weakened");
  }
  const policyDigest = digest(stableJson(canonical));
  if (policyDigest !== CANONICAL_POLICY_SHA256) {
    errors.push("canonical policy fingerprint does not match the validator");
  }
  const ids = new Set();
  for (const row of matrix?.rows ?? []) {
    if (typeof row.id !== "string" || !row.id) errors.push("every matrix row needs an id");
    else if (ids.has(row.id)) errors.push(`duplicate matrix row: ${row.id}`);
    else ids.add(row.id);
    if (typeof row.required !== "boolean") errors.push(`${row.id ?? "<unknown>"}: required must be boolean`);
  }
  return errors;
}

function validateDocumentSchema(document) {
  const errors = [];
  if (!document || typeof document !== "object" || Array.isArray(document)) {
    return ["evidence document must be an object"];
  }
  if (document.schema_version !== EVIDENCE_SCHEMA.properties.schema_version.const) {
    errors.push(`evidence schema_version must be ${EVIDENCE_SCHEMA.properties.schema_version.const}`);
  }
  if (document.matrix_id !== EVIDENCE_SCHEMA.properties.matrix_id.const) {
    errors.push(`evidence matrix_id must be ${EVIDENCE_SCHEMA.properties.matrix_id.const}`);
  }
  if (!Array.isArray(document.entries)) {
    errors.push("evidence entries must be an array");
    return errors;
  }
  for (const entry of document.entries) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      errors.push("evidence entry must be an object");
      continue;
    }
    if (typeof entry.id !== "string" || entry.id.length === 0) errors.push("evidence entry id is required");
    if (!STATUSES.has(entry.status)) errors.push(`${entry.id ?? "<missing>"}: evidence status is invalid`);
    if (typeof entry.notes !== "string") errors.push(`${entry.id ?? "<missing>"}: evidence notes must be a string`);
    if (entry.status === "passed") {
      if (!entry.source_run || typeof entry.source_run !== "object") {
        errors.push(`${entry.id}: passed evidence source_run is required`);
      }
      if (!entry.artifact || typeof entry.artifact !== "object") {
        errors.push(`${entry.id}: passed evidence artifact is required`);
      }
    }
  }
  return errors;
}

function parsePassedTests(log) {
  const tests = [];
  for (const line of log.split(/\r?\n/)) {
    const match = /^test (.+) \.\.\. ok$/.exec(line.trim());
    if (match) tests.push(match[1]);
  }
  return tests;
}

function countTests(tests) {
  const counts = new Map();
  for (const name of tests) counts.set(name, (counts.get(name) ?? 0) + 1);
  return counts;
}

function artifactBytes(entry, expected, errors, prefix) {
  if (typeof entry.artifact?.name !== "string" || !entry.artifact.name) {
    errors.push(`${prefix}: passed evidence needs artifact.name`);
    return null;
  }
  if (basename(entry.artifact.name) !== entry.artifact.name || isAbsolute(entry.artifact.name)) {
    errors.push(`${prefix}: artifact.name must be a plain file name`);
    return null;
  }
  if (!SHA256.test(entry.artifact.sha256 ?? "")) {
    errors.push(`${prefix}: passed evidence needs a sha256 artifact digest`);
  }
  if (!expected.artifactRoot) {
    errors.push(`${prefix}: artifact root is required to verify passed evidence`);
    return null;
  }
  const root = resolve(expected.artifactRoot);
  const path = resolve(root, entry.artifact.name);
  if (path !== root && !path.startsWith(`${root}${sep}`)) {
    errors.push(`${prefix}: artifact path escapes the artifact root`);
    return null;
  }
  if (!existsSync(path)) {
    errors.push(`${prefix}: artifact is missing: ${entry.artifact.name}`);
    return null;
  }
  const bytes = readFileSync(path);
  if (digest(bytes) !== entry.artifact.sha256) {
    errors.push(`${prefix}: artifact sha256 does not match the downloaded file`);
  }
  return bytes;
}

function validateSourceRun(entry, row, matrix, expected, errors) {
  const prefix = entry.id;
  const source = entry.source_run;
  const producer = producerFor(row);
  const fields = {
    repository: matrix.certification.repository,
    commit_sha: expected.commit,
    run_id: expected.runId,
    run_attempt: expected.runAttempt,
    workflow: matrix.certification.workflow,
    workflow_path: matrix.certification.workflow_path,
    event: expected.event,
    ref: expected.ref,
    job: producer.job,
    command: producer.command,
    runner_os: runnerFor(row),
    environment: row.expected_environment ?? null,
    job_conclusion: "success",
  };
  for (const [field, wanted] of Object.entries(fields)) {
    if (source?.[field] !== wanted) {
      errors.push(`${prefix}: source_run.${field} must be ${JSON.stringify(wanted)}`);
    }
  }
  if (expected.mode === "release") {
    if (source?.event !== matrix.certification.release_event) {
      errors.push(`${prefix}: release evidence must come from a push event`);
    }
    if (source?.ref !== matrix.certification.release_ref) {
      errors.push(`${prefix}: release evidence must come from refs/heads/main`);
    }
  }
}

function validateProtectedJobAttestation(row, expected, errors) {
  if (!row.protected) return null;
  const jobName = producerFor(row).job;
  const jobs = expected.runJobs?.jobs;
  if (!Array.isArray(jobs)) {
    errors.push(`${row.id}: protected pass requires an independently downloaded Actions jobs attestation`);
    return null;
  }
  const matches = jobs.filter((job) =>
    job?.name === jobName &&
    String(job.run_id) === expected.runId &&
    String(job.run_attempt) === expected.runAttempt);
  if (matches.length !== 1) {
    errors.push(`${row.id}: protected producer job is missing or ambiguous in the Actions API attestation`);
    return null;
  }
  const job = matches[0];
  if (job.conclusion !== "success") {
    errors.push(`${row.id}: protected producer job conclusion is not success in the Actions API attestation`);
  }
  if (job.head_sha !== expected.commit) {
    errors.push(`${row.id}: protected producer job commit does not match the Actions API attestation`);
  }
  const expectedJobUrl = `https://github.com/${CANONICAL_POLICY.certification.repository}/actions/runs/${expected.runId}/job/${job.id}`;
  if (job.html_url !== expectedJobUrl) {
    errors.push(`${row.id}: protected producer job URL does not bind the run and job id`);
  }
  return job;
}

function normalizedRef(ref) {
  return ref.startsWith("refs/heads/") ? ref.slice("refs/heads/".length) : ref;
}

function latestDeploymentStatus(deployment) {
  if (!Array.isArray(deployment.statuses) || deployment.statuses.length === 0) return null;
  return [...deployment.statuses].sort((left, right) => {
    const time = Date.parse(right.created_at ?? "") - Date.parse(left.created_at ?? "");
    if (Number.isFinite(time) && time !== 0) return time;
    return Number(right.id ?? 0) - Number(left.id ?? 0);
  })[0];
}

function validateProtectedDeploymentAttestation(row, expected, job, errors) {
  if (!row.protected || !job) return;
  const deployments = expected.deployments?.deployments;
  if (!Array.isArray(deployments)) {
    errors.push(`${row.id}: protected pass requires an independently downloaded deployment attestation`);
    return;
  }
  const candidates = deployments.filter((deployment) =>
    deployment?.environment === row.expected_environment &&
    deployment.sha === expected.commit &&
    normalizedRef(deployment.ref ?? "") === normalizedRef(expected.ref));
  if (candidates.length === 0) {
    errors.push(`${row.id}: no deployment matches the canonical environment, commit, and ref`);
    return;
  }
  const linked = candidates.map((deployment) => ({
    deployment,
    status: latestDeploymentStatus(deployment),
  })).filter(({ status }) => status?.log_url === job.html_url);
  if (linked.length !== 1) {
    errors.push(`${row.id}: deployment status is missing or ambiguously linked to the matched run and job`);
    return;
  }
  const { deployment, status } = linked[0];
  if (deployment.original_environment !== row.expected_environment) {
    errors.push(`${row.id}: deployment original_environment does not match canonical policy`);
  }
  if (status.environment !== row.expected_environment) {
    errors.push(`${row.id}: deployment status environment does not match canonical policy`);
  }
  if (status.state !== "success") {
    errors.push(`${row.id}: latest linked deployment status is not success`);
  }
  if (status.deployment_url !== deployment.url) {
    errors.push(`${row.id}: deployment status does not reference the matched deployment`);
  }
}

function validateContract(entry, row, bytes, errors) {
  const prefix = entry.id;
  if (!Array.isArray(entry.service_images) || entry.service_images.some((image) => !IMAGE_DIGEST.test(image))) {
    errors.push(`${prefix}: contract service images must be pinned by sha256 digest`);
  }
  if (!sameValues(entry.service_images, row.expected_service_images)) {
    errors.push(`${prefix}: service image digests do not match canonical policy`);
  }
  if (!bytes) return;
  const observed = parsePassedTests(bytes.toString("utf8"));
  const observedCounts = countTests(observed);
  if (observed.length === 0) errors.push(`${prefix}: contract artifact contains zero passed tests`);
  for (const test of row.expected_tests) {
    if ((observedCounts.get(test.name) ?? 0) !== test.count) {
      errors.push(`${prefix}: expected ${test.name} to pass ${test.count} time(s)`);
    }
  }
  const expectedNames = new Set(row.expected_tests.map((test) => test.name));
  for (const name of observedCounts.keys()) {
    if (name.includes("::fixed_") && !expectedNames.has(name)) {
      errors.push(`${prefix}: undeclared fixed contract test passed: ${name}`);
    }
  }
  if (!Array.isArray(entry.tests?.observed_passes) || !sameValues(entry.tests.observed_passes, observed)) {
    errors.push(`${prefix}: structured observed test set does not match the artifact`);
  }
  if (entry.tests?.passed_count !== observed.length) {
    errors.push(`${prefix}: structured passed_count does not match the artifact`);
  }
}

function parseRawResult(bytes, row, errors) {
  if (!bytes) return null;
  let raw;
  try {
    raw = JSON.parse(bytes.toString("utf8"));
  } catch {
    errors.push(`${row.id}: non-contract artifact must be machine-readable JSON`);
    return null;
  }
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    errors.push(`${row.id}: raw result must be an object`);
    return null;
  }
  const rawVersion = RAW_RESULT_SCHEMA.properties.schema_version.const;
  if (raw.schema_version !== rawVersion) errors.push(`${row.id}: raw result schema_version must be ${rawVersion}`);
  if (raw.row_id !== row.id) errors.push(`${row.id}: raw result row_id does not match`);
  if (raw.kind !== row.kind) errors.push(`${row.id}: raw result kind does not match`);
  if (raw.job_conclusion !== "success") errors.push(`${row.id}: raw result job_conclusion must be success`);
  return raw;
}

function successfulNames(results, field, prefix, errors) {
  if (!Array.isArray(results) || results.length === 0) {
    errors.push(`${prefix}: raw result needs a non-empty ${field} array`);
    return [];
  }
  const names = [];
  for (const result of results) {
    if (!result || typeof result.name !== "string" || result.status !== "passed") {
      errors.push(`${prefix}: every ${field} result needs a name and passed status`);
      continue;
    }
    names.push(result.name);
  }
  return names;
}

function validateKind(entry, row, matrix, bytes, errors) {
  const prefix = entry.id;
  if (row.kind === "contract") {
    validateContract(entry, row, bytes, errors);
    return;
  }
  const raw = parseRawResult(bytes, row, errors);
  if (!raw) return;
  if (row.kind === "platform-compile") {
    if (raw.compilation?.completed !== true) errors.push(`${prefix}: raw compile result needs compilation.completed=true`);
    return;
  }
  if (row.kind === "platform-conformance") {
    const expectedCoverage = row.implementations.flatMap((implementation) =>
      matrix.operations.map((operation) => `${implementation}:${operation}`));
    if (raw.conformance?.platform !== row.platform) errors.push(`${prefix}: conformance platform does not match`);
    if (!sameValues(raw.conformance?.implementations, row.implementations)) {
      errors.push(`${prefix}: conformance implementations do not match`);
    }
    const coverage = successfulNames(raw.conformance?.results, "conformance results", prefix, errors);
    if (!sameValues(coverage, expectedCoverage)) {
      errors.push(`${prefix}: conformance operation coverage is incomplete`);
    }
    return;
  }
  if (row.kind === "desktop-e2e") {
    successfulNames(raw.tests, "desktop tests", prefix, errors);
    return;
  }
  if (row.kind === "protected-service") {
    successfulNames(raw.tests, "protected service tests", prefix, errors);
    if (raw.service !== row.service) {
      errors.push(`${prefix}: protected service identity does not match`);
    }
    if (raw.environment !== row.expected_environment) {
      errors.push(`${prefix}: raw protected environment does not match canonical policy`);
    }
    return;
  }
  if (row.kind === "fault") {
    const scenarios = successfulNames(raw.scenarios, "fault scenarios", prefix, errors);
    if (!sameValues(scenarios, row.scenarios)) errors.push(`${prefix}: fault scenario set is incomplete`);
    return;
  }
  if (row.kind === "performance") {
    const measurement = raw.measurement;
    if (!measurement || typeof measurement !== "object") {
      errors.push(`${prefix}: raw result needs a structured measurement`);
      return;
    }
    if (row.operation) {
      const expectedBytes = row.size_gib * 1024 ** 3;
      if (measurement.operation !== row.operation) errors.push(`${prefix}: measured operation does not match`);
      if (measurement.bytes_transferred !== expectedBytes) errors.push(`${prefix}: measured bytes_transferred does not match ${expectedBytes}`);
      if (!(measurement.duration_ms > 0)) errors.push(`${prefix}: duration_ms must be positive`);
      if (!(measurement.peak_rss_mib > 0)) errors.push(`${prefix}: peak_rss_mib must be positive`);
    } else if (measurement.metric !== row.metric) {
      errors.push(`${prefix}: measured metric does not match`);
    } else if (row.minimum_ratio !== undefined) {
      const dbx = measurement.dbx_bytes_per_second;
      const opendal = measurement.opendal_bytes_per_second;
      if (!(dbx > 0) || !(opendal > 0)) {
        errors.push(`${prefix}: throughput result needs positive DBX and OpenDAL rates`);
        return;
      }
      const ratio = dbx / opendal;
      if (measurement.ratio !== undefined && Math.abs(measurement.ratio - ratio) > 1e-12) {
        errors.push(`${prefix}: reported throughput ratio does not match raw rates`);
      }
      if (ratio < row.minimum_ratio) {
        errors.push(`${prefix}: computed throughput ratio is below minimum_ratio ${row.minimum_ratio}`);
      }
    } else if (row.maximum !== undefined) {
      if (!(measurement.duration_ms >= 0) || !Number.isFinite(measurement.duration_ms)) {
        errors.push(`${prefix}: cancellation result needs a finite duration_ms`);
      } else if (measurement.duration_ms > row.maximum) {
        errors.push(`${prefix}: measured duration exceeds maximum ${row.maximum}`);
      }
    } else {
      if (measurement.progress_timeout_used !== row.expected_value) {
        errors.push(`${prefix}: stalled I/O result does not prove progress timeout use`);
      }
    }
    return;
  }
  if (row.kind === "release-measurement") {
    for (const metric of row.metrics) {
      if (!(raw.measurements?.[metric] > 0)) errors.push(`${prefix}: missing positive metric ${metric}`);
    }
    if (raw.review?.status !== "approved" || typeof raw.review?.reviewer !== "string" || raw.review.reviewer === "") {
      errors.push(`${prefix}: footprint result needs an approved named review`);
    }
    return;
  }
  if (row.kind === "release-gate") {
    if (raw.environment !== row.expected_environment) {
      errors.push(`${prefix}: raw release gate environment does not match canonical policy`);
    }
    const checks = successfulNames(raw.checks, "release gate checks", prefix, errors);
    if (!sameValues(checks, row.checks)) errors.push(`${prefix}: release gate check set is incomplete`);
    return;
  }
  errors.push(`${prefix}: unsupported evidence kind ${row.kind}`);
}

function validateEntry(entry, row, matrix, expected) {
  const errors = [];
  const prefix = entry?.id ?? "<unknown>";
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) return [`${prefix}: entry must be an object`];
  if (!STATUSES.has(entry.status)) errors.push(`${prefix}: unknown status ${entry.status}`);
  if (typeof entry.notes !== "string") errors.push(`${prefix}: notes must be a string`);
  if (entry.status === "not_applicable" && row?.required) {
    errors.push(`${prefix}: required rows cannot be not_applicable`);
  }
  if (entry.status !== "passed") return errors;

  if (row.kind !== "contract") {
    for (const field of ["tests", "compilation", "conformance", "scenarios", "service", "measurement", "measurements", "checks"]) {
      if (Object.hasOwn(entry, field)) {
        errors.push(`${prefix}: non-contract ${field} must come from the hashed raw result, not evidence JSON`);
      }
    }
  }
  if (!entry.observed_at || Number.isNaN(Date.parse(entry.observed_at))) {
    errors.push(`${prefix}: passed evidence needs observed_at`);
  }
  if (entry.exit_code !== 0) errors.push(`${prefix}: passed evidence exit_code must be 0`);
  validateSourceRun(entry, row, matrix, expected, errors);
  const protectedJob = validateProtectedJobAttestation(row, expected, errors);
  validateProtectedDeploymentAttestation(row, expected, protectedJob, errors);
  const bytes = artifactBytes(entry, expected, errors, prefix);
  validateKind(entry, row, matrix, bytes, errors);
  return errors;
}

export function validateEvidence(matrix, documents, expected, canonical = CANONICAL_POLICY) {
  const errors = validateMatrix(matrix, canonical);
  const rows = new Map((matrix.rows ?? []).map((row) => [row.id, row]));
  const supplied = new Map();

  for (const document of documents) {
    errors.push(...validateDocumentSchema(document));
    if (document?.matrix_id !== matrix.matrix_id) errors.push(`evidence matrix_id must be ${matrix.matrix_id}`);
    if (!Array.isArray(document?.entries)) {
      errors.push("evidence entries must be an array");
      continue;
    }
    for (const entry of document.entries) {
      if (!rows.has(entry?.id)) {
        errors.push(`unknown evidence row: ${entry?.id ?? "<missing>"}`);
        continue;
      }
      if (supplied.has(entry.id)) {
        errors.push(`duplicate evidence row: ${entry.id}`);
        continue;
      }
      supplied.set(entry.id, entry);
      errors.push(...validateEntry(entry, rows.get(entry.id), matrix, expected));
    }
  }

  const entries = matrix.rows.map((row) => supplied.get(row.id) ?? {
    id: row.id,
    status: "pending",
    notes: "No evidence was supplied for this matrix row.",
  });
  const counts = Object.fromEntries([...STATUSES].map((status) => [
    status,
    entries.filter((entry) => entry.status === status).length,
  ]));
  const blockers = entries
    .filter((entry) => rows.get(entry.id).required && entry.status !== "passed")
    .map((entry) => ({ id: entry.id, status: entry.status }));

  return {
    schema_version: 2,
    matrix_id: matrix.matrix_id,
    policy_sha256: CANONICAL_POLICY_SHA256,
    expected_source_run: {
      repository: matrix.certification.repository,
      commit_sha: expected.commit,
      run_id: expected.runId,
      run_attempt: expected.runAttempt,
      workflow: matrix.certification.workflow,
      workflow_path: matrix.certification.workflow_path,
      event: expected.event,
      ref: expected.ref,
    },
    verdict: errors.length === 0 && blockers.length === 0 ? "GO" : "NO-GO",
    counts,
    blockers,
    errors,
    entries,
  };
}

function makeRecord(options) {
  const status = requiredString(options.status, "status");
  if (!STATUSES.has(status)) throw new Error(`unknown status: ${status}`);
  const id = requiredString(options.id, "id");
  const matrix = readJson(options.matrix ?? "tests/file-manager-release/matrix.json");
  const policyErrors = validateMatrix(matrix);
  if (policyErrors.length > 0) throw new Error(policyErrors.join("; "));
  const row = matrix.rows.find((candidate) => candidate.id === id);
  if (!row) throw new Error(`unknown matrix row: ${id}`);
  const entry = { id, status, notes: options.notes ?? "" };
  if (status === "passed" || status === "failed") {
    const artifactPath = resolve(requiredString(options.artifact, "artifact"));
    if (!existsSync(artifactPath)) throw new Error(`artifact does not exist: ${artifactPath}`);
    const bytes = readFileSync(artifactPath);
    entry.source_run = {
      repository: requiredString(options.repository, "repository"),
      commit_sha: requiredString(options.commit, "commit"),
      run_id: requiredString(options.run_id, "run-id"),
      run_attempt: requiredString(options.run_attempt, "run-attempt"),
      workflow: requiredString(options.workflow, "workflow"),
      workflow_path: requiredString(options.workflow_path, "workflow-path"),
      event: requiredString(options.event, "event"),
      ref: requiredString(options.ref, "ref"),
      job: requiredString(options.job, "job"),
      command: requiredString(options.command, "command"),
      runner_os: requiredString(options.runner_os, "runner-os"),
      environment: options.environment ?? null,
      job_conclusion: requiredString(options.job_conclusion, "job-conclusion"),
    };
    entry.observed_at = new Date().toISOString();
    entry.exit_code = Number(options.exit_code);
    if (!Number.isInteger(entry.exit_code)) throw new Error("exit-code must be an integer");
    entry.artifact = { name: basename(artifactPath), sha256: digest(bytes) };
    if (row.kind === "contract") {
      entry.service_images = options.serviceImage;
      const observed = parsePassedTests(bytes.toString("utf8"));
      entry.tests = { observed_passes: observed, passed_count: observed.length };
    }
  }
  return {
    schema_version: 2,
    matrix_id: matrix.matrix_id,
    entries: [entry],
  };
}

function expectedFromOptions(options, mode) {
  return {
    commit: requiredString(options.commit, "commit"),
    runId: requiredString(options.run_id, "run-id"),
    runAttempt: requiredString(options.run_attempt, "run-attempt"),
    event: requiredString(options.event, "event"),
    ref: requiredString(options.ref, "ref"),
    artifactRoot: options.artifact_root,
    runJobs: options.run_jobs ? readJson(options.run_jobs) : null,
    deployments: options.deployments ? readJson(options.deployments) : null,
    mode,
  };
}

function printUsage() {
  console.error("usage: file-manager-release-report.mjs validate|record [options]");
}

export function main(argv = process.argv.slice(2)) {
  try {
    const { command, options } = parseArgs(argv);
    if (command === "record") {
      const output = requiredString(options.output, "output");
      writeFileSync(output, `${JSON.stringify(makeRecord(options), null, 2)}\n`);
      return 0;
    }
    if (command !== "validate") {
      printUsage();
      return 2;
    }
    const matrix = readJson(options.matrix ?? "tests/file-manager-release/matrix.json");
    const mode = options.mode ?? "audit";
    if (mode !== "audit" && mode !== "release") throw new Error("mode must be audit or release");
    const evidencePaths = options.evidence.length > 0
      ? options.evidence
      : ["tests/file-manager-release/baseline.json"];
    const report = validateEvidence(matrix, evidencePaths.map(readJson), expectedFromOptions(options, mode));
    if (options.report) writeFileSync(options.report, `${JSON.stringify(report, null, 2)}\n`);
    console.log(JSON.stringify({
      verdict: report.verdict,
      counts: report.counts,
      blockers: report.blockers,
      errors: report.errors,
    }, null, 2));
    if (report.errors.length > 0) return 2;
    return mode === "release" && report.verdict !== "GO" ? 1 : 0;
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    return 2;
  }
}

const isEntrypoint = process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isEntrypoint) process.exitCode = main();
