import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { validateEvidence } from "./file-manager-release-report.mjs";

const matrix = JSON.parse(readFileSync(new URL("../tests/file-manager-release/matrix.json", import.meta.url)));
const expectedBase = {
  commit: "a".repeat(40),
  runId: "42",
  runAttempt: "1",
  event: "push",
  ref: "refs/heads/main",
  mode: "release",
};
const script = new URL("./file-manager-release-report.mjs", import.meta.url);

function producerFor(row) {
  if (row.kind === "contract") {
    return { job: `contracts-${row.id}`, command: `bash tests/${row.id.slice("contract-".length)}-contract.sh` };
  }
  if (row.kind === "platform-compile") {
    return { job: row.id, command: "cargo test -p dbx --lib --no-default-features --no-run" };
  }
  return { job: row.id, command: `node scripts/file-manager-certify.mjs --row ${row.id}` };
}

function runnerFor(row) {
  if (row.platform === "macos") return "macOS";
  if (row.platform === "windows") return "Windows";
  return "Linux";
}

function contractLog(row) {
  return row.expected_tests.flatMap((item) =>
    Array.from({ length: item.count }, () => `test ${item.name} ... ok`)).join("\n") + "\n";
}

function rawResult(row) {
  const raw = { schema_version: 1, row_id: row.id, kind: row.kind, job_conclusion: "success" };
  if (row.kind === "platform-compile") raw.compilation = { completed: true };
  else if (row.kind === "platform-conformance") {
    raw.conformance = {
      platform: row.platform,
      implementations: row.implementations,
      results: row.implementations.flatMap((implementation) =>
        matrix.operations.map((operation) => ({ name: `${implementation}:${operation}`, status: "passed" }))),
    };
  } else if (row.kind === "desktop-e2e") {
    raw.tests = [{ name: "packaged-desktop-file-manager-e2e", status: "passed" }];
  } else if (row.kind === "fault") {
    raw.scenarios = row.scenarios.map((name) => ({ name, status: "passed" }));
  } else if (row.kind === "protected-service") {
    raw.service = row.service;
    raw.environment = row.expected_environment;
    raw.tests = [{ name: `${row.service}-contract`, status: "passed" }];
  } else if (row.kind === "performance" && row.operation) {
    raw.measurement = {
      operation: row.operation,
      bytes_transferred: row.size_gib * 1024 ** 3,
      duration_ms: 1000,
      peak_rss_mib: 256,
    };
  } else if (row.kind === "performance" && row.minimum_ratio !== undefined) {
    raw.measurement = {
      metric: row.metric,
      dbx_bytes_per_second: row.minimum_ratio * 1000,
      opendal_bytes_per_second: 1000,
      ratio: row.minimum_ratio,
    };
  } else if (row.kind === "performance" && row.maximum !== undefined) {
    raw.measurement = { metric: row.metric, duration_ms: row.maximum };
  } else if (row.kind === "performance") {
    raw.measurement = { metric: row.metric, progress_timeout_used: true };
  } else if (row.kind === "release-measurement") {
    raw.measurements = Object.fromEntries(row.metrics.map((metric) => [metric, 1]));
    raw.review = { status: "approved", reviewer: "release-reviewer" };
  } else if (row.kind === "release-gate") {
    raw.environment = row.expected_environment;
    raw.checks = row.checks.map((name) => ({ name, status: "passed" }));
  } else {
    throw new Error(`missing raw result fixture for ${row.kind}`);
  }
  return raw;
}

function passed(row, artifactRoot, options = {}) {
  const contract = row.kind === "contract";
  const contents = contract
    ? (options.contractLog ?? contractLog(row))
    : `${JSON.stringify(options.raw ?? rawResult(row), null, 2)}\n`;
  const name = contract ? `${row.id}.log` : `${row.id}.result`;
  writeFileSync(join(artifactRoot, name), contents);
  const producer = producerFor(row);
  const entry = {
    id: row.id,
    status: "passed",
    source_run: {
      repository: matrix.certification.repository,
      commit_sha: expectedBase.commit,
      run_id: expectedBase.runId,
      run_attempt: expectedBase.runAttempt,
      workflow: matrix.certification.workflow,
      workflow_path: matrix.certification.workflow_path,
      event: expectedBase.event,
      ref: expectedBase.ref,
      job: producer.job,
      command: producer.command,
      runner_os: runnerFor(row),
      environment: row.expected_environment ?? null,
      job_conclusion: "success",
    },
    observed_at: "2026-07-26T00:00:00.000Z",
    exit_code: 0,
    artifact: { name, sha256: createHash("sha256").update(contents).digest("hex") },
    notes: "",
  };
  if (contract) {
    const observed = contents.trim().split("\n").map((line) => line.slice(5, -7));
    entry.service_images = row.expected_service_images;
    entry.tests = { observed_passes: observed, passed_count: observed.length };
  }
  return Object.assign(entry, options.entry ?? {});
}

function document(entries) {
  return { schema_version: 2, matrix_id: matrix.matrix_id, entries };
}

function withArtifacts(run) {
  const artifactRoot = mkdtempSync(join(tmpdir(), "dbx-fm-release-"));
  try {
    const jobs = matrix.rows.filter((row) => row.protected).map((row, index) => ({
      id: index + 1,
      name: producerFor(row).job,
      run_id: Number(expectedBase.runId),
      run_attempt: Number(expectedBase.runAttempt),
      head_sha: expectedBase.commit,
      conclusion: "success",
      html_url: `https://github.com/${matrix.certification.repository}/actions/runs/${expectedBase.runId}/job/${index + 1}`,
    }));
    const deployments = matrix.rows.filter((row) => row.protected).map((row, index) => {
      const id = 1000 + index;
      const url = `https://api.github.com/repos/${matrix.certification.repository}/deployments/${id}`;
      return {
        id,
        url,
        sha: expectedBase.commit,
        ref: expectedBase.ref,
        environment: row.expected_environment,
        original_environment: row.expected_environment,
        statuses: [{
          id: 2000 + index,
          state: "success",
          environment: row.expected_environment,
          created_at: "2026-07-26T00:00:00Z",
          deployment_url: url,
          log_url: jobs[index].html_url,
        }],
      };
    });
    return run(artifactRoot, {
      ...expectedBase,
      artifactRoot,
      runJobs: { jobs },
      deployments: { deployments },
    });
  } finally {
    rmSync(artifactRoot, { recursive: true, force: true });
  }
}

test("complete GO is derived from contract logs and machine-readable raw result artifacts", () => {
  withArtifacts((artifactRoot, expected) => {
    const entries = matrix.rows.map((row) => passed(row, artifactRoot));
    const result = validateEvidence(matrix, [document(entries)], expected);
    assert.equal(result.verdict, "GO");
    assert.deepEqual(result.errors, []);
  });
});

test("missing and legacy rows remain release blockers", () => {
  withArtifacts((_artifactRoot, expected) => {
    const result = validateEvidence(matrix, [document([{
      id: "performance-write-100gib", status: "legacy", notes: "historical",
    }])], expected);
    assert.equal(result.verdict, "NO-GO");
    assert.equal(result.blockers.find((item) => item.id === "performance-write-100gib").status, "legacy");
    assert.ok(result.blockers.some((item) => item.id === "real-aws-s3" && item.status === "pending"));
  });
});

test("forged and missing artifact files are rejected", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "contract-s3");
    const forged = passed(row, artifactRoot);
    forged.artifact.sha256 = "0".repeat(64);
    assert.match(validateEvidence(matrix, [document([forged])], expected).errors.join("\n"), /sha256 does not match/);
    const missing = passed(row, artifactRoot);
    missing.artifact.name = "missing.log";
    assert.match(validateEvidence(matrix, [document([missing])], expected).errors.join("\n"), /artifact is missing/);
  });
});

test("untrusted producer, environment, and job conclusion are rejected", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "real-aws-s3");
    const raw = rawResult(row);
    raw.environment = "unprotected";
    raw.job_conclusion = "failure";
    const entry = passed(row, artifactRoot, { raw });
    entry.source_run.repository = "t8y2/dbx";
    entry.source_run.environment = "unprotected";
    entry.source_run.job_conclusion = "failure";
    const errors = validateEvidence(matrix, [document([entry])], expected).errors.join("\n");
    assert.match(errors, /source_run.repository/);
    assert.match(errors, /source_run.environment/);
    assert.match(errors, /source_run.job_conclusion/);
    assert.match(errors, /raw result job_conclusion/);
    assert.match(errors, /raw protected environment/);
  });
});

test("ordinary protected JSON cannot pass without independent Actions API job evidence", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "real-webdav");
    const withoutApiAttestation = { ...expected, runJobs: null };
    const errors = validateEvidence(
      matrix,
      [document([passed(row, artifactRoot)])],
      withoutApiAttestation,
    ).errors.join("\n");
    assert.match(errors, /independently downloaded Actions jobs attestation/);
  });
});

test("Jobs API success without a deployment cannot satisfy a protected row", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "real-webdav");
    const noDeployments = { ...expected, deployments: { deployments: [] } };
    const errors = validateEvidence(
      matrix,
      [document([passed(row, artifactRoot)])],
      noDeployments,
    ).errors.join("\n");
    assert.match(errors, /no deployment matches/);
  });
});

test("deployment with wrong environment or SHA cannot satisfy a protected row", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "target-hadoop-native");
    const wrongEnvironment = structuredClone(expected);
    const deployment = wrongEnvironment.deployments.deployments.find(
      (item) => item.environment === row.expected_environment,
    );
    deployment.environment = "unprotected";
    assert.match(
      validateEvidence(matrix, [document([passed(row, artifactRoot)])], wrongEnvironment).errors.join("\n"),
      /no deployment matches/,
    );

    const wrongSha = structuredClone(expected);
    wrongSha.deployments.deployments.find(
      (item) => item.environment === row.expected_environment,
    ).sha = "b".repeat(40);
    assert.match(
      validateEvidence(matrix, [document([passed(row, artifactRoot)])], wrongSha).errors.join("\n"),
      /no deployment matches/,
    );
  });
});

test("deployment status linked to the wrong run or job is rejected", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "webhdfs-production-gate");
    const wrongLink = structuredClone(expected);
    const deployment = wrongLink.deployments.deployments.find(
      (item) => item.environment === row.expected_environment,
    );
    deployment.statuses[0].log_url =
      `https://github.com/${matrix.certification.repository}/actions/runs/999/job/999`;
    const errors = validateEvidence(
      matrix,
      [document([passed(row, artifactRoot)])],
      wrongLink,
    ).errors.join("\n");
    assert.match(errors, /missing or ambiguously linked/);
  });
});

test("latest linked deployment status must be success", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "secrets-at-rest-gate");
    const failedDeployment = structuredClone(expected);
    const deployment = failedDeployment.deployments.deployments.find(
      (item) => item.environment === row.expected_environment,
    );
    deployment.statuses[0].state = "failure";
    const errors = validateEvidence(
      matrix,
      [document([passed(row, artifactRoot)])],
      failedDeployment,
    ).errors.join("\n");
    assert.match(errors, /latest linked deployment status is not success/);
  });
});

test("evidence JSON cannot self-report non-contract conformance fields", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "platform-conformance-linux");
    const raw = rawResult(row);
    raw.conformance.results = [];
    const entry = passed(row, artifactRoot, {
      raw,
      entry: { conformance: rawResult(row).conformance },
    });
    const errors = validateEvidence(matrix, [document([entry])], expected).errors.join("\n");
    assert.match(errors, /must come from the hashed raw result/);
    assert.match(errors, /operation coverage is incomplete/);
  });
});

test("reported throughput ratio cannot override rates below threshold", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "throughput-single");
    const raw = rawResult(row);
    raw.measurement.dbx_bytes_per_second = 899;
    raw.measurement.opendal_bytes_per_second = 1000;
    raw.measurement.ratio = 99;
    const errors = validateEvidence(matrix, [document([passed(row, artifactRoot, { raw })])], expected).errors.join("\n");
    assert.match(errors, /reported throughput ratio does not match/);
    assert.match(errors, /computed throughput ratio is below/);
  });
});

test("large transfer raw result must prove exact bytes, duration, and peak RSS", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "performance-write-100gib");
    const raw = rawResult(row);
    raw.measurement.bytes_transferred -= 1;
    raw.measurement.duration_ms = 0;
    raw.measurement.peak_rss_mib = 0;
    const errors = validateEvidence(matrix, [document([passed(row, artifactRoot, { raw })])], expected).errors.join("\n");
    assert.match(errors, /bytes_transferred does not match/);
    assert.match(errors, /duration_ms must be positive/);
    assert.match(errors, /peak_rss_mib must be positive/);
  });
});

test("footprint and release gates require raw approved review and passed checks", () => {
  withArtifacts((artifactRoot, expected) => {
    const footprint = matrix.rows.find((item) => item.id === "release-footprint");
    const footprintRaw = rawResult(footprint);
    delete footprintRaw.review;
    const gate = matrix.rows.find((item) => item.id === "secrets-at-rest-gate");
    const gateRaw = rawResult(gate);
    gateRaw.checks[0].status = "failed";
    const errors = validateEvidence(matrix, [document([
      passed(footprint, artifactRoot, { raw: footprintRaw }),
      passed(gate, artifactRoot, { raw: gateRaw }),
    ])], expected).errors.join("\n");
    assert.match(errors, /approved named review/);
    assert.match(errors, /every release gate checks result/);
  });
});

test("canonical policy rejects requirement shrinkage", () => {
  withArtifacts((_artifactRoot, expected) => {
    const weakened = structuredClone(matrix);
    weakened.rows = weakened.rows.filter((row) => row.id !== "secrets-at-rest-gate");
    assert.match(validateEvidence(weakened, [document([])], expected).errors.join("\n"), /differs from canonical policy/);
  });
});

test("contract artifact must contain complete expected tests", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "contract-webhdfs");
    const log = `test ${row.expected_tests[0].name} ... ok\n`;
    const errors = validateEvidence(matrix, [document([passed(row, artifactRoot, { contractLog: log })])], expected).errors.join("\n");
    assert.match(errors, /expected .* to pass 1 time/);
  });
});

test("compile-only raw result cannot satisfy platform conformance", () => {
  withArtifacts((artifactRoot, expected) => {
    const row = matrix.rows.find((item) => item.id === "platform-conformance-linux");
    const raw = {
      schema_version: 1,
      row_id: row.id,
      kind: row.kind,
      job_conclusion: "success",
      compilation: { completed: true },
    };
    const errors = validateEvidence(matrix, [document([passed(row, artifactRoot, { raw })])], expected).errors.join("\n");
    assert.match(errors, /conformance platform does not match/);
    assert.match(errors, /operation coverage is incomplete/);
  });
});

test("unknown, duplicate, and not-applicable required rows fail closed", () => {
  withArtifacts((artifactRoot, expected) => {
    const ftp = passed(matrix.rows[0], artifactRoot);
    const result = validateEvidence(matrix, [document([
      ftp,
      { ...ftp },
      { id: "invented-pass", status: "passed", notes: "" },
      { id: "platform-conformance-windows", status: "not_applicable", notes: "skipped" },
    ])], expected);
    const errors = result.errors.join("\n");
    assert.match(errors, /duplicate evidence row/);
    assert.match(errors, /unknown evidence row/);
    assert.match(errors, /required rows cannot be not_applicable/);
  });
});

test("CLI audit reports NO-GO while release mode fails", () => {
  const common = [
    fileURLToPath(script), "validate",
    "--commit", expectedBase.commit,
    "--run-id", expectedBase.runId,
    "--run-attempt", expectedBase.runAttempt,
    "--event", expectedBase.event,
    "--ref", expectedBase.ref,
  ];
  const cwd = new URL("..", import.meta.url);
  const audit = spawnSync(process.execPath, [...common, "--mode", "audit"], { cwd, encoding: "utf8" });
  assert.equal(audit.status, 0, audit.stderr);
  assert.match(audit.stdout, /"verdict": "NO-GO"/);
  const release = spawnSync(process.execPath, [...common, "--mode", "release"], { cwd, encoding: "utf8" });
  assert.equal(release.status, 1, release.stderr);
});
