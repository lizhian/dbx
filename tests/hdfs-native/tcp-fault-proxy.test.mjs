import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return server.address().port;
}

async function unusedPort() {
  const server = net.createServer();
  const port = await listen(server);
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function waitForControl(port) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health`);
      if (response.ok) {
        return;
      }
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
  throw new Error("fault proxy control endpoint did not become ready");
}

async function startProxy(context, upstreamPort) {
  const proxyPort = await unusedPort();
  const controlPort = await unusedPort();
  const tracePath = path.join(
    os.tmpdir(),
    `dbx-hdfs-proxy-test-${process.pid}-${Date.now()}.jsonl`,
  );
  const proxyPath = fileURLToPath(new URL("./tcp-fault-proxy.mjs", import.meta.url));
  const child = spawn(process.execPath, [proxyPath], {
    env: {
      ...process.env,
      DBX_HDFS_PROXY_PORT: String(proxyPort),
      DBX_HDFS_PROXY_CONTROL_PORT: String(controlPort),
      DBX_HDFS_PROXY_UPSTREAM_PORT: String(upstreamPort),
      DBX_HDFS_PROXY_TRACE: tracePath,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  context.after(async () => {
    const exit = child.exitCode === null
      ? new Promise((resolve) => child.once("exit", resolve))
      : Promise.resolve();
    child.kill("SIGTERM");
    await Promise.race([
      exit,
      new Promise((resolve) =>
        setTimeout(() => {
          child.kill("SIGKILL");
          resolve();
        }, 1_000),
      ),
    ]);
  });
  await waitForControl(controlPort);
  return { controlPort, proxyPort };
}

test("forwards a delayed trailing response after the client half-closes", async (context) => {
  const upstream = net.createServer({ allowHalfOpen: true }, (socket) => {
    socket.resume();
    socket.on("end", () => {
      socket.write("response-");
      setTimeout(() => socket.end("tail"), 30);
    });
  });
  const upstreamPort = await listen(upstream);
  const { proxyPort } = await startProxy(context, upstreamPort);
  context.after(async () => {
    await new Promise((resolve) => upstream.close(resolve));
  });

  const response = await new Promise((resolve, reject) => {
    const chunks = [];
    const client = net.createConnection(
      { host: "127.0.0.1", port: proxyPort, allowHalfOpen: true },
      () => client.end("request"),
    );
    client.on("data", (chunk) => chunks.push(chunk));
    client.on("end", () => {
      client.end();
      resolve(Buffer.concat(chunks).toString());
    });
    client.on("error", reject);
  });

  assert.equal(response, "response-tail");
});

test("suppresses EOF in a blackholed direction until the pair is dropped", async (context) => {
  const upstream = net.createServer({ allowHalfOpen: true }, (socket) => {
    socket.resume();
    socket.on("end", () => {
      socket.write("a");
      setTimeout(() => socket.end("tail"), 20);
    });
  });
  const upstreamPort = await listen(upstream);
  const { controlPort, proxyPort } = await startProxy(context, upstreamPort);
  context.after(async () => {
    await new Promise((resolve) => upstream.close(resolve));
  });

  const arm = await fetch(
    `http://127.0.0.1:${controlPort}/arm?action=blackhole&direction=downstream&bytes=1&label=delayed-ack&scope=next`,
    { method: "POST" },
  );
  assert.equal(arm.status, 200);

  let ended = false;
  const client = net.createConnection(
    { host: "127.0.0.1", port: proxyPort, allowHalfOpen: true },
    () => client.end("request"),
  );
  client.on("end", () => {
    ended = true;
    client.end();
  });
  client.on("error", () => {});
  await new Promise((resolve) => setTimeout(resolve, 100));

  const health = await fetch(`http://127.0.0.1:${controlPort}/health`).then((response) =>
    response.json()
  );
  assert.equal(ended, false);
  assert.equal(health.activeConnections, 1);
  assert.deepEqual(health.activePairs[0].blackholed, ["downstream"]);

  const drop = await fetch(`http://127.0.0.1:${controlPort}/drop`, { method: "POST" });
  assert.equal(drop.status, 200);
  client.destroy();
});
