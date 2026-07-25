import http from "node:http";
import net from "node:net";
import fs from "node:fs";

function portFromEnvironment(name) {
  const port = Number.parseInt(process.env[name] ?? "", 10);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`${name} must be a valid TCP port`);
  }
  return port;
}

const host = "127.0.0.1";
const listenPort = portFromEnvironment("DBX_SFTP_PROXY_PORT");
const controlPort = portFromEnvironment("DBX_SFTP_PROXY_CONTROL_PORT");
const upstreamHost = process.env.DBX_SFTP_PROXY_UPSTREAM_HOST ?? host;
const upstreamPort = portFromEnvironment("DBX_SFTP_PROXY_UPSTREAM_PORT");
const tracePath = process.env.DBX_SFTP_PROXY_TRACE ?? "";
const pairs = new Set();
let nextPairId = 1;
let armedFault = null;

function trace(event) {
  if (tracePath !== "") {
    fs.appendFileSync(tracePath, `${JSON.stringify({ at: Date.now(), ...event })}\n`);
  }
}

function destroyPair(pair) {
  pair.client.destroy();
  pair.upstream.destroy();
  pairs.delete(pair);
}

function resetPair(pair, reason) {
  trace({ event: "reset", pairId: pair.id, reason });
  pair.client.resetAndDestroy();
  pair.upstream.resetAndDestroy();
  pairs.delete(pair);
}

function forward(pair, direction, source, destination, chunk) {
  if (pair.blackholed.has(direction)) {
    return;
  }
  if (
    armedFault !== null &&
    (armedFault.pairId === null || armedFault.pairId === pair.id) &&
    (armedFault.direction === "either" || armedFault.direction === direction)
  ) {
    if (chunk.length >= armedFault.bytes) {
      const fault = armedFault;
      const allowed = chunk.subarray(0, fault.bytes);
      armedFault = null;
      if (allowed.length > 0) {
        destination.write(allowed);
      }
      trace({
        event: "trigger",
        pairId: pair.id,
        action: fault.action,
        direction,
        bytes: fault.originalBytes,
        label: fault.label,
        scope: fault.scope,
        boundPairId: fault.pairId,
      });
      if (fault.action === "reset") {
        resetPair(pair, `armed-${direction}`);
      } else {
        pair.blackholed.add(direction);
        source.resume();
      }
      return;
    }
    armedFault.bytes -= chunk.length;
  }
  if (!destination.write(chunk)) {
    source.pause();
    destination.once("drain", () => source.resume());
  }
}

const proxy = net.createServer((client) => {
  const upstream = net.createConnection({
    host: upstreamHost,
    port: upstreamPort,
  });
  const pair = {
    id: nextPairId++,
    client,
    upstream,
    blackholed: new Set(),
  };
  pairs.add(pair);
  trace({ event: "open", pairId: pair.id });
  if (armedFault?.scope === "next" && armedFault.pairId === null) {
    armedFault.pairId = pair.id;
    trace({
      event: "bind",
      pairId: pair.id,
      action: armedFault.action,
      direction: armedFault.direction,
      bytes: armedFault.originalBytes,
      label: armedFault.label,
      scope: armedFault.scope,
    });
  }

  client.on("data", (chunk) => forward(pair, "upstream", client, upstream, chunk));
  upstream.on("data", (chunk) => {
    forward(pair, "downstream", upstream, client, chunk);
  });

  const close = () => {
    trace({ event: "close", pairId: pair.id });
    destroyPair(pair);
  };
  client.on("error", close);
  client.on("close", close);
  upstream.on("error", close);
  upstream.on("close", close);
});

const control = http.createServer((request, response) => {
  const url = new URL(request.url ?? "/", `http://${host}:${controlPort}`);
  if (request.method === "GET" && url.pathname === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(
      JSON.stringify({
        activeConnections: pairs.size,
        activePairIds: [...pairs].map((pair) => pair.id),
        armedFault,
      }),
    );
    return;
  }
  if (request.method === "POST" && url.pathname === "/arm") {
    const bytes = Number.parseInt(url.searchParams.get("bytes") ?? "", 10);
    const action = url.searchParams.get("action") ?? "reset";
    const direction = url.searchParams.get("direction") ?? "downstream";
    const label = url.searchParams.get("label") ?? "";
    const scope = url.searchParams.get("scope") ?? "any";
    if (!Number.isInteger(bytes) || bytes < 1 || bytes > 16 * 1024 * 1024) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "bytes must be between 1 and 16777216" }));
      return;
    }
    if (!["reset", "blackhole"].includes(action)) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "action must be reset or blackhole" }));
      return;
    }
    if (!["upstream", "downstream", "either"].includes(direction)) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "direction must be upstream, downstream, or either" }));
      return;
    }
    if (!/^[a-z0-9-]{0,64}$/.test(label)) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "label must contain only lowercase letters, digits, and hyphens" }));
      return;
    }
    if (!["any", "next"].includes(scope)) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "scope must be any or next" }));
      return;
    }
    if (scope === "any" && pairs.size === 0) {
      response.writeHead(409, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "no active SSH connection" }));
      return;
    }
    armedFault = {
      action,
      direction,
      bytes,
      originalBytes: bytes,
      label,
      scope,
      pairId: null,
    };
    trace({
      event: "arm",
      action,
      direction,
      bytes,
      label,
      scope,
      boundPairId: null,
      activeConnections: pairs.size,
      activePairIds: [...pairs].map((pair) => pair.id),
    });
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ activeConnections: pairs.size, armedFault }));
    return;
  }
  if (request.method === "POST" && url.pathname === "/drop") {
    armedFault = null;
    for (const pair of [...pairs]) {
      resetPair(pair, "control-drop");
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ activeConnections: pairs.size }));
    return;
  }
  response.writeHead(404, { "content-type": "application/json" });
  response.end(JSON.stringify({ error: "not found" }));
});

function shutdown() {
  armedFault = null;
  for (const pair of [...pairs]) {
    destroyPair(pair);
  }
  proxy.close();
  control.close(() => process.exit());
}

for (const server of [proxy, control]) {
  server.on("error", (error) => {
    process.stderr.write(`${error.stack ?? error.message}\n`);
    process.exitCode = 1;
  });
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

proxy.listen(listenPort, host);
control.listen(controlPort, host);
