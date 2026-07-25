import fs from "node:fs";
import http from "node:http";
import net from "node:net";

function portFromEnvironment(name) {
  const port = Number.parseInt(process.env[name] ?? "", 10);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`${name} must be a valid TCP port`);
  }
  return port;
}

const host = "127.0.0.1";
const listenPort = portFromEnvironment("DBX_HDFS_PROXY_PORT");
const controlPort = portFromEnvironment("DBX_HDFS_PROXY_CONTROL_PORT");
const upstreamHost = process.env.DBX_HDFS_PROXY_UPSTREAM_HOST ?? host;
const upstreamPort = portFromEnvironment("DBX_HDFS_PROXY_UPSTREAM_PORT");
const tracePath = process.env.DBX_HDFS_PROXY_TRACE ?? "";
const pairs = new Set();
let nextPairId = 1;
let armedFault = null;
const totals = {
  openedConnections: 0,
  closedConnections: 0,
  upstreamBytes: 0,
  downstreamBytes: 0,
  upstreamChunks: 0,
  downstreamChunks: 0,
};

function trace(event) {
  if (tracePath !== "") {
    fs.appendFileSync(tracePath, `${JSON.stringify({ at: Date.now(), ...event })}\n`);
  }
}

function summarizePair(pair, reason) {
  if (pair.closed) {
    return;
  }
  pair.closed = true;
  totals.closedConnections += 1;
  trace({
    event: "close-summary",
    pairId: pair.id,
    reason,
    upstreamBytes: pair.upstreamBytes,
    downstreamBytes: pair.downstreamBytes,
    upstreamChunks: pair.upstreamChunks,
    downstreamChunks: pair.downstreamChunks,
  });
  pairs.delete(pair);
}

function destroyPair(pair, reason) {
  if (pair.closed) {
    return;
  }
  pair.client.destroy();
  pair.upstream.destroy();
  summarizePair(pair, reason);
}

function resetPair(pair, reason) {
  trace({ event: "reset", pairId: pair.id, reason });
  pair.client.resetAndDestroy();
  pair.upstream.resetAndDestroy();
  summarizePair(pair, reason);
}

function socketEnded(pair, side) {
  const direction = side === "client" ? "upstream" : "downstream";
  if (pair.blackholed.has(direction)) {
    trace({ event: "end-suppressed", pairId: pair.id, side, direction });
    return;
  }
  trace({ event: "end", pairId: pair.id, side, direction });
  if (side === "client") {
    pair.upstream.end();
  } else {
    pair.client.end();
  }
}

function socketClosed(pair, side) {
  trace({ event: "close", pairId: pair.id, side });
  if (side === "client") {
    pair.clientClosed = true;
  } else {
    pair.upstreamClosed = true;
  }
  if (pair.clientClosed && pair.upstreamClosed) {
    summarizePair(pair, "both-sides-closed");
  }
}

function socketErrored(pair, side, error) {
  trace({
    event: "error",
    pairId: pair.id,
    side,
    code: error?.code ?? "UNKNOWN",
  });
  destroyPair(pair, `${side}-error`);
}

function forward(pair, direction, source, destination, chunk) {
  if (direction === "upstream") {
    pair.upstreamBytes += chunk.length;
    pair.upstreamChunks += 1;
    totals.upstreamBytes += chunk.length;
    totals.upstreamChunks += 1;
  } else {
    pair.downstreamBytes += chunk.length;
    pair.downstreamChunks += 1;
    totals.downstreamBytes += chunk.length;
    totals.downstreamChunks += 1;
  }
  if (pair.blackholed.has(direction)) {
    source.resume();
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

const proxy = net.createServer({ allowHalfOpen: true }, (client) => {
  const upstream = net.createConnection({
    host: upstreamHost,
    port: upstreamPort,
    allowHalfOpen: true,
  });
  const pair = {
    id: nextPairId++,
    client,
    upstream,
    blackholed: new Set(),
    upstreamBytes: 0,
    downstreamBytes: 0,
    upstreamChunks: 0,
    downstreamChunks: 0,
    closed: false,
    clientClosed: false,
    upstreamClosed: false,
  };
  pairs.add(pair);
  totals.openedConnections += 1;
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
  upstream.on("data", (chunk) => forward(pair, "downstream", upstream, client, chunk));

  client.on("end", () => socketEnded(pair, "client"));
  upstream.on("end", () => socketEnded(pair, "upstream"));
  client.on("error", (error) => socketErrored(pair, "client", error));
  upstream.on("error", (error) => socketErrored(pair, "upstream", error));
  client.on("close", () => socketClosed(pair, "client"));
  upstream.on("close", () => socketClosed(pair, "upstream"));
});

const control = http.createServer((request, response) => {
  const url = new URL(request.url ?? "/", `http://${host}:${controlPort}`);
  if (request.method === "GET" && url.pathname === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(
      JSON.stringify({
        activeConnections: pairs.size,
        activePairs: [...pairs].map((pair) => ({
          pairId: pair.id,
          upstreamBytes: pair.upstreamBytes,
          downstreamBytes: pair.downstreamBytes,
          upstreamChunks: pair.upstreamChunks,
          downstreamChunks: pair.downstreamChunks,
          blackholed: [...pair.blackholed],
        })),
        totals,
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
    const scope = url.searchParams.get("scope") ?? "next";
    if (!Number.isInteger(bytes) || bytes < 1 || bytes > 64 * 1024 * 1024) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "bytes must be between 1 and 67108864" }));
      return;
    }
    if (!["reset", "blackhole"].includes(action)) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "action must be reset or blackhole" }));
      return;
    }
    if (!["upstream", "downstream", "either"].includes(direction)) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "invalid direction" }));
      return;
    }
    if (!/^[a-z0-9-]{1,64}$/.test(label)) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "label must contain lowercase letters, digits, or hyphens" }));
      return;
    }
    if (!["any", "next"].includes(scope)) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "scope must be any or next" }));
      return;
    }
    if (scope === "any" && pairs.size === 0) {
      response.writeHead(409, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "no active connection" }));
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
      activeConnections: pairs.size,
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
    destroyPair(pair, "proxy-shutdown");
  }
  proxy.close();
  control.closeAllConnections?.();
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
