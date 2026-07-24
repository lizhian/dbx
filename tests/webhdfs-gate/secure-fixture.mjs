import { createHash } from "node:crypto";
import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
import { createServer as createHttpsServer } from "node:https";
import { createServer as createHttpServer } from "node:http";
import { connect } from "node:net";

const tlsDir = process.env.WEBHDFS_GATE_TLS_DIR;
const readyFile = process.env.WEBHDFS_GATE_READY_FILE;
const proxyLog = process.env.WEBHDFS_GATE_PROXY_LOG;
if (!tlsDir || !readyFile || !proxyLog) throw new Error("fixture environment is incomplete");

const tls = {
  key: readFileSync(`${tlsDir}/server.key`),
  cert: readFileSync(`${tlsDir}/server.crt`),
};
const files = new Map();
const requiredDelegation = "gate-token";
const prefix = "/webhdfs/v1";

function json(response, status, value) {
  const body = Buffer.from(JSON.stringify(value));
  response.writeHead(status, { "content-type": "application/json", "content-length": body.length });
  response.end(body);
}

function authenticated(url, response) {
  const values = url.searchParams.getAll("delegation");
  if (values.length !== 1 || values[0] !== requiredDelegation) {
    json(response, 403, { RemoteException: { exception: "SecurityException", message: "invalid delegation token" } });
    return false;
  }
  return true;
}

function hdfsPath(url) {
  return decodeURIComponent(url.pathname.slice(prefix.length)) || "/";
}

const namenode = createHttpsServer(tls, (request, response) => {
  if (request.url === "/health") return json(response, 200, { ok: true });
  const url = new URL(request.url, "https://namenode.gate.test:19443");
  if (!authenticated(url, response)) return;
  const path = hdfsPath(url);
  const op = url.searchParams.get("op");

  if (op === "GETFILESTATUS") {
    if (path === "/" || path.endsWith("/.dbx-streaming")) {
      return json(response, 200, { FileStatus: { type: "DIRECTORY", length: 0 } });
    }
    const value = files.get(path);
    if (!value) return json(response, 404, { RemoteException: { exception: "FileNotFoundException" } });
    return json(response, 200, { FileStatus: { type: "FILE", length: value.length } });
  }
  if (op === "MKDIRS") return json(response, 200, { boolean: true });
  if (["CREATE", "OPEN", "GETFILECHECKSUM"].includes(op)) {
    const location = new URL(`https://datanode.gate.test:19444${url.pathname}`);
    for (const [key, value] of url.searchParams) location.searchParams.append(key, value);
    response.writeHead(307, { location: location.toString(), "content-length": 0 });
    return response.end();
  }
  if (op === "RENAME") {
    const destination = url.searchParams.get("destination");
    if (!destination || files.has(destination) || !files.has(path)) return json(response, 200, { boolean: false });
    files.set(destination, files.get(path));
    files.delete(path);
    return json(response, 200, { boolean: true });
  }
  if (op === "DELETE") {
    const existed = files.delete(path);
    return json(response, 200, { boolean: existed });
  }
  return json(response, 400, { error: `unsupported NameNode op ${op}` });
});

const datanode = createHttpsServer(tls, (request, response) => {
  const url = new URL(request.url, "https://datanode.gate.test:19444");
  if (!authenticated(url, response)) return;
  const path = hdfsPath(url);
  const op = url.searchParams.get("op");

  if (request.method === "PUT" && op === "CREATE") {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      files.set(path, Buffer.concat(chunks));
      response.writeHead(201, { "content-length": 0 });
      response.end();
    });
    return;
  }
  const value = files.get(path);
  if (!value) return json(response, 404, { error: "missing" });
  if (op === "OPEN") {
    const offset = Number(url.searchParams.get("offset") ?? 0);
    const length = Number(url.searchParams.get("length") ?? value.length - offset);
    const body = value.subarray(offset, offset + length);
    response.writeHead(200, { "content-length": body.length });
    return response.end(body);
  }
  if (op === "GETFILECHECKSUM") {
    return json(response, 200, {
      FileChecksum: { algorithm: "SHA-256", bytes: createHash("sha256").update(value).digest("hex") },
    });
  }
  return json(response, 400, { error: `unsupported DataNode op ${op}` });
});

const proxy = createHttpServer((_request, response) => {
  response.writeHead(405);
  response.end();
});
proxy.on("connect", (request, clientSocket, head) => {
  appendFileSync(proxyLog, `${request.url}\n`);
  const [, portText] = request.url.split(":");
  const upstream = connect(Number(portText), "127.0.0.1", () => {
    clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
    if (head.length) upstream.write(head);
    upstream.pipe(clientSocket);
    clientSocket.pipe(upstream);
  });
  upstream.on("error", () => clientSocket.destroy());
});

await Promise.all([
  new Promise((resolve) => namenode.listen(19443, "127.0.0.1", resolve)),
  new Promise((resolve) => datanode.listen(19444, "127.0.0.1", resolve)),
  new Promise((resolve) => proxy.listen(19445, "127.0.0.1", resolve)),
]);
writeFileSync(readyFile, "ready\n");

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    namenode.close();
    datanode.close();
    proxy.close(() => process.exit(0));
  });
}
