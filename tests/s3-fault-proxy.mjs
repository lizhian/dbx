#!/usr/bin/env node

import fs from "node:fs";
import http from "node:http";

const listenPort = Number.parseInt(process.env.DBX_S3_FAULT_PROXY_PORT ?? "", 10);
const upstream = new URL(process.env.DBX_S3_FAULT_PROXY_UPSTREAM ?? "");
const tracePath = process.env.DBX_S3_FAULT_PROXY_TRACE;

if (!Number.isInteger(listenPort) || listenPort < 1 || listenPort > 65535) {
  throw new Error("DBX_S3_FAULT_PROXY_PORT must be an integer between 1 and 65535");
}
if (upstream.protocol !== "http:") {
  throw new Error("DBX_S3_FAULT_PROXY_UPSTREAM must be an http URL");
}
if (!tracePath) {
  throw new Error("DBX_S3_FAULT_PROXY_TRACE is required");
}

const committedResponseLoss = new Set();
const failedMultipartAborts = new Set();

function trace(event, request) {
  fs.appendFileSync(
    tracePath,
    `${JSON.stringify({
      event,
      method: request.method,
      url: request.url,
    })}\n`,
  );
}

function requestPath(request) {
  const url = new URL(request.url, "http://proxy.invalid");
  try {
    return decodeURIComponent(url.pathname);
  } catch {
    return url.pathname;
  }
}

function xmlError(response, status, code, message) {
  const body =
    `<?xml version="1.0" encoding="UTF-8"?>` +
    `<Error><Code>${code}</Code><Message>${message}</Message>` +
    `<RequestId>dbx-fault-proxy</RequestId></Error>`;
  response.writeHead(status, {
    "content-type": "application/xml",
    "content-length": Buffer.byteLength(body),
    connection: "close",
  });
  response.end(body);
}

function isCopyRequest(request) {
  return request.method === "PUT" && request.headers["x-amz-copy-source"] !== undefined;
}

function proxyRequest(request, response, afterResponse) {
  const headers = { ...request.headers };
  headers.connection = "close";
  const upstreamRequest = http.request(
    {
      protocol: upstream.protocol,
      hostname: upstream.hostname,
      port: upstream.port,
      method: request.method,
      path: request.url,
      headers,
    },
    (upstreamResponse) => {
      if (afterResponse) {
        const chunks = [];
        upstreamResponse.on("data", (chunk) => chunks.push(chunk));
        upstreamResponse.on("end", () => {
          afterResponse(upstreamResponse, Buffer.concat(chunks));
        });
        return;
      }
      response.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers);
      upstreamResponse.pipe(response);
    },
  );
  upstreamRequest.on("error", (error) => {
    if (!response.headersSent) {
      xmlError(response, 502, "FaultProxyUpstreamError", error.message);
    } else {
      response.destroy(error);
    }
  });
  request.pipe(upstreamRequest);
}

const server = http.createServer((request, response) => {
  const path = requestPath(request);
  const url = new URL(request.url, "http://proxy.invalid");
  const isMultipartPartCopy =
    isCopyRequest(request) && url.searchParams.has("partNumber") && url.searchParams.has("uploadId");

  if (path.includes("fault-200-error") && isCopyRequest(request)) {
    trace("copy_200_error", request);
    request.resume();
    xmlError(response, 200, "InvalidRequest", "Injected 200-with-Error copy response");
    return;
  }

  if (path.includes("fault-abort-error") && isMultipartPartCopy) {
    trace("multipart_part_200_error", request);
    request.resume();
    xmlError(response, 200, "InvalidRequest", "Injected multipart part copy failure");
    return;
  }

  if (
    path.includes("fault-abort-error") &&
    request.method === "DELETE" &&
    url.searchParams.has("uploadId")
  ) {
    const key = request.url;
    request.resume();
    if (failedMultipartAborts.has(key)) {
      trace("multipart_abort_retry_rejected", request);
      xmlError(response, 400, "InvalidRequest", "Injected multipart abort retry rejection");
    } else {
      failedMultipartAborts.add(key);
      trace("multipart_abort_failure", request);
      xmlError(response, 500, "InternalError", "Injected multipart abort failure");
    }
    return;
  }

  if (path.includes("response-loss") && isCopyRequest(request)) {
    const key = request.url;
    if (committedResponseLoss.has(key)) {
      trace("after_commit_retry_rejected", request);
      request.resume();
      xmlError(response, 400, "InvalidRequest", "Injected retry rejection after committed copy");
      return;
    }
    proxyRequest(request, response, (upstreamResponse, body) => {
      if ((upstreamResponse.statusCode ?? 500) >= 200 && (upstreamResponse.statusCode ?? 500) < 300) {
        committedResponseLoss.add(key);
        trace("after_commit_response_loss", request);
        response.destroy();
        return;
      }
      response.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers);
      response.end(body);
    });
    return;
  }

  proxyRequest(request, response);
});

server.on("clientError", (error, socket) => {
  socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
});

server.listen(listenPort, "127.0.0.1", () => {
  process.stdout.write(`ready http://127.0.0.1:${listenPort}\n`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    server.close(() => process.exit(0));
  });
}
