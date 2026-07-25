import fs from "node:fs";
import http from "node:http";
import crypto from "node:crypto";

const port = Number(process.env.DBX_WEBDAV_PROXY_PORT);
const upstream = new URL(process.env.DBX_WEBDAV_PROXY_UPSTREAM);
const trace = process.env.DBX_WEBDAV_PROXY_TRACE;

function record(event) {
  fs.appendFileSync(trace, `${JSON.stringify(event)}\n`);
}

http
  .createServer((request, response) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", async () => {
      const body = Buffer.concat(chunks);
      const originalDestination = request.headers.destination;
      record({
        method: request.method,
        url: request.url,
        destination: originalDestination ?? null,
        overwrite: request.headers.overwrite ?? null,
        bodyBytes: body.length,
        authorizationScheme: request.headers.authorization?.split(" ", 1)[0] ?? null,
        authorizationHash: request.headers.authorization
          ? crypto.createHash("sha256").update(request.headers.authorization).digest("hex").slice(0, 12)
          : null,
      });
      const faultTarget = `${request.url} ${originalDestination ?? ""}`;
      const isMutation = request.method === "PUT" || request.method === "COPY" || request.method === "MOVE";
      if (isMutation && faultTarget.includes("reject-403")) {
        response.writeHead(403).end("injected 403");
        return;
      }
      if (isMutation && faultTarget.includes("reject-507")) {
        response.writeHead(507).end("injected 507");
        return;
      }
      if (
        request.method === "COPY" &&
        (request.url.includes("timeout-copy") || originalDestination?.includes("timeout-copy"))
      )
        return;

      const headers = { ...request.headers, host: upstream.host };
      if (request.url.includes("auth-anonymous")) {
        record({
          event: "auth_mode",
          mode: "anonymous",
          valid: request.headers.authorization === undefined,
        });
        headers.authorization = `Basic ${Buffer.from("dbx:dbx-password").toString("base64")}`;
      }
      if (request.url.includes("auth-bearer")) {
        record({
          event: "auth_mode",
          mode: "bearer",
          valid: request.headers.authorization === "Bearer dbx-bearer-token",
        });
        headers.authorization = `Basic ${Buffer.from("dbx:dbx-password").toString("base64")}`;
      }
      if (originalDestination) {
        const destination = new URL(originalDestination);
        destination.protocol = upstream.protocol;
        destination.host = upstream.host;
        headers.destination = destination.toString();
      }
      if (headers.if) {
        headers.if = headers.if.replaceAll(
          `http://127.0.0.1:${port}`,
          `${upstream.protocol}//${upstream.host}`,
        );
      }
      if (request.method === "DELETE" && request.url.includes("response-loss-delete")) {
        request.socket.destroy();
        setTimeout(async () => {
          const childUrl = new URL(`${request.url}late-child.txt`, upstream);
          const injected = await fetch(childUrl, {
            method: "PUT",
            headers: {
              authorization: `Basic ${Buffer.from("dbx:dbx-password").toString("base64")}`,
              "content-type": "application/octet-stream",
            },
            body: "must-remain-locked",
          });
          const injectedBody = await injected.text();
          record({
            event: "late_delete_concurrent_write",
            status: injected.status,
            rejectedByLock: injected.status === 423 || injectedBody.includes("423 Locked"),
          });
          const committed = await fetch(new URL(request.url, upstream), {
            method: "DELETE",
            headers: {
              authorization: headers.authorization,
              if: headers.if,
            },
          });
          await committed.text();
          record({ event: "late_delete_commit", status: committed.status });
        }, 400);
        return;
      }
      if (request.method === "DELETE" && request.url.includes("concurrent-delete")) {
        const injected = await fetch(new URL(`${request.url}late-child.txt`, upstream), {
          method: "PUT",
          headers: {
            authorization: request.headers.authorization,
            "content-type": "application/octet-stream",
          },
          body: "must-be-rejected",
        });
        const injectedBody = await injected.text();
        record({
          event: "concurrent_write",
          status: injected.status,
          rejectedByLock: injected.status === 423 || injectedBody.includes("423 Locked"),
        });
      }
      const forwarded = http.request(
        {
          protocol: upstream.protocol,
          hostname: upstream.hostname,
          port: upstream.port,
          method: request.method,
          path: request.url,
          headers,
        },
        (upstreamResponse) => {
          if (request.method === "LOCK" && request.url.includes("unsafe-timeout-delete")) {
            const responseChunks = [];
            upstreamResponse.on("data", (chunk) => responseChunks.push(chunk));
            upstreamResponse.on("end", () => {
              const rewritten = Buffer.from(
                Buffer.concat(responseChunks).toString("utf8").replace("Second-30", "Infinite"),
              );
              const responseHeaders = {
                ...upstreamResponse.headers,
                "content-length": String(rewritten.length),
              };
              delete responseHeaders["transfer-encoding"];
              record({ event: "unsafe_lock_timeout_injected", timeout: "Infinite" });
              response.writeHead(upstreamResponse.statusCode ?? 502, responseHeaders);
              response.end(rewritten);
            });
            return;
          }
          const loseResponse =
            (request.url.includes("response-loss") || originalDestination?.includes("response-loss")) &&
            (request.method === "COPY" || request.method === "MOVE" || request.method === "PUT");
          if (loseResponse) {
            upstreamResponse.resume();
            upstreamResponse.on("end", () => request.socket.destroy());
            return;
          }
          response.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers);
          upstreamResponse.pipe(response);
        },
      );
      forwarded.on("error", (error) => response.destroy(error));
      forwarded.end(body);
    });
  })
  .listen(port, "127.0.0.1");
