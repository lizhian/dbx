import net from "node:net";

const host = "127.0.0.1";
const port = Number.parseInt(process.env.DBX_SFTP_STALL_PORT ?? "", 10);

if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("DBX_SFTP_STALL_PORT must be a valid TCP port");
}

const sockets = new Set();
const server = net.createServer((socket) => {
  sockets.add(socket);
  socket.on("close", () => sockets.delete(socket));
  socket.on("error", () => {});
  // Accept TCP but never send an SSH identification string. This gives the
  // contract a deterministic SSH-handshake timeout instead of relying on an
  // unroutable address whose behavior differs between CI networks.
});

server.on("error", (error) => {
  process.stderr.write(`${error.stack ?? error.message}\n`);
  process.exitCode = 1;
});

function shutdown() {
  for (const socket of sockets) {
    socket.destroy();
  }
  server.close(() => process.exit());
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

server.listen(port, host);
