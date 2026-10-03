/**
 * Quick-start Socket.IO server on the reference JavaScript implementation,
 * serving the same events as `server.py`.
 */

import { createServer } from "node:http";
import { Server, type Socket } from "socket.io";

const options = ["Rust", "Python", "JavaScripts"];

async function session(socket: Socket): Promise<void> {
  socket.emit("greeting", "Welcome to the lobby!");

  const ack: unknown = await socket
    .timeout(5000)
    .emitWithAck("poll", "Favorite language?", options);
  if (typeof ack !== "number") {
    throw new TypeError(`expected a number vote, got ${JSON.stringify(ack)}`);
  }
  console.info(`poll vote from ${socket.id}: option ${options[ack]}`);

  // Close the whole connection, not just the namespace, so smoke tests
  // cover the client shutting down after a server-closed transport.
  socket.disconnect(true);
}

const httpServer = createServer();
const io = new Server(httpServer);

io.on("connection", (socket) => {
  socket.on("join", (room: string, ack: (count: number) => void) => {
    console.info(`${socket.id} joined room ${JSON.stringify(room)}`);
    ack(1);
  });

  socket.on("reply", (text: string) => {
    console.info(`reply from ${socket.id}: ${text}`);
  });

  socket.on("upload", (name: string, header: Buffer, body: Buffer) => {
    console.info(
      `upload from ${socket.id}: ${JSON.stringify(name)} header=${header.length}B body=${body.length}B`,
    );
    const chunk = Buffer.concat([Buffer.from("received: "), header.subarray(0, 4)]);
    socket.emit("chunk", name, chunk);
  });

  session(socket).catch((error: unknown) => {
    console.error(error);
    socket.disconnect(true);
  });
});

function listen(): Promise<void> {
  return new Promise((resolve) => {
    httpServer.listen(3000, "localhost", resolve);
  });
}

/**
 * Serve until a client command finishes, for smoke tests.
 *
 * The client starts only after the port is bound, so no sleep or retry is
 * needed, and the server exits even when the client fails.
 */
async function serveClient(command: string[]): Promise<number> {
  await listen();
  const child = Bun.spawn(command, { stdio: ["inherit", "inherit", "inherit"] });
  const code = await child.exited;
  await io.close();
  return code;
}

async function main(): Promise<void> {
  // Everything after `--client` is the client command, so smoke tests need
  // no shell quoting.
  const clientIndex = Bun.argv.indexOf("--client");
  if (clientIndex !== -1) {
    process.exit(await serveClient(Bun.argv.slice(clientIndex + 1)));
  }
  await listen();
  console.info("listening on http://localhost:3000");
}

if (import.meta.main) {
  await main();
}
