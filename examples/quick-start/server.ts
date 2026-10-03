/**
 * Quick-start Socket.IO chat server with a bot on the reference JavaScript
 * implementation, serving the same events as `server.py`.
 */

import { createServer } from "node:http";
import { Server } from "socket.io";

// Typed events mirror the Rust client's structs, field order included.
interface ClientToServer {
  join: (room: string, name: string, ack: (members: number) => void) => void;
  message: (text: string) => void;
  image: (name: string, data: Buffer) => void;
}

interface ServerToClient {
  message: (from: string, text: string) => void;
  notice: (text: string) => void;
  confirm: (question: string, ack: (leave: boolean) => void) => void;
  image: (name: string, data: Buffer) => void;
}

interface SocketData {
  room: string;
  name: string;
}

// The bot stands in for other room members, so a single client still sees
// broadcasts.
const BOT = "bot";

const httpServer = createServer();
const io = new Server<ClientToServer, ServerToClient, Record<string, never>, SocketData>(
  httpServer,
);

io.on("connection", (socket) => {
  socket.on("join", async (room, name, ack) => {
    socket.data = { room, name };
    await socket.join(room);
    io.to(room).emit("notice", `${name} joined ${room}`);
    console.info(`server <- join      ${name} wants room "${room}"`);
    const sockets = await io.in(room).fetchSockets();
    ack(sockets.length + 1);
  });

  socket.on("message", async (text) => {
    const { room, name } = socket.data;
    console.info(`server <- message   ${text}`);

    socket.to(room).emit("message", name, text);
    io.to(room).emit("message", BOT, `hi ${name}, you said "${text}"`);

    const leave: unknown = await socket.timeout(5000).emitWithAck("confirm", "Leave the room?");
    if (typeof leave !== "boolean") {
      throw new TypeError(`expected a bool answer, got ${JSON.stringify(leave)}`);
    }
    console.info(`server <- ack       confirm: ${leave ? "yes" : "no"}`);
    if (leave) {
      // Close the whole connection, not just the namespace, so smoke tests
      // cover the client shutting down after a server-closed transport.
      socket.disconnect(true);
    }
  });

  socket.on("image", (name, data) => {
    console.info(`server <- image     ${name} (${data.length} bytes)`);
    io.to(socket.data.room).emit("image", name, data);
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
