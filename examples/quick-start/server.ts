/**
 * Quick-start Socket.IO chat server with a bot on the reference JavaScript
 * implementation.
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

const app = createServer();
const sio = new Server<ClientToServer, ServerToClient, Record<string, never>, SocketData>(app);

sio.on("connection", (socket) => {
  socket.on("join", async (room, name, ack) => {
    console.info(`server <- join      ${name} wants room "${room}"`);
    socket.data = { room, name };
    await socket.join(room);
    sio.to(room).emit("notice", `${name} joined ${room}`);
    const members = (await sio.in(room).fetchSockets()).length + 1;
    ack(members);
  });

  socket.on("message", async (text) => {
    console.info(`server <- message   ${text}`);
    const { room, name } = socket.data;

    socket.to(room).emit("message", name, text);
    sio.to(room).emit("message", BOT, `hi ${name}, you said "${text}"`);

    const leave: unknown = await socket.timeout(5000).emitWithAck("confirm", "Leave the room?");
    if (typeof leave !== "boolean") {
      throw new TypeError(`expected a bool answer, got ${JSON.stringify(leave)}`);
    }
    console.info(`server <- ack       confirm: ${leave ? "yes" : "no"}`);
    if (leave) {
      // This example ends the whole session after the client confirms.
      socket.disconnect(true);
    }
  });

  socket.on("image", (name, data) => {
    console.info(`server <- image     ${name} (${data.length} bytes)`);
    const { room } = socket.data;
    sio.to(room).emit("image", name, data);
  });
});

function main(): void {
  app.listen(3000, "127.0.0.1", () => {
    console.info("listening on http://127.0.0.1:3000");
  });
}

if (import.meta.main) {
  main();
}
