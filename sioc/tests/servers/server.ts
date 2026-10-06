/**
 * Reference Socket.IO server for the end-to-end tests in `sioc/tests/servers.rs`.
 */

import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { Server } from "socket.io";

interface ClientToServer {
  flood: (count: number) => void;
  seen: (seq: number) => void;
  echo: (seq: number) => void;
  count: (ack: (count: number) => void) => void;
  blob: (data: Buffer, ack: (data: Buffer) => void) => void;
  kick: () => void;
  ask: (n: number) => void;
}

interface ServerToClient {
  item: (seq: number) => void;
  blob: (data: Buffer) => void;
  gone: () => void;
  question: (n: number, ack: (answer: number) => void) => void;
}

const app = createServer();
const sio = new Server<ClientToServer, ServerToClient>(app, {
  maxHttpBufferSize: 256,
  allowUpgrades: false,
});

sio.of("/denied").use((_socket, next) => {
  next(new Error("denied"));
});

const observers = sio.of("/observe");

sio.on("connection", (socket) => {
  let seen = 0;

  socket.on("flood", (count) => {
    for (let seq = 0; seq < count; seq++) {
      socket.emit("item", seq);
    }
  });

  socket.on("seen", () => {
    seen++;
  });

  socket.on("echo", (seq) => {
    seen++;
    socket.emit("item", seq);
  });

  socket.on("count", (ack) => {
    ack(seen);
  });

  socket.on("blob", (data, ack) => {
    socket.emit("blob", data);
    ack(data);
  });

  socket.on("kick", () => {
    socket.disconnect();
  });

  // Asks the client back, then reports the client's answer as an item.
  socket.on("ask", async (n) => {
    const answer = await socket.timeout(5000).emitWithAck("question", n);
    socket.emit("item", answer);
  });

  socket.on("disconnect", () => {
    observers.emit("gone");
  });
});

function main(): void {
  app.listen(0, "127.0.0.1", () => {
    const { port } = app.address() as AddressInfo;
    // The Rust test reads the port from the first stdout line.
    console.log(port);
  });
  // Closing stdin shuts down the server, including when a Rust test unwinds.
  process.stdin.on("end", () => {
    void sio.close();
  });
  process.stdin.resume();
}

if (import.meta.main) {
  main();
}
