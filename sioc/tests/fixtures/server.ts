/**
 * Reference Socket.IO server for the end-to-end tests in `sioc/tests/e2e.rs`.
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
  close_engine: () => void;
  close_engine_later: (ack: (count: number) => void) => void;
  hang_up: (ack: (count: number) => void) => void;
  engines: (ack: (count: number) => void) => void;
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

// Accepts each auth token once, so a CONNECT resent after a reconnection fails.
const tokens = new Set<string>();
sio.of("/once").use((socket, next) => {
  const token = String(socket.handshake.auth.token);
  if (tokens.has(token)) {
    next(new Error("used"));
    return;
  }
  tokens.add(token);
  next();
});

// Counts Engine.IO sessions, so a test can tell whether the client reconnected.
let engines = 0;
sio.engine.on("connection", () => {
  engines++;
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

  // Ends the Engine.IO session, which the client sees as a dropped connection.
  socket.on("close_engine", () => {
    socket.conn.close();
  });

  // Acks with the number of Engine.IO sessions so far, so the client can
  // leave the namespace after the server handled the event.
  socket.on("close_engine_later", (ack) => {
    setTimeout(() => socket.conn.close(), 100);
    ack(engines);
  });

  // Ends the Engine.IO session without answering the ack.
  socket.on("hang_up", () => {
    socket.conn.close();
  });

  socket.on("engines", (ack) => {
    ack(engines);
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
