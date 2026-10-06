/**
 * Reference Socket.IO server for protocol and backpressure integration tests
 * on the reference JavaScript implementation, serving the same events as
 * `server.py`.
 */

import { createServer } from "node:http";
import { Server } from "socket.io";

interface ClientToServer {
  flood: (count: number) => void;
  seen: (seq: number) => void;
  echo: (seq: number) => void;
  count: (ack: (count: number) => void) => void;
  blob: (data: Buffer, ack: (data: Buffer) => void) => void;
  kick: () => void;
}

interface ServerToClient {
  item: (seq: number) => void;
  blob: (data: Buffer) => void;
  gone: () => void;
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

  socket.on("disconnect", () => {
    observers.emit("gone");
  });
});

function listen(): Promise<number> {
  return new Promise((resolve, reject) => {
    app.listen(0, "127.0.0.1", () => {
      const address = app.address();
      if (!address || typeof address === "string") {
        reject(new Error("expected TCP address"));
        return;
      }
      resolve(address.port);
    });
  });
}

async function main(): Promise<void> {
  const port = await listen();
  // The Rust test reads the ephemeral port from the first stdout line.
  console.log(port);
  // Closing stdin shuts down the server, including when a Rust test unwinds.
  await Bun.stdin.stream().getReader().read();
  await sio.close();
}

if (import.meta.main) {
  await main();
}
