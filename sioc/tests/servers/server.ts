/** Reference server for protocol and backpressure integration tests. */
import { createServer } from "node:http";
import { Server } from "socket.io";

interface ClientEvents {
  flood: (count: number) => void;
  seen: (seq: number) => void;
  echo: (seq: number) => void;
  count: (ack: (count: number) => void) => void;
  blob: (data: Buffer, ack: (data: Buffer) => void) => void;
  kick: () => void;
}
interface ServerEvents {
  item: (seq: number) => void;
  blob: (data: Buffer) => void;
  gone: () => void;
}
const app = createServer();
const io = new Server<ClientEvents, ServerEvents>(app, {
  maxHttpBufferSize: 256,
  allowUpgrades: false,
});
io.of("/denied").use((_socket, next) => next(new Error("denied")));
const observers = io.of("/observe");
io.on("connection", (socket) => {
  let seen = 0;
  socket.on("flood", (count) => {
    for (let seq = 0; seq < count; seq++) socket.emit("item", seq);
  });
  socket.on("seen", () => {
    seen++;
  });
  socket.on("echo", (seq) => {
    seen++;
    socket.emit("item", seq);
  });
  socket.on("count", (ack) => ack(seen));
  socket.on("blob", (data, ack) => {
    socket.emit("blob", data);
    ack(data);
  });
  socket.on("kick", () => socket.disconnect());
  socket.on("disconnect", () => observers.emit("gone"));
});
await new Promise<void>((resolve) => app.listen(0, "127.0.0.1", resolve));
const address = app.address();
if (!address || typeof address === "string") throw new Error("expected TCP address");
console.log(address.port);
// Closing stdin shuts down the server, including when a Rust test unwinds.
await Bun.stdin.stream().getReader().read();
await io.close();
