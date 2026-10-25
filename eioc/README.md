# eioc

Async [Engine.IO protocol revision 4](https://socket.io/docs/v4/engine-io-protocol/) client for Rust, the transport layer of Socket.IO 3.x and 4.x.

## What is the Engine.IO protocol?

The [Engine.IO protocol](https://socket.io/docs/v4/engine-io-protocol/) provides bidirectional communication over HTTP long-polling and WebSocket. By default, this client starts with polling and upgrades to WebSocket when the server offers it. It also supports connecting directly over WebSocket.

## Usage

The higher-level [`sioc`](https://docs.rs/sioc) crate adds namespaces, events, and acknowledgements.

## License

MIT OR Apache-2.0.
