# eioc

Async [Engine.IO protocol revision 4](https://socket.io/docs/v4/engine-io-protocol/) client for Rust, the transport layer of Socket.IO 3.x and 4.x.

## What is the Engine.IO protocol?

The [Engine.IO protocol](https://socket.io/docs/v4/engine-io-protocol/) enables full-duplex, low-overhead communication between a client and a server. It is based on the WebSocket protocol and uses HTTP long-polling as fallback if a WebSocket connection cannot be established.

## Usage

Most users will never touch this crate directly. The higher-level [`sioc`](https://docs.rs/sioc) crate builds on top of it.

## License

MIT OR Apache-2.0.
