# eioc

Async [Engine.IO protocol v4](https://socket.io/docs/v4/engine-io-protocol/) client for Rust.

## What is the Engine.IO protocol?

The [Engine.IO protocol v4](https://socket.io/docs/v4/engine-io-protocol/) enables full-duplex, low-overhead communication between a client and a server. It is based on the WebSocket protocol and uses HTTP long-polling as fallback if a WebSocket connection cannot be established.

## Usage

Most users will never touch this crate directly. The higher-level [`sioc`](https://docs.rs/sioc) crate builds on top of it.

## License

MIT OR Apache-2.0.
