# sioc

A type-safe, async [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) client for Rust.

## What is the Socket.IO protocol?

The [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) is built on top of [Engine.IO protocol v4](https://socket.io/docs/v4/engine-io-protocol/).

It adds _namespaces_ for multiple channels over a single connection, _events_ as named messages with structured data, _acknowledgements_ for request/response patterns, and support for _binary attachments_.

## Design

- Async networking built on [Tokio](https://tokio.rs), [reqwest](https://docs.rs/reqwest), and [tokio-tungstenite](https://docs.rs/tokio-tungstenite).
- Actor-model client, communicating through channels, no internal locks.
- Derive macros for events and acks.
- Event handling lives in match arms, no callbacks with boxed futures.
- State lives in the enclosing scope, no Arc or Mutex.
- Zero-copy packet parsing via [`bytestring`](https://docs.rs/bytestring) and [`bytes`](https://docs.rs/bytes).

## License

MIT OR Apache-2.0.
