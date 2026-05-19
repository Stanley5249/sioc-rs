# sioc

A type-safe, async [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) client for Rust.

## What is the Socket.IO protocol?

The [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) is a real-time, bidirectional event-based communication protocol built on top of the [Engine.IO protocol v4](https://socket.io/docs/v4/engine-io-protocol/).

It adds features such as _namespaces_, which allow multiple channels over a single connection; _events_, which are named messages with structured data; _acknowledgements_, enabling request/response patterns over events; and support for _binary attachments_.

## Features

- Supports Socket.IO protocol v5 and Engine.IO protocol v4.
- Async networking built on [Tokio](https://tokio.rs), [reqwest](https://docs.rs/reqwest), and [tokio-tungstenite](https://docs.rs/tokio-tungstenite).
- No boxed futures and a lock-free actor model. Channel-based event loops avoid callback borrow issues and fit stateful applications naturally.
- Derive macros for events, acks, serialization, and routing.
- Zero-copy packet parsing via [`bytestring`](https://docs.rs/bytestring) and [`bytes`](https://docs.rs/bytes).

## License

Licensed under MIT OR Apache-2.0.
