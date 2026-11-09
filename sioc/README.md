# sioc

A type-safe, async [Socket.IO protocol revision 5](https://socket.io/docs/v4/socket-io-protocol/) client for Rust.

## Socket.IO protocol

The [Socket.IO protocol](https://socket.io/docs/v4/socket-io-protocol/) is built on top of the [Engine.IO protocol](https://socket.io/docs/v4/engine-io-protocol/).

It adds _namespaces_ for multiple channels over a single connection, _events_ as named messages with structured data, _acknowledgements_ for request/response patterns, and support for _binary attachments_.

## Compatibility

Socket.IO numbers its libraries and its protocols separately. `sioc` speaks Socket.IO protocol revision 5 over Engine.IO protocol revision 4, which Socket.IO 3.x and 4.x servers use, and the [v4 documentation](https://socket.io/docs/v4/) describes both. Its behavior follows the JavaScript client, socket.io-client.

## Design

- Derive macros type every event and ack, and events arrive in a match loop where the application state lives.
- Reconnection works like the JavaScript client. After a dropped connection, the client backs off, opens a new Engine.IO session, and connects every open namespace again.
- Networking uses [`tokio`](https://docs.rs/tokio), [`reqwest`](https://docs.rs/reqwest), and [`tokio-tungstenite`](https://docs.rs/tokio-tungstenite).

## License

MIT OR Apache-2.0.
