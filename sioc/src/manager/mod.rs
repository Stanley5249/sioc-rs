//! Socket.IO namespace router.
//!
//! [`client_packet`] sends what the namespace handles ask for, across
//! engines. It owns the [`engine`] state: it hands an [`open_request`] to
//! [`run_engines`](engine::run_engines) for each Engine.IO connection, and
//! another one after the backoff delay when a connection drops.
//! `run_engines` runs each engine beside [`server_message`], which delivers
//! what the server sends to each namespace. Each direction has its own loop,
//! so neither waits on the other, and both share the [`routes`].
//!
//! The reconnection follows socket.io-client 4.8.4:
//!
//! - [`manager.ts`]: `Manager.onclose`, `reconnect`, `onreconnect`,
//!   `maybeReconnectOnOpen`, and `_destroy`.
//! - [`socket.ts`]: `Socket.onopen`, `onclose`, `_clearAcks`, and
//!   `emitBuffered`.
//! - [`backo2.ts`]: `Backoff.duration`, which [`backoff`] follows.
//!
//! Each engine gets new channels, because the socket.io 4.8.4 server closes
//! the whole connection when a packet other than CONNECT arrives for a
//! namespace that has not connected, in `Client.ondecoded` of [`client.ts`].
//!
//! [`manager.ts`]: https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-client/lib/manager.ts
//! [`socket.ts`]: https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-client/lib/socket.ts
//! [`backo2.ts`]: https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-client/lib/contrib/backo2.ts
//! [`client.ts`]: https://github.com/socketio/socket.io/blob/socket.io@4.8.4/packages/socket.io/lib/client.ts

pub mod backoff;
pub mod client_packet;
pub mod connect_request;
pub mod engine;
pub mod open_request;
pub mod routes;
pub mod server_message;

#[cfg(test)]
mod tests;
