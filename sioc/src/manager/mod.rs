//! The Socket.IO manager, like socket.io-client's `Manager`: one Engine.IO
//! connection shared by every namespace of a client, and its reconnection.
//!
//! [`state::ManagerCore`] owns all namespace, ack, and reconnection state and
//! never awaits. [`actor::run`] is the one task that owns the I/O around it:
//! the engine, the reconnection timer, and the channel of every namespace's
//! handles. Each scope takes one kind of input and talks in two directions:
//!
//! ```text
//!  SocketSender --ClientPacket--> channel (= Socket.sendBuffer) --+
//!  Client::connect --ConnectRequest-------------------------------+--> actor
//!  engine --ServerMessage-----------------------------------------+     |
//!                                                     ManagerInput |     v
//!                                                     ManagerCore  <-----+
//!            SocketReceiver <--try_send / held packet--   |   --write buffer + permit--> engine
//! ```
//!
//! Invariants:
//!
//! - The actor reads a namespace's channel only when the packet can go to the
//!   engine at once on a permit it already holds, or is to be dropped. Unsent
//!   packets wait in the channel, which bounds them by
//!   `ChannelConfig::manager`.
//! - The write buffer holds CONNECT and DISCONNECT packets and the attachments
//!   of the one packet in flight. A server packet whose receiver is full is
//!   held alone, and the actor reads at most one more engine message, to see
//!   the engine hang up, until it is delivered.
//! - An ack waiter is registered when its event goes to the engine, so an
//!   engine's end fails exactly the waiters of events it took.
//! - An engine closes for the core only after its future finished and its
//!   server direction ended, and each engine gets new channels.
//! - Ending a namespace never waits on the application: its token closes the
//!   channel, which the actor reads to the end.
//!
//! The behavior follows socket.io-client 4.8.4:
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

pub mod actor;
pub mod backoff;
pub mod connect_request;
pub mod decoder;
pub mod namespace;
pub mod send_buffer;
pub mod state;

#[cfg(test)]
mod tests;
