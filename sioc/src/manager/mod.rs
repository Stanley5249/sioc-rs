//! The Socket.IO manager, like socket.io-client's `Manager`: one Engine.IO
//! connection shared by every namespace of a client, and its reconnection.
//!
//! [`actor::run`] is the one task that owns every channel end, the engine, and
//! the reconnection timer. It turns what they yield into inputs for the
//! synchronous [`core`], which owns all namespace, reconnection, and ack state,
//! and carries out the effects the core hands back. One owner means no lock
//! and no second registry.
//!
//! ```text
//!  Client --ConnectRequest--+                     +--> Open / Close engine
//!  namespace inboxes -------+--> actor::run ------+--> Schedule / Cancel wait
//!  engine --ServerMessage---+     | handle        +--> Listen (poll an inbox)
//!  engine future, sleep ----+     v
//!                           ManagerCore --next_message--> engine permit
//!                                       --try_send / deliver--> SocketReceiver
//! ```
//!
//! The actor never awaits a send in a handler. It waits for an engine permit,
//! a receiver slot, or a room permit as one `select!` branch beside the
//! others, so a full queue holds up only the data that needs it, while
//! connect, shutdown, and the other direction keep going. Every bound lives in
//! a channel capacity or a semaphore:
//!
//! - Each namespace inbox and send buffer holds `ChannelConfig::manager`
//!   packets. Draining a closed inbox ignores the room, so a buffer reaches at
//!   most twice that.
//! - The core holds one server packet for a full receiver, and one binary
//!   packet under reconstruction.
//! - Each namespace queues at most one CONNECT and one DISCONNECT.
//!
//! Shutdown never waits on the application. Once the client handle and every
//! inbox are gone, the core lets the engine go after its queued packets leave.
//! After an internal error, the core ends every namespace at once and the
//! actor drops the inboxes. Either way the actor reads the engine to its end
//! before it returns, so the engine never sees a failed send.
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
pub mod core;

#[cfg(test)]
mod tests;
