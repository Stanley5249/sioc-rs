//! The Socket.IO manager, like socket.io-client's `Manager`: one Engine.IO
//! connection shared by every namespace of a client, and its reconnection.
//!
//! [`state::ManagerCore`] owns all socket, ack, and reconnection state and
//! never awaits. The socket handles and [`driver::run`] share it behind one
//! mutex in [`driver::Manager`]: a handle runs its transition, such as an
//! emit or a disconnect, under the lock and wakes the driver, and the driver
//! owns the I/O: the engine, the reconnection timer, and one server packet
//! that waits for a full receiver.
//!
//! ```text
//!  Client::connect --connect----------+
//!  SocketSender --emit / ack / disconnect--+--> Mutex<ManagerCore> --write buffer + permit--> engine
//!                                     |          ^       |
//!                         wake.notify |  driver -+       +-- try_send / blocked packet --> SocketReceiver
//!  engine --ServerMessage-------------------> driver
//! ```
//!
//! Invariants:
//!
//! - No lock spans an `.await` or Engine construction, including connector
//!   cloning. Every transition runs whole under one lock. A handle wakes the
//!   driver after unlocking, including during unwinding. `Notify::notify_one`
//!   stores a permit while the sole driver is busy, so the next wait observes
//!   the change. Poisoned state is recovered only to shut down, never to
//!   resume.
//! - Each queued event or reply holds a slot of its accepting handles'
//!   capacity. One Socket owns exactly one participation at a time, so each
//!   name has one capacity pool. A replacement waits outside the core for the
//!   flushing participation's release token and then retries, so a cancelled
//!   wait leaves nothing behind. Server traffic cannot create sockets or
//!   capacity pools.
//! - A slot returns on header submission or cancellation. At most one packet's
//!   remaining attachments also occupy the write buffer, bounded by
//!   `MAX_ATTACHMENTS`. Capacities count packets, not retained bytes.
//! - A packet leaves the core only on an engine permit the driver holds, and a
//!   binary packet's attachments go next. An ack waiter is registered when its
//!   event leaves, so an engine's end fails exactly the waiters of events it
//!   took.
//! - Local disconnect closes admission but retains a `Disconnecting` owner and
//!   its ACK map until its FIFO events and DISCONNECT are submitted or
//!   cancelled. Only then is the name free for new handles. Server termination
//!   cancels that owner's unsubmitted packets and DISCONNECT, but never
//!   attachments whose header was already submitted. Engine loss cancels a
//!   local flush.
//! - A queued packet is resolved only to its still-owned participation, never a
//!   replacement's ACK map. The per-Socket ACK counter survives every
//!   replacement.
//! - Ended handles have a closed capacity, which makes every later call of
//!   theirs fail or do nothing, so they never touch new handles of the name.
//! - A reply is accepted only for the connection it captured before waiting for
//!   capacity, and engine loss discards queued replies and returns unsent
//!   events to their send buffer.
//! - A blocked server packet keeps its original receiver and token, and the
//!   driver reads no engine message until it is delivered or its handles end.
//!   The engine's session end still wakes the driver, which then drains the
//!   engine without waiting for that receiver.
//! - The driver ends every handle when it stops for any reason, including a
//!   panic or an aborted task.
//!
//! The behavior follows socket.io-client 4.8.4:
//!
//! - [`manager.ts`]: `Manager.onclose`, `reconnect`, `onreconnect`,
//!   `maybeReconnectOnOpen`, and `_destroy`.
//! - [`socket.ts`]: `Socket.onopen`, `onclose`, `_clearAcks`, and
//!   `emitBuffered`.
//! - [`backo2.ts`]: `Backoff.duration`, which [`backoff`] follows.
//! - [`engineSocket.ts`]: Engine.IO client 6.6.7's `Socket.readyState` and
//!   `close`, which may defer transport closure for buffered writes or an
//!   upgrade.
//!
//! Each engine gets new channels, because the socket.io 4.8.4 server closes
//! the whole connection when a packet other than CONNECT arrives for a
//! namespace that has not connected, in `Client.ondecoded` of [`client.ts`].
//!
//! [`manager.ts`]: https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-client/lib/manager.ts
//! [`socket.ts`]: https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-client/lib/socket.ts
//! [`backo2.ts`]: https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-client/lib/contrib/backo2.ts
//! [`engineSocket.ts`]: https://github.com/socketio/socket.io/blob/11a6f561a757264c3231c3383c4cdbf37bcf52de/packages/engine.io-client/lib/socket.ts
//! [`client.ts`]: https://github.com/socketio/socket.io/blob/socket.io@4.8.4/packages/socket.io/lib/client.ts

pub mod backoff;
pub mod decoder;
pub mod driver;
pub mod engine;
pub mod socket;
pub mod state;

#[cfg(test)]
mod tests;
