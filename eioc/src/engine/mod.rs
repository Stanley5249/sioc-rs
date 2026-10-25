//! Engine.IO protocol task.
//!
//! [`session`] runs one session: the `protocol` loops beside the transport,
//! with a `heartbeat` deadline on server pings.
//!
//! JavaScript reference: [engine.io-client 6.6.7's `Socket`].
//!
//! [engine.io-client 6.6.7's `Socket`]: https://github.com/socketio/socket.io/blob/11a6f561a757264c3231c3383c4cdbf37bcf52de/packages/engine.io-client/lib/socket.ts

mod heartbeat;
mod protocol;
pub mod session;

#[cfg(test)]
mod tests;
