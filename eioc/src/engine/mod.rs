//! The Engine.IO session, like engine.io-client's `Socket`.
//!
//! [`open`] spawns one task per session. Inside it, `EngineCore` owns every
//! session rule and never awaits, and `actor::run` carries data between four
//! bounded channels and polls the transport future, which owns its own I/O,
//! concurrency, and upgrade:
//!
//! ```text
//!  manager --ClientMessage--> run --ClientFrame--> transport (polling, websocket)
//!  manager <--ServerMessage-- run <--ServerFrame-- transport
//!                               |
//!                          EngineCore
//! ```
//!
//! Invariants:
//!
//! - Each direction takes its destination's permit before it reads, except
//!   server frames, which are read first so pings ahead of the next message are
//!   answered while the manager is full. That frame's message is the one item
//!   the task holds, and no frame is read until it is delivered. A ping behind
//!   an undelivered message waits, so a manager that stalls for a whole ping
//!   window times the session out.
//! - The deadline is its own branch, so it fires while either direction waits
//!   for capacity.
//! - Messages sent before the handshake wait in the transport channel, like
//!   `writeBuffer`, because a transport reads that channel only after the
//!   handshake. A hang-up during opening drops the transport, since the server
//!   holds no session yet. The task sees that hang-up only while the transport
//!   channel has room, so the manager sends nothing before Open.
//! - The transport checks that the first frame is Open, and ends the server
//!   direction at the server's Close packet, which `ServerFrame` cannot hold.
//!   `ClientFrame` holds only what the engine sends, because the transport owns
//!   Close, Upgrade, and the probe. The end of a channel is the only close
//!   signal. `closed_rx` is such a channel: it carries no value and ends with
//!   the session, ahead of the messages still queued for the manager.
//! - No wait cycle: the manager and this task each serve their inputs from one
//!   `select!` and never await in a handler, and the transport runs each
//!   direction on its own loop. When the transport ends the server direction,
//!   the task drops its transport sender, which the transport waits for. The
//!   task drops its manager sender before it waits for the manager to hang up,
//!   which the manager does at that end.
//! - Each owner keeps one fact. `EngineCore` knows whether the session is open,
//!   and returns a deadline failure as an `Err`. The task keeps the first
//!   failure in one `Result` while cleanup runs, and reads each closure from
//!   the end of a channel: the manager's end discards unread server messages,
//!   while the transport's end and a deadline failure still deliver the message
//!   already received. A deadline failure also drops the transport future,
//!   because the server no longer answers.
//!
//! JavaScript reference: [engine.io-client 6.6.7's `Socket`].
//!
//! [engine.io-client 6.6.7's `Socket`]: https://github.com/socketio/socket.io/blob/11a6f561a757264c3231c3383c4cdbf37bcf52de/packages/engine.io-client/lib/socket.ts

mod actor;
mod state;

#[cfg(test)]
mod tests;

pub use actor::{Engine, EngineOptions, open};

pub use crate::packet::{ClientMessage, Handshake, ServerMessage};
pub use crate::transport::TransportStrategy;
