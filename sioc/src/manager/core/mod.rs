//! The manager's synchronous core.
//!
//! [`manager_core::ManagerCore`] applies one [`input_output::ManagerInput`] at
//! a time and hands its effects out as [`input_output::ManagerOutput`], so the
//! namespace, reconnection, and ack state has one writer and needs no lock.
//! The core never awaits and spawns nothing. It touches the application's
//! channels only through `try_send`, `oneshot::Sender::send`, semaphore
//! permits, and permits that its caller hands in, so plain `#[test]`s drive
//! it.
//!
//! The pieces follow socket.io-client 4.8.4:
//!
//! - [`manager_core`]: `Manager`, in [`manager.ts`].
//! - [`namespaces`] and [`namespace`]: `Manager.nsps` and `Socket`, in
//!   [`socket.ts`], with [`send_buffer`] as its `sendBuffer` and [`acks`] as
//!   its `ids` and `acks`.
//! - [`decoder`]: `Decoder` and `BinaryReconstructor` of socket.io-parser.
//!
//! [`manager.ts`]: https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-client/lib/manager.ts
//! [`socket.ts`]: https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-client/lib/socket.ts

pub mod acks;
pub mod decoder;
pub mod input_output;
pub mod manager_core;
pub mod namespace;
pub mod namespaces;
pub mod send_buffer;

#[cfg(test)]
mod tests;
