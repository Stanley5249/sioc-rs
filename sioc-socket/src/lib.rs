//! Core Socket.IO v4 protocol implementation.
//!
//! Sits between the Engine.IO transport (`sioc-engine`) and the typed public API (`sioc`).
//! Implements namespace multiplexing, acknowledgement tracking, and binary reassembly.
//!
//! ```text
//! ┌──────────────┐
//! │     sioc     │  (typed events, derive macros)
//! ├──────────────┤
//! │  sioc-socket │  ← you are here
//! ├──────────────┤
//! │  sioc-engine │  (Engine.IO v4 transport)
//! └──────────────┘
//! ```
//!
//! Most users should depend on `sioc` instead. Depend on `sioc-socket` directly only for
//! raw [`Directive`](packet::Directive) / [`Packet`](packet::Packet) access.
//!
//! [spec]: https://socket.io/docs/v4/socket-io-protocol/

pub mod error;
pub mod manager;
pub mod packet;
pub mod parse;

pub mod prelude {
    pub use crate::manager::ManagerAction;
    pub use crate::packet::{Connect, ConnectError, DynAck, DynEvent, Ns};
}
