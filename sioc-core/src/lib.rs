//! Core Socket.IO v4 client protocol implementation.
//!
//! This crate sits between the wire-level Engine.IO transport (`sioc-engine`)
//! and the typed public API (`sioc`).  It implements the
//! [Socket.IO v4 protocol][spec]: packet encoding/decoding, namespace
//! multiplexing, acknowledgement tracking, and binary attachment reassembly.
//!
//! # Layering
//!
//! ```text
//! ┌─────────┐  typed events / acks
//! │  sioc    │  (derive macros, marker traits)
//! ├─────────┤
//! │sioc-core│  ← you are here (protocol logic)
//! ├─────────┤
//! │sioc-eng.│  (Engine.IO v4 transport)
//! └─────────┘
//! ```
//!
//! Most users should depend on `sioc` instead — it re-exports the types
//! from this crate that are part of the public surface.  Depend on
//! `sioc-core` directly only if you need raw [`Command`](packet::Command) /
//! [`Packet`](packet::Packet) access without the typed layer.
//!
//! # Key types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`Manager`](manager::Manager) | Internal event loop: routing, ack tracking, binary reassembly |
//! | [`Packet`](packet::Packet) | Fully assembled inbound packet |
//! | [`Command`](packet::Command) | Outbound packet ready for wire encoding |
//!
//! [spec]: https://socket.io/docs/v4/socket-io-protocol/

pub mod error;
pub mod manager;
pub mod packet;
pub mod parse;

pub mod prelude {
    pub use crate::error::{Error, ParseError, PayloadError, Result};
    pub use crate::manager::{CommandSender, ManagerAction};
    pub use crate::packet::{Connect, ConnectError, DynAck, DynEvent, Ns, Packet};
    pub use crate::parse::{split_attachments, split_id, split_namespace};
}
