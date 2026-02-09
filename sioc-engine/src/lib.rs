//! Async [Engine.IO v4][spec] client for Rust.
//!
//! Engine.IO is the low-level transport protocol underneath Socket.IO.
//! It negotiates a connection (HTTP long-polling or WebSocket), manages
//! heartbeats, and delivers raw [`Message`](packet::Message) frames.
//! Most users will never touch this crate directly — the higher-level
//! `sioc-core` and `sioc` crates build on top of it.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐
//! │   Engine    │
//! └──────┬──────┘
//!        │ owns
//! ┌──────▼──────┐
//! │  Transport  │  (Polling / WebSocket)
//! └─────────────┘
//! ```
//!
//! 1. [`transports::Transport`] connects and handshakes via [`transports::Transport::connect_polling`]
//!    or [`transports::Transport::connect_websocket`].
//! 2. [`Engine`](engine::Engine) drives the protocol loop: automatic
//!    ping/pong, heartbeat timeout, and message framing.
//!
//! [spec]: https://socket.io/docs/v4/engine-io-protocol/

pub mod engine;
pub mod error;
pub mod packet;
pub mod polling;
pub mod transports;
pub mod websocket;

/// The Engine.IO protocol version implemented by this crate (`EIO` query parameter).
pub const ENGINE_IO_VERSION: u64 = 4;

/// Convenience re-exports for common usage.
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::packet::{EioPacket, Frame, Handshake, Message};
    pub use crate::transports::Transport;
    pub use crate::websocket::{
        WebSocketConnector, WebSocketError, WebSocketStream, default_connect,
    };
}
