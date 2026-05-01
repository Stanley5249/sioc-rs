//! Async [Engine.IO v4][spec] client for Rust.
//!
//! Engine.IO is the low-level transport protocol underneath Socket.IO.
//! It negotiates a connection (HTTP long-polling or WebSocket), manages
//! heartbeats, and delivers raw [`Message`](packet::Message) frames.
//! Most users will never touch this crate directly — the higher-level
//! `sioc-socket` and `sioc` crates build on top of it.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐
//! │   Engine    │
//! └──────┬──────┘
//!        │ spawns
//! ┌──────▼──────┐
//! │  transport  │  (polling → optional WebSocket upgrade)
//! └─────────────┘
//! ```
//!
//! 1. [`Engine::connect`](engine::Engine::connect) spawns the engine and
//!    transport tasks, returning channel handles for both.
//! 2. The transport task performs the HTTP long-polling handshake and
//!    optionally upgrades to WebSocket transparently.
//!
//! [spec]: https://socket.io/docs/v4/engine-io-protocol/

pub mod engine;
pub mod error;
pub mod packet;
pub mod payload;
pub mod polling;
pub mod transport;
pub mod websocket;

/// The Engine.IO protocol version implemented by this crate (`EIO` query parameter).
pub const ENGINE_IO_VERSION: &str = "4";

/// Convenience re-exports for common usage.
pub mod prelude {
    pub use crate::engine::{EngineAction, EngineSender};
    pub use crate::packet::{Handshake, Message, Packet};
    pub use crate::transport::TransportStrategy;
    pub use crate::websocket::{WebSocketConnector, WebSocketStream};
}
