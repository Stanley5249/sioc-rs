#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(clippy::pedantic)]

pub mod engine;
pub mod error;
pub mod packet;
pub mod polling;
pub mod transport;
pub mod websocket;

/// The Engine.IO protocol version implemented by this crate (`EIO` query parameter).
pub const ENGINE_IO_VERSION: &str = "4";

/// Convenience re-exports for common usage.
pub mod prelude {
    pub use crate::engine::{EngineAction, FrameSender, MessageSender};
    pub use crate::packet::{Frame, Handshake, Message, Packet};
    pub use crate::transport::TransportStrategy;
    pub use crate::websocket::{WebSocketConnector, WebSocketStream};
}
