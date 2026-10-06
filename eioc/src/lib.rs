#![doc = include_str!("../README.md")]

pub mod connector;
pub mod engine;
pub mod error;
pub mod packet;
mod polling;
pub mod transport;
mod websocket;

/// The Engine.IO protocol version implemented by this crate (`EIO` query
/// parameter).
pub const ENGINE_IO_VERSION: &str = "4";

/// Convenience re-exports for common usage.
pub mod prelude {
    pub use crate::connector::{WebSocketConnector, WebSocketStream};
    pub use crate::packet::{Frame, Handshake, Message, Packet};
    pub use crate::transport::TransportStrategy;
}
