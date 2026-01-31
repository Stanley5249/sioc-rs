//! # Sioc Core
//!
//! Core types and client implementation for the Sioc async Socket.IO client.
//!
//! This crate provides the foundational types for building a high-performance,
//! zero-copy Socket.IO client.

#![warn(missing_docs, clippy::all)]
#![cfg_attr(docsrs, feature(doc_cfg))]

// Public modules
pub mod builder;
pub mod client;
pub mod error;
pub mod event;
pub mod packet;
pub mod router;

/// Prelude module with commonly used types.
///
/// Import this module to get access to the most frequently used types:
///
/// ```rust
/// use sioc_core::prelude::*;
/// ```
pub mod prelude {
    pub use crate::client::{SocketReceiver, SocketSender, connect, emit, event};
    pub use crate::error::{Error, Result};
    pub use crate::event::{Event, to_event};
    pub use crate::packet::{AckPacket, Attachments, BinaryPlaceholder, EventPacket, Packet};
    pub use crate::router::RouterCommand;
}

// Version information
/// Current version of sioc-core
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
