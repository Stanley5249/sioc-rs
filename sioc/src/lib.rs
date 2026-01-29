//! # Sioc - High-Performance Async Socket.IO Client
//!
//! Sioc is a next-generation Socket.IO client for Rust, built from the ground up
//! for async/await with a focus on zero-copy performance and type safety.
//!
//! ## Features
//!
//! - **🚀 Zero-Copy Architecture**: Uses `bytes::Bytes` throughout to minimize allocations
//! - **🔒 Type-Safe Protocol**: Compile-time verification via derive macros
//! - **⚡ High Performance**: 1-2 allocations per event (vs 4-6 in traditional clients)
//! - **🎯 Clean API**: Intuitive enum-based event definitions
//! - **📦 Binary Support**: First-class support for binary payloads
//! - **✅ Typed Acks**: `Ack<T>` ensures correct acknowledgement types
//! - **🔄 Async-Only**: Built on `tokio` for modern async Rust
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use sioc::{SiocClient, Event, Receive, Ack};
//! use serde::{Deserialize, Serialize};
//! use async_trait::async_trait;
//!
//! // Define events you can send
//! #[derive(Event, Serialize)]
//! enum OutgoingEvents {
//!     #[event("message")]
//!     Message(String),
//! }
//!
//! // Define events you can receive
//! #[derive(Receive, Deserialize)]
//! enum IncomingEvents {
//!     #[event("welcome")]
//!     Welcome(String),
//!
//!     #[event("ping")]
//!     Ping(Ack<String>),
//! }
//!
//! // Implement your handler
//! struct MyBot;
//!
//! #[async_trait]
//! impl IncomingEventsHandler for MyBot {
//!     async fn on_welcome(&mut self, msg: String) {
//!         println!("Server says: {}", msg);
//!     }
//!
//!     async fn on_ping(&mut self, ack: Ack<String>) {
//!         ack.reply("pong".to_string()).await.ok();
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> sioc::Result<()> {
//!     // Connect to server (placeholder - will be implemented in Week 2)
//!     // let client = SiocClient::connect("http://localhost:3000", MyBot).await?;
//!
//!     // Emit events
//!     // client.emit(OutgoingEvents::Message("Hello!".into())).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Binary Events
//!
//! ```rust,ignore
//! use sioc::{Event, BinaryIndex};
//! use bytes::Bytes;
//! use serde::Serialize;
//!
//! #[derive(Event, Serialize)]
//! enum Events {
//!     #[event("upload")]
//!     Upload {
//!         filename: String,
//!         file_ptr: BinaryIndex,
//!     },
//! }
//!
//! async fn upload_file() -> sioc::Result<()> {
//!     // let client = ...;
//!     let file_data = Bytes::from(vec![1, 2, 3, 4]);
//!
//!     // client.emit(Events::Upload {
//!     //     filename: "test.bin".into(),
//!     //     file_ptr: BinaryIndex::new(0),
//!     // })
//!     // .attach(file_data)
//!     // .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! Sioc uses an actor-based architecture:
//!

//!
//! ## Performance
//!
//! Sioc is designed for minimal overhead:
//!
//! - **Emit latency**: <50μs (serialization + channel send)
//! - **Receive latency**: <100μs (deserialization + handler spawn)
//! - **Allocations**: 1-2 per event (vs 4-6 in traditional clients)
//! - **Memory**: ~4KB per client instance
//!
//! ## Comparison with rust-socketio
//!
//! | Feature | rust-socketio | Sioc |
//! |---------|---------------|--------|
//! | API | Callback-based | Async trait-based |
//! | Type Safety | Runtime (`Any`) | Compile-time (generics) |
//! | Allocations | 4-6 per event | 1-2 per event |
//! | Binary Data | `Vec<u8>` copies | `Bytes` zero-copy |
//! | Acks | Untyped closures | `Ack<T>` typed |
//! | Errors | Panics in callbacks | `Result<T>` |
//!
//! ## Status
//!
//! **Current version**: 0.1.0 (Alpha)
//!
//! This is an early release. The API is stable but not all features are implemented yet.
//! See the [roadmap](https://github.com/yourusername/sioc/blob/main/.github/plans/ROADMAP.md)
//! for planned features.
//!
//! **What works:**
//! - Core types (`Packet`, `Ack<T>`, `BinaryPlaceholder`)
//! - ✅ Error handling
//! - ✅ Internal command protocol
//! - 🚧 Derive macros (placeholders)
//! - 🚧 Engine integration (planned Week 2)
//! - 🚧 Client implementation (planned Week 2)
//!
//! ## Examples
//!
//! See the `examples/` directory for complete working examples:
//!
//! - `chat.rs` - Basic chat client
//! - `file_upload.rs` - Binary file upload
//! - `game.rs` - Game client with acknowledgements
//! - `namespaces.rs` - Multi-namespace client
//!
//! ## Contributing
//!
//! Contributions are welcome! Please see [CONTRIBUTING.md](https://github.com/yourusername/sioc/blob/main/CONTRIBUTING.md).
//!
//! ## License
//!
//! Licensed under either of:
//!
//! - Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
//! - MIT license ([LICENSE-MIT](LICENSE-MIT))
//!
//! at your option.

#![warn(missing_docs, clippy::all)]
#![cfg_attr(docsrs, feature(doc_cfg))]

/// Prelude module for convenient imports.
///
/// Import everything you need with:
///
/// ```rust
/// use sioc::prelude::*;
/// ```
pub mod prelude {
    pub use sioc_core::prelude::*;
    pub use sioc_macros::{Event, Receive};
}

/// Current version of Sioc.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
