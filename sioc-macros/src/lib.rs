//! Procedural macros for Sioc - derive `Event` and `Receive`.
//!
//! This crate provides derive macros that generate type-safe Socket.IO protocol
//! definitions from Rust enums.
//!
//! ## Macros
//!
//! - [`Event`] - Generate Event trait implementation for outgoing events
//! - [`Receive`] - Generate Receive trait implementation for incoming events
//!
//! ## Example
//!
//! ```rust,ignore
//! use sioc_macros::{Event, Receive};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Event, Serialize)]
//! enum OutgoingEvents {
//!     #[event("message")]
//!     Message(String),
//!
//!     #[event("join")]
//!     JoinRoom { room: String },
//! }
//!
//! #[derive(Receive, Deserialize)]
//! enum IncomingEvents {
//!     #[event("welcome")]
//!     Welcome(String),
//!
//!     #[event("chat")]
//!     ChatMessage { user: String, text: String },
//! }
//! ```

#![warn(missing_docs, clippy::all)]

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod emit;
mod receive;
mod util;

// Derive macro for generating Event trait implementation.
///
/// This macro implements the `Event` trait which provides event name
/// extraction and binary detection for Socket.IO events.
///
/// # Example
///
/// ```rust,ignore
/// use sioc_macros::Event;
/// use serde::Serialize;
///
/// #[derive(Event, Serialize)]
/// enum Events {
///     #[event("ping")]
///     Ping,
///
///     #[event("message")]
///     Message(String),
///
///     #[event("upload")]
///     Upload {
///         filename: String,
///         data: BinaryIndex,
///     },
/// }
///
/// // Usage:
/// // client.emit(Events::Ping).await?;
/// // client.emit(Events::Message("hello".into())).await?;
/// ```
///
/// # Attributes
///
/// - #[event("name")] - Required. Specifies the Socket.IO event name.
///
/// # Generated Code
///
/// The macro generates:
/// - name() method returning the event name
/// - into_event_payload() method for serialization
/// - Full Event trait implementation
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    emit::expand(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Derive macro for generating Receive trait implementation.
///
/// This macro implements the `Receive` trait which deserializes
/// Socket.IO packets into typed event enums.
///
/// # Example
///
/// ```rust,ignore
/// use sioc_macros::Receive;
/// use serde::Deserialize;
///
/// #[derive(Receive, Deserialize)]
/// enum Events {
///     #[event("message")]
///     Message(String),
///
///     #[event("upload")]
///     Upload {
///         filename: String,
///         file_ptr: BinaryIndex,
///     },
/// }
///
/// // Usage:
/// // let event = Events::from_packet(&packet)?;
/// ```
///
/// # Attributes
///
/// - #[event("name")] - Required. Specifies the Socket.IO event name.
///
/// # Binary Event Support
///
/// Events with BinaryIndex fields are automatically supported.
/// The binary data is available via Incoming<T>.attachments().
///
/// # Generated Code
///
/// The macro generates:
/// - from_packet() method for deserialization
/// - Event name matching logic
/// - Payload deserialization for each variant
/// - Full Receive trait implementation
#[proc_macro_derive(Receive, attributes(event))]
pub fn derive_receive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    receive::expand(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
