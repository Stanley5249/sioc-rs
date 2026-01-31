//! Procedural macros for Sioc - derive `Event`.
//!
//! This crate provides derive macros that generate type-safe Socket.IO protocol
//! definitions from Rust enums.
//!
//! ## Macros
//!
//! - [`Event`] - Generate Event trait implementation for outgoing events
//!
//! ## Example
//!
//! ```rust,ignore
//! use sioc_macros::Event;
//! use serde::Serialize;
//!
//! #[derive(Event, Serialize)]
//! enum OutgoingEvents {
//!     #[event("message")]
//!     Message(String),
//!
//!     #[event("join")]
//!     JoinRoom { room: String },
//! }
//! ```

#![warn(missing_docs, clippy::all)]

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod emit;
mod util;

/// Derive macro for generating Event trait implementation.
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
/// - to_json() method for serialization
/// - Full Event trait implementation
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    emit::expand(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
