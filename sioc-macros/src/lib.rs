#![warn(missing_docs, clippy::all)]

//! Procedural macros for Sioc - derive `Event`.
//!
//! This crate provides derive macros that generate type-safe Socket.IO protocol
//! definitions from Rust enums and structs.
//!
//! ## Macros
//!
//! - [`Event`] - Generate Event trait implementation for outgoing events
//!
//! ## Example
//!
//! ```rust,ignore
//! use sioc_macros::Event;
//!
//! #[derive(Event)]
//! enum OutgoingEvents {
//!     #[sioc(event = "ping")]
//!     Ping,
//!
//!     #[sioc(event = "message")]
//!     Message(String),
//! }
//! ```

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod codegen;
mod emit;
mod input;

/// Derive macro for generating Event trait implementation.
///
/// This macro implements the `Event` trait which provides event name
/// extraction and JSON serialization for Socket.IO events.
///
/// # Example
///
/// ```rust,ignore
/// use sioc_macros::Event;
///
/// #[derive(Event)]
/// enum Events {
///     #[sioc(event = "ping")]
///     Ping,
///
///     #[sioc(event = "message")]
///     Message(String),
/// }
/// ```
///
/// # Attributes
///
/// - #[sioc(event = "name")] - Required. Specifies the Socket.IO event name.
#[proc_macro_derive(Event, attributes(sioc))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    emit::expand(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
