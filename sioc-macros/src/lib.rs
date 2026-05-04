//! Derive macros for Sioc — generate typed Socket.IO event and acknowledgement types.
//!
//! The four core traits are intentionally separate so you only derive what each
//! use-site needs:
//!
//! | Trait | Derive | Needed for |
//! |---|---|---|
//! | [`EventType`] | `#[derive(EventType)]` | event name constant + ack/binary policy |
//! | [`AckType`] | `#[derive(AckType)]` | ack binary policy |
//! | [`SerializePayload`] | `#[derive(SerializePayload)]` | emitting (outbound) |
//! | [`DeserializePayload`] | `#[derive(DeserializePayload)]` | receiving (inbound) |
//!
//! `#[derive(EventRouter)]` is a convenience that generates `TryFrom<DynEvent>` for
//! a dispatcher enum whose variants each wrap an `Event<E>`.
//!
//! # Struct-level attributes (`#[sioc(...)]`)
//!
//! | Attribute | Applies to | Effect |
//! |---|---|---|
//! | `event(name = "str")` | `EventType` | Override the event name (default: snake_case of struct name). |
//! | `event(ack = Type)` | `EventType` | Set ack policy to `HasAck<Type>`; default is `NoAck`. |
//! | `event(binary)` | `EventType` | Set binary policy to `HasBinary`; default is `NoBinary`. |
//! | `ack(binary)` | `AckType` | Set binary policy to `HasBinary`; default is `NoBinary`. |
//! | `strict` | `DeserializePayload` | Reject payloads with more elements than fields (error on trailing data). |
//!
//! # Field-level attributes (`#[sioc(...)]`)
//!
//! | Attribute | Applies to | Effect |
//! |---|---|---|
//! | `flatten` | `SerializePayload`, `DeserializePayload` | Serialize each element of this collection field as a separate array element; deserialize all remaining elements into it. Place last. |
//!
//! # Example
//!
//! ```rust,ignore
//! // ignore: sioc-macros cannot dev-depend on sioc (circular); see sioc crate for runnable examples.
//! use sioc::prelude::*;
//!
//! // Emit-only: only SerializePayload is required.
//! #[derive(EventType, SerializePayload)]
//! struct Ping;
//!
//! // Recv-only: only DeserializePayload is required.
//! #[derive(EventType, DeserializePayload)]
//! struct Pong;
//!
//! // Bidirectional with an explicit name.
//! #[derive(EventType, SerializePayload, DeserializePayload)]
//! #[sioc(event(name = "chat_message"))]
//! struct ChatMessage {
//!     text: String,
//! }
//!
//! // Ack receive-only.
//! #[derive(AckType, DeserializePayload)]
//! struct ChatAck {
//!     ok: bool,
//! }
//! ```

mod ack_type;
mod attrs;
mod deserialize_payload;
mod event_router;
mod event_type;
mod serialize_payload;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Derives the [`EventType`] trait, providing the event
/// name constant and ack/binary policy associated types.
///
/// Serialization and deserialization are **not** included — derive
/// [`SerializePayload`] for emit and [`DeserializePayload`] for recv as needed.
///
/// # Attributes
///
/// - `#[sioc(event(name = "str"))]` — override the wire name.
///   Without it the struct name is converted to snake_case.
/// - `#[sioc(event(ack = Type))]` — set `type Ack = HasAck<Type>` (default: `NoAck`).
/// - `#[sioc(event(binary))]` — set `type Binary = HasBinary` (default: `NoBinary`).
///
/// # Example
///
/// ```rust,ignore
/// // ignore: sioc-macros cannot dev-depend on sioc (circular); see sioc crate for runnable examples.
/// use sioc::prelude::*;
///
/// // Emit-only; name defaults to "server_ping".
/// #[derive(EventType, SerializePayload)]
/// struct ServerPing;
///
/// // Recv-only with an explicit name.
/// #[derive(EventType, DeserializePayload)]
/// #[sioc(event(name = "user_joined"))]
/// struct UserJoined {
///     id: u64,
///     name: String,
/// }
/// ```
#[proc_macro_derive(EventType, attributes(sioc))]
pub fn derive_event_type(input: TokenStream) -> TokenStream {
    event_type::expand(parse_macro_input!(input))
        .unwrap_or_else(|err| err.write_errors())
        .into()
}

/// Derives the [`AckType`] trait, providing the binary
/// policy associated type.
///
/// Serialization and deserialization are **not** included — derive
/// [`SerializePayload`] to send acks and [`DeserializePayload`] to receive them.
///
/// # Attributes
///
/// - `#[sioc(ack(binary))]` — set `type Binary = HasBinary` (default: `NoBinary`).
///
/// # Example
///
/// ```rust,ignore
/// // ignore: sioc-macros cannot dev-depend on sioc (circular); see sioc crate for runnable examples.
/// use sioc::prelude::*;
///
/// // Receive acks only.
/// #[derive(AckType, DeserializePayload)]
/// struct SaveAck {
///     saved: bool,
///     id: u64,
/// }
/// ```
#[proc_macro_derive(AckType, attributes(sioc))]
pub fn derive_ack_type(input: TokenStream) -> TokenStream {
    ack_type::expand(parse_macro_input!(input))
        .unwrap_or_else(|err| err.write_errors())
        .into()
}

/// Derives the [`SerializePayload`] trait.
///
/// Serializes the struct's fields as sequential elements of a JSON array.
/// Used for the **emit** (outbound) direction. Pair with
/// [`EventType`] or [`AckType`].
///
/// # Field attributes
///
/// - `#[sioc(flatten)]` — serialize each element of this collection field as a
///   separate array element. Recommended on the last field.
///
/// # Example
///
/// ```rust,ignore
/// // ignore: sioc-macros cannot dev-depend on sioc (circular); see sioc crate for runnable examples.
/// use sioc::prelude::*;
///
/// #[derive(EventType, SerializePayload)]
/// struct Move {
///     x: i32,
///     y: i32,
///     #[sioc(flatten)]
///     tags: Vec<String>,
/// }
/// ```
#[proc_macro_derive(SerializePayload, attributes(sioc))]
pub fn derive_serialize_payload(input: TokenStream) -> TokenStream {
    serialize_payload::expand(parse_macro_input!(input))
        .unwrap_or_else(|err| err.write_errors())
        .into()
}

/// Derives the [`DeserializePayload`] trait.
///
/// Deserializes the struct's fields from sequential elements of a JSON array.
/// Used for the **recv** (inbound) direction. Pair with
/// [`EventType`] or [`AckType`].
///
/// # Struct attributes
///
/// - `#[sioc(strict)]` — return an error if the array contains more elements
///   than the struct has fields. Without it trailing elements are silently ignored.
///
/// # Field attributes
///
/// - `#[sioc(flatten)]` — deserialize all remaining array elements into this
///   collection field. Recommended on the last field; fields after it cannot be
///   deserialized because flatten consumes the remainder of the sequence.
///
/// # Example
///
/// ```rust,ignore
/// // ignore: sioc-macros cannot dev-depend on sioc (circular); see sioc crate for runnable examples.
/// use sioc::prelude::*;
///
/// // Accepts trailing elements.
/// #[derive(EventType, DeserializePayload)]
/// struct Join { room: String }
///
/// // Rejects trailing elements.
/// #[derive(EventType, DeserializePayload)]
/// #[sioc(strict)]
/// struct Ping;
///
/// // Collects trailing elements.
/// #[derive(EventType, DeserializePayload)]
/// struct Stream {
///     id: String,
///     #[sioc(flatten)]
///     tags: Vec<serde_json::Value>,
/// }
/// ```
#[proc_macro_derive(DeserializePayload, attributes(sioc))]
pub fn derive_deserialize_payload(input: TokenStream) -> TokenStream {
    deserialize_payload::expand(parse_macro_input!(input))
        .unwrap_or_else(|err: darling::Error| err.write_errors())
        .into()
}

/// Derives `TryFrom<DynEvent>` for a dispatcher enum.
///
/// Each variant must be a newtype wrapping `Event<E>` for some `E: EventType + DeserializePayload`.
///
/// The macro generates an internal helper enum with a `serde::Deserialize` impl
/// that dispatches on the event name string, plus a `TryFrom<DynEvent>` impl that
/// routes each deserialized payload to the matching variant.
///
/// # Example
///
/// ```rust,ignore
/// // ignore: sioc-macros cannot dev-depend on sioc (circular); see sioc crate for runnable examples.
/// use sioc::prelude::*;
///
/// #[derive(Debug, EventType, DeserializePayload)]
/// struct ChatMsg { text: String }
///
/// #[derive(Debug, EventType, DeserializePayload)]
/// struct UserJoined { name: String }
///
/// #[derive(Debug, EventRouter)]
/// enum AppEvent {
///     ChatMsg(Event<ChatMsg>),
///     UserJoined(Event<UserJoined>),
/// }
/// ```
#[proc_macro_derive(EventRouter, attributes(sioc))]
pub fn derive_from_event(input: TokenStream) -> TokenStream {
    event_router::expand(parse_macro_input!(input))
        .unwrap_or_else(|err: darling::Error| err.write_errors())
        .into()
}
