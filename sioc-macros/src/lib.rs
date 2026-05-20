#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(clippy::doc_markdown)]

mod ack_type;
mod attrs;
mod deserialize_payload;
mod event_router;
mod event_type;
mod serialize_payload;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Derives the [`EventType`](https://docs.rs/sioc/latest/sioc/event/trait.EventType.html) trait,
/// providing the event name constant and ack/binary policy associated types.
///
/// See the [crate documentation](self) for the full attribute reference.
#[proc_macro_derive(EventType, attributes(sioc))]
pub fn derive_event_type(input: TokenStream) -> TokenStream {
    event_type::expand(parse_macro_input!(input))
        .unwrap_or_else(|err| err.write_errors())
        .into()
}

/// Derives the [`AckType`](https://docs.rs/sioc/latest/sioc/ack/trait.AckType.html) trait,
/// providing the binary policy associated type.
///
/// See the [crate documentation](self) for the full attribute reference.
#[proc_macro_derive(AckType, attributes(sioc))]
pub fn derive_ack_type(input: TokenStream) -> TokenStream {
    ack_type::expand(parse_macro_input!(input))
        .unwrap_or_else(|err| err.write_errors())
        .into()
}

/// Derives the [`SerializePayload`](https://docs.rs/sioc/latest/sioc/payload/trait.SerializePayload.html) trait.
///
/// Serializes the struct's fields as sequential JSON array elements for the emit (outbound) direction.
/// See the [crate documentation](self) for the full attribute reference.
#[proc_macro_derive(SerializePayload, attributes(sioc))]
pub fn derive_serialize_payload(input: TokenStream) -> TokenStream {
    serialize_payload::expand(parse_macro_input!(input))
        .unwrap_or_else(|err| err.write_errors())
        .into()
}

/// Derives the [`DeserializePayload`](https://docs.rs/sioc/latest/sioc/payload/trait.DeserializePayload.html) trait.
///
/// Deserializes the struct's fields from sequential JSON array elements for the recv (inbound) direction.
/// See the [crate documentation](self) for the full attribute reference.
#[proc_macro_derive(DeserializePayload, attributes(sioc))]
pub fn derive_deserialize_payload(input: TokenStream) -> TokenStream {
    deserialize_payload::expand(parse_macro_input!(input))
        .unwrap_or_else(|err| err.write_errors())
        .into()
}

/// Derives [`EventRouter`](https://docs.rs/sioc/latest/sioc/event/trait.EventRouter.html),
/// generating `TryFrom<DynEvent>` for a dispatcher enum.
///
/// Each variant must be a newtype wrapping `Event<E>` where `E: EventType + DeserializePayload`.
#[proc_macro_derive(EventRouter, attributes(sioc))]
pub fn derive_from_event(input: TokenStream) -> TokenStream {
    event_router::expand(parse_macro_input!(input))
        .unwrap_or_else(|err| err.write_errors())
        .into()
}
