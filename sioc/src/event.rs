//! Typed Socket.IO events — the primary way to send and receive data.
//!
//! In Socket.IO, every message is an **event**: a named JSON array where the
//! first element is the event name and the rest are the payload fields:
//!
//! ```json
//! ["chat_message", "alice", "hello world"]
//! ```
//!
//! The [`EventType`] trait (derived with `#[derive(EventType)]`) maps a Rust
//! struct to this wire format.  [`Event`] wraps an inbound event with
//! compile-time policy markers for ack and binary handling.  The [`EventHandler`]
//! trait constructs typed events from raw parts and powers the `EventRouter`
//! derive macro for multi-event dispatch enums.
//!
//! Outbound events are sent directly via [`SocketSender::emit`](crate::client::SocketSender::emit)
//! — blanket [`Emit`] impls handle serialization automatically for both
//! plain events and binary closures.
//!
//! The generic parameters `A` ([`AckMarker`]) and `B` ([`BinaryMarker`])
//! let the type system enforce whether a packet carries binary attachments
//! or expects an acknowledgement — no runtime checks needed.

use crate::ack::{AckHandle, AckType};
use crate::binary::AttachmentsBuilder;
use crate::client::Emit;
use crate::error::{EventError, PayloadError};
use crate::marker::{AckMarker, BinaryMarker, HasAck, HasBinary, NoAck, NoBinary};
use crate::payload::{
    DeserializePayload, EventPayload, SerializePayload, deserialize_event, serialize_event,
};
use bytes::Bytes;
use sioc_socket::packet::{Directive, DynEvent};
use tokio::sync::oneshot;

/// Maps a Rust struct to a Socket.IO event name and compile-time policies.
///
/// Prefer `#[derive(EventType)]` over a manual implementation.  The derive
/// generates `NAME` and the associated types.  Add `#[derive(SerializePayload)]`
/// for emit and `#[derive(DeserializePayload)]` for recv.
pub trait EventType: Sized {
    /// The Socket.IO event name used as the first element of the wire array.
    const NAME: &'static str;

    /// Ack policy: [`NoAck`] or [`HasAck<A>`](crate::marker::HasAck).
    type Ack: AckMarker;

    /// Binary policy: [`NoBinary`] or [`HasBinary`].
    type Binary: BinaryMarker;
}

/// A typed inbound event with compile-time ack and binary policy markers.
///
/// Construct via [`TryFrom<DynEvent>`] after receiving a [`DynEvent`]
/// from the namespace channel. The ack and binary policies are determined
/// by the associated types on `E`.
pub struct Event<E>
where
    E: EventType,
{
    /// The deserialized event payload.
    pub payload: E,
    /// Ack ID (populated only when `E::Ack` = [`HasAck`]).
    pub id: <E::Ack as AckMarker>::Id,
    /// Binary attachments (populated only when `E::Binary` = [`HasBinary`]).
    pub attachments: <E::Binary as BinaryMarker>::Attachments,
}

impl<E> std::fmt::Debug for Event<E>
where
    E: EventType + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        map.entry(&"payload", &self.payload);
        E::Ack::fmt_entry(&self.id, &mut map);
        E::Binary::fmt_entry(&self.attachments, &mut map);
        map.finish()
    }
}

impl<E> TryFrom<DynEvent> for Event<E>
where
    E: EventType + DeserializePayload,
{
    type Error = EventError;

    fn try_from(value: DynEvent) -> Result<Self, EventError> {
        let payload = deserialize_event(&value.payload)?;
        let id = E::Ack::parse(value.id)?;
        let attachments = E::Binary::parse(value.attachments)?;
        Ok(Self {
            payload,
            id,
            attachments,
        })
    }
}

pub trait EventHandler: Sized {
    type Payload: EventType;

    fn handle(
        payload: Self::Payload,
        id: Option<u64>,
        attachments: Option<Vec<Bytes>>,
    ) -> Result<Self, EventError>;
}

impl<E> EventHandler for Event<E>
where
    E: EventType + DeserializePayload,
{
    type Payload = E;

    fn handle(
        payload: Self::Payload,
        id: Option<u64>,
        attachments: Option<Vec<Bytes>>,
    ) -> Result<Self, EventError> {
        let id = E::Ack::parse(id)?;
        let attachments = E::Binary::parse(attachments)?;
        Ok(Self {
            payload,
            id,
            attachments,
        })
    }
}

impl<E> Emit<NoAck, NoBinary> for E
where
    E: EventType<Ack = NoAck, Binary = NoBinary> + SerializePayload,
{
    type Output = ();

    fn prepare(self) -> Result<(Directive, ()), PayloadError> {
        let data = crate::payload::serialize(&EventPayload(&self))?;
        Ok((
            Directive::Event {
                data: data.into(),
                tx: None,
                attachments: None,
            },
            (),
        ))
    }
}

impl<E, A> Emit<HasAck<A>, NoBinary> for E
where
    E: EventType<Ack = HasAck<A>, Binary = NoBinary> + SerializePayload,
    A: AckType,
{
    type Output = AckHandle<A>;

    fn prepare(self) -> Result<(Directive, AckHandle<A>), PayloadError> {
        let (tx, rx) = oneshot::channel();
        let data = serialize_event(&self)?.into();
        Ok((
            Directive::Event {
                data,
                tx: Some(tx),
                attachments: None,
            },
            AckHandle::new(rx),
        ))
    }
}

impl<F, E> Emit<NoAck, HasBinary> for F
where
    F: FnOnce(&mut AttachmentsBuilder) -> E,
    E: EventType<Ack = NoAck, Binary = HasBinary> + SerializePayload,
{
    type Output = ();

    fn prepare(self) -> Result<(Directive, ()), PayloadError> {
        let mut builder = AttachmentsBuilder::new();
        let data = serialize_event(&self(&mut builder))?.into();
        Ok((
            Directive::Event {
                data,
                tx: None,
                attachments: Some(builder.finish()),
            },
            (),
        ))
    }
}

impl<F, E, A> Emit<HasAck<A>, HasBinary> for F
where
    F: FnOnce(&mut AttachmentsBuilder) -> E,
    E: EventType<Ack = HasAck<A>, Binary = HasBinary> + SerializePayload,
    A: AckType,
{
    type Output = AckHandle<A>;

    fn prepare(self) -> Result<(Directive, AckHandle<A>), PayloadError> {
        let (tx, rx) = oneshot::channel();
        let mut builder = AttachmentsBuilder::new();
        let data = serialize_event(&self(&mut builder))?.into();
        Ok((
            Directive::Event {
                data,
                tx: Some(tx),
                attachments: Some(builder.finish()),
            },
            AckHandle::new(rx),
        ))
    }
}
