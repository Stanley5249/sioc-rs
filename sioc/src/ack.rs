//! Typed Socket.IO acknowledgements (request/response over events).
//!
//! When one side emits an event and the other side calls the callback, the
//! response is called an **acknowledgement** ("ack").  On the wire, acks are
//! JSON arrays `[arg0, arg1, ...]` — like events, but without a leading name.
//!
//! The [`AckType`] trait (or `#[derive(AckType)]`) maps a Rust struct to this format.
//!
//! - [`Ack`] — inbound: decoded from a [`DynAck`] via [`TryFrom`].
//! - [`AckHandle`] — a future that resolves when the server's ack arrives.
//!
//! Outbound acks are sent directly via
//! [`SocketSender::ack`](crate::client::SocketSender::ack) — blanket [`Acknowledge`] impls
//! handle serialization automatically for both plain acks and binary closures.

use crate::binary::AttachmentsBuilder;
use crate::client::Acknowledge;
use crate::error::{AckError, PayloadError};
use crate::marker::{BinaryMarker, HasBinary, NoBinary};
use crate::payload::{DeserializePayload, SerializePayload, deserialize_ack, serialize_ack};
use pin_project::pin_project;
use sioc_socket::packet::Directive;
use sioc_socket::packet::DynAck;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// Maps a Rust struct to the Socket.IO ack JSON-array encoding.
///
/// Prefer `#[derive(AckType)]` over a manual implementation.  The derive
/// generates the binary policy associated type.  Add `#[derive(SerializePayload)]`
/// to send acks and `#[derive(DeserializePayload)]` to receive them.
///
/// Unlike [`EventType`](crate::event::EventType), ack arrays have no leading name
/// element — the fields map directly to array positions.
///
/// # Example
///
/// ```rust
/// use sioc::prelude::*;
///
/// #[derive(AckType, SerializePayload)]
/// struct SaveAck { ok: bool, id: u64 }
///
/// fn main() {
///     assert_eq!(serialize_ack(&()).unwrap(), b"[]");
///     assert_eq!(serialize_ack(&SaveAck { ok: true, id: 7 }).unwrap(), b"[true,7]");
/// }
/// ```
pub trait AckType: Sized {
    /// Binary policy: [`NoBinary`] or [`HasBinary`].
    type Binary: BinaryMarker;
}

impl AckType for () {
    type Binary = NoBinary;
}

/// A typed inbound acknowledgement with a compile-time binary policy marker.
///
/// Construct via [`TryFrom<DynAck>`] after receiving a [`DynAck`]
/// from an ack oneshot channel. The binary policy is determined by `A::Binary`.
#[derive(Debug)]
pub struct Ack<A>
where
    A: AckType,
{
    /// The deserialized ack payload.
    pub payload: A,
    /// Binary attachments (populated only when `A::Binary` = [`HasBinary`]).
    pub attachments: <A::Binary as BinaryMarker>::Attachments,
}

impl<A> TryFrom<DynAck> for Ack<A>
where
    A: AckType + DeserializePayload,
{
    type Error = AckError;

    fn try_from(value: DynAck) -> Result<Self, AckError> {
        let payload = deserialize_ack(&value.data)?;
        let attachments = A::Binary::parse(value.attachments)?;
        Ok(Self {
            payload,
            attachments,
        })
    }
}

impl<A> Acknowledge<A, NoBinary> for A
where
    A: AckType<Binary = NoBinary> + SerializePayload,
{
    fn into_directive(self, id: u64) -> Result<Directive, PayloadError> {
        let data = serialize_ack(&self)?.into();
        Ok(Directive::Ack {
            data,
            id,
            attachments: None,
        })
    }
}

impl<F, A> Acknowledge<A, HasBinary> for F
where
    F: FnOnce(&mut AttachmentsBuilder) -> A,
    A: AckType<Binary = HasBinary> + SerializePayload,
{
    fn into_directive(self, id: u64) -> Result<Directive, PayloadError> {
        let mut builder = AttachmentsBuilder::new();
        let data = serialize_ack(&self(&mut builder))?.into();
        Ok(Directive::Ack {
            data,
            id,
            attachments: Some(builder.finish()),
        })
    }
}

/// Handle that resolves to [`Ack`] when the server's acknowledgement arrives.
///
/// Obtained from [`SocketSender::emit`](crate::client::SocketSender::emit).
/// Implements [`Future`]: `.await` it directly.
#[must_use = "AckHandle must be awaited to receive the ack"]
#[pin_project]
#[derive(Debug)]
pub struct AckHandle<A> {
    #[pin]
    rx: oneshot::Receiver<DynAck>,
    marker: PhantomData<A>,
}

impl<A: AckType> AckHandle<A> {
    /// Wraps a oneshot receiver into a typed ack handle.
    pub(crate) fn new(rx: oneshot::Receiver<DynAck>) -> Self {
        Self {
            rx,
            marker: PhantomData,
        }
    }
}
impl<A> Future for AckHandle<A>
where
    A: AckType + DeserializePayload,
{
    type Output = Result<Ack<A>, AckError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().rx.poll(cx)?.map(Ack::try_from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marker::{AckMarker, HasAck, HasBinary};
    use bytes::Bytes;

    #[derive(Debug, PartialEq)]
    struct BinaryBoolAck(bool);

    impl AckType for BinaryBoolAck {
        type Binary = HasBinary;
    }

    impl SerializePayload for BinaryBoolAck {
        fn serialize_payload<S>(&self, seq: &mut S) -> std::result::Result<(), S::Error>
        where
            S: serde::ser::SerializeSeq,
        {
            seq.serialize_element(&self.0)?;
            Ok(())
        }
    }

    impl DeserializePayload for BinaryBoolAck {
        fn deserialize_payload<'de, S>(seq: &mut S) -> std::result::Result<Self, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            let v = seq
                .next_element()?
                .ok_or_else(|| serde::de::Error::invalid_length(0, &"1 element"))?;
            Ok(Self(v))
        }
    }

    #[derive(Debug, PartialEq)]
    struct BinaryUnitAck;

    impl AckType for BinaryUnitAck {
        type Binary = HasBinary;
    }

    impl SerializePayload for BinaryUnitAck {
        fn serialize_payload<S>(&self, _seq: &mut S) -> std::result::Result<(), S::Error>
        where
            S: serde::ser::SerializeSeq,
        {
            Ok(())
        }
    }

    impl DeserializePayload for BinaryUnitAck {
        fn deserialize_payload<'de, S>(_seq: &mut S) -> std::result::Result<Self, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            Ok(Self)
        }
    }

    #[test]
    fn serialize_unit_ack() {
        assert_eq!(serialize_ack(&()).unwrap(), b"[]");
    }

    #[test]
    fn deserialize_unit_ack() {
        assert_eq!(deserialize_ack::<()>(b"[]").unwrap(), ());
    }

    #[test]
    fn from_ack_with_binary() {
        let attachment = Bytes::from_static(b"\xDE\xAD");
        let ack = DynAck {
            data: Bytes::from_static(b"[true]"),
            attachments: Some(vec![attachment.clone()]),
        };
        let ack: Ack<BinaryBoolAck> = ack.try_into().unwrap();
        assert_eq!(ack.payload, BinaryBoolAck(true));
        assert_eq!(ack.attachments.len(), 1);
        assert_eq!(ack.attachments[0], attachment);
    }

    #[test]
    fn from_ack_missing_binary_fails() {
        let ack = DynAck {
            data: Bytes::from_static(b"[]"),
            attachments: None,
        };
        let result: Result<Ack<BinaryUnitAck>, _> = ack.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn from_ack_unexpected_binary_fails() {
        let ack = DynAck {
            data: Bytes::from_static(b"[]"),
            attachments: Some(vec![Bytes::from_static(b"x")]),
        };
        let result: Result<Ack<()>, _> = ack.try_into();
        assert!(result.is_err());
    }
    #[test]
    fn send_ack_into_directive_binary() {
        let id = <HasAck<BinaryBoolAck>>::parse(Some(3)).unwrap();
        let directive = Acknowledge::<BinaryBoolAck, HasBinary>::into_directive(
            |builder: &mut AttachmentsBuilder| {
                let _p = builder.attach(Bytes::from_static(b"\xCA\xFE"));
                BinaryBoolAck(true)
            },
            id.get(),
        )
        .unwrap();
        match directive {
            Directive::Ack {
                data,
                id,
                attachments,
            } => {
                assert_eq!(&data[..], b"[true]");
                assert_eq!(id, 3);
                let att = attachments.expect("expected attachments");
                assert_eq!(att.len(), 1);
                assert_eq!(att[0], Bytes::from_static(b"\xCA\xFE"));
            }
            _ => panic!("expected Ack packet"),
        }
    }

    #[tokio::test]
    async fn ack_handle_closed_channel_errors() {
        let (tx, rx) = oneshot::channel::<DynAck>();
        let handle = AckHandle::<()>::new(rx);
        drop(tx);

        let result: Result<Ack<()>, _> = handle.await;
        assert!(result.is_err());
    }
}
