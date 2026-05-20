//! Typed Socket.IO acknowledgements (request/response over events).
//!
//! When one side emits an event and the other side calls the callback, the
//! response is called an **acknowledgement** ("ack").  On the wire, acks are
//! JSON arrays `[arg0, arg1, ...]`, like events, but without a leading name.
//!
//! The [`AckType`] trait (or `#[derive(AckType)]`) maps a Rust struct to this format.
//!
//! - [`Ack`]: inbound, decoded from a [`DynAck`] via [`TryFrom`].
//! - [`AckHandle`]: a future that resolves when the server's ack arrives.
//!
//! Outbound acks are sent directly via
//! [`SocketSender::acknowledge`](crate::client::SocketSender::acknowledge); blanket [`Acknowledge`] impls
//! handle serialization automatically for both plain acks and binary closures.

use crate::binary::AttachmentsBuilder;
use crate::client::Acknowledge;
use crate::error::{AckError, PayloadError};
use crate::marker::{BinaryMarker, HasBinary, NoBinary};
use crate::packet::Directive;
use crate::packet::DynAck;
use crate::payload::{DeserializePayload, SerializePayload, ack_from_json, ack_to_json};
use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;

/// Maps a Rust struct to the Socket.IO ack JSON-array encoding.
///
/// Prefer `#[derive(AckType)]` over a manual implementation.  The derive
/// generates the binary policy associated type.  Add `#[derive(SerializePayload)]`
/// to send acks and `#[derive(DeserializePayload)]` to receive them.
///
/// Unlike [`EventType`](crate::event::EventType), ack arrays have no leading name
/// element; the fields map directly to array positions.
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
///     assert_eq!(ack_to_json(&()).unwrap(), "[]");
///     assert_eq!(ack_to_json(&SaveAck { ok: true, id: 7 }).unwrap(), "[true,7]");
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
pub struct Ack<A>
where
    A: AckType,
{
    /// The deserialized ack payload.
    pub payload: A,
    /// Binary attachments (populated only when `A::Binary` = [`HasBinary`]).
    pub attachments: <A::Binary as BinaryMarker>::Attachments,
}

impl<A> std::fmt::Debug for Ack<A>
where
    A: std::fmt::Debug + AckType,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        map.entry(&"payload", &self.payload);
        A::Binary::format(&self.attachments, &mut map);
        map.finish()
    }
}

impl<A> TryFrom<DynAck> for Ack<A>
where
    A: AckType + DeserializePayload,
{
    type Error = AckError;

    fn try_from(value: DynAck) -> Result<Self, AckError> {
        let payload = ack_from_json(&value.payload)?;
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
        let payload = ack_to_json(&self)?.into();
        Ok(Directive::Ack {
            payload,
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
        let payload = ack_to_json(&self(&mut builder))?.into();
        Ok(Directive::Ack {
            payload,
            id,
            attachments: Some(builder.finish()),
        })
    }
}

/// Handle that resolves to [`Ack`] when the server's acknowledgement arrives.
///
/// Obtained from [`SocketSender::emit`](crate::client::SocketSender::emit).
/// Implements [`Future`]: `.await` it directly.
#[pin_project]
#[must_use = "AckHandle must be awaited to receive the ack"]
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
impl<A> AckHandle<A>
where
    A: AckType + DeserializePayload,
{
    /// Returns a future that resolves to [`AckError::Timeout`] if the server
    /// does not respond within `duration`.
    ///
    /// Mirrors [`tokio::time::timeout`].
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Timeout`] if `duration` elapses, or any error from the inner [`AckHandle`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// // Requires a live Socket.IO connection; AckHandle is obtained from SocketSender::emit.
    /// use sioc::prelude::*;
    /// use std::time::Duration;
    ///
    /// async fn example(handle: AckHandle<()>) {
    ///     let result = handle.timeout(Duration::from_secs(5)).await;
    /// }
    /// ```
    pub async fn timeout(self, duration: Duration) -> Result<Ack<A>, AckError> {
        tokio::time::timeout(duration, self).await?
    }

    /// Returns a future that resolves to [`AckError::Timeout`] if the server
    /// does not respond by `deadline`.
    ///
    /// Mirrors [`tokio::time::timeout_at`].
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Timeout`] if `deadline` passes, or any error from the inner [`AckHandle`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// // Requires a live Socket.IO connection; AckHandle is obtained from SocketSender::emit.
    /// use sioc::prelude::*;
    /// use tokio::time::{Instant, Duration};
    ///
    /// async fn example(handle: AckHandle<()>) {
    ///     let deadline = Instant::now() + Duration::from_secs(5);
    ///     let result = handle.timeout_at(deadline).await;
    /// }
    /// ```
    pub async fn timeout_at(self, deadline: Instant) -> Result<Ack<A>, AckError> {
        tokio::time::timeout_at(deadline, self).await?
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
    use crate::error::AckError;
    use crate::marker::{AckMarker, HasAck, HasBinary, NoBinary};
    use bytes::Bytes;
    use bytestring::ByteString;

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
    fn deserialize_unit_ack() {
        assert_eq!(ack_from_json::<()>("[]").unwrap(), ());
    }

    #[test]
    fn from_ack_with_binary() {
        let attachment = Bytes::from_static(b"\xDE\xAD");
        let ack = DynAck {
            payload: ByteString::from_static("[true]"),
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
            payload: ByteString::from_static("[]"),
            attachments: None,
        };
        let result: Result<Ack<BinaryUnitAck>, _> = ack.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn from_ack_unexpected_binary_fails() {
        let ack = DynAck {
            payload: ByteString::from_static("[]"),
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
                payload,
                id,
                attachments,
            } => {
                assert_eq!(&payload[..], "[true]");
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

    #[test]
    fn debug_ack_no_binary() {
        let ack = Ack {
            payload: (),
            attachments: (),
        };
        let s = format!("{ack:?}");
        assert!(s.contains("payload"));
    }

    #[test]
    fn debug_ack_with_binary() {
        let ack = Ack {
            payload: BinaryBoolAck(true),
            attachments: vec![Bytes::from_static(b"x")],
        };
        let s = format!("{ack:?}");
        assert!(s.contains("count"));
    }

    #[test]
    fn send_ack_into_directive_no_binary() {
        let directive = Acknowledge::<(), NoBinary>::into_directive((), 7).unwrap();
        match directive {
            Directive::Ack {
                payload,
                id,
                attachments,
            } => {
                assert_eq!(&payload[..], "[]");
                assert_eq!(id, 7);
                assert!(attachments.is_none());
            }
            _ => panic!("expected Ack directive"),
        }
    }

    #[tokio::test]
    async fn ack_handle_resolves_on_send() {
        let (tx, rx) = oneshot::channel::<DynAck>();
        let handle = AckHandle::<()>::new(rx);
        tx.send(DynAck {
            payload: ByteString::from_static("[]"),
            attachments: None,
        })
        .unwrap();
        let result: Result<Ack<()>, _> = handle.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ack_handle_timeout_expires() {
        use std::time::Duration;
        let (_tx, rx) = oneshot::channel::<DynAck>();
        let handle = AckHandle::<()>::new(rx);
        let result = handle.timeout(Duration::from_millis(1)).await;
        assert!(matches!(result, Err(AckError::Timeout(_))));
    }

    #[tokio::test]
    async fn ack_handle_timeout_resolves() {
        use std::time::Duration;
        let (tx, rx) = oneshot::channel::<DynAck>();
        let handle = AckHandle::<()>::new(rx);
        tx.send(DynAck {
            payload: ByteString::from_static("[]"),
            attachments: None,
        })
        .unwrap();
        let result = handle.timeout(Duration::from_secs(5)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ack_handle_timeout_at_resolves() {
        use std::time::Duration;
        use tokio::time::Instant;
        let (tx, rx) = oneshot::channel::<DynAck>();
        let handle = AckHandle::<()>::new(rx);
        tx.send(DynAck {
            payload: ByteString::from_static("[]"),
            attachments: None,
        })
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let result = handle.timeout_at(deadline).await;
        assert!(result.is_ok());
    }
}
