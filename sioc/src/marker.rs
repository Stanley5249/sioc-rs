//! Compile-time markers that encode whether a packet carries binary attachments
//! and/or expects an acknowledgement.
//!
//! Socket.IO packets vary along two independent axes:
//!
//! | Axis | "Yes" marker | "No" marker | Policy trait |
//! |------|-------------|-------------|--------------|
//! | Binary attachments | [`HasBinary`] | [`NoBinary`] | [`BinaryMarker`] |
//! | Acknowledgement | [`HasAck`] | [`NoAck`] | [`AckMarker`] |
//!
//! Generic types like [`Event`](crate::event::Event) and
//! [`Ack`](crate::ack::Ack) are parameterised over these markers
//! so the compiler catches misuse (e.g. sending binary data where none is
//! expected) at build time rather than at runtime.

use crate::ack::AckType;
use crate::error::{AckIdError, AttachmentsError};
use bytes::Bytes;
use std::fmt::Debug;
use std::marker::PhantomData;

/// Determines how acknowledgement IDs are handled at the type level.
pub trait AckMarker {
    /// [`AckId`] when an ack is expected, `()` when not.
    type Id: Sized + Debug;

    /// Validates and extracts an ack ID from the wire-level `Option`.
    fn parse(ack_id: Option<u64>) -> Result<Self::Id, AckIdError>;
}

/// Marker: this packet does **not** expect an acknowledgement.
#[derive(Debug)]
pub struct NoAck;

impl AckMarker for NoAck {
    type Id = ();

    fn parse(ack_id: Option<u64>) -> Result<Self::Id, AckIdError> {
        ack_id.map_or(Ok(()), |_| Err(AckIdError::Unexpected))
    }
}

/// Marker: this packet expects an acknowledgement of type `A`.
#[derive(Debug)]
pub struct HasAck<A>(PhantomData<A>);

impl<A> AckMarker for HasAck<A>
where
    A: AckType,
{
    type Id = AckId<A>;

    fn parse(ack_id: Option<u64>) -> Result<Self::Id, AckIdError> {
        ack_id.map(AckId::new).ok_or(AckIdError::Missing)
    }
}

/// A raw ack ID wrapped with the expected ack payload type `A`.
///
/// This ensures that when you send an ack via [`SocketSender::ack`](crate::client::SocketSender::ack),
/// the response type matches what the sender originally requested.
#[must_use = "AckId must be used to send an acknowledgment"]
pub struct AckId<A>(u64, PhantomData<A>);

impl<A> std::fmt::Debug for AckId<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AckId").field(&self.0).finish()
    }
}

impl<A> AckId<A> {
    fn new(id: u64) -> Self {
        Self(id, PhantomData)
    }

    /// Consumes the wrapper and returns the raw wire-level `u64` ID.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Determines how binary attachments are handled at the type level.
pub trait BinaryMarker {
    /// `Vec<Bytes>` when binary is present, `()` when absent.
    type Attachments: Sized + Debug;

    /// Validates and extracts attachments from the wire-level `Option`.
    fn parse(attachment: Option<Vec<Bytes>>) -> Result<Self::Attachments, AttachmentsError>;

    /// Converts typed attachments back into the wire-level `Option`.
    fn get(attachments: Self::Attachments) -> Option<Vec<Bytes>>;
}

/// Marker: this packet carries binary attachments.
#[derive(Debug)]
pub struct HasBinary;

/// Marker: this packet has no binary attachments.
#[derive(Debug)]
pub struct NoBinary;

impl BinaryMarker for NoBinary {
    type Attachments = ();

    fn parse(attachment: Option<Vec<Bytes>>) -> Result<Self::Attachments, AttachmentsError> {
        match attachment {
            Some(_) => Err(AttachmentsError::Unexpected),
            None => Ok(()),
        }
    }

    fn get(_attachments: Self::Attachments) -> Option<Vec<Bytes>> {
        None
    }
}

impl BinaryMarker for HasBinary {
    type Attachments = Vec<Bytes>;

    fn parse(attachment: Option<Vec<Bytes>>) -> Result<Self::Attachments, AttachmentsError> {
        attachment.ok_or(AttachmentsError::Missing)
    }

    fn get(attachments: Self::Attachments) -> Option<Vec<Bytes>> {
        Some(attachments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_binary_parse_none_succeeds() {
        assert!(NoBinary::parse(None).is_ok());
    }

    #[test]
    fn no_binary_parse_some_fails() {
        let attachments = Some(vec![Bytes::from_static(b"data")]);
        assert!(NoBinary::parse(attachments).is_err());
    }

    #[test]
    fn no_binary_get_returns_none() {
        assert!(NoBinary::get(()).is_none());
    }

    #[test]
    fn has_binary_parse_some_succeeds() {
        let input = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        let parsed = HasBinary::parse(Some(input.clone())).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], input[0]);
    }

    #[test]
    fn has_binary_parse_none_fails() {
        assert!(HasBinary::parse(None).is_err());
    }

    #[test]
    fn has_binary_get_returns_some() {
        let input = vec![Bytes::from_static(b"x")];
        let result = HasBinary::get(input.clone());
        assert_eq!(result.unwrap(), input);
    }

    #[test]
    fn no_ack_parse_none_succeeds() {
        assert!(NoAck::parse(None).is_ok());
    }

    #[test]
    fn no_ack_parse_some_fails() {
        assert!(NoAck::parse(Some(42)).is_err());
    }

    #[test]
    fn has_ack_parse_some_succeeds() {
        let id = <HasAck<()>>::parse(Some(7)).unwrap();
        assert_eq!(id.get(), 7);
    }

    #[test]
    fn has_ack_parse_none_fails() {
        assert!(<HasAck<()>>::parse(None).is_err());
    }
}
