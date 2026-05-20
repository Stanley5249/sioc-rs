//! Binary attachment support for Socket.IO events and acknowledgements.
//!
//! Socket.IO sends binary data as separate frames *after* the JSON text frame.
//! Each binary buffer in the JSON is replaced by a **placeholder** object
//! `{"_placeholder": true, "num": 0}` that references the frame by index.
//!
//! Use [`AttachmentsBuilder`] inside a binary emit/ack closure to register
//! binary payloads and obtain [`Placeholder`]s to embed in your struct fields.

use bytes::Bytes;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Deserializer, Serialize};

/// A binary attachment placeholder serialised as
/// `{"_placeholder": true, "num": <index>}`.
///
/// Obtained from [`AttachmentsBuilder::attach`].  Embed this in your event or
/// ack struct wherever you would normally put a `Bytes` field; the manager
/// sends the real binary data as a follow-up frame referenced by `num`.
pub struct Placeholder {
    num: usize,
}

impl Placeholder {
    /// Consumes the placeholder and returns its zero-based attachment index.
    #[must_use]
    pub fn num(self) -> usize {
        self.num
    }
}

impl Serialize for Placeholder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("Placeholder", 2)?;
        s.serialize_field("_placeholder", &true)?;
        s.serialize_field("num", &self.num)?;
        s.end()
    }
}

#[derive(Deserialize)]
struct RawPlaceholder {
    #[serde(rename = "_placeholder")]
    placeholder: bool,
    num: usize,
}

impl<'de> Deserialize<'de> for Placeholder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawPlaceholder::deserialize(deserializer)?;
        if !raw.placeholder {
            Err(serde::de::Error::custom("expected _placeholder to be true"))?;
        }
        Ok(Placeholder { num: raw.num })
    }
}

/// Accumulates binary buffers and hands back a [`Placeholder`] for each one.
///
/// Passed into the builder closure when emitting binary events or acks via
/// [`SocketSender::emit`](crate::client::SocketSender::emit) or
/// [`SocketSender::acknowledge`](crate::client::SocketSender::acknowledge).
#[derive(Default)]
pub struct AttachmentsBuilder {
    buffer: Vec<Bytes>,
}

impl AttachmentsBuilder {
    /// Creates an empty builder.
    #[must_use]
    pub fn new() -> Self {
        AttachmentsBuilder::default()
    }

    /// Registers `data` as the next binary attachment and returns a
    /// [`Placeholder`] to embed in the JSON payload.
    pub fn attach(&mut self, data: Bytes) -> Placeholder {
        let num = self.buffer.len();
        self.buffer.push(data);
        Placeholder { num }
    }

    /// Consumes the builder and returns the accumulated attachment buffers
    /// in registration order.
    #[must_use]
    pub fn finish(self) -> Vec<Bytes> {
        self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_assigns_sequential_indices() {
        let mut builder = AttachmentsBuilder::new();
        let p0 = builder.attach(Bytes::from_static(b"first"));
        let p1 = builder.attach(Bytes::from_static(b"second"));
        assert_eq!(p0.num, 0);
        assert_eq!(p1.num, 1);

        let buffers = builder.finish();
        assert_eq!(buffers.len(), 2);
        assert_eq!(buffers[0], Bytes::from_static(b"first"));
        assert_eq!(buffers[1], Bytes::from_static(b"second"));
    }

    #[test]
    fn placeholder_round_trips_through_json() {
        let original = Placeholder { num: 5 };
        let json = serde_json::to_vec(&original).unwrap();
        let decoded: Placeholder = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.num, 5);
    }

    #[test]
    fn placeholder_rejects_false_flag() {
        let json = r#"{"_placeholder":false,"num":0}"#;
        let result: Result<Placeholder, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn placeholder_num_consumes_value() {
        let p = Placeholder { num: 42 };
        assert_eq!(p.num(), 42);
    }
}
