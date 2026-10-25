//! Decodes what the engine delivers into packets for the namespaces.

use bytes::Bytes;
use bytestring::ByteString;

use crate::error::{ManagerError, PacketError};
use crate::packet::{Connect, ConnectError, DynAck, DynEvent, ServerPacket};
use crate::parser::packet::{Ns, Packet};

/// A decoded server packet, like the packet socket.io-parser's `Decoder`
/// emits as `"decoded"`.
#[derive(Debug)]
pub enum Decoded {
    /// A packet for the namespace's receiver, which `Socket.onpacket` routes.
    Packet(ServerPacket),
    /// The answer to an event, which `Socket.onack` takes.
    Ack {
        /// The id of the event being answered, like `packet.id`.
        id: u64,
        /// The answer, with its attachments, like `packet.data`.
        ack: DynAck,
    },
}

/// Turns the engine's text and binary messages into packets, like
/// socket.io-parser's `Decoder`, which holds the `BinaryReconstructor`.
#[derive(Debug, Default)]
pub struct Decoder {
    /// The binary packet awaiting attachments, like
    /// `Decoder.reconstructor`.
    reconstructor: Option<Ns<BinaryPacket>>,
}

impl Decoder {
    /// Decodes a text message, like `Decoder.add` with a string.
    ///
    /// Returns `None` for a binary packet that awaits its attachments.
    ///
    /// # Errors
    ///
    /// Returns an error if the server breaks the protocol.
    pub fn add_text(&mut self, text: ByteString) -> Result<Option<Ns<Decoded>>, ManagerError> {
        if self.reconstructor.is_some() {
            return Err(ManagerError::UnexpectedText(text));
        }

        let Ns(ns, packet) = text.try_into()?;

        tracing::trace!(%ns, %packet, "received packet");

        let decoded = match packet {
            Packet::Connect(payload) => {
                let connect: Connect = serde_json::from_str(&payload).map_err(PacketError::Json)?;
                Decoded::Packet(ServerPacket::Connect(connect))
            }
            Packet::Disconnect => Decoded::Packet(ServerPacket::Disconnect),
            Packet::Event { payload, id } => {
                Decoded::Packet(ServerPacket::Event(DynEvent::new(payload, id)))
            }
            Packet::Ack { payload, id } => Decoded::Ack {
                id,
                ack: DynAck::new(payload),
            },
            Packet::ConnectError(payload) => {
                let error: ConnectError =
                    serde_json::from_str(&payload).map_err(PacketError::Json)?;
                Decoded::Packet(ServerPacket::ConnectError(error))
            }
            Packet::BinaryEvent { payload, id, count } => {
                validate_placeholders(&payload, count)?;
                self.reconstructor = Some(Ns(ns, BinaryPacket::event(payload, id, count)));
                return Ok(None);
            }
            Packet::BinaryAck { payload, id, count } => {
                validate_placeholders(&payload, count)?;
                self.reconstructor = Some(Ns(ns, BinaryPacket::ack(payload, id, count)));
                return Ok(None);
            }
        };

        Ok(Some(Ns(ns, decoded)))
    }

    /// Decodes a binary message, like `Decoder.add` with a buffer.
    ///
    /// Returns `None` until the pending packet has every attachment.
    ///
    /// # Errors
    ///
    /// Returns an error if no binary packet awaits an attachment.
    pub fn add_binary(&mut self, bytes: Bytes) -> Result<Option<Ns<Decoded>>, ManagerError> {
        let length = bytes.len();

        let Some(Ns(ns, mut packet)) = self.reconstructor.take() else {
            return Err(ManagerError::UnexpectedBinary(bytes));
        };

        packet.attach(bytes);

        if !packet.is_complete() {
            tracing::trace!(length, status = "pending", "received attachment");
            self.reconstructor = Some(Ns(ns, packet));
            return Ok(None);
        }

        tracing::trace!(%ns, length, status = "complete", "received attachment");

        let decoded = match packet {
            BinaryPacket::Event {
                payload,
                id,
                attachments,
                ..
            } => {
                let event = DynEvent {
                    id,
                    ..DynEvent::new(payload, None)
                }
                .with_attachments(attachments);
                Decoded::Packet(ServerPacket::Event(event))
            }
            BinaryPacket::Ack {
                payload,
                id,
                attachments,
                ..
            } => Decoded::Ack {
                id,
                ack: DynAck::new(payload).with_attachments(attachments),
            },
        };

        Ok(Some(Ns(ns, decoded)))
    }

    /// Forgets the pending binary packet, because its attachments came with
    /// the engine that is gone. Like `Decoder.destroy`.
    pub fn reset(&mut self) {
        self.reconstructor = None;
    }
}

/// Validates before retaining attachment frames, so invalid references cannot
/// reach application indexing code or hold reconstruction open. Like the
/// index check in `reconstructPacket`, which JS runs only once every
/// attachment arrived.
fn validate_placeholders(payload: &str, count: usize) -> Result<(), PacketError> {
    let value: serde_json::Value = serde_json::from_str(payload)?;
    validate_placeholder_value(&value, count)
}

fn validate_placeholder_value(value: &serde_json::Value, count: usize) -> Result<(), PacketError> {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                validate_placeholder_value(value, count)?;
            }
        }
        serde_json::Value::Object(values) => {
            if values.get("_placeholder") == Some(&serde_json::Value::Bool(true)) {
                let slot = values.get("num").and_then(serde_json::Value::as_u64);
                if slot.is_none_or(|slot| slot >= count as u64) {
                    return Err(PacketError::InvalidPlaceholder);
                }
            } else {
                for value in values.values() {
                    validate_placeholder_value(value, count)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// A binary packet and the attachments that arrived so far, like
/// `BinaryReconstructor.reconPack`.
#[derive(Debug)]
enum BinaryPacket {
    /// A `BINARY_EVENT`, with its optional ack id.
    Event {
        payload: ByteString,
        id: Option<u64>,
        attachments: Vec<Bytes>,
        count: usize,
    },
    /// A `BINARY_ACK` answering event `id`.
    Ack {
        payload: ByteString,
        id: u64,
        attachments: Vec<Bytes>,
        count: usize,
    },
}

impl BinaryPacket {
    fn event(payload: ByteString, id: Option<u64>, count: usize) -> Self {
        Self::Event {
            payload,
            id,
            attachments: Vec::new(),
            count,
        }
    }

    fn ack(payload: ByteString, id: u64, count: usize) -> Self {
        Self::Ack {
            payload,
            id,
            attachments: Vec::new(),
            count,
        }
    }

    fn attach(&mut self, bytes: Bytes) {
        match self {
            Self::Event { attachments, .. } | Self::Ack { attachments, .. } => {
                attachments.push(bytes);
            }
        }
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::Event {
                attachments, count, ..
            }
            | Self::Ack {
                attachments, count, ..
            } => attachments.len() == *count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_outside_the_count_is_refused() {
        let mut decoder = Decoder::default();
        let text = ByteString::from_static(r#"51-["e",{"_placeholder":true,"num":1}]"#);
        assert!(matches!(
            decoder.add_text(text),
            Err(ManagerError::Packet(PacketError::InvalidPlaceholder))
        ));
        decoder.add_binary(Bytes::new()).unwrap_err();
    }

    #[test]
    fn binary_event_completes_with_its_attachment() {
        let mut decoder = Decoder::default();
        let text = ByteString::from_static(r#"51-["e",{"_placeholder":true,"num":0}]"#);
        assert!(decoder.add_text(text).unwrap().is_none());
        let Ns(ns, Decoded::Packet(ServerPacket::Event(event))) = decoder
            .add_binary(Bytes::from_static(b"x"))
            .unwrap()
            .unwrap()
        else {
            panic!("a binary event completes as an event");
        };
        assert_eq!(&*ns, "/");
        assert_eq!(event.attachments.unwrap().len(), 1);
    }

    #[test]
    fn reset_forgets_the_pending_packet() {
        let mut decoder = Decoder::default();
        let text = ByteString::from_static(r#"51-["e",{"_placeholder":true,"num":0}]"#);
        decoder.add_text(text).unwrap();
        decoder.reset();
        assert!(matches!(
            decoder.add_binary(Bytes::new()),
            Err(ManagerError::UnexpectedBinary(_))
        ));
    }
}
