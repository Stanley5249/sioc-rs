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
    reconstructor: Option<Reconstruction>,
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
                self.reconstructor = Some(Reconstruction {
                    packet: Some(Ns(
                        ns,
                        Decoded::Packet(ServerPacket::Event(DynEvent::new(payload, id))),
                    )),
                    remaining: count,
                    attachments: Vec::new(),
                });
                return Ok(None);
            }
            Packet::BinaryAck { payload, id, count } => {
                validate_placeholders(&payload, count)?;
                self.reconstructor = Some(Reconstruction {
                    packet: Some(Ns(
                        ns,
                        Decoded::Ack {
                            id,
                            ack: DynAck::new(payload),
                        },
                    )),
                    remaining: count,
                    attachments: Vec::new(),
                });
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

        let Some(reconstruction) = &mut self.reconstructor else {
            return Err(ManagerError::UnexpectedBinary(bytes));
        };
        if reconstruction.packet.is_some() {
            reconstruction.attachments.push(bytes);
        }
        reconstruction.remaining -= 1;
        if reconstruction.remaining != 0 {
            tracing::trace!(length, status = "pending", "received attachment");
            return Ok(None);
        }

        let reconstruction = self
            .reconstructor
            .take()
            .expect("the final frame completes a reconstruction");
        let Some(Ns(ns, mut packet)) = reconstruction.packet else {
            return Ok(None);
        };
        match &mut packet {
            Decoded::Packet(ServerPacket::Event(event)) => {
                event.attachments = Some(reconstruction.attachments);
            }
            Decoded::Ack { ack, .. } => ack.attachments = Some(reconstruction.attachments),
            Decoded::Packet(_) => unreachable!("only binary events and acks start reconstruction"),
        }
        tracing::trace!(%ns, length, status = "complete", "received attachment");
        Ok(Some(Ns(ns, packet)))
    }

    /// Forgets the pending binary packet, because its attachments came with
    /// the engine that is gone. Like `Decoder.destroy`.
    pub fn reset(&mut self) {
        self.reconstructor = None;
    }

    /// Detaches a namespace's partial packet while consuming its remaining
    /// attachments. Reuse cannot inherit the old reconstruction,
    /// and the current engine still needs its binary frame alignment.
    pub fn disconnect(&mut self, ns: &str) {
        if let Some(reconstruction) = &mut self.reconstructor
            && reconstruction
                .packet
                .as_ref()
                .is_some_and(|Ns(pending, _)| pending == ns)
        {
            reconstruction.packet = None;
            reconstruction.attachments.clear();
        }
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

/// The packet and buffers of JS `BinaryReconstructor`. The existing decoded
/// packet already distinguishes event ids from ACK ids, so reconstruction only
/// owns their shared frame accounting.
#[derive(Debug)]
struct Reconstruction {
    /// `BinaryReconstructor.reconPack`, detached when its namespace ends.
    packet: Option<Ns<Decoded>>,
    /// Frames still required to complete or discard the packet.
    remaining: usize,
    /// `BinaryReconstructor.buffers`, released when the packet is detached.
    attachments: Vec<Bytes>,
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

    #[test]
    fn namespace_cleanup_discards_partial_attachments_without_losing_alignment() {
        for header in [
            r#"53-/old,["e",{"_placeholder":true,"num":0},{"_placeholder":true,"num":1},{"_placeholder":true,"num":2}]"#,
            r#"63-/old,7[{"_placeholder":true,"num":0},{"_placeholder":true,"num":1},{"_placeholder":true,"num":2}]"#,
        ] {
            let mut decoder = Decoder::default();
            decoder.add_text(header.into()).unwrap();
            assert!(
                decoder
                    .add_binary(Bytes::from_static(b"first"))
                    .unwrap()
                    .is_none()
            );
            decoder.disconnect("/old");
            decoder.disconnect("/old");
            assert!(
                decoder
                    .add_binary(Bytes::from_static(b"second"))
                    .unwrap()
                    .is_none()
            );
            decoder.disconnect("/other");
            assert!(
                decoder
                    .add_binary(Bytes::from_static(b"third"))
                    .unwrap()
                    .is_none()
            );
            let Ns(ns, Decoded::Packet(ServerPacket::Event(event))) = decoder
                .add_text(r#"2/new,["fresh"]"#.into())
                .unwrap()
                .unwrap()
            else {
                panic!("the next packet decodes after the discarded frames");
            };
            assert_eq!(&*ns, "/new");
            assert_eq!(&*event.payload, r#"["fresh"]"#);
        }
    }
}
