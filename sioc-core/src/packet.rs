//! Strictly Typed Socket.IO Packet Data Model
//!
//! This module implements a hierarchical type system that prevents invalid packet states:
//! - BasePacket: Core data (namespace + payload)
//! - AckPacket: BasePacket + acknowledgement ID
//! - BinaryPacket: BasePacket + attachment count
//! - BinaryAckPacket: AckPacket + attachment count
//!
//! Protocol Context Enums:
//! - EventPacket: NoAck vs Ack variants
//! - BinaryEventPacket: NoAck vs Ack variants
//!
//! Top-Level Packet maps to Socket.IO protocol types 0-6.

use crate::error::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use sioc_engine::prelude::MessagePacket;
use smallvec::SmallVec;

/// Optimization: Most packets have 0 or 1 binary attachment.
pub type Attachments = SmallVec<[Bytes; 1]>;

// ============================================================================
// 1. Pure Data Containers (Intermediate Representation)
// ============================================================================

/// Fundamental data unit: Namespace + JSON Payload.
#[derive(Debug, Clone, PartialEq)]
pub struct BasePacket {
    /// The namespace this packet belongs to (e.g., "/" or "/admin").
    pub ns: String,
    /// The JSON payload as raw bytes.
    pub data: Bytes,
}

impl BasePacket {
    /// Create a new BasePacket.
    pub fn new(ns: String, data: Bytes) -> Self {
        Self { ns, data }
    }
}

/// Data unit with an acknowledgement ID.
#[derive(Debug, Clone, PartialEq)]
pub struct AckPacket {
    /// The underlying base packet.
    pub inner: BasePacket,
    /// The acknowledgement ID for request/response matching.
    pub ack_id: u64,
}

impl AckPacket {
    /// Create a new AckPacket.
    pub fn new(inner: BasePacket, ack_id: u64) -> Self {
        Self { inner, ack_id }
    }
}

/// Data unit with binary attachment count (Header Only).
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryPacket {
    /// The underlying base packet.
    pub inner: BasePacket,
    /// Number of binary attachments expected.
    pub attachments: u64,
}

impl BinaryPacket {
    /// Create a new BinaryPacket.
    pub fn new(inner: BasePacket, attachments: u64) -> Self {
        Self { inner, attachments }
    }
}

/// Data unit with acknowledgement ID and binary attachment count (Header Only).
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryAckPacket {
    /// The underlying ack packet.
    pub inner: AckPacket,
    /// Number of binary attachments expected.
    pub attachments: u64,
}

impl BinaryAckPacket {
    /// Create a new BinaryAckPacket.
    pub fn new(inner: AckPacket, attachments: u64) -> Self {
        Self { inner, attachments }
    }
}

// ============================================================================
// 2. Protocol Context Enums
// ============================================================================

/// Event packet (Type 2): With or without acknowledgement.
#[derive(Debug, Clone, PartialEq)]
pub enum EventPacket {
    /// Event without acknowledgement expected.
    NoAck(BasePacket),
    /// Event with acknowledgement expected.
    Ack(AckPacket),
}

/// Binary event packet (Type 5): With or without acknowledgement.
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryEventPacket {
    /// Binary event without acknowledgement expected.
    NoAck(BinaryPacket),
    /// Binary event with acknowledgement expected.
    Ack(BinaryAckPacket),
}

// ============================================================================
// 3. Top-Level Packet Enum (Maps to Socket.IO Protocol Types 0-6)
// ============================================================================

/// Top-level Socket.IO packet type.
#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    /// Type 0: Connect to a namespace.
    Connect {
        /// The namespace to connect to.
        ns: String,
        /// Optional session ID from server.
        sid: Option<String>,
    },
    /// Type 1: Disconnect from a namespace.
    Disconnect {
        /// The namespace to disconnect from.
        ns: String,
    },
    /// Type 2: Event packet.
    Event(EventPacket),
    /// Type 3: Acknowledgement packet.
    Ack(AckPacket),
    /// Type 4: Connect error.
    ConnectError {
        /// The namespace that failed to connect.
        ns: String,
        /// The error message.
        message: String,
    },
    /// Type 5: Binary event packet.
    BinaryEvent(BinaryEventPacket),
    /// Type 6: Binary acknowledgement packet.
    BinaryAck(BinaryAckPacket),
}

// ============================================================================
// 4. Encoding: Packet → MessagePacket (Text Header)
// ============================================================================

impl Packet {
    /// Encodes the Packet header into an Engine.IO MessagePacket (Text).
    ///
    /// This method replaces the stateful codec for encoding.
    pub fn to_message(&self) -> MessagePacket {
        let mut buf = BytesMut::new();

        // Step 1: Determine packet type character and attachment count
        let (type_char, attachment_count) = match self {
            Packet::Connect { .. } => ('0', 0),
            Packet::Disconnect { .. } => ('1', 0),
            Packet::Event(_) => ('2', 0),
            Packet::Ack(_) => ('3', 0),
            Packet::ConnectError { .. } => ('4', 0),
            Packet::BinaryEvent(ev) => {
                let count = match ev {
                    BinaryEventPacket::NoAck(p) => p.attachments,
                    BinaryEventPacket::Ack(p) => p.attachments,
                };
                ('5', count)
            }
            Packet::BinaryAck(ack) => ('6', ack.attachments),
        };

        // Write type
        buf.put_u8(type_char as u8);

        // Step 2: Write attachment count for binary types
        if attachment_count > 0 {
            buf.put(format!("{}-", attachment_count).as_bytes());
        }

        // Step 3: Write namespace (if not default)
        let ns = match self {
            Packet::Connect { ns, .. } => ns,
            Packet::Disconnect { ns } => ns,
            Packet::Event(ev) => match ev {
                EventPacket::NoAck(p) => &p.ns,
                EventPacket::Ack(ack) => &ack.inner.ns,
            },
            Packet::Ack(ack) => &ack.inner.ns,
            Packet::ConnectError { ns, .. } => ns,
            Packet::BinaryEvent(ev) => match ev {
                BinaryEventPacket::NoAck(p) => &p.inner.ns,
                BinaryEventPacket::Ack(ack) => &ack.inner.inner.ns,
            },
            Packet::BinaryAck(ack) => &ack.inner.inner.ns,
        };

        if ns != "/" {
            buf.put(ns.as_bytes());
            buf.put_u8(b',');
        }

        // Step 4: Write packet ID (for acks and events with ack)
        match self {
            Packet::Event(EventPacket::Ack(ack)) => {
                buf.put(ack.ack_id.to_string().as_bytes());
            }
            Packet::Ack(ack) => {
                buf.put(ack.ack_id.to_string().as_bytes());
            }
            Packet::BinaryEvent(BinaryEventPacket::Ack(ack)) => {
                buf.put(ack.inner.ack_id.to_string().as_bytes());
            }
            Packet::BinaryAck(ack) => {
                buf.put(ack.inner.ack_id.to_string().as_bytes());
            }
            _ => {}
        }

        // Step 5: Write data payload
        match self {
            Packet::Event(ev) => {
                let data = match ev {
                    EventPacket::NoAck(p) => &p.data,
                    EventPacket::Ack(ack) => &ack.inner.data,
                };
                buf.put(data.clone());
            }
            Packet::Ack(ack) => {
                buf.put(ack.inner.data.clone());
            }
            Packet::ConnectError { message, .. } => {
                buf.put(message.as_bytes());
            }
            Packet::BinaryEvent(ev) => {
                let data = match ev {
                    BinaryEventPacket::NoAck(p) => &p.inner.data,
                    BinaryEventPacket::Ack(ack) => &ack.inner.inner.data,
                };
                buf.put(data.clone());
            }
            Packet::BinaryAck(ack) => {
                buf.put(ack.inner.inner.data.clone());
            }
            _ => {}
        }

        MessagePacket::Text(buf.freeze())
    }
}

// ============================================================================
// 5. Decoding: MessagePacket → Packet (Stateless Parsing)
// ============================================================================

impl Packet {
    /// Parses an Engine.IO MessagePacket into a Packet header.
    ///
    /// This method replaces the stateful codec for decoding.
    /// For binary packets, this returns the header only. The caller must
    /// use the attachment count to buffer subsequent binary messages.
    pub fn try_from_message(msg: MessagePacket) -> Result<Self> {
        let bytes = match msg {
            MessagePacket::Text(b) => b,
            _ => return Err(Error::Protocol("Expected text packet".into())),
        };

        let text = std::str::from_utf8(&bytes)?;
        if text.is_empty() {
            return Err(Error::InvalidPacket);
        }

        // Parse components
        let (type_char, attachments, rest) = parse_type(text)?;
        let (ns_str, rest) = parse_namespace(rest)?;
        let (id, rest) = parse_id(rest)?;

        let ns = ns_str.to_string();
        let data = Bytes::copy_from_slice(rest.as_bytes());

        // Construct strictly typed packet
        match type_char {
            '0' => Ok(Packet::Connect { ns, sid: None }),
            '1' => Ok(Packet::Disconnect { ns }),
            '2' => {
                let base = BasePacket::new(ns, data);
                if let Some(ack_id) = id {
                    Ok(Packet::Event(EventPacket::Ack(AckPacket::new(
                        base, ack_id,
                    ))))
                } else {
                    Ok(Packet::Event(EventPacket::NoAck(base)))
                }
            }
            '3' => {
                let ack_id = id.ok_or(Error::Protocol("Ack missing ID".into()))?;
                let base = BasePacket::new(ns, data);
                Ok(Packet::Ack(AckPacket::new(base, ack_id)))
            }
            '4' => Ok(Packet::ConnectError {
                ns,
                message: rest.to_string(),
            }),
            '5' => {
                let base = BasePacket::new(ns, data);
                let bin = BinaryPacket::new(base, attachments);
                if let Some(ack_id) = id {
                    let ack = AckPacket::new(bin.inner.clone(), ack_id);
                    let bin_ack = BinaryAckPacket::new(ack, attachments);
                    Ok(Packet::BinaryEvent(BinaryEventPacket::Ack(bin_ack)))
                } else {
                    Ok(Packet::BinaryEvent(BinaryEventPacket::NoAck(bin)))
                }
            }
            '6' => {
                let ack_id = id.ok_or(Error::Protocol("Binary ack missing ID".into()))?;
                let base = BasePacket::new(ns, data);
                let ack = AckPacket::new(base, ack_id);
                let bin_ack = BinaryAckPacket::new(ack, attachments);
                Ok(Packet::BinaryAck(bin_ack))
            }
            _ => Err(Error::InvalidPacket),
        }
    }
}

// ============================================================================
// 6. Modular Parsing Helpers
// ============================================================================

/// Parses packet type character and optional attachment count.
fn parse_type(input: &str) -> Result<(char, u64, &str)> {
    let mut chars = input.chars();
    let type_char = chars.next().ok_or(Error::InvalidPacket)?;
    let mut rest = &input[1..];

    let mut attachments = 0u64;
    if type_char == '5' || type_char == '6' {
        // Binary types require attachment count
        if let Some((count_str, r)) = rest.split_once('-') {
            attachments = count_str
                .parse()
                .map_err(|_| Error::Protocol("Invalid attachment count".into()))?;
            rest = r;
        } else {
            return Err(Error::Protocol(
                "Binary packet missing attachment count".into(),
            ));
        }
    }
    Ok((type_char, attachments, rest))
}

/// Parses namespace (if present).
fn parse_namespace(input: &str) -> Result<(&str, &str)> {
    if input.starts_with('/') {
        if let Some((ns, rest)) = input.split_once(',') {
            Ok((ns, rest))
        } else {
            // Namespace without comma means no data follows
            Ok((input, ""))
        }
    } else {
        // Default namespace
        Ok(("/", input))
    }
}

/// Parses optional packet ID.
fn parse_id(input: &str) -> Result<(Option<u64>, &str)> {
    if input.is_empty() {
        return Ok((None, input));
    }

    let end_idx = input
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(input.len());

    if end_idx > 0 {
        let id = input[..end_idx]
            .parse()
            .map_err(|_| Error::Protocol("Invalid packet ID".into()))?;
        Ok((Some(id), &input[end_idx..]))
    } else {
        Ok((None, input))
    }
}

// ============================================================================
// 7. Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_packet() {
        let packet = Packet::Connect {
            ns: "/".to_string(),
            sid: None,
        };
        let msg = packet.to_message();
        assert_eq!(msg, MessagePacket::Text(Bytes::from("0")));

        let parsed = Packet::try_from_message(msg).unwrap();
        assert_eq!(parsed, packet);
    }

    #[test]
    fn test_event_no_ack() {
        let base = BasePacket::new("/".to_string(), Bytes::from(r#"["hello","world"]"#));
        let packet = Packet::Event(EventPacket::NoAck(base));
        let msg = packet.to_message();
        assert_eq!(
            msg,
            MessagePacket::Text(Bytes::from(r#"2["hello","world"]"#))
        );

        let parsed = Packet::try_from_message(msg).unwrap();
        assert_eq!(parsed, packet);
    }

    #[test]
    fn test_event_with_ack() {
        let base = BasePacket::new("/".to_string(), Bytes::from(r#"["ping"]"#));
        let ack = AckPacket::new(base, 42);
        let packet = Packet::Event(EventPacket::Ack(ack));
        let msg = packet.to_message();
        assert_eq!(msg, MessagePacket::Text(Bytes::from(r#"242["ping"]"#)));

        let parsed = Packet::try_from_message(msg).unwrap();
        assert_eq!(parsed, packet);
    }

    #[test]
    fn test_ack_packet() {
        let base = BasePacket::new("/".to_string(), Bytes::from(r#"["pong"]"#));
        let ack = AckPacket::new(base, 42);
        let packet = Packet::Ack(ack);
        let msg = packet.to_message();
        assert_eq!(msg, MessagePacket::Text(Bytes::from(r#"342["pong"]"#)));

        let parsed = Packet::try_from_message(msg).unwrap();
        assert_eq!(parsed, packet);
    }

    #[test]
    fn test_binary_event_no_ack() {
        let base = BasePacket::new(
            "/".to_string(),
            Bytes::from(r#"["image",{"_placeholder":true,"num":0}]"#),
        );
        let bin = BinaryPacket::new(base, 1);
        let packet = Packet::BinaryEvent(BinaryEventPacket::NoAck(bin));
        let msg = packet.to_message();
        assert_eq!(
            msg,
            MessagePacket::Text(Bytes::from(r#"51-["image",{"_placeholder":true,"num":0}]"#))
        );

        let parsed = Packet::try_from_message(msg).unwrap();
        assert_eq!(parsed, packet);
    }

    #[test]
    fn test_custom_namespace() {
        let base = BasePacket::new("/admin".to_string(), Bytes::from(r#"["users"]"#));
        let packet = Packet::Event(EventPacket::NoAck(base));
        let msg = packet.to_message();
        assert_eq!(
            msg,
            MessagePacket::Text(Bytes::from(r#"2/admin,["users"]"#))
        );

        let parsed = Packet::try_from_message(msg).unwrap();
        assert_eq!(parsed, packet);
    }
}
