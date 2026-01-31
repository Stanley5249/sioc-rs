//! Socket.IO packet types.

use crate::error::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use sioc_engine::packet::{Message as EngineMessage, Packet as EnginePacket};
use smallvec::SmallVec;
use std::convert::TryFrom;

/// Type alias for binary attachments collection.
pub type Attachments = SmallVec<[Bytes; 1]>;

/// Packet for sending/receiving events (types 2/5).
#[derive(Debug, Clone)]
pub struct EventPacket {
    /// Namespace.
    pub ns: String,
    /// Optional packet ID.
    pub id: Option<u64>,
    /// Event data.
    pub data: Bytes,
    /// Binary attachments.
    pub attachments: Attachments,
    /// Number of attachments.
    pub attachment_count: usize,
}

impl EventPacket {
    /// Create a new EventPacket with the given namespace and data.
    pub fn new(ns: String, data: Bytes) -> Self {
        Self {
            ns,
            id: None,
            data,
            attachments: Attachments::new(),
            attachment_count: 0,
        }
    }

    /// Get the number of binary attachments.
    pub fn attachment_count(&self) -> usize {
        self.attachments.len()
    }
}

/// Packet for acknowledgements (types 3/6).
#[derive(Debug, Clone)]
pub struct AckPacket {
    /// Namespace.
    pub ns: String,
    /// Acknowledgement ID.
    pub ack_id: u64,
    /// Acknowledgement data.
    pub data: Bytes,
    /// Binary attachments.
    pub attachments: Attachments,
    /// Number of attachments.
    pub attachment_count: usize,
}

impl AckPacket {
    /// Get the number of binary attachments.
    pub fn attachment_count(&self) -> usize {
        self.attachments.len()
    }
}

/// Top-level Packet Enum.
#[derive(Debug, Clone)]
pub enum Packet {
    /// Connect packet.
    Connect {
        /// Namespace.
        ns: String,
        /// Optional session ID.
        sid: Option<String>,
    },
    /// Disconnect packet.
    Disconnect {
        /// Namespace.
        ns: String,
    },
    /// Connect error packet.
    ConnectError {
        /// Namespace.
        ns: String,
        /// Error message.
        message: String,
    },
    /// Event packet.
    Event(EventPacket),
    /// Acknowledgement packet.
    Ack(AckPacket),
}

impl Packet {
    /// Convert this Socket.IO packet to an Engine.IO packet for transmission.
    pub fn to_engine_packet(&self) -> EnginePacket {
        let mut buf = BytesMut::new();

        // 1. Determine Type & Attachments
        let (type_char, has_attachments, count) = match self {
            Packet::Connect { .. } => ('0', false, 0),
            Packet::Disconnect { .. } => ('1', false, 0),
            Packet::Event(p) => {
                let has = !p.attachments.is_empty();
                (if has { '5' } else { '2' }, has, p.attachments.len())
            }
            Packet::Ack(p) => {
                let has = !p.attachments.is_empty();
                (if has { '6' } else { '3' }, has, p.attachments.len())
            }
            Packet::ConnectError { .. } => ('4', false, 0),
        };

        buf.put_u8(type_char as u8);

        // 2. Attachments Count
        if has_attachments {
            buf.put(format!("{}-", count).as_bytes());
        }

        // 3. Namespace (if not default)
        let ns = match self {
            Packet::Connect { ns, .. } => ns,
            Packet::Disconnect { ns } => ns,
            Packet::ConnectError { ns, .. } => ns,
            Packet::Event(p) => &p.ns,
            Packet::Ack(p) => &p.ns,
        };
        if ns != "/" {
            buf.put(ns.as_bytes());
            buf.put_u8(b',');
        }

        // 4. ID
        match self {
            Packet::Event(p) if p.id.is_some() => buf.put(p.id.unwrap().to_string().as_bytes()),
            Packet::Ack(p) => buf.put(p.ack_id.to_string().as_bytes()),
            _ => {}
        }

        // 5. Data
        let data = match self {
            Packet::Event(p) => &p.data,
            Packet::Ack(p) => &p.data,
            Packet::ConnectError { message, .. } => {
                return EnginePacket::Message(EngineMessage::Text(Bytes::from(message.clone())));
            }
            _ => &Bytes::new(),
        };
        buf.put(data.clone());

        EnginePacket::Message(EngineMessage::Text(buf.freeze()))
    }
}

// Modular Parsing Functions

/// Parses packet type and attachment count.
fn parse_type(input: &str) -> Result<(char, usize, &str)> {
    let mut chars = input.chars();
    let type_char = chars.next().ok_or(Error::InvalidPacket)?;
    let mut rest = &input[1..];

    let mut attachments = 0;
    if type_char == '5' || type_char == '6' {
        // Binary types
        if let Some((count_str, r)) = rest.split_once('-') {
            attachments = count_str.parse().map_err(|_| Error::InvalidPacket)?;
            rest = r;
        } else {
            return Err(Error::InvalidPacket);
        }
    }
    Ok((type_char, attachments, rest))
}

/// Parses namespace.
fn parse_namespace(input: &str) -> Result<(&str, &str)> {
    if input.starts_with('/') {
        if let Some((ns, rest)) = input.split_once(',') {
            Ok((ns, rest))
        } else {
            Ok((input, ""))
        }
    } else {
        Ok(("/", input))
    }
}

/// Parses packet ID.
fn parse_id(input: &str) -> Result<(Option<u64>, &str)> {
    if input.is_empty() {
        return Ok((None, input));
    }

    let end_idx = input
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(input.len());
    if end_idx > 0 {
        let id = input[..end_idx].parse().map_err(|_| Error::InvalidPacket)?;
        Ok((Some(id), &input[end_idx..]))
    } else {
        Ok((None, input))
    }
}

impl TryFrom<EnginePacket> for Packet {
    type Error = Error;

    fn try_from(packet: EnginePacket) -> Result<Self> {
        let EnginePacket::Message(msg) = packet else {
            return Err(Error::InvalidPacket);
        };

        let bytes = match msg {
            EngineMessage::Text(b) => b,
            _ => return Err(Error::InvalidPacket),
        };

        let text = std::str::from_utf8(&bytes)?;
        if text.is_empty() {
            return Err(Error::InvalidPacket);
        }

        let (type_char, attachments, rest) = parse_type(text)?;
        let (ns_str, rest) = parse_namespace(rest)?;
        let (id, rest) = parse_id(rest)?;

        let ns = ns_str.to_string();
        let data = Bytes::copy_from_slice(rest.as_bytes());

        match type_char {
            '0' => Ok(Packet::Connect { ns, sid: None }),
            '1' => Ok(Packet::Disconnect { ns }),
            '2' | '5' => Ok(Packet::Event(EventPacket {
                ns,
                id,
                data,
                attachments: Attachments::new(),
                attachment_count: attachments,
            })),
            '3' | '6' => {
                let ack_id = id.ok_or(Error::InvalidPacket)?;
                Ok(Packet::Ack(AckPacket {
                    ns,
                    ack_id,
                    data,
                    attachments: Attachments::new(),
                    attachment_count: attachments,
                }))
            }
            '4' => Ok(Packet::ConnectError {
                ns,
                message: rest.to_string(),
            }),
            _ => Err(Error::InvalidPacket),
        }
    }
}

/// Placeholder for binary data in JSON payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryPlaceholder {
    /// Placeholder flag.
    #[serde(rename = "_placeholder")]
    pub placeholder: bool,
    /// Index of the binary data.
    #[serde(rename = "num")]
    pub index: usize,
}
impl BinaryPlaceholder {
    /// Create a new BinaryPlaceholder.
    pub fn new(index: usize) -> Self {
        Self {
            placeholder: true,
            index,
        }
    }
}
