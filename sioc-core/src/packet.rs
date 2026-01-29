//! Socket.IO packet types and wire protocol representation.

use crate::error::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use sioc_engine::packet::{Message as EngineMessage, Packet as EnginePacket};
use smallvec::SmallVec;
use std::convert::TryFrom;

/// Type alias for binary attachments collection.
pub type Attachments = SmallVec<[Bytes; 1]>;

/// Payload for Event packets (Socket.IO types 2 and 5).
///
/// Contains the event data, optional acknowledgement ID, and any binary attachments.
#[derive(Debug, Clone)]
pub struct EventPayload {
    /// Request ID. If `Some`, this is an active request expecting a reply.
    pub id: Option<u64>,
    /// Raw, pre-serialized JSON payload.
    pub data: Bytes,
    /// Binary attachments (if any).
    pub attachments: Attachments,
}

impl EventPayload {
    /// Create a new EventPayload with the given data.
    pub fn new(data: Bytes) -> Self {
        Self {
            id: None,
            data,
            attachments: Attachments::new(),
        }
    }
    /// Add binary attachments to this EventPayload.
    pub fn with_attachments(mut self, attachments: Vec<Bytes>) -> Self {
        self.attachments = Attachments::from_vec(attachments);
        self
    }
    /// Set the acknowledgement ID for this EventPayload.
    pub fn with_id(mut self, id: u64) -> Self {
        self.id = Some(id);
        self
    }
    /// Check if this EventPayload has binary attachments.
    pub fn has_attachments(&self) -> bool {
        !self.attachments.is_empty()
    }
    /// Get the number of binary attachments.
    pub fn attachment_count(&self) -> usize {
        self.attachments.len()
    }
}

/// Payload for Ack packets (Socket.IO types 3 and 6).
///
/// Contains the acknowledgement ID and response data with optional attachments.
#[derive(Debug, Clone)]
pub struct AckPayload {
    /// Target ID. This is a passive reply to a previous request.
    pub ack_id: u64,
    /// Raw, pre-serialized JSON payload.
    pub data: Bytes,
    /// Binary attachments (if any).
    pub attachments: Attachments,
}

impl AckPayload {
    /// Create a new AckPayload with the given acknowledgement ID and data.
    pub fn new(ack_id: u64, data: Bytes) -> Self {
        Self {
            ack_id,
            data,
            attachments: Attachments::new(),
        }
    }
    /// Add binary attachments to this AckPayload.
    pub fn with_attachments(mut self, attachments: Vec<Bytes>) -> Self {
        self.attachments = Attachments::from_vec(attachments);
        self
    }
    /// Check if this AckPayload has binary attachments.
    pub fn has_attachments(&self) -> bool {
        !self.attachments.is_empty()
    }
    /// Get the number of binary attachments.
    pub fn attachment_count(&self) -> usize {
        self.attachments.len()
    }
}

/// Unified payload enum for all Socket.IO packet types.
///
/// This enum encapsulates the different kinds of messages that can be sent
/// or received over a Socket.IO connection.
#[derive(Debug, Clone)]
pub enum Payload {
    /// Connect to a namespace (type 0).
    Connect,
    /// Disconnect from a namespace (type 1).
    Disconnect,
    /// Connection error from server (type 4).
    ConnectError(String),
    /// Event packet with JSON payload (types 2/5).
    Event(EventPayload),
    /// Acknowledgement response (types 3/6).
    Ack(AckPayload),
}

/// Core packet structure - the atomic unit of Socket.IO communication.
///
/// A packet represents a single message exchanged between client and server.
/// The `inner` field contains the typed payload data.
#[derive(Debug, Clone)]
pub struct Packet {
    /// Namespace (usually "/" for default).
    pub ns: String,
    /// The payload data for this packet.
    pub inner: Payload,
}

impl Packet {
    /// Set the namespace for this packet.
    pub fn with_namespace<S: Into<String>>(mut self, ns: S) -> Self {
        self.ns = ns.into();
        self
    }

    /// Convert this Socket.IO packet to an Engine.IO packet for transmission.
    pub fn to_engine_packet(&self) -> EnginePacket {
        let mut buf = BytesMut::new();
        let (type_char, has_attachments) = match &self.inner {
            Payload::Connect => ('0', false),
            Payload::Disconnect => ('1', false),
            Payload::Event(p) => (
                if p.has_attachments() { '5' } else { '2' },
                p.has_attachments(),
            ),
            Payload::Ack(p) => (
                if p.has_attachments() { '6' } else { '3' },
                p.has_attachments(),
            ),
            Payload::ConnectError(_) => ('4', false),
        };

        buf.put_u8(type_char as u8);

        if has_attachments {
            let count = match &self.inner {
                Payload::Event(p) => p.attachment_count(),
                Payload::Ack(p) => p.attachment_count(),
                _ => 0,
            };
            buf.put(format!("{}-", count).as_bytes());
        }

        if self.ns != "/" {
            buf.put(self.ns.as_bytes());
            buf.put_u8(b',');
        }

        match &self.inner {
            Payload::Event(p) if p.id.is_some() => buf.put(p.id.unwrap().to_string().as_bytes()),
            Payload::Ack(p) => buf.put(p.ack_id.to_string().as_bytes()),
            _ => {}
        }

        let data = match &self.inner {
            Payload::Event(p) => p.data.clone(),
            Payload::Ack(p) => p.data.clone(),
            Payload::ConnectError(e) => Bytes::from(e.clone()),
            _ => Bytes::new(),
        };
        buf.put(data);

        EnginePacket::Message(EngineMessage::Text(buf.freeze()))
    }
}

impl TryFrom<EnginePacket> for Packet {
    type Error = Error;

    fn try_from(engine_packet: EnginePacket) -> Result<Self> {
        let EnginePacket::Message(message) = engine_packet else {
            return Err(Error::InvalidPacket);
        };
        let bytes = match message {
            EngineMessage::Text(b) => b,
            _ => return Err(Error::InvalidPacket),
        };
        if bytes.is_empty() {
            return Err(Error::InvalidPacket);
        }

        let mut cursor = 0;
        let type_char = bytes[cursor] as char;
        cursor += 1;

        let (ns, packet_type) = match type_char {
            '0' => ("/".to_string(), Payload::Connect),
            '1' => ("/".to_string(), Payload::Disconnect),
            '2' => {
                let (ns, id, data) = parse_event_data(&bytes, cursor)?;
                (
                    ns,
                    Payload::Event(EventPayload {
                        id,
                        data,
                        attachments: Attachments::new(),
                    }),
                )
            }
            '3' => {
                let (ns, ack_id, data) = parse_ack_data(&bytes, cursor)?;
                (
                    ns,
                    Payload::Ack(AckPayload {
                        ack_id,
                        data,
                        attachments: Attachments::new(),
                    }),
                )
            }
            '4' => {
                let error_msg =
                    std::str::from_utf8(&bytes[cursor..]).map_err(|_| Error::InvalidPacket)?;
                (
                    "/".to_string(),
                    Payload::ConnectError(error_msg.to_string()),
                )
            }
            '5' => {
                let (_attachment_count, new_cursor) = parse_attachments(&bytes, cursor)?;
                cursor = new_cursor;
                let (ns, new_cursor) = parse_namespace(&bytes, cursor)?;
                cursor = new_cursor;
                let (id, new_cursor) = parse_id(&bytes, cursor)?;
                cursor = new_cursor;
                let data = bytes.slice(cursor..);
                (
                    ns,
                    Payload::Event(EventPayload {
                        id,
                        data,
                        attachments: Attachments::new(), // Will be filled by caller
                    }),
                )
            }
            '6' => {
                let (_attachment_count, new_cursor) = parse_attachments(&bytes, cursor)?;
                cursor = new_cursor;
                let (ns, new_cursor) = parse_namespace(&bytes, cursor)?;
                cursor = new_cursor;
                let (ack_id, new_cursor) = parse_id(&bytes, cursor)?;
                cursor = new_cursor;
                let data = bytes.slice(cursor..);
                let ack_id = ack_id.ok_or(Error::InvalidPacket)?;
                (
                    ns,
                    Payload::Ack(AckPayload {
                        ack_id,
                        data,
                        attachments: Attachments::new(), // Will be filled by caller
                    }),
                )
            }
            _ => return Err(Error::InvalidPacket),
        };

        Ok(Packet {
            ns,
            inner: packet_type,
        })
    }
}

fn parse_attachments(bytes: &[u8], mut cursor: usize) -> Result<(u8, usize)> {
    let mut attachment_count = 0;
    if cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
        let start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if cursor < bytes.len() && bytes[cursor] == b'-' {
            let count_str =
                std::str::from_utf8(&bytes[start..cursor]).map_err(|_| Error::InvalidPacket)?;
            attachment_count = count_str.parse().map_err(|_| Error::InvalidPacket)?;
            cursor += 1; // skip '-'
        } else {
            cursor = start; // reset if not followed by '-'
        }
    }
    Ok((attachment_count, cursor))
}

fn parse_namespace(bytes: &[u8], mut cursor: usize) -> Result<(String, usize)> {
    let mut ns = "/".to_string();
    if cursor < bytes.len() && bytes[cursor] == b'/' {
        let start = cursor;
        cursor += 1; // skip '/'
        while cursor < bytes.len() && bytes[cursor] != b',' {
            cursor += 1;
        }
        if cursor < bytes.len() {
            ns = std::str::from_utf8(&bytes[start..cursor])
                .map_err(|_| Error::InvalidPacket)?
                .to_string();
            cursor += 1; // skip ','
        } else {
            return Err(Error::InvalidPacket);
        }
    }
    Ok((ns, cursor))
}

fn parse_id(bytes: &[u8], mut cursor: usize) -> Result<(Option<u64>, usize)> {
    let mut id = None;
    if cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
        let start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        let id_str =
            std::str::from_utf8(&bytes[start..cursor]).map_err(|_| Error::InvalidPacket)?;
        id = Some(id_str.parse().map_err(|_| Error::InvalidPacket)?);
    }
    Ok((id, cursor))
}

fn parse_event_data(bytes: &[u8], cursor: usize) -> Result<(String, Option<u64>, Bytes)> {
    let (ns, cursor) = parse_namespace(bytes, cursor)?;
    let (id, cursor) = parse_id(bytes, cursor)?;
    let data = Bytes::copy_from_slice(&bytes[cursor..]);
    Ok((ns, id, data))
}

fn parse_ack_data(bytes: &[u8], cursor: usize) -> Result<(String, u64, Bytes)> {
    let (ns, cursor) = parse_namespace(bytes, cursor)?;
    let (id, cursor) = parse_id(bytes, cursor)?;
    let id = id.ok_or(Error::InvalidPacket)?;
    let data = Bytes::copy_from_slice(&bytes[cursor..]);
    Ok((ns, id, data))
}

/// Placeholder for binary data in JSON payloads.
///
/// When sending binary events, the JSON payload contains `BinaryPlaceholder` placeholders
/// that reference positions in the `attachments` array.
///
/// # Wire Format
///
/// Serializes to: `{"_placeholder":true,"num":<index>}`
///
/// # Example
///
/// ```rust
/// use sioc_core::packet::BinaryPlaceholder;
///
/// let idx = BinaryPlaceholder::new(0);
/// let json = serde_json::to_string(&idx).unwrap();
/// assert!(json.contains("_placeholder"));
/// assert!(json.contains("\"num\":0"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryPlaceholder {
    /// Always `true` to identify this as a placeholder.
    #[serde(rename = "_placeholder")]
    pub placeholder: bool,
    /// Index into the attachments array.
    #[serde(rename = "num")]
    pub index: usize,
}
impl BinaryPlaceholder {
    /// Create a new binary index placeholder.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sioc_core::packet::BinaryPlaceholder;
    ///
    /// let idx = BinaryPlaceholder::new(0);
    /// assert_eq!(idx.index, 0);
    /// assert!(idx.placeholder);
    /// ```
    pub fn new(index: usize) -> Self {
        Self {
            placeholder: true,
            index,
        }
    }
}
