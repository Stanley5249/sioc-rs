//! Socket.IO v4 packet types.

use crate::parse::{hint_packet_size, write_packet};
use bytes::{Bytes, BytesMut};
use serde::Deserialize;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// A value tagged with a Socket.IO namespace path (e.g. `"/chat"`).
#[derive(Debug)]
pub struct Ns<T>(pub String, pub T);

/// Server payload confirming a successful namespace connection.
#[derive(Debug, Deserialize)]
pub struct Connect {
    /// Server-assigned session ID.
    pub sid: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Server payload for a rejected namespace connection.
#[derive(Debug, Error, Deserialize)]
#[error("{message}")]
pub struct ConnectError {
    pub message: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Type-erased inbound event after binary reassembly.
#[derive(Clone)]
pub struct DynEvent {
    pub data: Bytes,
    pub id: Option<u64>,
    pub attachments: Option<Vec<Bytes>>,
}

impl std::fmt::Debug for DynEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("DynEvent");
        debug.field("data", &self.data);
        if let Some(id) = self.id {
            debug.field("id", &id);
        }
        if let Some(attachments) = &self.attachments {
            debug.field("count", &attachments.len());
        }
        debug.finish()
    }
}

impl DynEvent {
    pub fn new(data: Bytes, id: Option<u64>) -> Self {
        Self {
            data,
            id,
            attachments: None,
        }
    }

    pub fn with_attachments(mut self, attachments: Vec<Bytes>) -> Self {
        self.attachments = Some(attachments);
        self
    }
}

/// Type-erased inbound acknowledgement after binary reassembly.
///
/// Convert to a typed `sioc::Ack` via `TryFrom<DynAck>`.
#[derive(Clone)]
pub struct DynAck {
    pub data: Bytes,
    pub attachments: Option<Vec<Bytes>>,
}

impl std::fmt::Debug for DynAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("DynAck");
        debug.field("data", &self.data);
        if let Some(attachments) = &self.attachments {
            debug.field("count", &attachments.len());
        }
        debug.finish()
    }
}

impl DynAck {
    pub fn new(data: Bytes) -> Self {
        Self {
            data,
            attachments: None,
        }
    }

    pub fn with_attachments(mut self, attachments: Vec<Bytes>) -> Self {
        self.attachments = Some(attachments);
        self
    }
}

/// A fully decoded inbound packet.
///
/// The default `E = DynEvent` carries raw events; use [`cast`](Packet::cast) to
/// convert to a typed event.
#[derive(Debug)]
pub enum Packet<E = DynEvent> {
    /// The server confirmed the namespace connection.
    Connect(Connect),
    /// The server (or client) closed the namespace.
    Disconnect,
    /// The server rejected the namespace connection.
    ConnectError(ConnectError),
    /// An application-level event (possibly with binary attachments).
    Event(E),
}

impl Packet {
    /// Converts the [`Event`](Packet::Event) variant via `TryFrom<DynEvent>`, passing other variants through.
    pub fn cast<E>(self) -> Result<Packet<E>, E::Error>
    where
        E: TryFrom<DynEvent>,
    {
        match self {
            Self::Connect(c) => Ok(Packet::Connect(c)),
            Self::Disconnect => Ok(Packet::Disconnect),
            Self::ConnectError(e) => Ok(Packet::ConnectError(e)),
            Self::Event(event) => Ok(Packet::Event(E::try_from(event)?)),
        }
    }
}

/// An outbound packet to be encoded and sent to the server.
#[derive(Debug)]
pub enum Command {
    /// Opens a namespace; `data` is an optional authentication payload.
    Connect {
        tx: mpsc::Sender<Packet>,
        data: Bytes,
    },
    /// Closes the namespace.
    Disconnect,
    /// Emits an event; if `tx` is set, an ack ID is assigned and the response routed to it.
    Event {
        data: Bytes,
        tx: Option<oneshot::Sender<DynAck>>,
        attachments: Option<Vec<Bytes>>,
    },
    /// Acknowledges a previously received event.
    Ack {
        data: Bytes,
        id: u64,
        attachments: Option<Vec<Bytes>>,
    },
}

/// A wire-level packet decoded from a single text frame.
///
/// Binary variants carry an attachment count; the socket router collects
/// the follow-up binary frames and reassembles them into a [`Packet`].
#[derive(Debug)]
pub enum RawPacket {
    /// Type `0` — namespace connection confirmed.
    Connect(Bytes),
    /// Type `1` — namespace disconnection.
    Disconnect,
    /// Types `2` — event (text or binary).
    Event {
        data: Bytes,
        id: Option<u64>,
    },
    /// Types `3` — acknowledgement (text or binary).
    Ack {
        data: Bytes,
        id: u64,
    },
    /// Type `4` — namespace connection rejected.
    ConnectError(Bytes),

    BinaryEvent {
        data: Bytes,
        id: Option<u64>,
        count: usize,
    },

    BinaryAck {
        data: Bytes,
        id: u64,
        count: usize,
    },
}

impl RawPacket {
    /// Returns a conservative upper bound on the serialised text-frame byte length.
    pub fn size_hint(&self, ns: &str) -> usize {
        match self {
            Self::Connect(data) => hint_packet_size(ns, false, false, Some(data)),
            Self::Disconnect => hint_packet_size(ns, false, false, None),
            Self::Event { data, id } => hint_packet_size(ns, false, id.is_some(), Some(data)),
            Self::Ack { data, .. } => hint_packet_size(ns, false, true, Some(data)),
            Self::ConnectError(bytes) => hint_packet_size(ns, false, false, Some(bytes)),
            Self::BinaryEvent { data, id, .. } => {
                hint_packet_size(ns, true, id.is_some(), Some(data))
            }
            Self::BinaryAck { data, .. } => hint_packet_size(ns, true, true, Some(data)),
        }
    }

    pub fn encode(&self, ns: &str) -> Bytes {
        let mut buffer = BytesMut::with_capacity(self.size_hint(ns));

        match self {
            Self::Connect(bytes) => write_packet(&mut buffer, b'0', None, ns, None, Some(bytes)),

            Self::Disconnect => write_packet(&mut buffer, b'1', None, ns, None, None),
            Self::Event { data, id } => write_packet(&mut buffer, b'2', None, ns, *id, Some(data)),
            Self::Ack { data, id } => {
                write_packet(&mut buffer, b'3', None, ns, Some(*id), Some(data))
            }
            Self::ConnectError(data) => write_packet(&mut buffer, b'4', None, ns, None, Some(data)),
            Self::BinaryEvent { data, id, count } => {
                write_packet(&mut buffer, b'5', Some(*count), ns, *id, Some(data))
            }
            Self::BinaryAck { data, id, count } => {
                write_packet(&mut buffer, b'6', Some(*count), ns, Some(*id), Some(data))
            }
        }

        buffer.freeze()
    }
}

impl Ns<RawPacket> {
    pub fn size_hint(&self) -> usize {
        self.1.size_hint(&self.0)
    }

    pub fn encode(&self) -> Bytes {
        self.1.encode(&self.0)
    }
}
