//! Socket.IO v5 packet types.
//!
//! Defines the inbound ([`Packet`]) and outbound ([`Command`]) packet
//! envelopes plus the supporting data types.  Wire-level encoding and decoding
//! helpers live in [`crate::parse`].

use crate::parse::packet_size_hint;
use bytes::Bytes;
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
    /// Server-assigned session identifier for this namespace socket.
    pub sid: String,
    /// Additional fields sent by the server beyond the mandatory `sid`.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Server payload when a namespace connection is rejected.
#[derive(Debug, Error, Deserialize)]
#[error("{message}")]
pub struct ConnectError {
    /// Human-readable rejection reason from the server.
    pub message: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Type-erased inbound event after binary reassembly.
///
/// Convert to a typed `sioc::Event` via `TryFrom<DynEvent>`.
#[derive(Debug)]
pub struct DynEvent {
    pub data: Bytes,
    pub id: Option<u64>,
    pub attachments: Option<Vec<Bytes>>,
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
#[derive(Debug)]
pub struct DynAck {
    pub data: Bytes,
    pub attachments: Option<Vec<Bytes>>,
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

/// A fully decoded inbound packet, ready for consumption by callers.
///
/// Produced by [`Manager`](crate::manager::Manager) after binary reassembly.
/// The default `E = DynEvent` carries raw events; use [`cast`](Packet::cast) to
/// convert into a typed event via `TryFrom<DynEvent>`.
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
    /// Converts the [`Event`](Packet::Event) variant from [`DynEvent`] into `E`
    /// via `TryFrom<DynEvent>`, passing other variants through unchanged.
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

/// An outbound packet to be serialised and sent to the server.
#[derive(Debug)]
pub enum Command {
    /// Opens a namespace.
    ///
    /// `sender` receives inbound packets for this namespace. `data` carries
    /// an optional authentication payload.
    Connect {
        sender: mpsc::Sender<Packet>,
        data: Option<Bytes>,
    },
    /// Closes the namespace.
    Disconnect,
    /// Emits an event.
    ///
    /// If `sender` is set the manager assigns an ack ID and routes the
    /// server's response back through the oneshot.
    Event {
        data: Bytes,
        sender: Option<oneshot::Sender<DynAck>>,
        attachments: Option<Vec<Bytes>>,
    },
    /// Acknowledges a previously received event.
    Ack {
        data: Bytes,
        id: u64,
        attachments: Option<Vec<Bytes>>,
    },
}

impl Command {
    pub fn type_id(&self) -> u8 {
        match self {
            Command::Connect { .. } => b'0',
            Command::Disconnect => b'1',
            Command::Event {
                attachments: None, ..
            } => b'2',
            Command::Event {
                attachments: Some(..),
                ..
            } => b'5',
            Command::Ack {
                attachments: None, ..
            } => b'3',
            Command::Ack {
                attachments: Some(..),
                ..
            } => b'6',
        }
    }

    /// Returns a conservative upper bound on the serialised text-frame byte length.
    pub fn size_hint(&self, ns: &str) -> usize {
        match self {
            Command::Connect { data, .. } => packet_size_hint(ns, false, false, data.as_deref()),
            Command::Disconnect => packet_size_hint(ns, false, false, None),
            Command::Event {
                data,
                sender,
                attachments,
            } => packet_size_hint(ns, sender.is_some(), attachments.is_some(), Some(data)),
            Command::Ack {
                data, attachments, ..
            } => packet_size_hint(ns, false, attachments.is_some(), Some(data)),
        }
    }
}

/// A wire-level packet decoded directly from a single text frame.
///
/// Binary variants (types 5 and 6) carry an attachment *count* but no actual
/// binary data yet — [`Manager`](crate::manager::Manager) collects follow-up
/// binary frames and promotes the result to [`Packet`] once all
/// attachments have arrived.
#[derive(Debug)]
pub enum SioPacket {
    /// Type `0` — namespace connection confirmed.
    Connect(Connect),
    /// Type `1` — namespace disconnection.
    Disconnect,
    /// Types `2` / `5` — event (text or binary).
    Event {
        data: Bytes,
        id: Option<u64>,
        /// `Some(n)` for binary events (type 5); `None` for text (type 2).
        count: Option<usize>,
    },
    /// Type `4` — namespace connection rejected.
    ConnectError(ConnectError),
    /// Types `3` / `6` — acknowledgement (text or binary).
    Ack {
        data: Bytes,
        id: u64,
        /// `Some(n)` for binary acks (type 6); `None` for text (type 3).
        count: Option<usize>,
    },
}
