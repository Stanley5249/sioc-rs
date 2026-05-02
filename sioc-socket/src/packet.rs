//! Socket.IO v4 packet types.

use crate::parse::{hint_packet_size, write_packet};
use bytes::Bytes;
use bytestring::ByteString;
use serde::Deserialize;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// A value tagged with a Socket.IO namespace path (e.g. `"/chat"`).
#[derive(Debug)]
pub struct Ns<T>(pub ByteString, pub T);

/// Server payload confirming a successful namespace connection.
#[derive(Debug, Deserialize)]
pub struct Connect {
    /// Server-assigned session ID.
    pub sid: ByteString,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Server payload for a rejected namespace connection.
#[derive(Debug, Error, Deserialize)]
#[error("{message}")]
pub struct ConnectError {
    pub message: ByteString,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Type-erased inbound event after binary reassembly.
#[derive(Debug, Clone)]
pub struct DynEvent {
    pub payload: ByteString,
    pub id: Option<u64>,
    pub attachments: Option<Vec<Bytes>>,
}

impl std::fmt::Display for DynEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        map.entry(&"payload", &format_args!("{}", self.payload));
        if let Some(id) = self.id {
            map.entry(&"id", &id);
        }
        if let Some(attachments) = &self.attachments {
            map.entry(&"count", &attachments.len());
        }
        map.finish()
    }
}

impl DynEvent {
    pub fn new<T>(payload: T, id: Option<u64>) -> Self
    where
        T: Into<ByteString>,
    {
        Self {
            payload: payload.into(),
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
#[derive(Debug, Clone)]
pub struct DynAck {
    pub payload: ByteString,
    pub attachments: Option<Vec<Bytes>>,
}

impl std::fmt::Display for DynAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        map.entry(&"payload", &format_args!("{}", self.payload));
        if let Some(attachments) = &self.attachments {
            map.entry(&"count", &attachments.len());
        }
        map.finish()
    }
}

impl DynAck {
    pub fn new<T>(payload: T) -> Self
    where
        T: Into<ByteString>,
    {
        Self {
            payload: payload.into(),
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
/// The default `E = DynEvent` carries raw events; use [`cast`](Signal::cast) to
/// convert to a typed event.
#[derive(Debug)]
pub enum Signal<E = DynEvent> {
    /// The server confirmed the namespace connection.
    Connect(Connect),
    /// The server (or client) closed the namespace.
    Disconnect,
    /// The server rejected the namespace connection.
    ConnectError(ConnectError),
    /// An application-level event (possibly with binary attachments).
    Event(E),
}

impl Signal {
    /// Converts the [`Event`](Signal::Event) variant via `TryFrom<DynEvent>`, passing other variants through.
    pub fn cast<E>(self) -> Result<Signal<E>, E::Error>
    where
        E: TryFrom<DynEvent>,
    {
        match self {
            Self::Connect(c) => Ok(Signal::Connect(c)),
            Self::Disconnect => Ok(Signal::Disconnect),
            Self::ConnectError(e) => Ok(Signal::ConnectError(e)),
            Self::Event(event) => Ok(Signal::Event(E::try_from(event)?)),
        }
    }
}

/// An outbound packet to be encoded and sent to the server.
#[derive(Debug)]
pub enum Directive {
    /// Opens a namespace; `data` is an optional authentication payload.
    Connect {
        tx: mpsc::Sender<Signal>,
        payload: ByteString,
    },
    /// Closes the namespace.
    Disconnect,
    /// Emits an event; if `tx` is set, an ack ID is assigned and the response routed to it.
    Event {
        payload: ByteString,
        tx: Option<oneshot::Sender<DynAck>>,
        attachments: Option<Vec<Bytes>>,
    },
    /// Acknowledges a previously received event.
    Ack {
        payload: ByteString,
        id: u64,
        attachments: Option<Vec<Bytes>>,
    },
}

/// A wire-level packet decoded from a single text frame.
///
/// Binary variants carry an attachment count; the socket router collects
/// the follow-up binary frames and reassembles them into a [`Signal`].
#[derive(Debug)]
pub enum Packet {
    /// Type `0` — namespace connection confirmed.
    Connect(ByteString),
    /// Type `1` — namespace disconnection.
    Disconnect,
    /// Type `2` — event (text or binary).
    Event {
        payload: ByteString,
        id: Option<u64>,
    },
    /// Type `3` — acknowledgement (text or binary).
    Ack { payload: ByteString, id: u64 },
    /// Type `4` — namespace connection rejected.
    ConnectError(ByteString),

    /// Type `5` — binary event with `count` follow-up binary frames.
    BinaryEvent {
        payload: ByteString,
        id: Option<u64>,
        count: usize,
    },

    /// Type `6` — binary acknowledgement with `count` follow-up binary frames.
    BinaryAck {
        payload: ByteString,
        id: u64,
        count: usize,
    },
}

impl Packet {
    /// Returns a conservative upper bound on the serialised text-frame byte length.
    pub fn size_hint(&self, ns: &str) -> usize {
        match self {
            Self::Connect(payload) => hint_packet_size(ns, false, false, Some(payload)),
            Self::Disconnect => hint_packet_size(ns, false, false, None),
            Self::Event { payload, id } => hint_packet_size(ns, false, id.is_some(), Some(payload)),
            Self::Ack { payload, .. } => hint_packet_size(ns, false, true, Some(payload)),
            Self::ConnectError(payload) => hint_packet_size(ns, false, false, Some(payload)),
            Self::BinaryEvent { payload, id, .. } => {
                hint_packet_size(ns, true, id.is_some(), Some(payload))
            }
            Self::BinaryAck { payload, .. } => hint_packet_size(ns, true, true, Some(payload)),
        }
    }

    pub fn encode(&self, ns: &str) -> String {
        let mut buffer = String::with_capacity(self.size_hint(ns));

        match self {
            Self::Connect(bytes) => write_packet(&mut buffer, b'0', None, ns, None, Some(bytes)),

            Self::Disconnect => write_packet(&mut buffer, b'1', None, ns, None, None),
            Self::Event { payload, id } => {
                write_packet(&mut buffer, b'2', None, ns, *id, Some(payload))
            }
            Self::Ack { payload, id } => {
                write_packet(&mut buffer, b'3', None, ns, Some(*id), Some(payload))
            }
            Self::ConnectError(payload) => {
                write_packet(&mut buffer, b'4', None, ns, None, Some(payload))
            }
            Self::BinaryEvent { payload, id, count } => {
                write_packet(&mut buffer, b'5', Some(*count), ns, *id, Some(payload))
            }
            Self::BinaryAck { payload, id, count } => write_packet(
                &mut buffer,
                b'6',
                Some(*count),
                ns,
                Some(*id),
                Some(payload),
            ),
        }

        buffer
    }
}

impl std::fmt::Display for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(payload) => f
                .debug_tuple("Connect")
                .field(&format_args!("{}", payload))
                .finish(),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::Event { payload, id } => {
                let mut s = f.debug_struct("Event");
                s.field("payload", &format_args!("{}", payload));
                if let Some(id) = id {
                    s.field("id", id);
                }
                s.finish()
            }
            Self::Ack { payload, id } => f
                .debug_struct("Ack")
                .field("payload", &format_args!("{}", payload))
                .field("id", id)
                .finish(),
            Self::ConnectError(payload) => f
                .debug_tuple("ConnectError")
                .field(&format_args!("{}", payload))
                .finish(),
            Self::BinaryEvent { payload, id, count } => {
                let mut s = f.debug_struct("BinaryEvent");
                s.field("payload", &format_args!("{}", payload));
                if let Some(id) = id {
                    s.field("id", id);
                }
                s.field("count", count);
                s.finish()
            }
            Self::BinaryAck { payload, id, count } => f
                .debug_struct("BinaryAck")
                .field("payload", &format_args!("{}", payload))
                .field("id", id)
                .field("count", count)
                .finish(),
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for Signal<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(c) => f
                .debug_tuple("Connect")
                .field(&format_args!("{}", c.sid))
                .finish(),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::ConnectError(e) => f
                .debug_tuple("ConnectError")
                .field(&format_args!("{}", e))
                .finish(),
            Self::Event(e) => f
                .debug_tuple("Event")
                .field(&format_args!("{}", e))
                .finish(),
        }
    }
}

impl Ns<Packet> {
    pub fn size_hint(&self) -> usize {
        self.1.size_hint(&self.0)
    }

    pub fn encode(&self) -> String {
        self.1.encode(&self.0)
    }
}
