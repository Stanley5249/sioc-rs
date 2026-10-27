//! Application packets and payloads for Socket.IO protocol revision 5.
//! Wire framing belongs to the private parser.

use bytes::Bytes;
use bytestring::ByteString;
use miette::Diagnostic;
use serde::Deserialize;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Server payload confirming a successful namespace connection.
#[derive(Debug, Deserialize)]
pub struct Connect {
    /// Server-assigned session ID.
    pub sid: ByteString,
    /// Additional server-defined fields from the connection payload.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Server payload for a rejected namespace connection.
#[derive(Debug, Error, Diagnostic, Deserialize)]
#[error("{message}")]
#[diagnostic(
    code(sioc::socket::connect_error),
    help(
        "Server rejected the namespace connection. Verify your auth payload and server middleware."
    ),
    url("https://socket.io/docs/v4/socket-io-protocol/#connection-to-a-namespace")
)]
pub struct ConnectError {
    /// Human-readable rejection reason from the server.
    pub message: ByteString,
    /// Additional server-defined fields from the rejection payload.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Type-erased inbound event after binary reassembly.
#[derive(Debug, Clone)]
pub struct DynEvent {
    /// Raw JSON payload (includes the event name as the first array element).
    pub payload: ByteString,
    /// Ack ID assigned by the sender, if any.
    pub id: Option<DynAckId>,
    /// Reassembled binary attachments, if any.
    pub attachments: Option<Vec<Bytes>>,
}

impl std::fmt::Display for DynEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        map.entry(&"payload", &format_args!("{}", self.payload));
        if let Some(id) = &self.id {
            map.entry(&"id", &id.get());
        }
        if let Some(attachments) = &self.attachments {
            map.entry(&"count", &attachments.len());
        }
        map.finish()
    }
}

impl DynEvent {
    /// Creates a `DynEvent` with no attachments. Its ack id belongs to no
    /// connection, so a reply to it is always rejected.
    pub fn new<T>(payload: T, id: Option<u64>) -> Self
    where
        T: Into<ByteString>,
    {
        Self {
            payload: payload.into(),
            id: id.map(DynAckId::new),
            attachments: None,
        }
    }

    /// Attaches binary payloads to this event.
    #[must_use]
    pub fn with_attachments(mut self, attachments: Vec<Bytes>) -> Self {
        self.attachments = Some(attachments);
        self
    }
}

/// Type-erased ack ID of a received event, which
/// [`AckMarker::parse`](crate::marker::AckMarker::parse) turns into a typed
/// [`AckId`](crate::marker::AckId).
///
/// It remembers the namespace connection that delivered its event, so a
/// reply goes only to that connection. A server may count ack ids per
/// connection, as python-socketio and socketioxide do, so a reply on a later
/// connection could answer a different request.
#[derive(Clone)]
pub struct DynAckId {
    id: u64,
    /// The namespace and connection token of the participation that
    /// delivered the event. `None` for an id made outside the manager, whose
    /// reply is always rejected.
    connection: Option<(ByteString, CancellationToken)>,
}

impl std::fmt::Debug for DynAckId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DynAckId").field(&self.id).finish()
    }
}

impl DynAckId {
    /// An id that belongs to no connection.
    pub(crate) fn new(id: u64) -> Self {
        Self {
            id,
            connection: None,
        }
    }

    /// Returns the raw wire-level `u64` ID.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.id
    }

    /// Ties the id to the connection of `ns` that delivers its event.
    pub(crate) fn bind(&mut self, ns: &ByteString, connection: &CancellationToken) {
        self.connection = Some((ns.clone(), connection.clone()));
    }

    /// Returns the delivering connection's token if the event came to `ns`.
    pub(crate) fn connection(&self, ns: &str) -> Option<&CancellationToken> {
        self.connection
            .as_ref()
            .filter(|(origin, _)| origin == ns)
            .map(|(_, connection)| connection)
    }
}

/// Type-erased inbound acknowledgement after binary reassembly.
///
/// Convert to a typed `sioc::Ack` via `TryFrom<DynAck>`.
#[derive(Debug, Clone)]
pub struct DynAck {
    /// Raw JSON ack payload.
    pub payload: ByteString,
    /// Reassembled binary attachments, if any.
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
    /// Creates a `DynAck` with no attachments.
    pub fn new<T>(payload: T) -> Self
    where
        T: Into<ByteString>,
    {
        Self {
            payload: payload.into(),
            attachments: None,
        }
    }

    /// Attaches binary payloads to this ack.
    #[must_use]
    pub fn with_attachments(mut self, attachments: Vec<Bytes>) -> Self {
        self.attachments = Some(attachments);
        self
    }
}

/// A packet the server sent to one namespace, as delivered to its
/// [`SocketReceiver`](crate::client::SocketReceiver).
///
/// Acks are resolved into their
/// [`AckHandle`](crate::ack::AckHandle) and binary attachments are reassembled.
#[derive(Debug)]
pub enum ServerPacket<E = DynEvent> {
    /// The server confirmed the namespace connection.
    Connect(Connect),
    /// The server disconnected the namespace.
    ///
    /// Identifies a server-initiated namespace disconnect. The receiver ends
    /// after this packet; a client disconnect or a session ending simply ends
    /// the receiver. To reopen the namespace, open a new namespace handle with
    /// [`Client::connect`](crate::client::Client::connect).
    Disconnect,
    /// The server refused the namespace. The receiver ends right after this
    /// packet.
    ConnectError(ConnectError),
    /// An application-level event (possibly with binary attachments).
    Event(E),
}

impl<E> std::fmt::Display for ServerPacket<E>
where
    E: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(c) => f
                .debug_tuple("Connect")
                .field(&format_args!("{}", c.sid))
                .finish(),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::ConnectError(e) => f
                .debug_tuple("ConnectError")
                .field(&format_args!("{e}"))
                .finish(),
            Self::Event(e) => f.debug_tuple("Event").field(&format_args!("{e}")).finish(),
        }
    }
}

impl<E> ServerPacket<E> {
    /// Returns the inner event if this is [`Event`](ServerPacket::Event),
    /// otherwise `None`.
    pub fn take_event(self) -> Option<E> {
        match self {
            Self::Event(e) => Some(e),
            _ => None,
        }
    }

    /// Applies `f` to the inner event if this is
    /// [`Event`](ServerPacket::Event), otherwise returns `None`.
    pub fn and_then<F, T>(self, f: F) -> Option<T>
    where
        F: FnOnce(E) -> Option<T>,
    {
        match self {
            Self::Event(e) => f(e),
            _ => None,
        }
    }

    /// Applies `f` to the event in [`Event`](ServerPacket::Event), passing
    /// other variants through unchanged.
    pub fn map<F, U>(self, f: F) -> ServerPacket<U>
    where
        F: FnOnce(E) -> U,
    {
        match self {
            Self::Connect(c) => ServerPacket::Connect(c),
            Self::Disconnect => ServerPacket::Disconnect,
            Self::ConnectError(e) => ServerPacket::ConnectError(e),
            Self::Event(e) => ServerPacket::Event(f(e)),
        }
    }
}

/// A packet this client sends to one namespace, like `Socket.packet`.
///
/// Rust channel: public [`Emit`](crate::event::Emit) and
/// [`Acknowledge`](crate::ack::Acknowledge) implementations construct these
/// values before the manager encodes them.
#[derive(Debug)]
pub enum ClientPacket {
    /// Emits an event; if `ack_tx` is set, an ack ID is assigned and the
    /// response routed to it.
    Event {
        /// The JSON array of the event name and arguments.
        payload: ByteString,
        /// Receives the server's ack, if the event asks for one.
        ack_tx: Option<oneshot::Sender<DynAck>>,
        /// Binary attachments, sent as frames after the packet.
        attachments: Option<Vec<Bytes>>,
    },
    /// Acknowledges a previously received event.
    Ack {
        /// The JSON array of the ack arguments.
        payload: ByteString,
        /// The ack ID of the event being acknowledged, which also names the
        /// connection the reply may go to.
        id: DynAckId,
        /// Binary attachments, sent as frames after the packet.
        attachments: Option<Vec<Bytes>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bss(s: &'static str) -> ByteString {
        ByteString::from_static(s)
    }

    #[test]
    fn dyn_event_new_has_no_attachments() {
        let ev = DynEvent::new(r#"["ping"]"#, None);
        assert!(ev.attachments.is_none());
        assert!(ev.id.is_none());
    }

    #[test]
    fn dyn_event_with_attachments() {
        let ev =
            DynEvent::new(r#"["ping"]"#, None).with_attachments(vec![Bytes::from_static(b"\x01")]);
        assert_eq!(ev.attachments.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn dyn_event_display_shows_payload() {
        let ev = DynEvent::new(r#"["ping"]"#, Some(3));
        let s = format!("{ev}");
        assert_eq!(s, r#"{"payload": ["ping"], "id": 3}"#);
    }

    #[test]
    fn dyn_ack_new_has_no_attachments() {
        let ack = DynAck::new("[true]");
        assert!(ack.attachments.is_none());
    }

    #[test]
    fn dyn_ack_with_attachments() {
        let ack = DynAck::new("[true]").with_attachments(vec![Bytes::from_static(b"\x02")]);
        assert_eq!(ack.attachments.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn dyn_ack_display_shows_payload() {
        let ack = DynAck::new("[true]");
        let s = format!("{ack}");
        assert_eq!(s, r#"{"payload": [true]}"#);
    }

    fn make_connect() -> Connect {
        Connect {
            sid: bss("t"),
            extra: Map::new(),
        }
    }

    fn make_connect_error() -> ConnectError {
        ConnectError {
            message: bss("bad"),
            extra: Map::new(),
        }
    }

    #[test]
    fn server_packet_take_event_returns_some() {
        let sig = ServerPacket::Event(DynEvent::new("[]", None));
        assert!(sig.take_event().is_some());
    }

    #[test]
    fn server_packet_take_event_on_non_event_returns_none() {
        assert!(ServerPacket::<u8>::Disconnect.take_event().is_none());
        assert!(
            ServerPacket::<u8>::Connect(make_connect())
                .take_event()
                .is_none()
        );
        assert!(
            ServerPacket::<u8>::ConnectError(make_connect_error())
                .take_event()
                .is_none()
        );
    }

    #[test]
    fn server_packet_map_transforms_event() {
        let sig: ServerPacket<u8> = ServerPacket::Event(3u8);
        assert!(matches!(sig.map(|x| x * 2), ServerPacket::Event(6)));
    }

    #[test]
    fn server_packet_map_passes_non_event_through() {
        assert!(matches!(
            ServerPacket::<u8>::Disconnect.map(|x: u8| x * 2),
            ServerPacket::Disconnect
        ));
        assert!(matches!(
            ServerPacket::<u8>::Connect(make_connect()).map(|x: u8| x * 2),
            ServerPacket::Connect(_)
        ));
        assert!(matches!(
            ServerPacket::<u8>::ConnectError(make_connect_error()).map(|x: u8| x * 2),
            ServerPacket::ConnectError(_)
        ));
    }

    #[test]
    fn server_packet_and_then_returns_some_on_event() {
        let sig: ServerPacket<u8> = ServerPacket::Event(3u8);
        assert_eq!(
            sig.and_then(|x| if x > 0 { Some(x) } else { None }),
            Some(3)
        );
    }

    #[test]
    fn server_packet_and_then_returns_none_when_f_returns_none() {
        let sig: ServerPacket<u8> = ServerPacket::Event(0u8);
        assert!(
            sig.and_then(|x| if x > 0 { Some(x) } else { None })
                .is_none()
        );
    }

    #[test]
    fn server_packet_and_then_returns_none_on_non_event() {
        assert!(
            ServerPacket::<u8>::Disconnect
                .and_then(|x: u8| Some(x))
                .is_none()
        );
        assert!(
            ServerPacket::<u8>::Connect(make_connect())
                .and_then(|x: u8| Some(x))
                .is_none()
        );
    }

    #[test]
    fn server_packet_display_variants() {
        assert_eq!(format!("{}", ServerPacket::<u8>::Disconnect), "Disconnect");
        let ev_sig = ServerPacket::Event(DynEvent::new("[]", None));
        assert_eq!(format!("{ev_sig}"), r#"Event({"payload": []})"#);
        let connect_sig: ServerPacket<u8> = ServerPacket::Connect(make_connect());
        assert_eq!(format!("{connect_sig}"), "Connect(t)");
        let err_sig: ServerPacket<u8> = ServerPacket::ConnectError(make_connect_error());
        assert_eq!(format!("{err_sig}"), "ConnectError(bad)");
    }

    #[test]
    fn dyn_event_display_with_attachments() {
        let ev = DynEvent::new("[]", None).with_attachments(vec![Bytes::from_static(b"\x01")]);
        let s = format!("{ev}");
        assert_eq!(s, r#"{"payload": [], "count": 1}"#);
    }

    #[test]
    fn dyn_ack_display_with_attachments() {
        let ack = DynAck::new("[]").with_attachments(vec![Bytes::from_static(b"\x01")]);
        let s = format!("{ack}");
        assert_eq!(s, r#"{"payload": [], "count": 1}"#);
    }
}
