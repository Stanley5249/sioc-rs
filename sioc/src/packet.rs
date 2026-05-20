//! Socket.IO v4 packet types and wire encoding.

use crate::error::PacketError;
use bytes::Bytes;
use bytestring::ByteString;
use miette::Diagnostic;
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
    /// Additional server-defined fields from the connection payload.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Server payload for a rejected namespace connection.
#[derive(Debug, Error, Diagnostic, Deserialize)]
#[error("{message}")]
#[diagnostic(
    code(sioc::connect_error),
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
    pub id: Option<u64>,
    /// Reassembled binary attachments, if any.
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
    /// Creates a `DynEvent` with no attachments.
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

    /// Attaches binary payloads to this event.
    #[must_use]
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

/// A fully decoded inbound packet.
#[derive(Debug)]
pub enum Signal<E = DynEvent> {
    /// The server confirmed the namespace connection.
    Connect(Connect),
    /// The namespace was disconnected. Does not close the receiver; a reconnect delivers a new [`Signal::Connect`].
    Disconnect,
    /// The server rejected a namespace connection attempt. Does not close the receiver.
    ConnectError(ConnectError),
    /// An application-level event (possibly with binary attachments).
    Event(E),
}

impl<E> std::fmt::Display for Signal<E>
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

impl<E> Signal<E> {
    /// Returns the inner event if this is [`Event`](Signal::Event), otherwise `None`.
    pub fn take_event(self) -> Option<E> {
        match self {
            Self::Event(e) => Some(e),
            _ => None,
        }
    }

    /// Applies `f` to the inner event if this is [`Event`](Signal::Event), otherwise returns `None`.
    pub fn and_then<F, T>(self, f: F) -> Option<T>
    where
        F: FnOnce(E) -> Option<T>,
    {
        match self {
            Self::Event(e) => f(e),
            _ => None,
        }
    }

    /// Applies `f` to the event in [`Event`](Signal::Event), passing other variants through unchanged.
    pub fn map<F, U>(self, f: F) -> Signal<U>
    where
        F: FnOnce(E) -> U,
    {
        match self {
            Self::Connect(c) => Signal::Connect(c),
            Self::Disconnect => Signal::Disconnect,
            Self::ConnectError(e) => Signal::ConnectError(e),
            Self::Event(e) => Signal::Event(f(e)),
        }
    }
}

/// An outbound packet to be encoded and sent to the server.
#[derive(Debug)]
#[allow(missing_docs)]
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
    /// Sent by `SocketSenderInner::drop`; the manager warns if the namespace is still live.
    Dropped,
}

/// A wire-level packet decoded from a single text frame.
///
/// Binary variants carry an attachment count; the socket router collects
/// the follow-up binary frames and reassembles them into a [`Signal`].
#[derive(Debug)]
#[allow(missing_docs)]
pub enum Packet {
    /// Type `0`: namespace connection confirmed.
    Connect(ByteString),
    /// Type `1`: namespace disconnection.
    Disconnect,
    /// Type `2`: event (text or binary).
    Event {
        payload: ByteString,
        id: Option<u64>,
    },
    /// Type `3`: acknowledgement (text or binary).
    Ack { payload: ByteString, id: u64 },
    /// Type `4`: namespace connection rejected.
    ConnectError(ByteString),

    /// Type `5`: binary event with `count` follow-up binary frames.
    BinaryEvent {
        payload: ByteString,
        id: Option<u64>,
        count: usize,
    },

    /// Type `6`: binary acknowledgement with `count` follow-up binary frames.
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
            Self::Connect(payload) | Self::ConnectError(payload) => {
                hint_packet_size(ns, false, false, Some(payload))
            }
            Self::Disconnect => hint_packet_size(ns, false, false, None),
            Self::Event { payload, id } => hint_packet_size(ns, false, id.is_some(), Some(payload)),
            Self::Ack { payload, .. } => hint_packet_size(ns, false, true, Some(payload)),
            Self::BinaryEvent { payload, id, .. } => {
                hint_packet_size(ns, true, id.is_some(), Some(payload))
            }
            Self::BinaryAck { payload, .. } => hint_packet_size(ns, true, true, Some(payload)),
        }
    }

    /// Encodes this packet into a [`String`] using the given namespace.
    pub fn encode(&self, ns: &str) -> String {
        let mut buffer = String::with_capacity(self.size_hint(ns));

        match self {
            Self::Connect(bytes) => {
                write_packet(&mut buffer, b'0', None, ns, None, Some(bytes));
            }
            Self::Disconnect => {
                write_packet(&mut buffer, b'1', None, ns, None, None);
            }
            Self::Event { payload, id } => {
                write_packet(&mut buffer, b'2', None, ns, *id, Some(payload));
            }
            Self::Ack { payload, id } => {
                write_packet(&mut buffer, b'3', None, ns, Some(*id), Some(payload));
            }
            Self::ConnectError(payload) => {
                write_packet(&mut buffer, b'4', None, ns, None, Some(payload));
            }
            Self::BinaryEvent { payload, id, count } => {
                write_packet(&mut buffer, b'5', Some(*count), ns, *id, Some(payload));
            }
            Self::BinaryAck { payload, id, count } => {
                write_packet(
                    &mut buffer,
                    b'6',
                    Some(*count),
                    ns,
                    Some(*id),
                    Some(payload),
                );
            }
        }

        buffer
    }
}

impl std::fmt::Display for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(payload) => f
                .debug_tuple("Connect")
                .field(&format_args!("{payload}"))
                .finish(),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::Event { payload, id } => {
                let mut s = f.debug_struct("Event");
                s.field("payload", &format_args!("{payload}"));
                if let Some(id) = id {
                    s.field("id", id);
                }
                s.finish()
            }
            Self::Ack { payload, id } => f
                .debug_struct("Ack")
                .field("payload", &format_args!("{payload}"))
                .field("id", id)
                .finish(),
            Self::ConnectError(payload) => f
                .debug_tuple("ConnectError")
                .field(&format_args!("{payload}"))
                .finish(),
            Self::BinaryEvent { payload, id, count } => {
                let mut s = f.debug_struct("BinaryEvent");
                s.field("payload", &format_args!("{payload}"));
                if let Some(id) = id {
                    s.field("id", id);
                }
                s.field("count", count);
                s.finish()
            }
            Self::BinaryAck { payload, id, count } => f
                .debug_struct("BinaryAck")
                .field("payload", &format_args!("{payload}"))
                .field("id", id)
                .field("count", count)
                .finish(),
        }
    }
}

impl Ns<Packet> {
    /// Returns a conservative upper bound on the serialised byte length.
    pub fn size_hint(&self) -> usize {
        self.1.size_hint(&self.0)
    }

    /// Encodes the packet for the stored namespace.
    pub fn encode(&self) -> String {
        self.1.encode(&self.0)
    }
}

impl TryFrom<ByteString> for Ns<Packet> {
    type Error = PacketError;

    fn try_from(bytes: ByteString) -> Result<Self, PacketError> {
        let mut chars = bytes.chars();

        let id = chars.next().ok_or(PacketError::Empty)?;

        let bytes = bytes.slice_ref(chars.as_str());

        let packet = match id {
            '0' => {
                let (ns, payload) = split_namespace(bytes)?;
                Ns(ns, Packet::Connect(payload))
            }
            '1' => {
                let (ns, _) = split_namespace(bytes)?;
                Ns(ns, Packet::Disconnect)
            }
            '2' => {
                let (count, bytes) = split_attachments(bytes)?;
                if let Some(count) = count {
                    return Err(PacketError::UnexpectedAttachments { count });
                }
                let (ns, bytes) = split_namespace(bytes)?;
                let (id, payload) = split_id(bytes)?;
                Ns(ns, Packet::Event { payload, id })
            }
            '3' => {
                let (ns, bytes) = split_namespace(bytes)?;
                let (id, payload) = split_id(bytes)?;
                let id = id.ok_or(PacketError::MissingAckId)?;
                Ns(ns, Packet::Ack { payload, id })
            }
            '4' => {
                let (ns, payload) = split_namespace(bytes)?;
                Ns(ns, Packet::ConnectError(payload))
            }
            '5' => {
                let (count, bytes) = split_attachments(bytes)?;
                let count = count.ok_or(PacketError::MissingAttachmentCount)?;
                let (ns, bytes) = split_namespace(bytes)?;
                let (id, payload) = split_id(bytes)?;
                Ns(ns, Packet::BinaryEvent { payload, id, count })
            }
            '6' => {
                let (count, bytes) = split_attachments(bytes)?;
                let count = count.ok_or(PacketError::MissingAttachmentCount)?;
                let (ns, bytes) = split_namespace(bytes)?;
                let (id, payload) = split_id(bytes)?;
                let id = id.ok_or(PacketError::MissingAckId)?;
                Ns(ns, Packet::BinaryAck { payload, id, count })
            }
            id => return Err(PacketError::InvalidId { id }),
        };

        Ok(packet)
    }
}

const U64_MAX_LEN: usize = 20; // max decimal digits in a u64

const fn ack_size_hint() -> usize {
    U64_MAX_LEN
}

const fn binary_size_hint() -> usize {
    U64_MAX_LEN + 1
}

/// Returns the encoded byte length of a namespace field (`0` for the default `"/"`).
fn namespace_size(ns: &str) -> usize {
    if ns == "/" { 0 } else { ns.len() + 1 }
}

pub(crate) fn hint_packet_size(ns: &str, binary: bool, ack: bool, payload: Option<&str>) -> usize {
    let mut n = 1 + namespace_size(ns);
    if ack {
        n += ack_size_hint();
    }
    if binary {
        n += binary_size_hint();
    }
    if let Some(payload) = payload {
        n += payload.len();
    }
    n
}

/// Writes `<count>-` into `buffer`.
fn write_attachments(buffer: &mut String, count: usize) {
    buffer.push_str(&count.to_string());
    buffer.push('-');
}

/// Writes the namespace followed by `,`; skipped for the default `"/"`.
fn write_namespace(buffer: &mut String, ns: &str) {
    if ns != "/" {
        buffer.push_str(ns);
        buffer.push(',');
    }
}

/// Writes a decimal ack ID.
fn write_id(buffer: &mut String, id: u64) {
    buffer.push_str(&id.to_string());
}

fn write_payload(buffer: &mut String, payload: &str) {
    buffer.push_str(payload);
}

pub(crate) fn write_packet(
    buffer: &mut String,
    type_id: u8,
    count: Option<usize>,
    ns: &str,
    id: Option<u64>,
    payload: Option<&str>,
) {
    buffer.push(type_id as char);
    if let Some(count) = count {
        write_attachments(buffer, count);
    }
    write_namespace(buffer, ns);
    if let Some(id) = id {
        write_id(buffer, id);
    }
    if let Some(payload) = payload {
        write_payload(buffer, payload);
    }
}

/// Consumes a `<n>-` attachment count prefix.
///
/// Only matches when the data starts with one or more ASCII digits immediately
/// followed by `-`. This avoids misinterpreting a namespace that contains a
/// hyphen (e.g. `/admin-ns,["data"]`) as an attachment count.
///
/// Returns `(Some(count), rest)` when the prefix is present, `(None, bytes)`
/// when absent.
///
/// # Errors
///
/// Returns an error if the count prefix is present but not a valid integer.
pub fn split_attachments(bytes: ByteString) -> Result<(Option<usize>, ByteString), PacketError> {
    let pair = match bytes.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((i, '-')) => {
            let count = bytes[..i]
                .parse()
                .map_err(PacketError::InvalidAttachmentCount)?;

            let rest = bytes.slice_ref(&bytes[i + 1..]);

            (Some(count), rest)
        }
        _ => (None, bytes),
    };

    Ok(pair)
}

/// Consumes a `/ns,` prefix and returns `(namespace, rest)`.
///
/// Returns `"/"` for the default namespace.
///
/// # Errors
///
/// Returns an error if the namespace is present but missing its trailing `,` delimiter.
pub fn split_namespace(bytes: ByteString) -> Result<(ByteString, ByteString), PacketError> {
    match bytes.chars().next() {
        Some('/') => match bytes.split_once(',') {
            Some((ns, payload)) => Ok((bytes.slice_ref(ns), bytes.slice_ref(payload))),
            None => Err(PacketError::MissingNamespaceDelimiter),
        },
        _ => Ok((ByteString::from_static("/"), bytes)),
    }
}

/// Consumes a run of leading ASCII digits as a `u64`.
///
/// Returns `(Some(id), rest)` when digits are present and fit in `u64`,
/// `(None, bytes)` when no leading digits are found or the input is empty.
///
/// # Errors
///
/// Returns an error if the digit run overflows `u64`.
pub fn split_id(bytes: ByteString) -> Result<(Option<u64>, ByteString), PacketError> {
    let pair = match bytes.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((i, _)) if i > 0 => {
            let id = bytes[..i].parse().map_err(PacketError::InvalidAckId)?;
            let rest = bytes.slice_ref(&bytes[i..]);
            (Some(id), rest)
        }
        _ => (None, bytes),
    };

    Ok(pair)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use bytestring::ByteString;
    use serde_json::Map;

    use crate::error::PacketError;

    fn bss(s: &'static str) -> ByteString {
        ByteString::from_static(s)
    }

    fn decode(s: &'static str) -> Result<Ns<Packet>, PacketError> {
        bss(s).try_into()
    }

    fn encode_ns(ns: &'static str, pkt: Packet) -> (String, usize) {
        let np = Ns(bss(ns), pkt);
        let hint = np.size_hint();
        (np.encode(), hint)
    }

    // --- split_attachments ---

    #[test]
    fn split_attachments_empty_input() {
        let (count, rest) = split_attachments(ByteString::new()).unwrap();
        assert!(count.is_none());
        assert!(rest.is_empty());
    }

    #[test]
    fn split_attachments_no_prefix() {
        let (count, rest) = split_attachments(bss("rest")).unwrap();
        assert!(count.is_none());
        assert_eq!(&*rest, "rest");
    }

    #[test]
    fn split_attachments_valid_prefix() {
        let (count, rest) = split_attachments(bss("2-rest")).unwrap();
        assert_eq!(count, Some(2));
        assert_eq!(&*rest, "rest");
    }

    #[test]
    fn split_attachments_zero_count() {
        let (count, rest) = split_attachments(bss("0-")).unwrap();
        assert_eq!(count, Some(0));
        assert!(rest.is_empty());
    }

    #[test]
    fn split_attachments_namespace_not_misinterpreted() {
        let (count, rest) = split_attachments(bss("/ns,data")).unwrap();
        assert!(count.is_none());
        assert_eq!(&*rest, "/ns,data");
    }

    // --- split_namespace ---

    #[test]
    fn split_namespace_default_ns() {
        let (ns, rest) = split_namespace(bss("payload")).unwrap();
        assert_eq!(&*ns, "/");
        assert_eq!(&*rest, "payload");
    }

    #[test]
    fn split_namespace_non_default() {
        let (ns, rest) = split_namespace(bss(r#"/chat,["x"]"#)).unwrap();
        assert_eq!(&*ns, "/chat");
        assert_eq!(&*rest, r#"["x"]"#);
    }

    #[test]
    fn split_namespace_missing_delimiter() {
        assert!(matches!(
            split_namespace(bss("/bad")),
            Err(PacketError::MissingNamespaceDelimiter)
        ));
    }

    #[test]
    fn split_namespace_empty_payload_after_comma() {
        let (ns, rest) = split_namespace(bss("/,")).unwrap();
        assert_eq!(&*ns, "/");
        assert!(rest.is_empty());
    }

    // --- split_id ---

    #[test]
    fn split_id_empty() {
        let (id, rest) = split_id(ByteString::new()).unwrap();
        assert!(id.is_none());
        assert!(rest.is_empty());
    }

    #[test]
    fn split_id_no_leading_digits() {
        let (id, rest) = split_id(bss(r#"["x"]"#)).unwrap();
        assert!(id.is_none());
        assert_eq!(&*rest, r#"["x"]"#);
    }

    #[test]
    fn split_id_with_digits() {
        let (id, rest) = split_id(bss(r#"7["x"]"#)).unwrap();
        assert_eq!(id, Some(7));
        assert_eq!(&*rest, r#"["x"]"#);
    }

    #[test]
    fn split_id_all_digits_no_payload() {
        let (id, rest) = split_id(bss("123")).unwrap();
        assert!(id.is_none());
        assert_eq!(&*rest, "123");
    }

    // --- decode: happy paths ---

    #[test]
    fn decode_empty_is_error() {
        assert!(matches!(decode(""), Err(PacketError::Empty)));
    }

    #[test]
    fn decode_invalid_id_is_error() {
        assert!(matches!(decode("9"), Err(PacketError::InvalidId { id: '9' })));
    }

    #[test]
    fn decode_connect_default_ns() {
        let Ns(ns, pkt) = decode(r#"0{"sid":"s"}"#).unwrap();
        assert_eq!(&*ns, "/");
        assert!(matches!(pkt, Packet::Connect(_)));
    }

    #[test]
    fn decode_connect_non_default_ns() {
        let Ns(ns, pkt) = decode(r#"0/chat,{"sid":"s"}"#).unwrap();
        assert_eq!(&*ns, "/chat");
        assert!(matches!(pkt, Packet::Connect(_)));
    }

    #[test]
    fn decode_disconnect() {
        let Ns(ns, pkt) = decode("1").unwrap();
        assert_eq!(&*ns, "/");
        assert!(matches!(pkt, Packet::Disconnect));
    }

    #[test]
    fn decode_event_no_id() {
        let Ns(_, pkt) = decode(r#"2["evt"]"#).unwrap();
        assert!(matches!(pkt, Packet::Event { id: None, .. }));
    }

    #[test]
    fn decode_event_with_id() {
        let Ns(_, pkt) = decode(r#"27["evt"]"#).unwrap();
        assert!(matches!(pkt, Packet::Event { id: Some(7), .. }));
    }

    #[test]
    fn decode_ack() {
        let Ns(_, pkt) = decode(r#"37["ok"]"#).unwrap();
        assert!(matches!(pkt, Packet::Ack { id: 7, .. }));
    }

    #[test]
    fn decode_connect_error() {
        let Ns(_, pkt) = decode(r#"4{"message":"bad"}"#).unwrap();
        assert!(matches!(pkt, Packet::ConnectError(_)));
    }

    #[test]
    fn decode_binary_event_no_id() {
        let Ns(_, pkt) = decode(r#"52-["img"]"#).unwrap();
        assert!(matches!(
            pkt,
            Packet::BinaryEvent {
                count: 2,
                id: None,
                ..
            }
        ));
    }

    #[test]
    fn decode_binary_event_with_id() {
        let Ns(_, pkt) = decode(r#"52-3["img"]"#).unwrap();
        assert!(matches!(
            pkt,
            Packet::BinaryEvent {
                count: 2,
                id: Some(3),
                ..
            }
        ));
    }

    #[test]
    fn decode_binary_ack() {
        let Ns(_, pkt) = decode(r#"62-7["ok"]"#).unwrap();
        assert!(matches!(
            pkt,
            Packet::BinaryAck {
                count: 2,
                id: 7,
                ..
            }
        ));
    }

    // --- decode: error paths ---

    #[test]
    fn decode_ack_missing_id_is_error() {
        assert!(matches!(
            decode(r#"3["noid"]"#),
            Err(PacketError::MissingAckId)
        ));
    }

    #[test]
    fn decode_event_unexpected_attachments_is_error() {
        assert!(matches!(
            decode(r#"22-["x"]"#),
            Err(PacketError::UnexpectedAttachments { count: 2 })
        ));
    }

    #[test]
    fn decode_binary_event_missing_count_is_error() {
        assert!(matches!(
            decode(r#"5["x"]"#),
            Err(PacketError::MissingAttachmentCount)
        ));
    }

    #[test]
    fn decode_namespace_missing_delimiter_is_error() {
        assert!(matches!(
            decode(r#"0/chat["x"]"#),
            Err(PacketError::MissingNamespaceDelimiter)
        ));
    }

    // --- encode roundtrips ---

    #[test]
    fn encode_connect_default_ns() {
        let (enc, hint) = encode_ns("/", Packet::Connect("{}".into()));
        assert_eq!(enc, "0{}");
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_connect_non_default_ns() {
        let (enc, hint) = encode_ns("/chat", Packet::Connect("{}".into()));
        assert_eq!(enc, "0/chat,{}");
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_disconnect() {
        let (enc, hint) = encode_ns("/", Packet::Disconnect);
        assert_eq!(enc, "1");
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_event_no_id() {
        let (enc, hint) = encode_ns(
            "/",
            Packet::Event {
                payload: r#"["evt"]"#.into(),
                id: None,
            },
        );
        assert_eq!(enc, r#"2["evt"]"#);
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_event_with_id() {
        let (enc, hint) = encode_ns(
            "/",
            Packet::Event {
                payload: r#"["evt"]"#.into(),
                id: Some(7),
            },
        );
        assert_eq!(enc, r#"27["evt"]"#);
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_ack() {
        let (enc, hint) = encode_ns(
            "/",
            Packet::Ack {
                payload: r#"["ok"]"#.into(),
                id: 7,
            },
        );
        assert_eq!(enc, r#"37["ok"]"#);
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_connect_error() {
        let (enc, hint) = encode_ns("/", Packet::ConnectError(r#"{"message":"bad"}"#.into()));
        assert_eq!(enc, r#"4{"message":"bad"}"#);
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_binary_event() {
        let (enc, hint) = encode_ns(
            "/",
            Packet::BinaryEvent {
                payload: r#"["img"]"#.into(),
                id: None,
                count: 2,
            },
        );
        assert_eq!(enc, r#"52-["img"]"#);
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_binary_ack() {
        let (enc, hint) = encode_ns(
            "/",
            Packet::BinaryAck {
                payload: r#"["ok"]"#.into(),
                id: 7,
                count: 2,
            },
        );
        assert_eq!(enc, r#"62-7["ok"]"#);
        assert!(enc.len() <= hint);
    }

    // --- DynEvent ---

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
        assert!(s.contains("ping"));
    }

    // --- DynAck ---

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
        assert!(s.contains("true"));
    }

    // --- Signal ---

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
    fn signal_take_event_returns_some() {
        let sig = Signal::Event(DynEvent::new("[]", None));
        assert!(sig.take_event().is_some());
    }

    #[test]
    fn signal_take_event_on_non_event_returns_none() {
        assert!(Signal::<u8>::Disconnect.take_event().is_none());
        assert!(Signal::<u8>::Connect(make_connect()).take_event().is_none());
        assert!(Signal::<u8>::ConnectError(make_connect_error())
            .take_event()
            .is_none());
    }

    #[test]
    fn signal_map_transforms_event() {
        let sig: Signal<u8> = Signal::Event(3u8);
        assert!(matches!(sig.map(|x| x * 2), Signal::Event(6)));
    }

    #[test]
    fn signal_map_passes_non_event_through() {
        assert!(matches!(
            Signal::<u8>::Disconnect.map(|x: u8| x * 2),
            Signal::Disconnect
        ));
        assert!(matches!(
            Signal::<u8>::Connect(make_connect()).map(|x: u8| x * 2),
            Signal::Connect(_)
        ));
        assert!(matches!(
            Signal::<u8>::ConnectError(make_connect_error()).map(|x: u8| x * 2),
            Signal::ConnectError(_)
        ));
    }

    #[test]
    fn signal_and_then_returns_some_on_event() {
        let sig: Signal<u8> = Signal::Event(3u8);
        assert_eq!(sig.and_then(|x| if x > 0 { Some(x) } else { None }), Some(3));
    }

    #[test]
    fn signal_and_then_returns_none_when_f_returns_none() {
        let sig: Signal<u8> = Signal::Event(0u8);
        assert!(sig
            .and_then(|x| if x > 0 { Some(x) } else { None })
            .is_none());
    }

    #[test]
    fn signal_and_then_returns_none_on_non_event() {
        assert!(Signal::<u8>::Disconnect
            .and_then(|x: u8| Some(x))
            .is_none());
        assert!(Signal::<u8>::Connect(make_connect())
            .and_then(|x: u8| Some(x))
            .is_none());
    }

    #[test]
    fn signal_display_variants() {
        assert_eq!(format!("{}", Signal::<u8>::Disconnect), "Disconnect");
        let ev_sig = Signal::Event(DynEvent::new("[]", None));
        assert!(format!("{ev_sig}").contains("Event"));
        let connect_sig: Signal<u8> = Signal::Connect(make_connect());
        assert!(format!("{connect_sig}").contains("Connect"));
        let err_sig: Signal<u8> = Signal::ConnectError(make_connect_error());
        assert!(format!("{err_sig}").contains("ConnectError"));
    }

    #[test]
    fn packet_display_variants() {
        assert_eq!(format!("{}", Packet::Disconnect), "Disconnect");
        assert!(format!("{}", Packet::Connect("{}".into())).contains("Connect"));
        assert!(format!(
            "{}",
            Packet::Event {
                payload: "[]".into(),
                id: None,
            }
        )
        .contains("Event"));
    }
}
