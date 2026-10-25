//! Socket.IO wire packets, like `socket.io-parser`'s `Packet`, `Encoder` and
//! `Decoder`. The manager owns binary reconstruction and application routing.

use bytestring::ByteString;

use crate::error::PacketError;

/// A value tagged with the `nsp` of a socket.io-parser packet.
/// Rust channel: namespace identity travels with the value between owners.
#[derive(Debug)]
pub struct Ns<T>(pub ByteString, pub T);

/// A wire-level packet, like socket.io-parser's `Packet`.
///
/// Binary variants carry an attachment count; the socket router collects
/// the follow-up binary frames and reassembles them into a
/// [`ServerPacket`](crate::packet::ServerPacket).
#[derive(Debug)]
pub enum Packet {
    /// Type `0`: namespace connection confirmed.
    Connect(ByteString),
    /// Type `1`: namespace disconnection.
    Disconnect,
    /// Type `2`: event (text or binary).
    Event {
        /// The JSON array of the event name and arguments.
        payload: ByteString,
        /// The ack ID, if the sender asks for an ack.
        id: Option<u64>,
    },
    /// Type `3`: acknowledgement (text or binary).
    Ack {
        /// The JSON array of the ack arguments.
        payload: ByteString,
        /// The ack ID of the event being acknowledged.
        id: u64,
    },
    /// Type `4`: namespace connection rejected.
    ConnectError(ByteString),

    /// Type `5`: binary event with `count` follow-up binary frames.
    BinaryEvent {
        /// The JSON array of the event name and arguments, with placeholders.
        payload: ByteString,
        /// The ack ID, if the sender asks for an ack.
        id: Option<u64>,
        /// The number of binary frames that follow.
        count: usize,
    },

    /// Type `6`: binary acknowledgement with `count` follow-up binary frames.
    BinaryAck {
        /// The JSON array of the ack arguments, with placeholders.
        payload: ByteString,
        /// The ack ID of the event being acknowledged.
        id: u64,
        /// The number of binary frames that follow.
        count: usize,
    },
}

impl Packet {
    /// Returns a conservative upper bound on the serialised text-frame byte
    /// length.
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
                let (count, bytes) = split_binary_attachments(bytes)?;
                let (ns, bytes) = split_namespace(bytes)?;
                let (id, payload) = split_id(bytes)?;
                Ns(ns, Packet::BinaryEvent { payload, id, count })
            }
            '6' => {
                let (count, bytes) = split_binary_attachments(bytes)?;
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

fn split_binary_attachments(bytes: ByteString) -> Result<(usize, ByteString), PacketError> {
    let (count, bytes) = split_attachments(bytes)?;
    let count = count.ok_or(PacketError::MissingAttachmentCount)?;
    if !(1..=crate::binary::MAX_ATTACHMENTS).contains(&count) {
        return Err(PacketError::AttachmentCount { count });
    }
    Ok((count, bytes))
}

const U64_MAX_LEN: usize = 20; // max decimal digits in a u64

const fn ack_size_hint() -> usize {
    U64_MAX_LEN
}

const fn binary_size_hint() -> usize {
    U64_MAX_LEN + 1
}

/// Returns the encoded byte length of a namespace field (`0` for the default
/// `"/"`).
fn namespace_size(ns: &str) -> usize {
    if ns == "/" { 0 } else { ns.len() + 1 }
}

fn hint_packet_size(ns: &str, binary: bool, ack: bool, payload: Option<&str>) -> usize {
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

fn write_packet(
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
fn split_attachments(bytes: ByteString) -> Result<(Option<usize>, ByteString), PacketError> {
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
/// Returns an error if the namespace is present but missing its trailing `,`
/// delimiter.
fn split_namespace(bytes: ByteString) -> Result<(ByteString, ByteString), PacketError> {
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
fn split_id(bytes: ByteString) -> Result<(Option<u64>, ByteString), PacketError> {
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

    fn bss(s: &'static str) -> ByteString {
        ByteString::from_static(s)
    }

    fn decode(s: &'static str) -> Result<Ns<Packet>, PacketError> {
        bss(s).try_into()
    }

    fn encode_ns(ns: &'static str, pkt: &Packet) -> (String, usize) {
        let hint = pkt.size_hint(ns);
        (pkt.encode(ns), hint)
    }

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
    fn binary_packets_reject_attachment_counts_outside_the_limit() {
        for payload in [r#"50-["x"]"#, "60-1[]", r#"511-["x"]"#, "611-1[]"] {
            assert!(
                matches!(
                    Ns::<Packet>::try_from(bss(payload)),
                    Err(PacketError::AttachmentCount { .. })
                ),
                "{payload}"
            );
        }
        for payload in [r#"510-["x"]"#, "610-1[]"] {
            Ns::<Packet>::try_from(bss(payload)).unwrap();
        }
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

    #[test]
    fn decode_empty_is_error() {
        assert!(matches!(decode(""), Err(PacketError::Empty)));
    }

    #[test]
    fn decode_invalid_id_is_error() {
        assert!(matches!(
            decode("9"),
            Err(PacketError::InvalidId { id: '9' })
        ));
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

    #[test]
    fn encode_connect_default_ns() {
        let (enc, hint) = encode_ns("/", &Packet::Connect("{}".into()));
        assert_eq!(enc, "0{}");
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_connect_non_default_ns() {
        let (enc, hint) = encode_ns("/chat", &Packet::Connect("{}".into()));
        assert_eq!(enc, "0/chat,{}");
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_disconnect() {
        let (enc, hint) = encode_ns("/", &Packet::Disconnect);
        assert_eq!(enc, "1");
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_event_no_id() {
        let (enc, hint) = encode_ns(
            "/",
            &Packet::Event {
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
            &Packet::Event {
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
            &Packet::Ack {
                payload: r#"["ok"]"#.into(),
                id: 7,
            },
        );
        assert_eq!(enc, r#"37["ok"]"#);
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_connect_error() {
        let (enc, hint) = encode_ns("/", &Packet::ConnectError(r#"{"message":"bad"}"#.into()));
        assert_eq!(enc, r#"4{"message":"bad"}"#);
        assert!(enc.len() <= hint);
    }

    #[test]
    fn encode_binary_event() {
        let (enc, hint) = encode_ns(
            "/",
            &Packet::BinaryEvent {
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
            &Packet::BinaryAck {
                payload: r#"["ok"]"#.into(),
                id: 7,
                count: 2,
            },
        );
        assert_eq!(enc, r#"62-7["ok"]"#);
        assert!(enc.len() <= hint);
    }

    #[test]
    fn packet_display_variants() {
        assert_eq!(format!("{}", Packet::Disconnect), "Disconnect");
        assert_eq!(format!("{}", Packet::Connect("{}".into())), "Connect({})");
        assert_eq!(
            format!(
                "{}",
                Packet::Event {
                    payload: "[]".into(),
                    id: None
                }
            ),
            "Event { payload: [] }"
        );
        assert_eq!(
            format!(
                "{}",
                Packet::Event {
                    payload: "[]".into(),
                    id: Some(3)
                }
            ),
            "Event { payload: [], id: 3 }"
        );
        assert_eq!(
            format!(
                "{}",
                Packet::Ack {
                    payload: "[]".into(),
                    id: 5
                }
            ),
            "Ack { payload: [], id: 5 }"
        );
        assert_eq!(
            format!("{}", Packet::ConnectError("{}".into())),
            "ConnectError({})"
        );
        assert_eq!(
            format!(
                "{}",
                Packet::BinaryEvent {
                    payload: "[]".into(),
                    id: None,
                    count: 2
                }
            ),
            "BinaryEvent { payload: [], count: 2 }"
        );
        assert_eq!(
            format!(
                "{}",
                Packet::BinaryEvent {
                    payload: "[]".into(),
                    id: Some(3),
                    count: 2
                }
            ),
            "BinaryEvent { payload: [], id: 3, count: 2 }"
        );
        assert_eq!(
            format!(
                "{}",
                Packet::BinaryAck {
                    payload: "[]".into(),
                    id: 5,
                    count: 2
                }
            ),
            "BinaryAck { payload: [], id: 5, count: 2 }"
        );
    }

    #[test]
    fn decode_binary_ack_missing_count_is_error() {
        assert!(matches!(
            decode(r#"6["ok"]"#),
            Err(PacketError::MissingAttachmentCount)
        ));
    }

    #[test]
    fn decode_binary_ack_missing_id_is_error() {
        assert!(matches!(
            decode(r#"61-["ok"]"#),
            Err(PacketError::MissingAckId)
        ));
    }
}
