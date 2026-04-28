//! Wire-level Socket.IO packet parsing and serialisation helpers.
//!
//! Provides free functions for splitting inbound byte frames into their
//! constituent fields ([`split_namespace`], [`split_id`],
//! [`split_attachments`]) and for writing outbound packets into a [`String`]
//! buffer ([`write_packet`]).
//!
//! Also implements [`TryFrom<ByteString>`] for [`Ns<Packet>`], which is the
//! primary inbound decoding entry-point.

use crate::error::PacketError;
use crate::packet::{Ns, Packet};
use bytestring::ByteString;

impl TryFrom<ByteString> for Ns<Packet> {
    type Error = PacketError;

    fn try_from(bytes: ByteString) -> Result<Self, PacketError> {
        let mut chars = bytes.chars();

        let id = chars.next().ok_or(PacketError::Empty)?;

        let bytes = bytes.slice_ref(chars.as_str());

        let packet = match id {
            '0' => {
                let (ns, data) = split_namespace(bytes)?;
                Ns(ns, Packet::Connect(data))
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
                let (id, data) = split_id(bytes)?;
                Ns(ns, Packet::Event { data, id })
            }
            '3' => {
                let (ns, bytes) = split_namespace(bytes)?;
                let (id, data) = split_id(bytes)?;
                let id = id.ok_or(PacketError::MissingAckId)?;
                Ns(ns, Packet::Ack { data, id })
            }
            '4' => {
                let (ns, data) = split_namespace(bytes)?;
                Ns(ns, Packet::ConnectError(data))
            }
            '5' => {
                let (count, bytes) = split_attachments(bytes)?;
                let count = count.ok_or(PacketError::MissingAttachmentCount)?;
                let (ns, bytes) = split_namespace(bytes)?;
                let (id, data) = split_id(bytes)?;
                Ns(ns, Packet::BinaryEvent { data, id, count })
            }
            '6' => {
                let (count, bytes) = split_attachments(bytes)?;
                let count = count.ok_or(PacketError::MissingAttachmentCount)?;
                let (ns, bytes) = split_namespace(bytes)?;
                let (id, data) = split_id(bytes)?;
                let id = id.ok_or(PacketError::MissingAckId)?;
                Ns(ns, Packet::BinaryAck { data, id, count })
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

pub fn hint_packet_size(ns: &str, binary: bool, ack: bool, data: Option<&str>) -> usize {
    let mut n = 1 + namespace_size(ns);
    if ack {
        n += ack_size_hint();
    }
    if binary {
        n += binary_size_hint();
    }
    if let Some(data) = data {
        n += data.len();
    }
    n
}

/// Writes `<count>-` into `buffer`.
fn write_attachments(buffer: &mut String, count: usize) {
    buffer.push_str(&count.to_string());
    buffer.push('-');
}

/// Writes the namespace followed by `,` — skipped for the default `"/"`.
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

fn write_data(buffer: &mut String, data: &str) {
    buffer.push_str(data);
}

pub fn write_packet(
    buffer: &mut String,
    type_id: u8,
    count: Option<usize>,
    ns: &str,
    id: Option<u64>,
    data: Option<&str>,
) {
    buffer.push(type_id as char);
    if let Some(count) = count {
        write_attachments(buffer, count);
    }
    write_namespace(buffer, ns);
    if let Some(id) = id {
        write_id(buffer, id);
    }
    if let Some(data) = data {
        write_data(buffer, data);
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
pub fn split_namespace(bytes: ByteString) -> Result<(ByteString, ByteString), PacketError> {
    match bytes.chars().next() {
        Some('/') => match bytes.split_once(',') {
            Some((ns, data)) => Ok((bytes.slice_ref(ns), bytes.slice_ref(data))),
            None => Err(PacketError::MissingNamespaceDelimiter),
        },
        _ => Ok((ByteString::from_static("/"), bytes)),
    }
}

/// Consumes a run of leading ASCII digits as a `u64`.
///
/// Returns `(Some(id), rest)` when digits are present and fit in `u64`,
/// `(None, bytes)` when no leading digits are found or the input is empty.
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

    #[test]
    fn split_namespace_default() {
        let data = ByteString::from_static("hello");
        let (ns, rest) = split_namespace(data).unwrap();
        assert_eq!(ns, "/");
        assert_eq!(&rest[..], "hello");
    }

    #[test]
    fn split_namespace_custom() {
        let data = ByteString::from_static("/chat,payload");
        let (ns, rest) = split_namespace(data).unwrap();
        assert_eq!(ns, "/chat");
        assert_eq!(&rest[..], "payload");
    }

    #[test]
    fn split_namespace_missing_delimiter() {
        let data = ByteString::from_static("/chat");
        let err = split_namespace(data).unwrap_err();
        assert!(matches!(err, PacketError::MissingNamespaceDelimiter));
    }

    #[test]
    fn split_id_digits() {
        let data = ByteString::from_static("42[\"hello\"]");
        let (id, rest) = split_id(data).unwrap();
        assert_eq!(id, Some(42));
        assert_eq!(&rest[..], "[\"hello\"]");
    }

    #[test]
    fn split_id_no_digits() {
        let data = ByteString::from_static("[\"hello\"]");
        let (id, rest) = split_id(data.clone()).unwrap();
        assert!(id.is_none());
        assert_eq!(rest, data);
    }

    #[test]
    fn split_id_overflow() {
        // u64::MAX + 1
        let data = ByteString::from_static("99999999999999999999999[\"hello\"]");
        assert!(split_id(data).is_err());
    }

    #[test]
    fn split_id_empty() {
        let data = ByteString::new();
        let (id, rest) = split_id(data).unwrap();
        assert!(id.is_none());
        assert!(rest.is_empty());
    }

    #[test]
    fn split_attachments_valid() {
        let data = ByteString::from_static("3-payload");
        let (count, rest) = split_attachments(data).unwrap();
        assert_eq!(count, Some(3));
        assert_eq!(&rest[..], "payload");
    }

    #[test]
    fn split_attachments_missing() {
        let data = ByteString::from_static("payload");
        let (count, rest) = split_attachments(data.clone()).unwrap();
        assert!(count.is_none());
        assert_eq!(rest, data);
    }

    #[test]
    fn split_attachments_non_digit_prefix() {
        let data = ByteString::from_static("abc-payload");
        let (count, rest) = split_attachments(data.clone()).unwrap();
        assert!(count.is_none());
        assert_eq!(rest, data);
    }

    #[test]
    fn split_attachments_namespace_with_hyphen() {
        let data = ByteString::from_static("/admin-ns,[\"data\"]");
        let (count, rest) = split_attachments(data.clone()).unwrap();
        assert!(count.is_none());
        assert_eq!(rest, data);
    }

    #[test]
    fn split_attachments_digits_without_dash() {
        let data = ByteString::from_static("42[\"data\"]");
        let (count, rest) = split_attachments(data.clone()).unwrap();
        assert!(count.is_none());
        assert_eq!(rest, data);
    }

    #[test]
    fn parse_connect_packet() {
        let data = ByteString::from_static("0");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        match ns_packet.1 {
            Packet::Connect(payload) => assert!(payload.is_empty()),
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn parse_connect_packet_with_payload() {
        let data = ByteString::from_static("0{\"sid\":\"abc\",\"token\":\"xyz\"}");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        match ns_packet.1 {
            Packet::Connect(payload) => {
                assert_eq!(payload, "{\"sid\":\"abc\",\"token\":\"xyz\"}");
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn parse_connect_packet_custom_namespace() {
        let data = ByteString::from_static("0/admin,");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/admin");
        assert!(matches!(ns_packet.1, Packet::Connect(_)));
    }

    #[test]
    fn parse_disconnect_packet() {
        let data = ByteString::from_static("1");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        assert!(matches!(ns_packet.1, Packet::Disconnect));
    }

    #[test]
    fn parse_event_packet() {
        let data = ByteString::from_static("2[\"hello\",\"world\"]");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        match ns_packet.1 {
            Packet::Event { data, id } => {
                assert_eq!(data, "[\"hello\",\"world\"]");
                assert!(id.is_none());
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn parse_event_with_ack_id() {
        let data = ByteString::from_static("242[\"hello\"]");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        match ns_packet.1 {
            Packet::Event { id, .. } => {
                assert_eq!(id, Some(42));
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn parse_ack_packet() {
        let data = ByteString::from_static("37[\"ok\"]");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        match ns_packet.1 {
            Packet::Ack { id, data } => {
                assert_eq!(id, 7);
                assert_eq!(data, "[\"ok\"]");
            }
            _ => panic!("expected Ack"),
        }
    }

    #[test]
    fn parse_connect_error_packet() {
        let data = ByteString::from_static("4{\"message\":\"forbidden\"}");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        match ns_packet.1 {
            Packet::ConnectError(payload) => {
                assert_eq!(payload, "{\"message\":\"forbidden\"}");
            }
            _ => panic!("expected ConnectError"),
        }
    }

    #[test]
    fn parse_connect_error_with_extra_fields() {
        let data = ByteString::from(r#"4{"message":"bad","data":{"code":401}}"#);
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        match ns_packet.1 {
            Packet::ConnectError(payload) => {
                assert!(payload.starts_with("{\"message\":"));
            }
            _ => panic!("expected ConnectError"),
        }
    }

    #[test]
    fn parse_binary_event_packet() {
        let data = ByteString::from_static("52-[\"bin\"]");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        match ns_packet.1 {
            Packet::BinaryEvent { count, id, .. } => {
                assert_eq!(count, 2);
                assert!(id.is_none());
            }
            _ => panic!("expected Event (binary)"),
        }
    }

    #[test]
    fn parse_binary_ack_packet() {
        let data = ByteString::from_static("61-5[\"ok\"]");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        match ns_packet.1 {
            Packet::BinaryAck { count, id, .. } => {
                assert_eq!(count, 1);
                assert_eq!(id, 5);
            }
            _ => panic!("expected Ack (binary)"),
        }
    }

    #[test]
    fn parse_unknown_packet_type() {
        let data = ByteString::from_static("9");
        let err: std::result::Result<Ns<Packet>, _> = data.try_into();
        assert!(matches!(
            err.unwrap_err(),
            PacketError::InvalidId { id: '9' }
        ));
    }

    #[test]
    fn parse_empty_packet() {
        let data = ByteString::new();
        let err: std::result::Result<Ns<Packet>, _> = data.try_into();
        assert!(matches!(err.unwrap_err(), PacketError::Empty));
    }

    #[test]
    fn parse_event_with_custom_namespace() {
        let data = ByteString::from_static("2/chat,[\"msg\"]");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/chat");
        assert!(matches!(ns_packet.1, Packet::Event { .. }));
    }

    #[test]
    fn parse_ack_missing_id() {
        let data = ByteString::from_static("3[\"ok\"]");
        let err: std::result::Result<Ns<Packet>, _> = data.try_into();
        assert!(matches!(err.unwrap_err(), PacketError::MissingAckId));
    }

    #[test]
    fn parse_text_event_rejects_attachment_prefix() {
        let data = ByteString::from_static("21-[\"oops\"]");
        let err: std::result::Result<Ns<Packet>, _> = data.try_into();
        assert!(matches!(
            err.unwrap_err(),
            PacketError::UnexpectedAttachments { count: 1 }
        ));
    }

    #[test]
    fn parse_event_with_hyphenated_namespace() {
        let data = ByteString::from_static("2/admin-ns,[\"msg\"]");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/admin-ns");
        match ns_packet.1 {
            Packet::Event { data, id } => {
                assert_eq!(data, "[\"msg\"]");
                assert!(id.is_none());
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn parse_binary_event_with_hyphenated_namespace() {
        let data = ByteString::from_static("51-/admin-ns,[\"bin\"]");
        let ns_packet: Ns<Packet> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/admin-ns");
        match ns_packet.1 {
            Packet::BinaryEvent { count, .. } => {
                assert_eq!(count, 1);
            }
            _ => panic!("expected binary Event"),
        }
    }

    #[test]
    fn parse_binary_event_missing_attachment_count() {
        let data = ByteString::from_static("5[\"oops\"]");
        let err: std::result::Result<Ns<Packet>, _> = data.try_into();
        assert!(matches!(
            err.unwrap_err(),
            PacketError::MissingAttachmentCount
        ));
    }

    #[test]
    fn parse_binary_ack_missing_attachment_count() {
        let data = ByteString::from_static("65[\"ok\"]");
        let err: std::result::Result<Ns<Packet>, _> = data.try_into();
        assert!(matches!(
            err.unwrap_err(),
            PacketError::MissingAttachmentCount
        ));
    }

    #[test]
    fn namespace_size_default() {
        assert_eq!(namespace_size("/"), 0);
    }

    #[test]
    fn namespace_size_custom() {
        // "/chat" is 5 chars + 1 comma = 6
        assert_eq!(namespace_size("/chat"), 6);
    }

    #[test]
    fn write_namespace_default_is_noop() {
        let mut buffer = String::new();
        write_namespace(&mut buffer, "/");
        assert!(buffer.is_empty());
    }

    #[test]
    fn write_namespace_custom_appends_comma() {
        let mut buffer = String::new();
        write_namespace(&mut buffer, "/chat");
        assert_eq!(buffer, "/chat,");
    }

    #[test]
    fn write_id_formats_decimal() {
        let mut buffer = String::new();
        write_id(&mut buffer, 42);
        assert_eq!(buffer, "42");
    }

    #[test]
    fn write_data_appends_bytes() {
        let mut buffer = String::new();
        write_data(&mut buffer, "[\"hello\"]");
        assert_eq!(buffer, "[\"hello\"]");
    }

    #[test]
    fn write_attachments_formats_count_dash() {
        let mut buffer = String::new();
        write_attachments(&mut buffer, 3);
        assert_eq!(buffer, "3-");
    }
}
