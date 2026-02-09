//! Wire-level Socket.IO packet parsing and serialisation helpers.
//!
//! Provides free functions for splitting inbound byte frames into their
//! constituent fields ([`split_namespace`], [`split_id`],
//! [`split_attachments`]) and for writing outbound fields into a
//! [`bytes::BytesMut`] buffer (`write_namespace`, `write_id`,
//! `write_data`, `write_attachments`).
//!
//! Also implements [`TryFrom<Bytes>`] for [`Ns<SioPacket>`], which is the
//! primary inbound decoding entry-point.

use crate::error::{ParseError, PayloadError, Result};
use crate::packet::{Connect, ConnectError, Ns, SioPacket};
use bytes::{Buf, BufMut, Bytes};

impl TryFrom<Bytes> for Ns<SioPacket> {
    type Error = ParseError;

    fn try_from(mut data: Bytes) -> Result<Self, ParseError> {
        let first = data.first().cloned().ok_or(ParseError::EmptyPacket)?;
        data.advance(1);

        let packet = match first {
            b'0' => {
                let ns = split_namespace(&mut data)?;
                let inner: Connect = serde_json::from_slice(&data)
                    .map_err(|e| PayloadError::new::<Connect>(e).with_slice(&data))?;
                Ns(ns, SioPacket::Connect(inner))
            }
            b'1' => {
                let ns = split_namespace(&mut data)?;
                Ns(ns, SioPacket::Disconnect)
            }
            b'2' => {
                if let Some(count) = split_attachments(&mut data).transpose()? {
                    return Err(ParseError::UnexpectedAttachments { count });
                }
                let ns = split_namespace(&mut data)?;
                let id = split_id(&mut data).transpose()?;
                Ns(
                    ns,
                    SioPacket::Event {
                        data,
                        id,
                        count: None,
                    },
                )
            }
            b'3' => {
                let ns = split_namespace(&mut data)?;
                let id = split_id(&mut data)
                    .transpose()?
                    .ok_or(ParseError::MissingAckId)?;
                Ns(
                    ns,
                    SioPacket::Ack {
                        data,
                        id,
                        count: None,
                    },
                )
            }
            b'4' => {
                let ns = split_namespace(&mut data)?;
                let inner: ConnectError = serde_json::from_slice(&data)
                    .map_err(|e| PayloadError::new::<ConnectError>(e).with_slice(&data))?;
                Ns(ns, SioPacket::ConnectError(inner))
            }
            b'5' => {
                let count = split_attachments(&mut data)
                    .transpose()?
                    .ok_or(ParseError::MissingAttachmentCount)?;
                let ns = split_namespace(&mut data)?;
                let id = split_id(&mut data).transpose()?;
                Ns(
                    ns,
                    SioPacket::Event {
                        data,
                        id,
                        count: Some(count),
                    },
                )
            }
            b'6' => {
                let count = split_attachments(&mut data)
                    .transpose()?
                    .ok_or(ParseError::MissingAttachmentCount)?;
                let ns = split_namespace(&mut data)?;
                let id = split_id(&mut data)
                    .transpose()?
                    .ok_or(ParseError::MissingAckId)?;
                Ns(
                    ns,
                    SioPacket::Ack {
                        data,
                        id,
                        count: Some(count),
                    },
                )
            }
            byte => return Err(ParseError::UnknownPacketType { byte }),
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

/// Writes `<count>-` into `buffer`.
fn write_attachments(buffer: &mut impl BufMut, count: usize) {
    buffer.put_slice(count.to_string().as_bytes());
    buffer.put_u8(b'-');
}

/// Writes the namespace followed by `,` — skipped for the default `"/"`.
fn write_namespace(buffer: &mut impl BufMut, ns: &str) {
    if ns != "/" {
        buffer.put(ns.as_bytes());
        buffer.put_u8(b',');
    }
}

/// Writes a decimal ack ID.
fn write_id(buffer: &mut impl BufMut, id: u64) {
    buffer.put_slice(id.to_string().as_bytes());
}

fn write_data(buffer: &mut impl BufMut, data: &[u8]) {
    buffer.put_slice(data);
}

pub fn packet_size_hint(ns: &str, binary: bool, ack: bool, data: Option<&[u8]>) -> usize {
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

pub fn write_packet(
    buffer: &mut impl BufMut,
    type_id: u8,
    count: Option<usize>,
    ns: &str,
    id: Option<u64>,
    data: Option<&[u8]>,
) {
    buffer.put_u8(type_id);
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

/// Consumes a `<n>-` attachment count prefix from the front of `data`.
///
/// Only matches when the data starts with one or more ASCII digits
/// immediately followed by `-`.  This avoids misinterpreting a namespace
/// that contains a hyphen (e.g. `/admin-ns,["data"]`) as an attachment
/// count.
///
/// Returns `Some(Ok(count))` when the prefix is present and valid.
pub fn split_attachments(data: &mut Bytes) -> Option<Result<usize, ParseError>> {
    let digit_len = data.iter().take_while(|b| b.is_ascii_digit()).count();
    if digit_len == 0 {
        return None;
    }
    if data.get(digit_len) != Some(&b'-') {
        return None;
    }
    let count = match std::str::from_utf8(&data[..digit_len]) {
        Ok(s) => match s.parse() {
            Ok(n) => n,
            Err(_) => return Some(Err(ParseError::InvalidAttachmentCount)),
        },
        Err(e) => return Some(Err(e.into())),
    };
    data.advance(digit_len + 1);
    Some(Ok(count))
}

/// Consumes a `/ns,` prefix and returns the namespace string.
///
/// Returns `"/"` for the default namespace.
pub fn split_namespace(data: &mut Bytes) -> Result<String, ParseError> {
    match data.first() {
        Some(&b'/') => match data.iter().position(|&b| b == b',') {
            Some(i) => {
                let ns = std::str::from_utf8(&data[..i])?.to_string();
                data.advance(i + 1);
                Ok(ns)
            }
            None => Err(ParseError::MissingNamespaceDelimiter),
        },
        _ => Ok("/".to_string()),
    }
}

/// Consumes a run of leading ASCII digits from `data` as a `u64`.
///
/// Returns `Some(Ok(id))` when digits are present and fit in `u64`.
pub fn split_id(data: &mut Bytes) -> Option<Result<u64, ParseError>> {
    let i = match data.iter().position(|b| !b.is_ascii_digit()) {
        Some(0) => return None,
        Some(i) => i,
        None if data.is_empty() => return None,
        None => data.len(),
    };

    let id = parse_id(&data[..i]);
    data.advance(i);
    Some(id)
}

fn parse_id(data: &[u8]) -> Result<u64, ParseError> {
    Ok(std::str::from_utf8(data)?.parse()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn split_namespace_default() {
        let mut data = Bytes::from_static(b"hello");
        let ns = split_namespace(&mut data).unwrap();
        assert_eq!(ns, "/");
        assert_eq!(&data[..], b"hello");
    }

    #[test]
    fn split_namespace_custom() {
        let mut data = Bytes::from_static(b"/chat,payload");
        let ns = split_namespace(&mut data).unwrap();
        assert_eq!(ns, "/chat");
        assert_eq!(&data[..], b"payload");
    }

    #[test]
    fn split_namespace_missing_delimiter() {
        let mut data = Bytes::from_static(b"/chat");
        let err = split_namespace(&mut data).unwrap_err();
        assert!(matches!(err, ParseError::MissingNamespaceDelimiter));
    }

    #[test]
    fn split_id_digits() {
        let mut data = Bytes::from_static(b"42[\"hello\"]");
        let id = split_id(&mut data).unwrap().unwrap();
        assert_eq!(id, 42);
        assert_eq!(&data[..], b"[\"hello\"]");
    }

    #[test]
    fn split_id_no_digits() {
        let mut data = Bytes::from_static(b"[\"hello\"]");
        assert!(split_id(&mut data).is_none());
    }

    #[test]
    fn split_id_overflow() {
        // u64::MAX + 1
        let mut data = Bytes::from_static(b"99999999999999999999999[\"hello\"]");
        let result = split_id(&mut data).unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn split_id_empty() {
        let mut data = Bytes::new();
        assert!(split_id(&mut data).is_none());
    }

    #[test]
    fn split_attachments_valid() {
        let mut data = Bytes::from_static(b"3-payload");
        let count = split_attachments(&mut data).unwrap().unwrap();
        assert_eq!(count, 3);
        assert_eq!(&data[..], b"payload");
    }

    #[test]
    fn split_attachments_missing() {
        let mut data = Bytes::from_static(b"payload");
        assert!(split_attachments(&mut data).is_none());
    }

    #[test]
    fn split_attachments_non_digit_prefix() {
        let mut data = Bytes::from_static(b"abc-payload");
        assert!(split_attachments(&mut data).is_none());
        assert_eq!(&data[..], b"abc-payload");
    }

    #[test]
    fn split_attachments_namespace_with_hyphen() {
        let mut data = Bytes::from_static(b"/admin-ns,[\"data\"]");
        assert!(split_attachments(&mut data).is_none());
        assert_eq!(&data[..], b"/admin-ns,[\"data\"]");
    }

    #[test]
    fn split_attachments_digits_without_dash() {
        let mut data = Bytes::from_static(b"42[\"data\"]");
        assert!(split_attachments(&mut data).is_none());
        assert_eq!(&data[..], b"42[\"data\"]");
    }

    #[test]
    fn parse_connect_packet() {
        let data = Bytes::from_static(b"0{\"sid\":\"abc\"}");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        match ns_packet.1 {
            SioPacket::Connect(c) => {
                assert_eq!(c.sid, "abc");
                assert!(c.extra.is_empty());
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn parse_connect_packet_with_extra_fields() {
        let data = Bytes::from(r#"0{"sid":"abc","pid":"xyz","offset":"val"}"#);
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        match ns_packet.1 {
            SioPacket::Connect(c) => {
                assert_eq!(c.sid, "abc");
                assert_eq!(c.extra["pid"], "xyz");
                assert_eq!(c.extra["offset"], "val");
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn parse_disconnect_packet() {
        let data = Bytes::from_static(b"1");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        assert!(matches!(ns_packet.1, SioPacket::Disconnect));
    }

    #[test]
    fn parse_event_packet() {
        let data = Bytes::from_static(b"2[\"hello\",\"world\"]");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/");
        match ns_packet.1 {
            SioPacket::Event { data, id, count } => {
                assert_eq!(&data[..], b"[\"hello\",\"world\"]");
                assert!(id.is_none());
                assert!(count.is_none());
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn parse_event_with_ack_id() {
        let data = Bytes::from_static(b"242[\"hello\"]");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        match ns_packet.1 {
            SioPacket::Event { id, count, .. } => {
                assert_eq!(id, Some(42));
                assert!(count.is_none());
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn parse_ack_packet() {
        let data = Bytes::from_static(b"37[\"ok\"]");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        match ns_packet.1 {
            SioPacket::Ack { id, data, count } => {
                assert_eq!(id, 7);
                assert_eq!(&data[..], b"[\"ok\"]");
                assert!(count.is_none());
            }
            _ => panic!("expected Ack"),
        }
    }

    #[test]
    fn parse_connect_error_packet() {
        let data = Bytes::from_static(b"4{\"message\":\"forbidden\"}");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        match ns_packet.1 {
            SioPacket::ConnectError(e) => {
                assert_eq!(e.message, "forbidden");
                assert!(e.extra.is_empty());
            }
            _ => panic!("expected ConnectError"),
        }
    }

    #[test]
    fn parse_connect_error_with_data() {
        let data = Bytes::from(r#"4{"message":"bad","data":{"code":401}}"#);
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        match ns_packet.1 {
            SioPacket::ConnectError(e) => {
                assert_eq!(e.message, "bad");
                assert_eq!(e.extra["data"]["code"], 401);
            }
            _ => panic!("expected ConnectError"),
        }
    }

    #[test]
    fn parse_binary_event_packet() {
        let data = Bytes::from_static(b"52-[\"bin\"]");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        match ns_packet.1 {
            SioPacket::Event { count, id, .. } => {
                assert_eq!(count, Some(2));
                assert!(id.is_none());
            }
            _ => panic!("expected Event (binary)"),
        }
    }

    #[test]
    fn parse_binary_ack_packet() {
        let data = Bytes::from_static(b"61-5[\"ok\"]");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        match ns_packet.1 {
            SioPacket::Ack { count, id, .. } => {
                assert_eq!(count, Some(1));
                assert_eq!(id, 5);
            }
            _ => panic!("expected Ack (binary)"),
        }
    }

    #[test]
    fn parse_unknown_packet_type() {
        let data = Bytes::from_static(b"9");
        let err: std::result::Result<Ns<SioPacket>, _> = data.try_into();
        assert!(matches!(
            err.unwrap_err(),
            ParseError::UnknownPacketType { byte: b'9' }
        ));
    }

    #[test]
    fn parse_empty_packet() {
        let data = Bytes::new();
        let err: std::result::Result<Ns<SioPacket>, _> = data.try_into();
        assert!(matches!(err.unwrap_err(), ParseError::EmptyPacket));
    }

    #[test]
    fn parse_event_with_custom_namespace() {
        let data = Bytes::from_static(b"2/chat,[\"msg\"]");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/chat");
        assert!(matches!(ns_packet.1, SioPacket::Event { .. }));
    }

    #[test]
    fn parse_ack_missing_id() {
        let data = Bytes::from_static(b"3[\"ok\"]");
        let err: std::result::Result<Ns<SioPacket>, _> = data.try_into();
        assert!(matches!(err.unwrap_err(), ParseError::MissingAckId));
    }

    #[test]
    fn parse_text_event_rejects_attachment_prefix() {
        let data = Bytes::from_static(b"21-[\"oops\"]");
        let err: std::result::Result<Ns<SioPacket>, _> = data.try_into();
        assert!(matches!(
            err.unwrap_err(),
            ParseError::UnexpectedAttachments { count: 1 }
        ));
    }

    #[test]
    fn parse_event_with_hyphenated_namespace() {
        let data = Bytes::from_static(b"2/admin-ns,[\"msg\"]");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/admin-ns");
        match ns_packet.1 {
            SioPacket::Event { data, id, count } => {
                assert_eq!(&data[..], b"[\"msg\"]");
                assert!(id.is_none());
                assert!(count.is_none());
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn parse_binary_event_with_hyphenated_namespace() {
        let data = Bytes::from_static(b"51-/admin-ns,[\"bin\"]");
        let ns_packet: Ns<SioPacket> = data.try_into().unwrap();
        assert_eq!(ns_packet.0, "/admin-ns");
        match ns_packet.1 {
            SioPacket::Event { count, .. } => {
                assert_eq!(count, Some(1));
            }
            _ => panic!("expected binary Event"),
        }
    }

    #[test]
    fn parse_binary_event_missing_attachment_count() {
        let data = Bytes::from_static(b"5[\"oops\"]");
        let err: std::result::Result<Ns<SioPacket>, _> = data.try_into();
        assert!(matches!(
            err.unwrap_err(),
            ParseError::MissingAttachmentCount
        ));
    }

    #[test]
    fn parse_binary_ack_missing_attachment_count() {
        let data = Bytes::from_static(b"65[\"ok\"]");
        let err: std::result::Result<Ns<SioPacket>, _> = data.try_into();
        assert!(matches!(
            err.unwrap_err(),
            ParseError::MissingAttachmentCount
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
        let mut buf = BytesMut::new();
        write_namespace(&mut buf, "/");
        assert!(buf.is_empty());
    }

    #[test]
    fn write_namespace_custom_appends_comma() {
        let mut buf = BytesMut::new();
        write_namespace(&mut buf, "/chat");
        assert_eq!(&buf[..], b"/chat,");
    }

    #[test]
    fn write_id_formats_decimal() {
        let mut buf = BytesMut::new();
        write_id(&mut buf, 42);
        assert_eq!(&buf[..], b"42");
    }

    #[test]
    fn write_data_appends_bytes() {
        let mut buf = BytesMut::new();
        write_data(&mut buf, b"[\"hello\"]");
        assert_eq!(&buf[..], b"[\"hello\"]");
    }

    #[test]
    fn write_attachments_formats_count_dash() {
        let mut buf = BytesMut::new();
        write_attachments(&mut buf, 3);
        assert_eq!(&buf[..], b"3-");
    }
}
