//! Text decoding, like socket.io-parser's `Decoder.decodeString`.
//! Prefix validation and packet dispatch stay together so each field is
//! checked at the point the decoder consumes it.

use bytestring::ByteString;

use crate::error::PacketError;
use crate::parser::packet::{Ns, Packet};

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
}
