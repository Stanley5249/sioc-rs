//! Text framing, like socket.io-parser's `Encoder.encodeAsString`.

use crate::parser::packet::Packet;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_ns(ns: &'static str, pkt: &Packet) -> (String, usize) {
        let hint = pkt.size_hint(ns);
        (pkt.encode(ns), hint)
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
}
