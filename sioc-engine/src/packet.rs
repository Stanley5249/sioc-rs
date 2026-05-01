use std::time::Duration;

use crate::error::PacketError;
use bytes::Bytes;
use bytestring::ByteString;
use serde::Deserialize;

pub const PROBE: ByteString = ByteString::from_static("probe");

/// Content exchanged between the Socket.IO and Engine.IO layers.
#[derive(Clone, PartialEq, Eq)]
pub enum Message {
    /// A UTF-8 text payload.
    Text(ByteString),
    /// A raw binary payload.
    Binary(Bytes),
    /// Signals a clean connection close.
    Close,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text(bytes) => f.debug_tuple("Text").field(bytes).finish(),
            Self::Binary(bytes) => f.debug_struct("Binary").field("len", &bytes.len()).finish(),
            Self::Close => write!(f, "Close"),
        }
    }
}

/// A wire-level frame exchanged with the transport layer.
///
/// [`Frame::Packet`] carries a fully-encoded Engine.IO text packet (e.g. `"4hello"`).
/// [`Frame::Binary`] carries a raw binary payload with no packet-type prefix.
#[derive(Clone, PartialEq, Eq)]
pub enum Frame {
    /// A UTF-8 text frame (contains a complete Engine.IO packet).
    Packet(Packet),
    /// A raw binary frame.
    Binary(Bytes),
}

impl From<Packet> for Frame {
    fn from(packet: Packet) -> Self {
        Frame::Packet(packet)
    }
}

impl From<Bytes> for Frame {
    fn from(bytes: Bytes) -> Self {
        Frame::Binary(bytes)
    }
}

impl std::fmt::Debug for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Packet(packet) => f.debug_tuple("Packet").field(packet).finish(),
            Self::Binary(bytes) => f.debug_struct("Binary").field("len", &bytes.len()).finish(),
        }
    }
}

/// Data exchanged in the Engine.IO v4 handshake (the `open` packet payload).
///
/// # Wire format
///
/// ```json
/// {
///   "sid": "lv_VI97HAXpY6yYWAAAC",
///   "upgrades": ["websocket"],
///   "pingInterval": 25000,
///   "pingTimeout": 20000,
///   "maxPayload": 1000000
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Handshake {
    /// Session identifier; sent as the `sid` query parameter in all subsequent requests.
    pub sid: ByteString,

    /// Available transport upgrades (e.g. `["websocket"]`).
    pub upgrades: Vec<ByteString>,

    /// Server ping interval in milliseconds.
    #[serde(rename = "pingInterval")]
    pub ping_interval: u64,

    /// Server ping timeout in milliseconds.
    #[serde(rename = "pingTimeout")]
    pub ping_timeout: u64,

    /// Maximum bytes per chunk, used by the client to aggregate packets into payloads.
    #[serde(rename = "maxPayload")]
    pub max_payload: u64,
}

impl Handshake {
    pub fn can_upgrade_to_websocket(&self) -> bool {
        self.upgrades.iter().any(|u| u == "websocket")
    }

    /// Combined `pingInterval + pingTimeout` as a [`Duration`].
    pub fn ping_window(&self) -> Duration {
        Duration::from_millis(self.ping_interval + self.ping_timeout)
    }
}

/// A packet in the Engine.IO v4 protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    /// `0` — Handshake data from the server.
    Open(Handshake),
    /// `1` — The transport can be closed.
    Close,
    /// `2` — Heartbeat from the server (client must reply with [`Packet::Pong`]).
    Ping(ByteString),
    /// `3` — Heartbeat reply.
    Pong(ByteString),
    /// `4` — Application-level text data.
    Message(ByteString),
    /// `5` — Sent by the client to finalise a transport upgrade.
    Upgrade,
    /// `6` — Sent by the server to flush a pending long-poll GET during upgrade.
    Noop,
}

impl Packet {
    /// Write this packet into a [`String`].
    pub fn write(&self, buffer: &mut String) {
        match self {
            Self::Open(..) => panic!("client should never encode an Open packet"),
            Self::Close => buffer.push('1'),
            Self::Ping(data) => {
                buffer.reserve(1 + data.len());
                buffer.push('2');
                buffer.push_str(data);
            }
            Self::Pong(data) => {
                buffer.reserve(1 + data.len());
                buffer.push('3');
                buffer.push_str(data);
            }
            Self::Message(data) => {
                buffer.reserve(1 + data.len());
                buffer.push('4');
                buffer.push_str(data);
            }
            Self::Upgrade => buffer.push('5'),
            Self::Noop => panic!("client should never encode a Noop packet"),
        }
    }

    /// Encodes this packet into a [`String`].
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        self.write(&mut buffer);
        buffer
    }

    /// Decodes a single packet from a text frame.
    ///
    /// The first character is the packet-type digit (`'0'`..=`'6'`).
    pub fn decode(bytes: ByteString) -> Result<Self, PacketError> {
        let mut chars = bytes.chars();

        let id = match chars.next() {
            Some(c) => c,
            None => return Err(PacketError::Empty),
        };

        let rest = chars.as_str();

        match id {
            '0' => Ok(Packet::Open(serde_json::from_str(rest)?)),
            '1' => {
                if rest.is_empty() {
                    Ok(Packet::Close)
                } else {
                    Err(PacketError::Payload {
                        id: '1',
                        payload: bytes.slice_ref(rest),
                    })
                }
            }
            '2' => Ok(Packet::Ping(bytes.slice_ref(rest))),
            '3' => Ok(Packet::Pong(bytes.slice_ref(rest))),
            '4' => Ok(Packet::Message(bytes.slice_ref(rest))),
            '5' => {
                if rest.is_empty() {
                    Ok(Packet::Upgrade)
                } else {
                    Err(PacketError::Payload {
                        id: '5',
                        payload: bytes.slice_ref(rest),
                    })
                }
            }
            '6' => {
                if rest.is_empty() {
                    Ok(Packet::Noop)
                } else {
                    Err(PacketError::Payload {
                        id: '6',
                        payload: bytes.slice_ref(rest),
                    })
                }
            }
            id => Err(PacketError::InvalidId { id })?,
        }
    }
}

/// Converts an inbound WebSocket text frame into a [`ByteString`] without copying.
pub(crate) fn bytestring_from_utf8_bytes(
    utf8: tokio_tungstenite::tungstenite::Utf8Bytes,
) -> ByteString {
    // SAFETY: `tungstenite::Utf8Bytes` guarantees the inner `Bytes` is valid UTF-8.
    unsafe { ByteString::from_bytes_unchecked(utf8.into()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_handshake() -> Handshake {
        Handshake {
            sid: "abc".into(),
            upgrades: vec!["websocket".into()],
            ping_interval: 25000,
            ping_timeout: 5000,
            max_payload: 1_000_000,
        }
    }

    #[test]
    fn handshake_missing_required_field_is_error() {
        let json = serde_json::json!({
            "sid": "test-456",
            "upgrades": [],
            "pingInterval": 25000,
            "pingTimeout": 20000
        });
        let bytes = ByteString::from(format!("0{}", json));
        assert!(Packet::decode(bytes).is_err());
    }

    #[test]
    fn handshake_ping_window() {
        let hs = test_handshake();
        assert_eq!(
            hs.ping_window(),
            std::time::Duration::from_millis(25000 + 5000)
        );
    }

    #[test]
    fn decode_invalid_packet_id() {
        assert!(matches!(
            Packet::decode(ByteString::from_static("9")),
            Err(PacketError::InvalidId { id: '9' })
        ));
    }

    #[test]
    fn decode_empty_packet() {
        assert!(matches!(
            Packet::decode(ByteString::new()),
            Err(PacketError::Empty)
        ));
    }

    #[test]
    fn decode_open() {
        let hs = test_handshake();
        let json = r#"{"sid":"abc","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":5000,"maxPayload":1000000}"#;
        let bytes = ByteString::from(format!("0{json}"));
        assert_eq!(Packet::decode(bytes).unwrap(), Packet::Open(hs));
    }

    #[test]
    fn encode_decode_close_round_trip() {
        let encoded = Packet::Close.encode();
        assert_eq!(
            Packet::decode(ByteString::from(encoded)).unwrap(),
            Packet::Close
        );
    }

    #[test]
    fn encode_decode_ping_round_trip() {
        let payload = ByteString::from_static("probe");
        let encoded = Packet::Ping(payload.clone()).encode();
        assert_eq!(
            Packet::decode(ByteString::from(encoded)).unwrap(),
            Packet::Ping(payload)
        );
    }

    #[test]
    fn encode_decode_ping_empty_payload() {
        let encoded = Packet::Ping(ByteString::new()).encode();
        assert_eq!(
            Packet::decode(ByteString::from(encoded)).unwrap(),
            Packet::Ping(ByteString::new())
        );
    }

    #[test]
    fn encode_decode_pong_round_trip() {
        let payload = ByteString::from_static("probe");
        let encoded = Packet::Pong(payload.clone()).encode();
        assert_eq!(
            Packet::decode(ByteString::from(encoded)).unwrap(),
            Packet::Pong(payload)
        );
    }

    #[test]
    fn encode_decode_message_round_trip() {
        let text = ByteString::from_static("hello world");
        let encoded = Packet::Message(text.clone()).encode();
        assert_eq!(
            Packet::decode(ByteString::from(encoded)).unwrap(),
            Packet::Message(text)
        );
    }

    #[test]
    fn encode_decode_upgrade_round_trip() {
        let encoded = Packet::Upgrade.encode();
        assert_eq!(
            Packet::decode(ByteString::from(encoded)).unwrap(),
            Packet::Upgrade
        );
    }

    #[test]
    fn decode_noop() {
        assert_eq!(
            Packet::decode(ByteString::from_static("6")).unwrap(),
            Packet::Noop
        );
    }
}
