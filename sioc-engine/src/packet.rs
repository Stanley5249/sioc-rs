use std::time::Duration;

use crate::error::{Error, PacketError, PayloadError, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Deserialize;

pub const PROBE: Bytes = Bytes::from_static(b"probe");

/// Content exchanged between the Socket.IO and Engine.IO layers.
#[derive(Clone, PartialEq, Eq)]
pub enum Message {
    /// A UTF-8 text payload.
    Text(Bytes),
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
/// `Text` carries a fully-encoded Engine.IO text packet (e.g. `b"4hello"`).
/// `Binary` carries a raw binary payload with no packet-type prefix.
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

impl Frame {
    pub fn unexpected(self, description: impl Into<String>) -> Error {
        Error::UnexpectedFrame {
            description: description.into(),
            frame: self,
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
    pub sid: String,

    /// Available transport upgrades (e.g. `["websocket"]`).
    pub upgrades: Vec<String>,

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

    /// Combined `pingInterval + pingTimeout` as a [`Duration`](std::time::Duration).
    pub fn ping_window(&self) -> Duration {
        Duration::from_millis(self.ping_interval + self.ping_timeout)
    }
}

fn encode_packet(prefix: u8, data: &[u8]) -> Bytes {
    let mut buffer = BytesMut::with_capacity(1 + data.len());
    buffer.put_u8(prefix);
    buffer.put_slice(data);
    buffer.freeze()
}

/// A packet in the Engine.IO v4 protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    /// `0` — Handshake data from the server.
    Open(Handshake),
    /// `1` — The transport can be closed.
    Close,
    /// `2` — Heartbeat from the server (client must reply with [`EioPacket::Pong`]).
    Ping(Bytes),
    /// `3` — Heartbeat reply.
    Pong(Bytes),
    /// `4` — Application-level text data.
    Message(Bytes),
    /// `5` — Sent by the client to finalise a transport upgrade.
    Upgrade,
    /// `6` — Sent by the server to flush a pending long-poll GET during upgrade.
    Noop,
}

impl Packet {
    /// Encodes this packet into a [`Frame`] for transport.
    pub fn encode(&self) -> Bytes {
        match self {
            Self::Open(..) => panic!("client should never encode an Open packet"),
            Self::Close => Bytes::from_static(b"1"),
            Self::Ping(data) => encode_packet(b'2', data),
            Self::Pong(data) => encode_packet(b'3', data),
            Self::Message(data) => encode_packet(b'4', data),
            Self::Upgrade => Bytes::from_static(b"5"),
            Self::Noop => panic!("client should never encode a Noop packet"),
        }
    }

    /// Decodes a single packet from a text frame.
    ///
    /// The first byte is the packet-type digit (`b'0'`..=`b'6'`).
    pub fn decode(mut data: Bytes) -> Result<Self, PacketError> {
        if data.is_empty() {
            Err(PacketError::Empty)?;
        }

        match data.get_u8() {
            b'0' => {
                let handshake = serde_json::from_slice(&data)
                    .map_err(|e| PayloadError::new::<Handshake>(e).with_slice(&data))?;

                Ok(Packet::Open(handshake))
            }
            b'1' => Ok(Packet::Close),
            b'2' => Ok(Packet::Ping(data)),
            b'3' => Ok(Packet::Pong(data)),
            b'4' => Ok(Packet::Message(data)),
            b'5' => Ok(Packet::Upgrade),
            b'6' => Ok(Packet::Noop),
            id => Err(PacketError::InvalidId { id })?,
        }
    }

    pub fn unexpected(self, description: impl Into<String>) -> Error {
        Error::UnexpectedPacket {
            description: description.into(),
            packet: self,
        }
    }
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
        let bytes = Bytes::from(format!("0{}", json));
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
            Packet::decode(Bytes::from_static(b"9")),
            Err(crate::error::PacketError::InvalidId { id: b'9' })
        ));
    }

    #[test]
    fn decode_empty_packet() {
        assert!(matches!(
            Packet::decode(Bytes::new()),
            Err(crate::error::PacketError::Empty)
        ));
    }

    #[test]
    fn encode_decode_open_round_trip() {
        let hs = test_handshake();
        let Frame::Packet(packet) = Packet::Open(hs.clone()).into() else {
            panic!("expected Packet frame");
        };
        let Packet::Open(decoded) = packet else {
            panic!("expected Open");
        };
        assert_eq!(decoded, hs);
    }

    #[test]
    fn encode_decode_close_round_trip() {
        let Frame::Packet(packet) = Packet::Close.into() else {
            panic!("expected Packet frame");
        };
        assert!(matches!(packet, Packet::Close));
    }

    #[test]
    fn encode_decode_ping_round_trip() {
        let payload = Bytes::from_static(b"probe");
        let Frame::Packet(packet) = Packet::Ping(payload.clone()).into() else {
            panic!("expected Packet frame");
        };
        let Packet::Ping(decoded) = packet else {
            panic!("expected Ping");
        };
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_decode_pong_round_trip() {
        let payload = Bytes::from_static(b"probe");
        let Frame::Packet(packet) = Packet::Pong(payload.clone()).into() else {
            panic!("expected Packet frame");
        };
        let Packet::Pong(decoded) = packet else {
            panic!("expected Pong");
        };
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_decode_text_message_round_trip() {
        let text = Bytes::from_static(b"hello world");
        let Frame::Packet(packet) = Packet::Message(text.clone()).into() else {
            panic!("expected Packet frame");
        };
        let Packet::Message(decoded) = packet else {
            panic!("expected Message");
        };
        assert_eq!(decoded, text);
    }

    #[test]
    fn encode_decode_upgrade_round_trip() {
        let Frame::Packet(packet) = Packet::Upgrade.into() else {
            panic!("expected Packet frame");
        };
        assert!(matches!(packet, Packet::Upgrade));
    }

    #[test]
    fn encode_decode_noop_round_trip() {
        let Frame::Packet(packet) = Packet::Noop.into() else {
            panic!("expected Packet frame");
        };
        assert!(matches!(packet, Packet::Noop));
    }

    #[test]
    fn encode_ping_empty_payload() {
        let Frame::Packet(packet) = Packet::Ping(Bytes::new()).into() else {
            panic!("expected Packet frame");
        };
        let Packet::Ping(decoded) = packet else {
            panic!("expected Ping");
        };
        assert!(decoded.is_empty());
    }
}
