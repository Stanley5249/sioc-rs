use crate::error::{Error, PacketError, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

pub const PROBE: Bytes = Bytes::from_static(b"probe");

fn encode_prefixed(prefix: u8, data: &[u8]) -> Bytes {
    let mut buffer = BytesMut::with_capacity(1 + data.len());
    buffer.put_u8(prefix);
    buffer.extend_from_slice(data);
    buffer.freeze()
}

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
    Packet(EioPacket),
    /// A raw binary frame.
    Binary(Bytes),
}

impl From<EioPacket> for Frame {
    fn from(packet: EioPacket) -> Self {
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

impl From<Frame> for Bytes {
    fn from(frame: Frame) -> Self {
        match frame {
            Frame::Packet(packet) => packet.encode(),
            Frame::Binary(bytes) => bytes,
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
    pub fn ping_window(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.ping_interval + self.ping_timeout)
    }
}

/// A packet in the Engine.IO v4 protocol.
///
/// | Type      | ID  | Direction           |
/// |-----------|-----|---------------------|
/// | `Open`    | `0` | server → client     |
/// | `Close`   | `1` | both                |
/// | `Ping`    | `2` | server → client     |
/// | `Pong`    | `3` | client → server     |
/// | `Message` | `4` | both                |
/// | `Upgrade` | `5` | client → server     |
/// | `Noop`    | `6` | server → client     |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EioPacket {
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

impl EioPacket {
    /// Encodes this packet into a [`Frame`] for transport.
    ///
    /// | Variant | Result |
    /// |---------|--------|
    /// | `Message(data)` | `Text("4" + data)` |
    /// | `Ping(data)` | `Text("2" + data)` |
    /// | `Pong(data)` | `Text("3" + data)` |
    /// | `Open(hs)` | `Text("0" + JSON)` |
    /// | `Close` | `Text("1")` |
    /// | `Upgrade` | `Text("5")` |
    /// | `Noop` | `Text("6")` |
    pub fn encode(&self) -> Bytes {
        match self {
            EioPacket::Message(data) => encode_prefixed(b'4', data),
            EioPacket::Ping(data) => encode_prefixed(b'2', data),
            EioPacket::Pong(data) => encode_prefixed(b'3', data),
            EioPacket::Open(handshake) => encode_prefixed(
                b'0',
                &serde_json::to_vec(&handshake).expect("handshake should serialize to JSON"),
            ),
            EioPacket::Close => Bytes::from_static(b"1"),
            EioPacket::Upgrade => Bytes::from_static(b"5"),
            EioPacket::Noop => Bytes::from_static(b"6"),
        }
    }

    /// Decodes a single packet from a text frame.
    ///
    /// The first byte is the packet-type digit (`b'0'`..=`b'6'`).
    pub fn decode(mut data: Bytes) -> Result<Self, PacketError> {
        if data.is_empty() {
            Err(PacketError::Empty)?;
        }
        let first = data[0];
        data.advance(1);

        match first {
            b'0' => Ok(EioPacket::Open(serde_json::from_slice(&data).map_err(
                |e| crate::error::PayloadError::new::<Handshake>(e).with_slice(&data),
            )?)),
            b'1' => Ok(EioPacket::Close),
            b'2' => Ok(EioPacket::Ping(data)),
            b'3' => Ok(EioPacket::Pong(data)),
            b'4' => Ok(EioPacket::Message(data)),
            b'5' => Ok(EioPacket::Upgrade),
            b'6' => Ok(EioPacket::Noop),
            _ => Err(PacketError::InvalidId { byte: first })?,
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
    use serde_json::json;

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
        let json = json!({
            "sid": "test-456",
            "upgrades": [],
            "pingInterval": 25000,
            "pingTimeout": 20000
        });
        let bytes = Bytes::from(format!("0{}", json));
        assert!(EioPacket::decode(bytes).is_err());
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
            EioPacket::decode(Bytes::from_static(b"9")),
            Err(crate::error::PacketError::InvalidId { byte: b'9' })
        ));
    }

    #[test]
    fn decode_empty_packet() {
        assert!(matches!(
            EioPacket::decode(Bytes::new()),
            Err(crate::error::PacketError::Empty)
        ));
    }

    #[test]
    fn encode_decode_open_round_trip() {
        let hs = test_handshake();
        let Frame::Packet(packet) = EioPacket::Open(hs.clone()).into() else {
            panic!("expected Packet frame");
        };
        let EioPacket::Open(decoded) = packet else {
            panic!("expected Open");
        };
        assert_eq!(decoded, hs);
    }

    #[test]
    fn encode_decode_close_round_trip() {
        let Frame::Packet(packet) = EioPacket::Close.into() else {
            panic!("expected Packet frame");
        };
        assert!(matches!(packet, EioPacket::Close));
    }

    #[test]
    fn encode_decode_ping_round_trip() {
        let payload = Bytes::from_static(b"probe");
        let Frame::Packet(packet) = EioPacket::Ping(payload.clone()).into() else {
            panic!("expected Packet frame");
        };
        let EioPacket::Ping(decoded) = packet else {
            panic!("expected Ping");
        };
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_decode_pong_round_trip() {
        let payload = Bytes::from_static(b"probe");
        let Frame::Packet(packet) = EioPacket::Pong(payload.clone()).into() else {
            panic!("expected Packet frame");
        };
        let EioPacket::Pong(decoded) = packet else {
            panic!("expected Pong");
        };
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_decode_text_message_round_trip() {
        let text = Bytes::from_static(b"hello world");
        let Frame::Packet(packet) = EioPacket::Message(text.clone()).into() else {
            panic!("expected Packet frame");
        };
        let EioPacket::Message(decoded) = packet else {
            panic!("expected Message");
        };
        assert_eq!(decoded, text);
    }

    #[test]
    fn encode_decode_upgrade_round_trip() {
        let Frame::Packet(packet) = EioPacket::Upgrade.into() else {
            panic!("expected Packet frame");
        };
        assert!(matches!(packet, EioPacket::Upgrade));
    }

    #[test]
    fn encode_decode_noop_round_trip() {
        let Frame::Packet(packet) = EioPacket::Noop.into() else {
            panic!("expected Packet frame");
        };
        assert!(matches!(packet, EioPacket::Noop));
    }

    #[test]
    fn encode_ping_empty_payload() {
        let Frame::Packet(packet) = EioPacket::Ping(Bytes::new()).into() else {
            panic!("expected Packet frame");
        };
        let EioPacket::Ping(decoded) = packet else {
            panic!("expected Ping");
        };
        assert!(decoded.is_empty());
    }
}
