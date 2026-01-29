use base64::prelude::*;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use crate::error::{Error, Result};

/// Message content for Engine.IO packets
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Text(Bytes),
    Binary(Bytes),
}

impl Message {
    /// Consumes the message and returns the underlying bytes
    pub fn into_bytes(self) -> Bytes {
        match self {
            Message::Text(bytes) | Message::Binary(bytes) => bytes,
        }
    }
}

/// Data which gets exchanged in a handshake as defined by the server.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub sid: String,
    pub upgrades: Vec<String>,
    #[serde(rename = "pingInterval")]
    pub ping_interval: u64,
    #[serde(rename = "pingTimeout")]
    pub ping_timeout: u64,
}

/// A `Packet` sent via the `engine.io` protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Open(Handshake),
    Close,
    Ping(Bytes),
    Pong(Bytes),
    Message(Message),
    Upgrade,
    Noop,
}

impl TryFrom<Bytes> for Packet {
    type Error = Error;
    /// Decodes a single `Packet` from an `u8` byte stream.
    fn try_from(bytes: Bytes) -> Result<Self> {
        if bytes.is_empty() {
            return Err(Error::IncompletePacket);
        }

        let first_byte = *bytes.first().ok_or(Error::IncompletePacket)?;
        let data = bytes.slice(1..);

        match first_byte {
            b'0' => {
                // Parse JSON handshake
                let handshake: Handshake = serde_json::from_slice(data.as_ref())?;
                Ok(Packet::Open(handshake))
            }
            b'1' => Ok(Packet::Close),
            b'2' => Ok(Packet::Ping(data)),
            b'3' => Ok(Packet::Pong(data)),
            b'4' => Ok(Packet::Message(Message::Text(data))),
            b'b' => {
                // Base64 decode binary message
                let decoded = BASE64_STANDARD.decode(data.as_ref())?;
                Ok(Packet::Message(Message::Binary(Bytes::from(decoded))))
            }
            b'5' => Ok(Packet::Upgrade),
            b'6' => Ok(Packet::Noop),
            _ => Err(Error::InvalidPacketId(first_byte)),
        }
    }
}

impl Packet {
    pub fn into_message(self) -> Message {
        match self {
            Packet::Message(Message::Binary(data)) => Message::Binary(data),
            Packet::Message(Message::Text(data)) => {
                let mut buf = BytesMut::with_capacity(1 + data.len());
                buf.put_u8(b'4');
                buf.extend_from_slice(&data);
                Message::Text(buf.freeze())
            }
            Packet::Ping(data) => {
                let mut buf = BytesMut::with_capacity(1 + data.len());
                buf.put_u8(b'2');
                buf.extend_from_slice(&data);
                Message::Text(buf.freeze())
            }
            Packet::Pong(data) => {
                let mut buf = BytesMut::with_capacity(1 + data.len());
                buf.put_u8(b'3');
                buf.extend_from_slice(&data);
                Message::Text(buf.freeze())
            }
            Packet::Open(handshake) => {
                let json = serde_json::to_string(&handshake).unwrap();
                let mut buf = BytesMut::with_capacity(1 + json.len());
                buf.put_u8(b'0');
                buf.extend_from_slice(json.as_bytes());
                Message::Text(buf.freeze())
            }
            Packet::Close => Message::Text(Bytes::from_static(b"1")),
            Packet::Upgrade => Message::Text(Bytes::from_static(b"5")),
            Packet::Noop => Message::Text(Bytes::from_static(b"6")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_handshake_packet_parsing() {
        let handshake_json = json!({
            "sid": "test-123",
            "upgrades": ["websocket"],
            "pingInterval": 25000,
            "pingTimeout": 5000
        });

        let data = Bytes::from(handshake_json.to_string());
        let packet_bytes = Bytes::from(format!("0{}", std::str::from_utf8(&data).unwrap()));
        let packet = Packet::try_from(packet_bytes).unwrap();

        match packet {
            Packet::Open(handshake) => {
                assert_eq!(handshake.sid, "test-123");
                assert_eq!(handshake.upgrades, vec!["websocket"]);
                assert_eq!(handshake.ping_interval, 25000);
                assert_eq!(handshake.ping_timeout, 5000);
            }
            _ => panic!("Expected open packet"),
        }
    }

    #[test]
    fn test_packet_type_conversions() {
        // Test parsing from bytes
        let handshake_json = r#"{"sid":"test","upgrades":[],"pingInterval":0,"pingTimeout":0}"#;
        assert!(matches!(
            Packet::try_from(Bytes::from(format!("0{}", handshake_json))).unwrap(),
            Packet::Open(_)
        ));
        assert!(matches!(
            Packet::try_from(Bytes::from("1")).unwrap(),
            Packet::Close
        ));
        assert!(matches!(
            Packet::try_from(Bytes::from("2probe")).unwrap(),
            Packet::Ping(_)
        ));
        assert!(matches!(
            Packet::try_from(Bytes::from("3probe")).unwrap(),
            Packet::Pong(_)
        ));
        assert!(matches!(
            Packet::try_from(Bytes::from("4hello")).unwrap(),
            Packet::Message(Message::Text(_))
        ));
        assert!(matches!(
            Packet::try_from(Bytes::from("5")).unwrap(),
            Packet::Upgrade
        ));
        assert!(matches!(
            Packet::try_from(Bytes::from("6")).unwrap(),
            Packet::Noop
        ));
    }

    #[test]
    fn test_binary_message_parsing_from_base64() {
        // Test parsing binary message from 'b' + Base64 format
        let binary_data = vec![0, 1, 2, 255, 254];
        let encoded = BASE64_STANDARD.encode(&binary_data);
        let input = format!("b{}", encoded);

        let packet = Packet::try_from(Bytes::from(input)).unwrap();
        match packet {
            Packet::Message(Message::Binary(data)) => {
                assert_eq!(data.as_ref(), binary_data.as_slice())
            }
            _ => panic!("Expected binary message"),
        }
    }

    #[test]
    fn test_text_message_parsing_from_prefixed() {
        // Test parsing text message from '4' + text format
        let text_data = "hello world";
        let input = format!("4{}", text_data);

        let packet = Packet::try_from(Bytes::from(input)).unwrap();
        match packet {
            Packet::Message(Message::Text(data)) => assert_eq!(data.as_ref(), text_data.as_bytes()),
            _ => panic!("Expected text message"),
        }
    }
}
