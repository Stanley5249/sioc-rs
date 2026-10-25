//! Wire packet definitions, like socket.io-parser's `Packet` and its `nsp`.

use bytestring::ByteString;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
