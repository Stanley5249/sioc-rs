//! Engine.IO transport coordination.
//!
//! Manages the transport lifecycle: HTTP long-polling handshake, optional
//! upgrade to WebSocket, and shutdown once the engine closes the transport
//! channel.

use bytes::Bytes;
use bytestring::ByteString;
use tokio::sync::mpsc;
use url::Url;

use crate::connector::WebSocketConnector;
use crate::error::TransportError;
use crate::packet::{Frame, Handshake, Packet};

/// Selects which transport to use when opening an Engine.IO connection, like
/// the `transports` option. Non-exhaustive, because engine.io-client also has
/// a WebTransport transport.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub enum TransportStrategy {
    /// Start with HTTP long-polling, then upgrade to WebSocket when the server
    /// offers it.
    #[default]
    Polling,
    /// Connect directly over WebSocket, skipping the polling handshake.
    WebSocket,
}

/// A frame the transport hands to the engine, like the packets of a
/// `Transport`'s `"packet"` event. The server's Close packet has no variant,
/// because the transport ends the channel at it instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerFrame {
    /// The handshake, first on every channel. Later ones are out of place.
    Open(Handshake),
    /// A heartbeat that the engine answers.
    Ping(ByteString),
    /// Out of place, because the transport consumes the probe's pong.
    Pong(ByteString),
    /// A text message.
    Message(ByteString),
    /// Out of place, because the client sends Upgrade.
    Upgrade,
    /// A no-op, like the one that ends a paused poll.
    Noop,
    /// A binary message.
    Binary(Bytes),
}

impl ServerFrame {
    /// Returns `None` at the server's Close packet, which ends the server
    /// direction.
    #[must_use]
    pub fn from_frame(frame: Frame) -> Option<Self> {
        Some(match frame {
            Frame::Packet(Packet::Open(handshake)) => Self::Open(handshake),
            Frame::Packet(Packet::Close) => return None,
            Frame::Packet(Packet::Ping(payload)) => Self::Ping(payload),
            Frame::Packet(Packet::Pong(payload)) => Self::Pong(payload),
            Frame::Packet(Packet::Message(payload)) => Self::Message(payload),
            Frame::Packet(Packet::Upgrade) => Self::Upgrade,
            Frame::Packet(Packet::Noop) => Self::Noop,
            Frame::Binary(bytes) => Self::Binary(bytes),
        })
    }
}

/// A frame the engine sends, like the packets of `Transport.send`. The
/// transport owns Close, Upgrade, and the probe's ping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientFrame {
    /// A text message.
    Message(ByteString),
    /// A binary message.
    Binary(Bytes),
    /// The answer to the server's last ping.
    Pong(ByteString),
}

impl From<ClientFrame> for Frame {
    fn from(frame: ClientFrame) -> Self {
        match frame {
            ClientFrame::Message(payload) => Packet::Message(payload).into(),
            ClientFrame::Binary(bytes) => bytes.into(),
            ClientFrame::Pong(payload) => Packet::Pong(payload).into(),
        }
    }
}

/// Opens a transport with `strategy` and runs it until the session ends.
///
/// The first frame on `server_frame_tx` is the server's Open packet, and a
/// different first frame fails with [`TransportError::Open`]. The server's
/// Close packet ends that channel. Frames on `client_frame_rx` are read only
/// after the handshake, so the engine may queue them while opening. Dropping
/// its sender makes the transport send the Close packet.
///
/// # Errors
///
/// Returns an error if the transport encounters a protocol or I/O failure.
pub async fn open<C>(
    strategy: TransportStrategy,
    base_url: Url,
    http_client: reqwest::Client,
    connector: C,
    server_frame_tx: mpsc::Sender<ServerFrame>,
    client_frame_rx: mpsc::Receiver<ClientFrame>,
) -> Result<(), TransportError>
where
    C: WebSocketConnector,
{
    match strategy {
        TransportStrategy::Polling => {
            crate::polling::session::run(
                http_client,
                base_url,
                connector,
                server_frame_tx,
                client_frame_rx,
            )
            .await
        }
        TransportStrategy::WebSocket => {
            let stream = crate::websocket::stream::connect(base_url, None, connector).await?;

            crate::websocket::session::run(stream, true, server_frame_tx, client_frame_rx).await
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Error as TungsteniteError;
    use url::Url;

    use super::*;
    use crate::error::TransportError;

    fn handshake() -> Handshake {
        Handshake {
            sid: "sid".into(),
            upgrades: vec![],
            ping_interval: 25_000,
            ping_timeout: 5_000,
            max_payload: 1_000_000,
        }
    }

    #[test]
    fn server_frames_keep_every_packet_but_close() {
        let bytes = Bytes::from_static(b"\x01");
        let cases = [
            (
                Packet::Open(handshake()).into(),
                ServerFrame::Open(handshake()),
            ),
            (
                Packet::Ping("p".into()).into(),
                ServerFrame::Ping("p".into()),
            ),
            (
                Packet::Pong("p".into()).into(),
                ServerFrame::Pong("p".into()),
            ),
            (
                Packet::Message("m".into()).into(),
                ServerFrame::Message("m".into()),
            ),
            (Packet::Upgrade.into(), ServerFrame::Upgrade),
            (Packet::Noop.into(), ServerFrame::Noop),
            (Frame::Binary(bytes.clone()), ServerFrame::Binary(bytes)),
        ];
        for (frame, expected) in cases {
            assert_eq!(ServerFrame::from_frame(frame), Some(expected));
        }

        // The transport ends the server direction at Close instead.
        assert_eq!(ServerFrame::from_frame(Packet::Close.into()), None);
    }

    #[test]
    fn client_frames_encode_as_their_wire_frames() {
        let bytes = Bytes::from_static(b"\x01");
        let cases = [
            (
                ClientFrame::Message("m".into()),
                Packet::Message("m".into()).into(),
            ),
            (
                ClientFrame::Pong("p".into()),
                Packet::Pong("p".into()).into(),
            ),
            (ClientFrame::Binary(bytes.clone()), Frame::Binary(bytes)),
        ];
        for (frame, expected) in cases {
            assert_eq!(Frame::from(frame), expected);
        }
    }

    #[test]
    fn transport_strategy_default_is_polling() {
        assert!(matches!(
            TransportStrategy::default(),
            TransportStrategy::Polling
        ));
    }

    #[tokio::test]
    async fn websocket_strategy_propagates_connector_error() {
        let base_url = Url::parse("ws://127.0.0.1:1/").unwrap();
        let http_client = reqwest::Client::new();
        let connector = async |_| Err(TungsteniteError::ConnectionClosed);
        let (server_frame_tx, _frame_rx) = mpsc::channel(1);
        let (_transport_tx, client_frame_rx) = mpsc::channel(1);

        let result = crate::transport::open(
            TransportStrategy::WebSocket,
            base_url,
            http_client,
            connector,
            server_frame_tx,
            client_frame_rx,
        )
        .await;

        assert!(matches!(result, Err(TransportError::WebSocket(_))));
    }
}
