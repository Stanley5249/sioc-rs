//! Error types for Engine.IO operations.

use bytestring::ByteString;
use miette::Diagnostic;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::packet::{Event, Frame, Handshake};

/// Top-level error aggregator for `eioc`.
#[derive(Debug, Error, Diagnostic)]
#[error(transparent)]
#[diagnostic(transparent)]
pub enum Error {
    /// Error from the transport coordination task.
    Transport(#[from] TransportError),

    /// Error from the engine protocol task.
    Engine(#[from] EngineError),
}

impl Error {
    /// Returns whether the error is a library bug, such as a broken channel
    /// between library tasks, rather than a network or server fault.
    ///
    /// A caller can retry the session after any other error.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        // No wildcard arm, so a new variant must be classified here.
        match self {
            Self::Transport(
                TransportError::WebSocket(_) | TransportError::Polling(_) | TransportError::Open(_),
            )
            | Self::Engine(EngineError::HeartbeatTimeout | EngineError::HandshakeTimeout) => false,

            Self::Transport(TransportError::ServerFrame(_) | TransportError::Handshake(_))
            | Self::Engine(
                EngineError::ClientFrame(_) | EngineError::Event(_) | EngineError::Handshake(_),
            ) => true,
        }
    }
}

/// Errors that occur during an active Engine.IO session.
#[derive(Debug, Error, Diagnostic)]
pub enum EngineError {
    /// Sending a client frame to the transport task failed because the channel
    /// is closed.
    #[error("client frame channel closed")]
    #[diagnostic(code(eioc::engine::client_frame), help("library bug, please report"))]
    ClientFrame(#[from] mpsc::error::SendError<Frame>),

    /// Delivering an event to the upper layer failed because its receiver is
    /// gone.
    #[error("event channel closed")]
    #[diagnostic(code(eioc::engine::event), help("library bug, please report"))]
    Event(#[from] mpsc::error::SendError<Event>),

    /// The handshake oneshot channel was dropped before the server responded.
    #[error("failed to receive Engine.IO handshake")]
    #[diagnostic(code(eioc::engine::handshake), help("library bug, please report"))]
    Handshake(#[from] oneshot::error::RecvError),

    /// The server stopped sending heartbeat pings within the expected window.
    #[error("heartbeat timeout")]
    #[diagnostic(
        code(eioc::engine::heartbeat_timeout),
        help("the server stopped responding; check the network or server load")
    )]
    HeartbeatTimeout,

    /// The server did not accept the handshake in time.
    #[error("handshake timeout")]
    #[diagnostic(
        code(eioc::engine::handshake_timeout),
        help("the server did not answer in time; check the URL, the network, or server load")
    )]
    HandshakeTimeout,
}

/// Errors that occur during Engine.IO connection setup and transport
/// coordination.
#[derive(Debug, Error, Diagnostic)]
pub enum TransportError {
    /// A WebSocket transport error.
    #[error(transparent)]
    #[diagnostic(transparent)]
    WebSocket(#[from] WebSocketError),

    /// An HTTP polling transport error.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Polling(#[from] PollingError),

    /// Sending a server frame to the engine task failed because the channel is
    /// closed.
    #[error("server frame channel closed")]
    #[diagnostic(
        code(eioc::transport::server_frame),
        help("library bug, please report")
    )]
    ServerFrame(#[from] mpsc::error::SendError<Frame>),

    /// Handshake data could not be forwarded to the engine task.
    #[error("failed to send handshake to engine task")]
    #[diagnostic(code(eioc::transport::handshake), help("library bug, please report"))]
    Handshake(Handshake),

    /// The first frame received was not an Open packet.
    #[error("expected Open packet as first frame")]
    #[diagnostic(
        code(eioc::transport::open),
        help(
            "the server did not send the expected Open packet; check the server implementation and Engine.IO version"
        )
    )]
    Open(Frame),
}

/// Errors specific to the WebSocket transport.
#[derive(Debug, Error, Diagnostic)]
pub enum WebSocketError {
    /// An error from the `tokio-tungstenite` library.
    #[error(transparent)]
    #[diagnostic(code(eioc::transport::websocket::tungstenite))]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),

    /// The WebSocket stream ended without a close frame.
    #[error("WebSocket stream closed unexpectedly")]
    #[diagnostic(
        code(eioc::transport::websocket::closed),
        help("the server closed the TCP connection without sending a WebSocket close frame")
    )]
    Closed,

    /// The server did not respond to the probe Ping with a Pong.
    #[error("probe Pong not received")]
    #[diagnostic(
        code(eioc::transport::websocket::probe),
        help(
            "the server did not respond to the probe Ping with a Pong; check the server implementation and Engine.IO version"
        )
    )]
    Probe(Frame),

    /// A packet decoding error within a WebSocket text frame.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Packet(#[from] PacketError),
}

/// Errors specific to the HTTP long-polling transport.
#[derive(Debug, Error, Diagnostic)]
pub enum PollingError {
    /// An HTTP client error.
    #[error(transparent)]
    #[diagnostic(code(eioc::transport::polling::reqwest))]
    Reqwest(#[from] reqwest::Error),

    /// Binary attachment is not valid base64.
    #[error("failed to decode base64 attachment")]
    #[diagnostic(code(eioc::transport::polling::base64))]
    Base64(#[from] base64::DecodeError),

    /// HTTP polling POST returned a non-`ok` response body.
    #[error("unexpected polling response: {0}")]
    #[diagnostic(
        code(eioc::transport::polling::response),
        help(
            "the server returned an unexpected body; verify the server implementation and Engine.IO version"
        )
    )]
    Response(String),

    /// A packet decoding error within a polling payload.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Packet(#[from] PacketError),
}

/// Errors from decoding a raw Engine.IO packet.
#[derive(Debug, Error, Diagnostic)]
pub enum PacketError {
    /// Packet bytes are empty.
    #[error("empty packet")]
    #[diagnostic(code(eioc::packet::empty))]
    Empty,

    /// First char of a packet is not a valid Engine.IO type id.
    #[error("invalid type id {id}")]
    #[diagnostic(code(eioc::packet::invalid_id))]
    InvalidId {
        /// The first char of the packet.
        id: char,
    },

    /// Open packet's JSON payload is malformed.
    #[error("failed to parse Open payload")]
    #[diagnostic(code(eioc::packet::handshake))]
    Handshake(#[from] serde_json::Error),

    /// Unexpected payload for the packet type.
    #[error("unexpected payload for type id {id}")]
    #[diagnostic(code(eioc::packet::payload))]
    Payload {
        /// The packet type id.
        id: char,
        /// The payload that the type does not accept.
        payload: ByteString,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::Message;

    fn frame() -> Frame {
        crate::packet::Packet::Noop.into()
    }

    #[test]
    fn network_and_server_faults_are_not_internal() {
        let errors: [Error; 5] = [
            TransportError::WebSocket(WebSocketError::Closed).into(),
            TransportError::Polling(PollingError::Response(String::new())).into(),
            TransportError::Open(frame()).into(),
            EngineError::HeartbeatTimeout.into(),
            EngineError::HandshakeTimeout.into(),
        ];
        for error in errors {
            assert!(!error.is_internal(), "{error:?}");
        }
    }

    #[tokio::test]
    async fn channel_failures_are_internal() {
        let (handshake_tx, handshake_rx) = oneshot::channel::<Handshake>();
        drop(handshake_tx);
        let handshake = Handshake {
            sid: "sid".into(),
            upgrades: vec![],
            ping_interval: 25_000,
            ping_timeout: 5_000,
            max_payload: 1_000_000,
        };

        let errors: [Error; 5] = [
            TransportError::ServerFrame(mpsc::error::SendError(frame())).into(),
            TransportError::Handshake(handshake).into(),
            EngineError::ClientFrame(mpsc::error::SendError(frame())).into(),
            EngineError::Event(mpsc::error::SendError(Event::Message(Message::Text(
                "".into(),
            ))))
            .into(),
            EngineError::Handshake(handshake_rx.await.unwrap_err()).into(),
        ];
        for error in errors {
            assert!(error.is_internal(), "{error:?}");
        }
    }

    #[test]
    fn packet_error_empty_display() {
        assert_eq!(PacketError::Empty.to_string(), "empty packet");
    }

    #[test]
    fn packet_error_invalid_id_display() {
        assert_eq!(
            PacketError::InvalidId { id: '9' }.to_string(),
            "invalid type id 9"
        );
    }

    #[test]
    fn packet_error_payload_display() {
        let e = PacketError::Payload {
            id: '1',
            payload: "extra".into(),
        };
        assert_eq!(e.to_string(), "unexpected payload for type id 1");
    }

    #[test]
    fn engine_error_heartbeat_timeout_display() {
        assert_eq!(
            EngineError::HeartbeatTimeout.to_string(),
            "heartbeat timeout"
        );
    }

    #[test]
    fn polling_error_response_display() {
        let e = PollingError::Response("forbidden".to_string());
        assert_eq!(e.to_string(), "unexpected polling response: forbidden");
    }

    #[test]
    fn transport_error_from_polling() {
        let e: TransportError = PollingError::Response("x".into()).into();
        assert!(matches!(e, TransportError::Polling(_)));
    }

    #[test]
    fn error_from_engine_error() {
        let e: Error = EngineError::HeartbeatTimeout.into();
        assert!(matches!(e, Error::Engine(_)));
    }

    #[test]
    fn error_from_transport_error() {
        let e: Error =
            TransportError::Open(crate::packet::Frame::Packet(crate::packet::Packet::Close)).into();
        assert!(matches!(e, Error::Transport(_)));
    }
}
