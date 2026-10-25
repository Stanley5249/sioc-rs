//! Error types for Engine.IO operations.

use bytestring::ByteString;
use miette::Diagnostic;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::packet::Frame;
use crate::transport::ServerFrame;

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

            Self::Transport(TransportError::ServerFrameClosed)
            | Self::Engine(EngineError::ServerMessageClosed) => true,
        }
    }
}

/// Errors that occur during an active Engine.IO session.
#[derive(Debug, Error, Diagnostic)]
pub enum EngineError {
    /// Delivering a server message to the upper layer failed because it
    /// dropped its receiver before the engine hung up.
    #[error("server message channel closed")]
    #[diagnostic(code(eioc::engine::server_message), help("library bug, please report"))]
    ServerMessageClosed,

    /// The server stopped sending heartbeat pings within the expected window.
    #[error("heartbeat timeout")]
    #[diagnostic(
        code(eioc::engine::heartbeat_timeout),
        help(
            "the server stopped responding, or the application did not read events for a whole ping window; check the network, server load, or the receive loop"
        )
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

    /// Sending a server frame to the engine failed because it dropped its
    /// receiver before the transport ended.
    #[error("server frame channel closed")]
    #[diagnostic(
        code(eioc::transport::server_frame),
        help("library bug, please report")
    )]
    ServerFrameClosed,

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

impl From<mpsc::error::SendError<ServerFrame>> for TransportError {
    fn from(_: mpsc::error::SendError<ServerFrame>) -> Self {
        Self::ServerFrameClosed
    }
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

    #[test]
    fn channel_failures_are_internal() {
        let errors: [Error; 2] = [
            TransportError::ServerFrameClosed.into(),
            EngineError::ServerMessageClosed.into(),
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
