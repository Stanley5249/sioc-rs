#![allow(unused_assignments)] // named fields in #[error("...")] trigger this spuriously

//! Error types for Engine.IO operations.

use crate::engine::EngineAction;
use crate::packet::{Frame, Handshake, Packet};
use miette::Diagnostic;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// JSON serialization or deserialization failure.
#[derive(Debug, Error, Diagnostic)]
#[error("payload error for `{type_name}`: {source}")]
#[diagnostic(code(sioc_engine::payload))]
pub struct PayloadError {
    pub type_name: &'static str,
    #[source]
    pub source: serde_path_to_error::Error<serde_json::Error>,
}

impl PayloadError {
    /// Creates a `PayloadError` for type `T`.
    pub fn new<T>(source: serde_path_to_error::Error<serde_json::Error>) -> Self {
        Self {
            type_name: std::any::type_name::<T>(),
            source,
        }
    }
}

/// Top-level error aggregator for `sioc-engine`.
#[derive(Debug, Error, Diagnostic)]
#[error(transparent)]
#[diagnostic(transparent)]
pub enum Error {
    /// Error from the transport coordination task.
    Transport(#[from] TransportError),

    /// Error from the engine protocol task.
    Engine(#[from] EngineError),
}

/// Errors from decoding a raw Engine.IO packet.
#[derive(Debug, Error, Diagnostic)]
pub enum PacketError {
    /// Packet bytes are empty.
    #[error("empty packet")]
    #[diagnostic(code(sioc_engine::packet::empty))]
    Empty,

    /// First byte is not a recognised packet-type digit.
    #[error("invalid type id {id:#04x}")]
    #[diagnostic(code(sioc_engine::packet::invalid_id))]
    InvalidId { id: u8 },

    /// Packet JSON payload is malformed.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Payload(#[from] PayloadError),
}

/// Errors that occur during an active Engine.IO session.
#[derive(Debug, Error, Diagnostic)]
pub enum EngineError {
    /// The handshake oneshot channel was dropped before the server responded.
    #[error("failed to receive Engine.IO handshake")]
    #[diagnostic(
        code(sioc_engine::handshake::recv),
        help(
            "the transport task exited before completing the handshake; check the transport for prior errors"
        )
    )]
    RecvHandshake(#[from] oneshot::error::RecvError),

    /// Outbound frame channel to the transport task is closed.
    #[error("outbound frame channel closed")]
    #[diagnostic(
        code(sioc_engine::send::frame),
        help("the transport task exited; check for prior transport errors")
    )]
    SendFrame(#[from] mpsc::error::SendError<Frame>),

    /// Inbound transit delivery to the Socket.IO layer failed.
    #[error("inbound transit channel closed")]
    #[diagnostic(
        code(sioc_engine::send::transit),
        help("the consumer of inbound messages was dropped")
    )]
    SendTransit(#[source] BoxedError),

    /// An unexpected packet was received during the session.
    #[error("unexpected packet {packet:?}: {message}")]
    #[diagnostic(
        code(sioc_engine::unexpected_packet),
        help(
            "the server sent a packet that violates the Engine.IO state machine; likely a server bug or version mismatch"
        )
    )]
    Packet { packet: Packet, message: String },

    /// The server stopped sending heartbeat pings within the expected window.
    #[error("heartbeat timeout")]
    #[diagnostic(
        code(sioc_engine::heartbeat_timeout),
        help("the server stopped responding; check the network or server load")
    )]
    HeartbeatTimeout,

    /// A background engine task panicked or was cancelled.
    #[error("background task failed")]
    #[diagnostic(code(sioc_engine::task))]
    Task(#[from] JoinError),
}

impl EngineError {
    /// Constructs an [`EngineError::Packet`] with a descriptive message.
    pub fn packet(packet: Packet, message: impl Into<String>) -> Self {
        Self::Packet {
            packet,
            message: message.into(),
        }
    }
}

/// Errors that occur during Engine.IO connection setup and transport coordination.
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

    /// A frame arrived that is not valid in the current protocol state.
    #[error("unexpected frame {frame:?}: {message}")]
    #[diagnostic(
        code(sioc_engine::unexpected_frame),
        help(
            "the server sent a frame that violates the Engine.IO state machine; likely a server bug or version mismatch"
        )
    )]
    Frame { frame: Frame, message: String },

    /// A packet decoding error within a WebSocket frame.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Packet(#[from] PacketError),

    /// A WebSocket text frame contained invalid UTF-8.
    #[error("invalid UTF-8 in WebSocket frame")]
    #[diagnostic(code(sioc_engine::transport::websocket::utf8))]
    Utf8(#[from] std::str::Utf8Error),

    /// Handshake data could not be forwarded to the engine task.
    #[error("failed to send handshake to engine task")]
    #[diagnostic(
        code(sioc_engine::handshake::send),
        help(
            "the engine task exited before the handshake arrived; check the engine for prior errors"
        )
    )]
    SendHandshake(Handshake),

    /// Sending an action to the engine task failed because the channel is closed.
    #[error("engine action channel closed")]
    #[diagnostic(
        code(sioc_engine::send::action),
        help("the engine task exited; check for prior engine errors")
    )]
    SendAction(#[from] mpsc::error::SendError<EngineAction>),

    /// A background transport task panicked or was cancelled.
    #[error("background transport task failed")]
    #[diagnostic(code(sioc_engine::transport::task))]
    Task(#[from] JoinError),
}

impl TransportError {
    /// Constructs a [`TransportError::Frame`] with a descriptive message.
    pub fn frame(frame: Frame, message: impl Into<String>) -> Self {
        Self::Frame {
            frame,
            message: message.into(),
        }
    }
}

/// Errors specific to the WebSocket transport.
#[derive(Debug, Error, Diagnostic)]
pub enum WebSocketError {
    /// An error from the `tokio-tungstenite` library.
    #[error(transparent)]
    #[diagnostic(code(sioc_engine::transport::websocket))]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),

    /// The WebSocket stream ended without a close frame.
    #[error("WebSocket stream closed unexpectedly")]
    #[diagnostic(
        code(sioc_engine::transport::websocket::closed),
        help("the server closed the TCP connection without sending a WebSocket close frame")
    )]
    Closed,
}

/// Errors specific to the HTTP long-polling transport.
#[derive(Debug, Error, Diagnostic)]
pub enum PollingError {
    /// An HTTP client error.
    #[error(transparent)]
    #[diagnostic(code(sioc_engine::transport::http))]
    Http(#[from] reqwest::Error),

    /// HTTP polling POST returned a non-`ok` response body.
    #[error("unexpected polling response: {response}")]
    #[diagnostic(
        code(sioc_engine::transport::unexpected_response),
        help(
            "the server returned an unexpected body; verify the server implementation and Engine.IO version"
        )
    )]
    UnexpectedResponse { response: String },

    /// Binary attachment is not valid base64.
    #[error("base64 decode error")]
    #[diagnostic(code(sioc_engine::packet::base64))]
    Base64(#[from] base64::DecodeError),
}
