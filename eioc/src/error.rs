#![allow(unused_assignments)] // named fields in #[error("...")] trigger this spuriously

//! Error types for Engine.IO operations.

use crate::engine::EngineAction;
use crate::packet::{Frame, Handshake, Packet};
use bytestring::ByteString;
use miette::Diagnostic;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio::time::error::Elapsed;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

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

/// Errors that occur during an active Engine.IO session.
#[derive(Debug, Error, Diagnostic)]
pub enum EngineError {
    /// Outbound frame channel to the transport task is closed.
    #[error("outbound frame channel closed")]
    #[diagnostic(
        code(eioc::engine::send_transport),
        help("the transport task exited; check for prior transport errors")
    )]
    SendTransport(#[from] mpsc::error::SendError<Frame>),

    /// Inbound message delivery to the Socket.IO layer failed.
    #[error("inbound message sink closed")]
    #[diagnostic(
        code(eioc::engine::send_sink),
        help("the consumer of inbound messages was dropped")
    )]
    // Keep as `#[source]` (not `#[from]`) so type-erased errors do not
    // implicitly convert into `EngineError` at unrelated call sites.
    SendSink(#[source] BoxedError),

    /// The handshake oneshot channel was dropped before the server responded.
    #[error("failed to receive Engine.IO handshake")]
    #[diagnostic(
        code(eioc::engine::recv_handshake),
        help(
            "the transport task exited before completing the handshake; check the transport for prior errors"
        )
    )]
    RecvHandshake(#[from] oneshot::error::RecvError),

    /// A background engine task panicked or was cancelled.
    #[error("background task failed")]
    #[diagnostic(code(eioc::engine::join))]
    Join(#[from] JoinError),

    /// The server stopped sending heartbeat pings within the expected window.
    #[error("heartbeat timeout")]
    #[diagnostic(
        code(eioc::engine::heartbeat_timeout),
        help("the server stopped responding; check the network or server load")
    )]
    HeartbeatTimeout(#[from] Elapsed),

    /// An unexpected packet was received during the session.
    #[error("unexpected packet {0:?}")]
    #[diagnostic(
        code(eioc::engine::server),
        help(
            "the server sent a packet that violates the Engine.IO state machine; likely a server bug or version mismatch"
        )
    )]
    Server(Packet),
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

    /// Sending an action to the engine task failed because the channel is closed.
    #[error("engine action channel closed")]
    #[diagnostic(
        code(eioc::transport::send_engine),
        help("the engine task exited; check for prior engine errors")
    )]
    SendEngine(#[from] mpsc::error::SendError<EngineAction>),

    /// Handshake data could not be forwarded to the engine task.
    #[error("failed to send handshake to engine task")]
    #[diagnostic(
        code(eioc::transport::send_handshake),
        help(
            "the engine task exited before the handshake arrived; check the engine for prior errors"
        )
    )]
    SendHandshake(Handshake),

    /// A background transport task panicked or was cancelled.
    #[error("background transport task failed")]
    #[diagnostic(code(eioc::transport::join))]
    Join(#[from] JoinError),

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
    InvalidId { id: char },

    /// Open packet's JSON payload is malformed.
    #[error("failed to parse Open payload")]
    #[diagnostic(code(eioc::packet::handshake))]
    Handshake(#[from] serde_json::Error),

    /// Unexpected payload for the packet type.
    #[error("unexpected payload for type id {id}")]
    #[diagnostic(code(eioc::packet::payload))]
    Payload { id: char, payload: ByteString },
}
