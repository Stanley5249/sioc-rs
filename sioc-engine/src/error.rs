//! Error types for Engine.IO operations.

use crate::packet::{EioPacket, Frame, Message};
use miette::Diagnostic;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinError};

/// Errors from decoding a raw Engine.IO packet.
#[derive(Debug, Error, Diagnostic)]
pub enum PacketError {
    #[error("empty packet")]
    #[diagnostic(code(sioc_engine::packet::empty))]
    Empty,

    #[error("invalid packet type: {byte:#04x}")]
    #[diagnostic(code(sioc_engine::packet::invalid_type))]
    InvalidId { byte: u8 },

    // TODO: add sioc_error::PayloadError
    // only on open handsake
    #[error("JSON decode")]
    #[diagnostic(code(sioc_engine::packet::json))]
    Json(#[from] serde_json::Error),

    #[error("base64 decode")]
    #[diagnostic(code(sioc_engine::packet::base64))]
    Base64(#[from] base64::DecodeError),
}

/// The top-level error type for all `sioc-engine` public APIs.
#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    /// The connection was closed unexpectedly.
    #[error("connection closed")]
    #[diagnostic(code(sioc_engine::closed))]
    Close,

    /// Received a packet not valid in the current protocol state.
    #[error("unexpected packet {packet:?}: {description}")]
    #[diagnostic(code(sioc_engine::unexpected_packet))]
    UnexpectedPacket {
        description: String,
        packet: EioPacket,
    },

    #[error("unexpected frame {frame:?}: {description}")]
    #[diagnostic(code(sioc_engine::unexpected_frame))]
    UnexpectedFrame { description: String, frame: Frame },

    #[error(transparent)]
    #[diagnostic(transparent)]
    Packet(#[from] PacketError),

    #[error("frame channel closed")]
    #[diagnostic(code(sioc_engine::frame_channel_closed))]
    FrameSend(#[from] mpsc::error::SendError<Frame>),

    #[error("inbound message channel closed")]
    #[diagnostic(code(sioc_engine::inbound_closed))]
    InboundClosed(#[from] mpsc::error::SendError<Message>),

    #[error("unexpected body: {body}")]
    #[diagnostic(code(sioc_engine::transport::unexpected_body))]
    UnexpectedBody { description: String, body: String },

    #[error(transparent)]
    #[diagnostic(code(sioc::engine::transport::http))]
    Http(#[from] reqwest::Error),

    #[error(transparent)]
    #[diagnostic(code(sioc_engine::transport::websocket))]
    Websocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error(transparent)]
    #[diagnostic(code(sioc_engine::utf8))]
    Utf8(#[from] std::str::Utf8Error),

    #[error("heartbeat timeout")]
    #[diagnostic(code(sioc_engine::heartbeat_timeout))]
    HeartbeatTimeout,

    #[error("upgrade task failed")]
    #[diagnostic(code(sioc_engine::upgrade_task_fail))]
    UpgradeFailed(#[from] JoinError),
}

/// Result alias for all `sioc-engine` operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
