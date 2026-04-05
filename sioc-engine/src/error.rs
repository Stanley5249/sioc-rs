#![allow(unused_assignments)] // named fields in #[error("...")] trigger this spuriously

//! Error types for Engine.IO operations.

use crate::engine::EngineAction;
use crate::packet::{EioPacket, Frame, Handshake};
use miette::{Diagnostic, SourceOffset};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;

/// JSON serialization or deserialization failed.
#[derive(Debug, Error, Diagnostic)]
#[error("JSON error for {type_name}")]
pub struct PayloadError {
    pub type_name: &'static str,
    #[source]
    pub source: serde_json::Error,
    #[source_code]
    pub json: Option<String>,
    #[label("{source}")]
    pub offset: Option<SourceOffset>,
}

impl PayloadError {
    /// Creates a `PayloadError` for type `T` from a `serde_json::Error`.
    pub fn new<T>(source: serde_json::Error) -> Self {
        Self {
            type_name: std::any::type_name::<T>(),
            source,
            json: None,
            offset: None,
        }
    }

    /// Attaches the raw JSON bytes that failed to parse.
    pub fn with_slice(self, json: &[u8]) -> Self {
        self.with_json(String::from_utf8_lossy(json).into_owned())
    }

    /// Attaches the JSON string that failed to parse.
    pub fn with_json(self, json: String) -> Self {
        let offset = SourceOffset::from_location(&json, self.source.line(), self.source.column());
        Self {
            json: Some(json),
            offset: Some(offset),
            ..self
        }
    }
}

/// Errors from decoding a raw Engine.IO packet.
#[derive(Debug, Error, Diagnostic)]
pub enum PacketError {
    #[error("empty packet")]
    #[diagnostic(code(sioc_engine::packet::empty))]
    Empty,

    #[error("invalid packet type: {byte:#04x}")]
    #[diagnostic(code(sioc_engine::packet::invalid_type))]
    InvalidId { byte: u8 },

    #[error(transparent)]
    #[diagnostic(transparent)]
    Payload(#[from] PayloadError),

    #[error("base64 decode")]
    #[diagnostic(code(sioc_engine::packet::base64))]
    Base64(#[from] base64::DecodeError),
}

type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The top-level error type for all `sioc-engine` public APIs.
#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    /// The connection was closed unexpectedly.
    #[error("connection closed")]
    #[diagnostic(code(sioc_engine::closed))]
    Close,

    #[error(transparent)]
    #[diagnostic(transparent)]
    Packet(#[from] PacketError),

    #[error("outbound frame channel closed")]
    #[diagnostic(code(sioc_engine::frame_channel_closed))]
    SendFrame(#[from] mpsc::error::SendError<Frame>),

    #[error("inbound message channel closed")]
    #[diagnostic(code(sioc_engine::inbound_closed))]
    SendMessage(#[source] BoxedError),

    #[error("engine data channel closed")]
    #[diagnostic(code(sioc_engine::engine_data_closed))]
    SendAction(#[from] mpsc::error::SendError<EngineAction>),

    #[error("handshake response channel closed")]
    #[diagnostic(code(sioc_engine::handshake_channel_closed))]
    SendHandshake(Handshake),

    #[error("connection handshake failed")]
    #[diagnostic(code(sioc_engine::connection_failed))]
    RecvHandshake(#[from] oneshot::error::RecvError),

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

    #[error("unexpected response: {response}")]
    #[diagnostic(code(sioc_engine::transport::unexpected_body))]
    UnexpectedResponse {
        description: String,
        response: String,
    },

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

    #[error("background task failed")]
    #[diagnostic(code(sioc_engine::task))]
    Task(#[from] JoinError),
}

/// Result alias for all `sioc-engine` operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
