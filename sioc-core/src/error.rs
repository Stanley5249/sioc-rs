//! Error types for `sioc-core`.

use crate::packet::{DynAck, Packet};
use bytes::Bytes;
use miette::Diagnostic;
pub use sioc_engine::error::Error as EngineError;
pub use sioc_engine::prelude::PayloadError;
use thiserror::Error;
use tokio::sync::mpsc;

/// Errors from decoding a raw Socket.IO packet.
///
/// Callers receive this wrapped in [`Error::Parse`].
#[derive(Debug, Error, Diagnostic)]
pub enum ParseError {
    #[error("packet is empty")]
    #[diagnostic(code(sioc::parse::empty_packet))]
    EmptyPacket,

    #[error("unknown packet type: {byte:#04x}")]
    #[diagnostic(code(sioc::parse::unknown_packet_type))]
    UnknownPacketType { byte: u8 },

    #[error("binary packet missing attachment count prefix")]
    #[diagnostic(code(sioc::parse::missing_attachment_count))]
    MissingAttachmentCount,

    #[error("attachment count in not a valid integer")]
    #[diagnostic(code(sioc::parse::invalid_attachment_count))]
    InvalidAttachmentCount,

    #[error("namespace missing trailing `,` delimiter")]
    #[diagnostic(code(sioc::parse::missing_namespace_delimiter))]
    MissingNamespaceDelimiter,

    #[error("ack packet missing numeric ID")]
    #[diagnostic(code(sioc::parse::missing_ack_id))]
    MissingAckId,

    #[error("packet ID is not a valid integer")]
    #[diagnostic(code(sioc::parse::invalid_ack_id))]
    InvalidAckId {
        #[from]
        source: std::num::ParseIntError,
    },

    #[error("text event packet has unexpected attachment count ({count})")]
    #[diagnostic(code(sioc::parse::unexpected_attachments))]
    UnexpectedAttachments { count: usize },

    #[error("invalid UTF-8 in packet")]
    #[diagnostic(code(sioc::parse::utf8))]
    Utf8 {
        #[from]
        source: std::str::Utf8Error,
    },

    #[error("invalid JSON in packet")]
    #[diagnostic(code(sioc::parse::json))]
    Payload(#[from] PayloadError),
}

/// The top-level error type for all `sioc-core` public APIs.
#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Engine(#[from] EngineError),

    /// Inbound packet delivery to a namespace channel failed.
    #[error("manager send failed for namespace `{ns}`")]
    #[diagnostic(code(sioc::manager_send))]
    SendPacket {
        ns: String,
        #[source]
        source: mpsc::error::SendError<Packet>,
    },

    /// Ack response channel was closed before the server replied.
    #[error("ack channel closed for namespace `{ns}`")]
    #[diagnostic(code(sioc::ack_closed))]
    SendAck { ns: String, ack: DynAck },

    /// Wraps a [`ParseError`] from packet decoding.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Parse(#[from] ParseError),

    /// Received a text frame while a binary reassembly was in progress.
    #[error("unexpected text frame: {0:?}")]
    #[diagnostic(code(sioc::unexpected_packet))]
    UnexpectedText(Bytes),

    /// Received a binary frame with no pending reassembly.
    #[error("unexpected binary frame: {0:?}")]
    #[diagnostic(code(sioc::unexpected_binary_packet))]
    UnexpectedBinary(Bytes),

    /// Packet addressed to a namespace that is not open.
    #[error("packet for unknown namespace `{ns}`")]
    #[diagnostic(code(sioc::packet_unknown_namespace))]
    UnknownNamespace { ns: String },

    /// Ack ID in a server response has no registered handler.
    #[error("ack for unknown ID {id} in namespace `{ns}`")]
    #[diagnostic(code(sioc::ack_unknown_id))]
    UnknownAckId { ns: String, id: u64 },

    /// A `Connect` packet was sent for a namespace already open.
    #[error("namespace conflict: `{ns}`")]
    #[diagnostic(code(sioc::namespace_conflict))]
    NamespaceConflict { ns: String },
}

/// Result alias for all `sioc-core` operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
