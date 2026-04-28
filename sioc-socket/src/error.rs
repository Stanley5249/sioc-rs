#![allow(unused_assignments)] // named fields in #[error("...")] trigger this spuriously

//! Error types for `sioc-socket`.

use crate::packet::{DynAck, Signal};
use bytes::Bytes;
use bytestring::ByteString;
use miette::Diagnostic;
use sioc_engine::engine::EngineAction;
use thiserror::Error;
use tokio::sync::mpsc;

pub use sioc_engine::error::PayloadError;

/// Errors from decoding a raw Socket.IO packet.
///
/// Callers receive this wrapped in [`ManagerError::Packet`].
#[derive(Debug, Error, Diagnostic)]
pub enum PacketError {
    /// JSON payload in the packet is malformed.
    #[error(transparent)]
    #[diagnostic(code(sioc_socket::parse::json))]
    Payload(#[from] PayloadError),

    /// Packet bytes are not valid UTF-8.
    #[error("invalid UTF-8 in packet")]
    #[diagnostic(code(sioc_socket::parse::utf8))]
    Utf8(#[from] std::str::Utf8Error),

    /// No bytes were available to read.
    #[error("packet is empty")]
    #[diagnostic(code(sioc_socket::parse::empty_packet))]
    Empty,

    /// First byte does not map to any known packet type.
    #[error("unknown packet type {id}")]
    #[diagnostic(code(sioc_socket::parse::unknown_packet_type))]
    InvalidId { id: char },

    /// Binary packet header has no attachment count before the `-` separator.
    #[error("binary packet missing attachment count prefix")]
    #[diagnostic(code(sioc_socket::parse::missing_attachment_count))]
    MissingAttachmentCount,

    /// Text event packet carries a non-zero attachment count.
    #[error("text event packet has unexpected attachment count ({count})")]
    #[diagnostic(code(sioc_socket::parse::unexpected_attachments))]
    UnexpectedAttachments { count: usize },

    /// Attachment count prefix is present but not a valid integer.
    #[error("attachment count is not a valid integer")]
    #[diagnostic(code(sioc_socket::parse::invalid_attachment_count))]
    InvalidAttachmentCount(#[source] std::num::ParseIntError),

    /// Non-default namespace is missing the `,` delimiter after the path.
    #[error("namespace missing trailing `,` delimiter")]
    #[diagnostic(code(sioc_socket::parse::missing_namespace_delimiter))]
    MissingNamespaceDelimiter,

    /// Ack packet has no numeric ID field.
    #[error("ack packet missing numeric ID")]
    #[diagnostic(code(sioc_socket::parse::missing_ack_id))]
    MissingAckId,

    /// Ack ID field is present but not a valid integer.
    #[error("packet ID is not a valid integer")]
    #[diagnostic(code(sioc_socket::parse::invalid_ack_id))]
    InvalidAckId(#[source] std::num::ParseIntError),
}

/// The top-level error type for `sioc-socket` manager operations.
#[derive(Debug, Error, Diagnostic)]
pub enum ManagerError {
    /// Error propagated from the Engine.IO transport layer.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Engine(#[from] sioc_engine::error::Error),

    /// Wraps a [`PacketError`] from packet decoding.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Packet(#[from] PacketError),

    /// Sending an action to the engine layer failed because the channel is closed.
    #[error("engine action channel closed")]
    #[diagnostic(
        code(sioc_socket::manager::send_engine),
        help("the receiver was dropped; the socket is probably shut down")
    )]
    SendEngine(#[from] mpsc::error::SendError<EngineAction>),

    /// Inbound packet delivery to a namespace channel failed.
    #[error("manager send failed for namespace `{ns}`")]
    #[diagnostic(
        code(sioc_socket::manager::send_socket),
        help("the receiver was dropped; the socket is probably shut down")
    )]
    SendSocket {
        ns: ByteString,
        #[source]
        source: mpsc::error::SendError<Signal>,
    },

    /// Server ack arrived but the caller's receiver was already dropped.
    #[error("ack channel closed for namespace `{ns}`")]
    #[diagnostic(
        code(sioc_socket::manager::send_ack),
        help("the ack receiver was dropped; the namespace may have disconnected")
    )]
    SendAck { ns: ByteString, ack: DynAck },

    /// Received a text frame while a binary reassembly was in progress.
    #[error("unexpected text frame: {0:?}")]
    #[diagnostic(
        code(sioc_socket::manager::unexpected_text),
        help(
            "the server sent a text frame while binary reassembly was in progress; likely a server protocol bug"
        )
    )]
    UnexpectedText(ByteString),

    /// Received a binary frame with no pending reassembly.
    #[error("unexpected binary frame: {0:?}")]
    #[diagnostic(
        code(sioc_socket::manager::unexpected_binary),
        help(
            "the server sent a binary frame while no reassembly was pending; likely a server protocol bug"
        )
    )]
    UnexpectedBinary(Bytes),

    /// Operation on a namespace that is not open.
    #[error("unknown namespace `{ns}`")]
    #[diagnostic(
        code(sioc_socket::manager::unknown_namespace),
        help("connect the namespace before sending or receiving on it")
    )]
    UnknownNamespace { ns: ByteString },

    /// Ack ID in a server response has no registered handler.
    #[error("ack for unknown ID {id} in namespace `{ns}`")]
    #[diagnostic(
        code(sioc_socket::manager::unknown_ack_id),
        help(
            "the server sent an ack for an unregistered ID; the server may be replying to an already-acknowledged event"
        )
    )]
    UnknownAckId { ns: ByteString, id: u64 },

    /// Attempted to open a namespace that is already open.
    #[error("namespace conflict: `{ns}`")]
    #[diagnostic(
        code(sioc_socket::manager::namespace_conflict),
        help("the namespace is already open; drop the existing handle before reconnecting")
    )]
    NamespaceConflict { ns: ByteString },
}
