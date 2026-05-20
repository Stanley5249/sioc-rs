#![allow(unused_assignments)] // named fields in #[error("...")] trigger this spuriously

//! Error types for the `sioc` crate.
//!
//! Each fallible operation returns a specific error type.  [`enum@Error`] is a
//! top-level convenience wrapper that aggregates all of them via [`From`] impls,
//! intended for application-level code that wants a single error type.

use crate::manager::ManagerAction;
use crate::packet::{DynAck, Signal};
use bytes::Bytes;
use bytestring::ByteString;
use eioc::engine::EngineAction;
use miette::Diagnostic;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio::time::error::Elapsed;

/// JSON serialization or deserialization failure.
#[derive(Debug, Error, Diagnostic)]
#[error("payload error for `{type_name}`: {source}")]
#[diagnostic(code(sioc::payload))]
pub struct PayloadError {
    type_name: &'static str,
    #[source]
    source: serde_path_to_error::Error<serde_json::Error>,
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

/// Shorthand result type defaulting to the top-level [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Top-level error aggregator for the `sioc` public API.
#[derive(Debug, Error, Diagnostic)]
#[error(transparent)]
#[diagnostic(transparent)]
pub enum Error {
    /// Error from [`ClientBuilder::open`](crate::client::ClientBuilder::open).
    Builder(#[from] ClientBuilderError),
    /// Error from [`Client::join`](crate::client::Client::join).
    Client(#[from] ClientError),
    /// Error from a [`SocketSender`](crate::client::SocketSender) operation.
    Socket(#[from] SocketError),
    /// Error converting a [`DynEvent`](crate::packet::DynEvent) into a typed event.
    Event(#[from] EventError),
    /// Error receiving or parsing an acknowledgement.
    Ack(#[from] AckError),
}

/// Error returned by [`ClientBuilder::open`](crate::client::ClientBuilder::open).
#[derive(Debug, Error, Diagnostic)]
pub enum ClientBuilderError {
    /// URL construction failed.
    #[error("invalid URL")]
    #[diagnostic(code(sioc::builder::url))]
    Url(#[from] url::ParseError),
}

/// Error returned by [`Client::join`](crate::client::Client::join).
#[derive(Debug, Error, Diagnostic)]
pub enum ClientError {
    /// Error propagated from the socket manager.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Manager(#[from] ManagerError),

    /// Background manager task panicked or was cancelled.
    #[error("failed to join socket manager task")]
    #[diagnostic(code(sioc::client::join))]
    Join(#[from] JoinError),
}

/// Error returned by [`SocketSender`](crate::client::SocketSender) operations.
#[derive(Debug, Error, Diagnostic)]
pub enum SocketError {
    /// Directive channel to the socket manager is closed.
    #[error("failed to send directive to socket manager")]
    #[diagnostic(code(sioc::socket::send))]
    Send(#[from] mpsc::error::SendError<ManagerAction>),

    /// Event payload serialization failed.
    #[error("failed to serialize payload")]
    #[diagnostic(code(sioc::socket::payload))]
    Payload(#[from] PayloadError),
}

/// Error converting a [`DynEvent`](crate::packet::DynEvent) into a typed [`Event`](crate::event::Event).
#[derive(Debug, Error, Diagnostic)]
pub enum EventError {
    /// Event payload deserialization failed.
    #[error("failed to deserialize event payload")]
    #[diagnostic(code(sioc::event::payload))]
    Payload(#[from] PayloadError),

    /// Ack ID presence did not match the event type's policy.
    #[error("invalid ack ID for the event type")]
    #[diagnostic(code(sioc::event::ack_id))]
    AckId(#[from] AckIdError),

    /// Attachment presence did not match the event type's policy.
    #[error("invalid attachments for the event type")]
    #[diagnostic(code(sioc::event::attachments))]
    Attachments(#[from] AttachmentsError),
}

/// Error returned when receiving or parsing a typed acknowledgement.
#[derive(Debug, Error, Diagnostic)]
pub enum AckError {
    /// Server dropped the ack channel before responding.
    #[error("failed to receive ack")]
    #[diagnostic(
        code(sioc::ack::recv),
        help("the ack sender was dropped before responding; the connection may have been lost")
    )]
    Recv(#[from] oneshot::error::RecvError),

    /// Ack payload deserialization failed.
    #[error("failed to parse ack payload")]
    #[diagnostic(code(sioc::ack::payload))]
    Payload(#[from] PayloadError),

    /// Attachment presence did not match the ack type's policy.
    #[error("invalid attachments for the ack type")]
    #[diagnostic(code(sioc::ack::attachments))]
    Attachments(#[from] AttachmentsError),

    /// The deadline elapsed before the server responded.
    #[error("ack timed out")]
    #[diagnostic(code(sioc::ack::timeout))]
    Timeout(#[from] Elapsed),
}

/// Ack ID presence mismatch between the inbound packet and the event type's policy.
#[derive(Debug, Error, Diagnostic)]
pub enum AckIdError {
    /// Event declares `HasAck` but the server sent no ack ID.
    #[error("ack ID was missing")]
    #[diagnostic(
        code(sioc::ack_id::missing),
        help(
            "event type declares `HasAck` but the server sent no ack ID; verify the server's protocol"
        )
    )]
    Missing,

    /// Event declares `NoAck` but the server sent an ack ID.
    #[error("ack ID was unexpected")]
    #[diagnostic(
        code(sioc::ack_id::unexpected),
        help(
            "event type declares `NoAck` but the server sent an ack ID; consider using `HasAck<A>`"
        )
    )]
    Unexpected,
}

/// Attachment presence mismatch between the inbound packet and the type's binary policy.
#[derive(Debug, Error, Diagnostic)]
pub enum AttachmentsError {
    /// Type declares `HasBinary` but no attachments were in the packet.
    #[error("attachments were missing")]
    #[diagnostic(
        code(sioc::attachments::missing),
        help(
            "type declares `HasBinary` but no attachments were in the packet; verify the server's protocol"
        )
    )]
    Missing,

    /// Type declares `NoBinary` but the packet contained attachments.
    #[error("attachments were unexpected")]
    #[diagnostic(
        code(sioc::attachments::unexpected),
        help(
            "type declares `NoBinary` but the packet contained attachments; consider using `HasBinary`"
        )
    )]
    Unexpected,
}

/// Errors from decoding a raw Socket.IO packet.
///
/// Callers receive this wrapped in [`ManagerError::Packet`].
#[derive(Debug, Error, Diagnostic)]
#[allow(missing_docs)]
pub enum PacketError {
    /// JSON payload in the packet is malformed.
    #[error(transparent)]
    #[diagnostic(code(sioc::parse::json))]
    Json(#[from] serde_json::Error),

    /// Packet bytes are not valid UTF-8.
    #[error("invalid UTF-8 in packet")]
    #[diagnostic(code(sioc::parse::utf8))]
    Utf8(#[from] std::str::Utf8Error),

    /// No bytes were available to read.
    #[error("packet is empty")]
    #[diagnostic(code(sioc::parse::empty_packet))]
    Empty,

    /// First byte does not map to any known packet type.
    #[error("unknown packet type {id}")]
    #[diagnostic(code(sioc::parse::unknown_packet_type))]
    InvalidId { id: char },

    /// Binary packet header has no attachment count before the `-` separator.
    #[error("binary packet missing attachment count prefix")]
    #[diagnostic(code(sioc::parse::missing_attachment_count))]
    MissingAttachmentCount,

    /// Text event packet carries a non-zero attachment count.
    #[error("text event packet has unexpected attachment count ({count})")]
    #[diagnostic(code(sioc::parse::unexpected_attachments))]
    UnexpectedAttachments { count: usize },

    /// Attachment count prefix is present but not a valid integer.
    #[error("attachment count is not a valid integer")]
    #[diagnostic(code(sioc::parse::invalid_attachment_count))]
    InvalidAttachmentCount(#[source] std::num::ParseIntError),

    /// Non-default namespace is missing the `,` delimiter after the path.
    #[error("namespace missing trailing `,` delimiter")]
    #[diagnostic(code(sioc::parse::missing_namespace_delimiter))]
    MissingNamespaceDelimiter,

    /// Ack packet has no numeric ID field.
    #[error("ack packet missing numeric ID")]
    #[diagnostic(code(sioc::parse::missing_ack_id))]
    MissingAckId,

    /// Ack ID field is present but not a valid integer.
    #[error("packet ID is not a valid integer")]
    #[diagnostic(code(sioc::parse::invalid_ack_id))]
    InvalidAckId(#[source] std::num::ParseIntError),
}

/// The top-level error type for Socket.IO manager operations.
#[derive(Debug, Error, Diagnostic)]
#[allow(missing_docs)]
pub enum ManagerError {
    /// Error propagated from the Engine.IO transport layer.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Engine(#[from] eioc::error::Error),

    /// Wraps a [`PacketError`] from packet decoding.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Packet(#[from] PacketError),

    /// Sending an action to the engine layer failed because the channel is closed.
    #[error("engine action channel closed")]
    #[diagnostic(
        code(sioc::manager::send_engine),
        help("the receiver was dropped; the socket is probably shut down")
    )]
    SendEngine(#[from] mpsc::error::SendError<EngineAction>),

    /// Inbound packet delivery to a namespace channel failed.
    #[error("manager send failed for namespace `{ns}`")]
    #[diagnostic(
        code(sioc::manager::send_socket),
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
        code(sioc::manager::send_ack),
        help("the ack receiver was dropped; the namespace may have disconnected")
    )]
    SendAck { ns: ByteString, ack: DynAck },

    /// Received a text frame while a binary reassembly was in progress.
    #[error("unexpected text frame: {0:?}")]
    #[diagnostic(
        code(sioc::manager::unexpected_text),
        help(
            "the server sent a text frame while binary reassembly was in progress; likely a server protocol bug"
        )
    )]
    UnexpectedText(ByteString),

    /// Received a binary frame with no pending reassembly.
    #[error("unexpected binary frame: {0:?}")]
    #[diagnostic(
        code(sioc::manager::unexpected_binary),
        help(
            "the server sent a binary frame while no reassembly was pending; likely a server protocol bug"
        )
    )]
    UnexpectedBinary(Bytes),

    /// Operation on a namespace that is not open.
    #[error("unknown namespace `{ns}`")]
    #[diagnostic(
        code(sioc::manager::unknown_namespace),
        help("connect the namespace before sending or receiving on it")
    )]
    UnknownNamespace { ns: ByteString },

    /// Ack ID in a server response has no registered handler.
    #[error("ack for unknown ID {id} in namespace `{ns}`")]
    #[diagnostic(
        code(sioc::manager::unknown_ack_id),
        help(
            "the server sent an ack for an unregistered ID; the server may be replying to an already-acknowledged event"
        )
    )]
    UnknownAckId { ns: ByteString, id: u64 },

    /// Attempted to open a namespace that is already open.
    #[error("namespace conflict: `{ns}`")]
    #[diagnostic(
        code(sioc::manager::namespace_conflict),
        help("the namespace is already open; drop the existing handle before reconnecting")
    )]
    NamespaceConflict { ns: ByteString },
}
