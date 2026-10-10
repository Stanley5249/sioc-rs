//! Error types for the `sioc` crate.
//!
//! Each fallible operation returns a specific error type.  [`enum@Error`] is a
//! top-level convenience wrapper that aggregates all of them via [`From`]
//! impls, intended for application-level code that wants a single error type.

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
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
    #[must_use]
    pub fn new<T>(source: serde_path_to_error::Error<serde_json::Error>) -> Self {
        Self {
            type_name: std::any::type_name::<T>(),
            source,
        }
    }
}

/// Shorthand result type defaulting to the top-level [`enum@Error`].
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
    /// Error converting a [`DynEvent`](crate::packet::DynEvent) into a typed
    /// event.
    Event(#[from] EventError),
    /// Error receiving or parsing an acknowledgement.
    Ack(#[from] AckError),
}

/// Error returned by
/// [`ClientBuilder::open`](crate::client::ClientBuilder::open).
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
    /// A namespace with this name is already open on the client.
    #[error("namespace conflict: `{ns}`")]
    #[diagnostic(code(sioc::socket::namespace_conflict))]
    NamespaceConflict {
        /// The namespace that already has an open route.
        ns: ByteString,
    },
    /// The namespace or the whole session has closed.
    #[error("socket closed")]
    #[diagnostic(
        code(sioc::socket::closed),
        help("the namespace was disconnected or the client ended")
    )]
    Closed,

    /// Event payload serialization failed.
    #[error("failed to serialize payload")]
    #[diagnostic(code(sioc::socket::payload))]
    Payload(#[from] PayloadError),
}

/// Error converting a [`DynEvent`](crate::packet::DynEvent) into a typed
/// [`Event`](crate::event::Event).
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

/// Ack ID presence mismatch between the inbound packet and the event type's
/// policy.
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

/// Attachment presence mismatch between the inbound packet and the type's
/// binary policy.
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
    InvalidId {
        /// The first byte of the packet.
        id: char,
    },

    /// Binary packet header has no attachment count before the `-` separator.
    #[error("binary packet missing attachment count prefix")]
    #[diagnostic(code(sioc::parse::missing_attachment_count))]
    MissingAttachmentCount,

    /// Text event packet carries a non-zero attachment count.
    #[error("text event packet has unexpected attachment count ({count})")]
    #[diagnostic(code(sioc::parse::unexpected_attachments))]
    UnexpectedAttachments {
        /// The attachment count the packet declared.
        count: usize,
    },

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
pub enum ManagerError {
    /// Error propagated from the Engine.IO transport layer.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Engine(#[from] eioc::error::Error),

    /// Wraps a [`PacketError`] from packet decoding.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Packet(#[from] PacketError),

    /// Sending a client message to the engine failed because the channel is
    /// closed.
    #[error("client message channel closed")]
    #[diagnostic(
        code(sioc::manager::client_message),
        help("library bug, please report")
    )]
    ClientMessage(#[from] mpsc::error::SendError<Message>),

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

    /// Telling the client-packet loop that the engine opened or the server
    /// confirmed a namespace failed because the channel is closed.
    #[error("server event channel closed")]
    #[diagnostic(code(sioc::manager::server_event), help("library bug, please report"))]
    ServerEvent,
}

impl ManagerError {
    /// Returns whether the error is a library bug, such as a broken channel
    /// between library tasks, rather than a network or server fault.
    ///
    /// The client reconnects after any other error.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        // No wildcard arm, so a new variant must be classified here.
        match self {
            Self::Engine(error) => error.is_internal(),
            Self::Packet(_) | Self::UnexpectedText(_) | Self::UnexpectedBinary(_) => false,
            Self::ClientMessage(_) | Self::ServerEvent => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use eioc::error::EngineError;

    use super::*;

    #[test]
    fn manager_error_server_faults_are_not_internal() {
        let errors = [
            ManagerError::Engine(EngineError::HeartbeatTimeout.into()),
            ManagerError::Packet(PacketError::Empty),
            ManagerError::UnexpectedText(ByteString::new()),
            ManagerError::UnexpectedBinary(Bytes::new()),
        ];
        for error in errors {
            assert!(!error.is_internal(), "{error:?}");
        }
    }

    #[test]
    fn manager_error_channel_failures_are_internal() {
        let errors = [
            ManagerError::Engine(
                EngineError::ClientFrame(mpsc::error::SendError(Bytes::new().into())).into(),
            ),
            ManagerError::ClientMessage(mpsc::error::SendError(Message::Text(ByteString::new()))),
            ManagerError::ServerEvent,
        ];
        for error in errors {
            assert!(error.is_internal(), "{error:?}");
        }
    }
}
