//! Error types for the `sioc` public API.
//!
//! Each fallible operation returns a specific error type.  [`enum@Error`] is a
//! top-level convenience wrapper that aggregates all of them via [`From`] impls,
//! intended for application-level code that wants a single error type.

use miette::Diagnostic;
use sioc_socket::manager::ManagerAction;
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

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Top-level error aggregator for the `sioc` public API.
#[derive(Debug, Error, Diagnostic)]
#[error(transparent)]
#[diagnostic(transparent)]
pub enum Error {
    Builder(#[from] ClientBuilderError),
    Client(#[from] ClientError),
    Socket(#[from] SocketError),
    Event(#[from] EventError),
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
    Manager(#[from] sioc_socket::error::ManagerError),

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

/// Error converting a [`DynEvent`](sioc_socket::packet::DynEvent) into a typed [`Event`](crate::event::Event).
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
    #[error("ack ID was missing")]
    #[diagnostic(
        code(sioc::ack_id::missing),
        help(
            "event type declares `HasAck` but the server sent no ack ID; verify the server's protocol"
        )
    )]
    Missing,

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
    #[error("attachments were missing")]
    #[diagnostic(
        code(sioc::attachments::missing),
        help(
            "type declares `HasBinary` but no attachments were in the packet; verify the server's protocol"
        )
    )]
    Missing,

    #[error("attachments were unexpected")]
    #[diagnostic(
        code(sioc::attachments::unexpected),
        help(
            "type declares `NoBinary` but the packet contained attachments; consider using `HasBinary`"
        )
    )]
    Unexpected,
}
