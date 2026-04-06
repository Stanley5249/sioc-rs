// thiserror/miette proc-macros read these fields via generated impls, but
// rustc's unused_assignments lint doesn't see those reads.
#![allow(unused_assignments)]

//! Error types for the `sioc` public API.
//!
//! [`enum@Error`] is the top-level enum returned by all fallible operations in this
//! crate.  It wraps protocol errors from `sioc-socket`, JSON serialization
//! failures, and application-level validation errors from the marker-policy
//! system.

use miette::Diagnostic;
pub use sioc_socket::error::PayloadError;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::JoinError;

/// A marker-policy validation failed.
#[derive(Debug, Error, Diagnostic)]
pub enum MarkerError {
    /// An ack ID was present but the event type uses [`NoAck`](crate::marker::NoAck).
    #[error("unexpected ack ID was provided")]
    #[diagnostic(code(sioc::marker::unexpected_ack_id))]
    UnexpectedAckId,

    /// No ack ID was present but the event type uses [`HasAck`](crate::marker::HasAck).
    #[error("expected ack ID was not provided")]
    #[diagnostic(code(sioc::marker::missing_ack_id))]
    MissingAckId,

    /// Binary attachments were present but the type uses [`NoBinary`](crate::marker::NoBinary).
    #[error("unexpected binary attachments were provided")]
    #[diagnostic(code(sioc::marker::unexpected_binary))]
    UnexpectedBinary,

    /// No binary attachments were present but the type uses [`HasBinary`](crate::marker::HasBinary).
    #[error("expected binary attachments were not provided")]
    #[diagnostic(code(sioc::marker::missing_binary))]
    MissingBinary,
}

/// Top-level error returned by all `sioc` public APIs.
#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    /// JSON serialization failed.
    #[error(transparent)]
    #[diagnostic(code(sioc::ser), help("ensure all values are JSON-compatible"))]
    Payload(#[from] PayloadError),

    /// URL construction failed.
    #[error("invalid URL")]
    #[diagnostic(code(sioc::invalid_url))]
    Url(#[from] url::ParseError),

    /// A marker-policy validation failed.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Marker(#[from] MarkerError),

    /// The ack oneshot channel was closed before a response arrived.
    #[error("ack channel closed")]
    #[diagnostic(code(sioc::ack_channel_closed))]
    ReceiveAck(#[from] oneshot::error::RecvError),

    /// The socket router task panicked or was cancelled.
    #[error("socket router task failed")]
    #[diagnostic(code(sioc::task))]
    Task(#[from] JoinError),

    /// A protocol or transport error propagated from the core layer.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Core(#[from] sioc_socket::error::Error),

    /// A transport or Engine.IO error.
    #[error(transparent)]
    #[diagnostic(transparent)]
    Engine(#[from] sioc_engine::error::Error),
}

/// Convenience alias used throughout `sioc`.
pub type Result<T, E = Error> = std::result::Result<T, E>;
