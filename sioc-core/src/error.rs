//! Error types for Sioc operations.

use miette::Diagnostic;
use std::time::Duration;

/// Main error type for Sioc operations.
#[derive(Debug, Diagnostic, thiserror::Error)]
pub enum Error {
    /// JSON serialization/deserialization failed
    #[error("JSON error: {0}")]
    #[diagnostic(code(sioc::json_error))]
    Json(#[from] serde_json::Error),

    /// UTF-8 decoding error
    #[error("UTF-8 error: {0}")]
    #[diagnostic(code(sioc::utf8_error))]
    Utf8(#[from] std::str::Utf8Error),

    /// Network I/O error
    #[error("Network error: {0}")]
    #[diagnostic(code(sioc::network_error))]
    Network(#[from] std::io::Error),

    /// URL parse error
    #[error("URL parse error: {0}")]
    #[diagnostic(code(sioc::url_parse_error))]
    UrlParse(#[from] url::ParseError),

    /// Engine.IO error
    #[error("Engine.IO error: {0}")]
    #[diagnostic(code(sioc::engine_error))]
    Engine(#[from] sioc_engine::error::Error),

    /// Channel closed or connection lost
    #[error("Connection closed")]
    #[diagnostic(code(sioc::connection_closed))]
    Closed,

    /// Acknowledgement timeout
    #[error("Acknowledgement timeout after {0:?}")]
    #[diagnostic(code(sioc::ack_timeout))]
    AckTimeout(Duration),

    /// Invalid protocol or packet format
    #[error("Protocol error: {0}")]
    #[diagnostic(code(sioc::protocol_error))]
    Protocol(String),

    /// Invalid packet format
    #[error("Invalid packet format")]
    #[diagnostic(code(sioc::invalid_packet))]
    InvalidPacket,

    /// Unknown event received
    #[error("Unknown event: {0}")]
    #[diagnostic(code(sioc::unknown_event))]
    UnknownEvent(String),

    /// Binary attachment placeholder out of bounds
    #[error("Binary attachment {index} not found (total: {total})")]
    #[diagnostic(code(sioc::invalid_binary_placeholder))]
    InvalidBinaryPlaceholder {
        /// The requested index
        index: usize,
        /// Total number of attachments available
        total: usize,
    },
}

/// Result type alias for Sioc operations.
pub type Result<T> = std::result::Result<T, Error>;
