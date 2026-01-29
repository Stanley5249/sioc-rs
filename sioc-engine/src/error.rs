//! Error types for Engine.IO operations.

use miette::Diagnostic;

/// Main error type for Engine.IO operations.
#[derive(Debug, Diagnostic, thiserror::Error)]
pub enum Error {
    /// Invalid packet ID received
    #[error("Invalid packet id: {0}")]
    #[diagnostic(code(sioc_engine::invalid_packet_id))]
    InvalidPacketId(u8),

    /// Error while parsing an incomplete packet
    #[error("Error while parsing an incomplete packet")]
    #[diagnostic(code(sioc_engine::incomplete_packet))]
    IncompletePacket,

    /// Got an invalid packet which did not follow the protocol format
    #[error("Got an invalid packet which did not follow the protocol format")]
    #[diagnostic(code(sioc_engine::invalid_packet))]
    InvalidPacket,

    /// Network request returned with status code
    #[error("Network request returned with status code: {0}")]
    #[diagnostic(code(sioc_engine::incomplete_http))]
    IncompleteHttp(u16),

    /// Called an action before the connection was established
    #[error("Called an action before the connection was established")]
    #[diagnostic(code(sioc_engine::illegal_action_before_open))]
    IllegalActionBeforeOpen,

    /// Server did not allow upgrading to websockets
    #[error("Server did not allow upgrading to websockets")]
    #[diagnostic(code(sioc_engine::illegal_websocket_upgrade))]
    IllegalWebsocketUpgrade,

    /// HTTP request error
    #[error("HTTP request error: {0}")]
    #[diagnostic(code(sioc_engine::http_error))]
    Http(#[from] reqwest::Error),

    /// WebSocket connection error
    #[error("WebSocket error: {0}")]
    #[diagnostic(code(sioc_engine::websocket_error))]
    Websocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// UTF-8 decoding error
    #[error("UTF-8 decoding error: {0}")]
    #[diagnostic(code(sioc_engine::utf8_error))]
    Utf8(#[from] std::str::Utf8Error),

    /// UTF-8 string decoding error
    #[error("UTF-8 string decoding error: {0}")]
    #[diagnostic(code(sioc_engine::utf8_string_error))]
    Utf8String(#[from] std::string::FromUtf8Error),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    #[diagnostic(code(sioc_engine::json_error))]
    Json(#[from] serde_json::Error),

    /// Base64 decoding error
    #[error("Base64 decoding error: {0}")]
    #[diagnostic(code(sioc_engine::base64_error))]
    Base64(#[from] base64::DecodeError),

    /// Connection was closed
    #[error("Connection was closed")]
    #[diagnostic(code(sioc_engine::connection_closed))]
    Closed,
}

pub type Result<T> = std::result::Result<T, Error>;
