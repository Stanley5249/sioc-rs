use crate::error::Result;
use bytes::Bytes;

/// Trait for types that can be emitted as Socket.IO events.
pub trait Event: serde::Serialize {
    /// The event name (e.g., "message").
    fn name(&self) -> &'static str;

    /// Serializes the event into JSON bytes.
    /// Returns `["event_name", payload]`.
    fn to_json(&self) -> Result<Bytes>;
}
