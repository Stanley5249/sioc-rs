//! Event trait and utilities.

use crate::{error::Result, packet::EventPacket};
use bytes::Bytes;

/// Trait for types that can be emitted as Socket.IO events.
pub trait Event: Sized {
    /// The event name (e.g., "message").
    fn name(&self) -> &'static str;

    /// Serializes the event into a JSON byte vector.
    /// Returns `["event_name", payload]`.
    fn to_payload(&self) -> Result<Vec<u8>>;

    /// Deserializes the event from a JSON byte slice.
    fn from_payload(payload: &[u8]) -> Result<Self>;
}

/// Create a new event packet from a typed event.
pub fn to_event<E: Event>(event: E) -> Result<EventPacket> {
    let data = event.to_payload()?;
    // Zero-copy conversion from Vec<u8> to Bytes
    Ok(EventPacket::new("/".into(), Bytes::from(data)))
}

/// Parse a typed event from an incoming packet.
pub fn from_event<E: Event>(packet: EventPacket) -> Result<E> {
    // Pass slice to from_payload
    E::from_payload(&packet.data)
}
