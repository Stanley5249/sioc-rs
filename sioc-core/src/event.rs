//! Event trait and utilities for typed Socket.IO events.
//!
//! This module provides the [`Event`] trait for types that can be emitted
//! as Socket.IO events, along with helper functions for event serialization.

use crate::{error::Result, packet::EventPacket};
use bytes::Bytes;

/// Trait for types that can be emitted as Socket.IO events.
pub trait Event {
    /// The event name (e.g., "message").
    fn name(&self) -> &'static str;

    /// Serializes the event into JSON bytes.
    /// Returns `["event_name", payload]`.
    fn to_payload(&self) -> Result<Bytes>;
}

/// Create a new event packet from a typed event.
///
/// This helper converts any type implementing `Event` into a
/// `EventPacket` ready for transmission.
pub fn to_event<E: Event>(event: E) -> Result<EventPacket> {
    let data = event.to_payload()?;
    Ok(EventPacket::new("/".into(), data))
}
