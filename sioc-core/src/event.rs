//! Event trait and utilities for Socket.IO events.
//!
//! This module provides a trait for defining typed Socket.IO events and utilities
//! for converting between events and packets.

use crate::builder::EventBuilder;
use crate::error::Result;
use crate::packet::BasePacket;
use crate::router::RouterCommand;
use bytes::Bytes;
use tokio::sync::mpsc;

/// Trait for types that can be emitted as Socket.IO events.
///
/// This trait is typically implemented using the `#[derive(Event)]` macro.
///
/// # Example
/// ```ignore
/// use sioc_macros::Event;
///
/// #[derive(Event)]
/// #[sioc(event = "message")]
/// struct Message {
///     text: String,
/// }
/// ```
pub trait Event: Sized {
    /// The event name (e.g., "message").
    fn name(&self) -> &'static str;

    /// Serializes the event into a JSON byte vector.
    ///
    /// Returns `["event_name", payload]` format.
    fn to_payload(&self) -> Result<Vec<u8>>;

    /// Deserializes the event from a JSON byte slice.
    ///
    /// Expects `["event_name", payload]` format.
    fn from_payload(payload: &[u8]) -> Result<Self>;
}

/// Create a new event builder from a typed event.
///
/// This function converts an event into a BasePacket and wraps it in an EventBuilder
/// for fluent API usage.
///
/// # Arguments
/// * `router_tx` - The router command sender
/// * `event` - The event to convert
///
/// # Returns
/// An EventBuilder for fluent emission.
///
/// # Example
/// ```ignore
/// use sioc_core::event::to_event;
///
/// let builder = to_event(client.sender(), MyEvent { data: "hello".into() })?;
/// builder.emit().await?;
/// ```
pub fn to_event(router_tx: mpsc::Sender<RouterCommand>, event: impl Event) -> Result<EventBuilder> {
    let data = event.to_payload()?;
    let packet = BasePacket::new("/".into(), Bytes::from(data));
    Ok(EventBuilder::new(router_tx, packet))
}

/// Parse a typed event from an incoming packet's data.
///
/// # Arguments
/// * `data` - The raw bytes from a BasePacket
///
/// # Returns
/// The deserialized event.
///
/// # Example
/// ```ignore
/// use sioc_core::event::from_event;
///
/// let event: MyEvent = from_event(&packet.data)?;
/// ```
pub fn from_event<E: Event>(data: &[u8]) -> Result<E> {
    E::from_payload(data)
}
