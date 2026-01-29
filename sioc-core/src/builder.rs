//! Builder pattern for fluent event emission.
//!
//! Provides a type-safe, fluent API for constructing events with
//! optional attachments and choosing between Fast Path and Safe Path
//! emission strategies.

use crate::client::SocketSender;
use crate::error::Result;
use crate::event::Event;
use crate::packet::Attachments;
use crate::packet::Payload;
use bytes::Bytes;
use tokio::sync::oneshot;

/// Builder for fluently constructing and emitting events.
///
/// Provides a fluent API for attaching binary data and choosing
/// between Fast Path and Safe Path emission.
#[derive(Debug)]
pub struct EventBuilder<'a, E: Event> {
    /// The socket sender.
    sender: &'a SocketSender,

    /// Namespace for the event.
    ns: String,

    /// The event data.
    event: E,

    /// Binary attachments to include.
    attachments: Attachments,
}

impl<'a, E: Event> EventBuilder<'a, E> {
    /// Create a new EventBuilder.
    ///
    /// # Arguments
    /// * `sender` - The socket sender
    /// * `ns` - The namespace
    /// * `event` - The event to build
    pub fn new(sender: &'a SocketSender, ns: String, event: E) -> Self {
        Self {
            sender,
            ns,
            event,
            attachments: Attachments::new(),
        }
    }

    /// Attach binary data to this event.
    ///
    /// # Arguments
    /// * `bin` - The binary data to attach
    ///
    /// # Returns
    /// Self for chaining.
    pub fn attach(mut self, bin: Bytes) -> Self {
        self.attachments.push(bin);
        self
    }

    /// Emit this event via the Fast Path (no acknowledgement expected).
    ///
    /// Serializes the event, constructs a Packet, and sends it directly
    /// to the Engine without Router involvement.
    pub async fn emit(self) -> Result<()> {
        let mut payload = self.event.into_event_payload()?;

        // Merge manual attachments
        if !self.attachments.is_empty() {
            payload.attachments.extend(self.attachments);
        }

        let packet = crate::packet::Packet {
            ns: self.ns,
            inner: crate::packet::Payload::Event(payload),
        };

        self.sender.emit(packet).await
    }

    /// Emit this event via the Safe Path (acknowledgement expected).
    ///
    /// Serializes the event, constructs an EventPayload, and sends it
    /// through the Router for ID assignment and reply registration.
    ///
    /// # Returns
    /// A oneshot receiver for the acknowledgement reply.
    pub async fn ack(self) -> Result<oneshot::Receiver<Payload>> {
        let mut payload = self.event.into_event_payload()?;

        // Merge manual attachments
        if !self.attachments.is_empty() {
            payload.attachments.extend(self.attachments);
        }

        self.sender.emit_with_ack(self.ns, payload).await
    }
}
