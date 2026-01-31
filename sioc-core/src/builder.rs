//! Builder pattern for fluent event emission.
//!
//! Provides a type-safe, fluent API for constructing events with
//! optional attachments and choosing between Fast Path and Safe Path
//! emission strategies.

use crate::client::SocketSender;
use crate::error::Result;
use crate::packet::EventPacket;
use bytes::Bytes;
use tokio::sync::oneshot;

/// Builder for fluently constructing and emitting events.
///
/// Provides a fluent API for attaching binary data and choosing
/// between Fast Path and Safe Path emission.
#[derive(Debug)]
pub struct EventBuilder<'a> {
    /// The socket sender.
    sender: &'a SocketSender,

    /// The event packet.
    packet: EventPacket,
}

impl<'a> EventBuilder<'a> {
    /// Create a new EventBuilder.
    ///
    /// # Arguments
    /// * `sender` - The socket sender
    /// * `packet` - The event packet
    pub fn new(sender: &'a SocketSender, packet: EventPacket) -> Self {
        Self { sender, packet }
    }

    /// Attach binary data to this event.
    ///
    /// # Arguments
    /// * `bin` - The binary data to attach
    ///
    /// # Returns
    /// Self for chaining.
    pub fn attach(mut self, bin: Bytes) -> Self {
        self.packet.attachments.push(bin);
        self
    }

    /// Emit this event via the Fast Path (no acknowledgement expected).
    ///
    /// Sends the packet directly to the Engine without Router involvement.
    pub async fn emit(self) -> Result<()> {
        let packet = crate::packet::Packet::Event(self.packet);
        self.sender.emit(packet).await
    }

    /// Emit this event via the Safe Path (acknowledgement expected).
    ///
    /// Sends the packet through the Router for ID assignment and reply registration.
    ///
    /// # Returns
    /// A oneshot receiver for the acknowledgement reply.
    pub async fn ack(self) -> Result<oneshot::Receiver<crate::packet::Packet>> {
        self.sender.emit_with_ack(self.packet).await
    }
}
