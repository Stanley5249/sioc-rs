//! Builder pattern for fluent event emission.
//!
//! Provides a type-safe, fluent API for constructing events with
//! optional attachments and choosing between fire-and-forget or
//! acknowledgement-based emission strategies.

use crate::error::{Error, Result};
use crate::packet::{Attachments, BasePacket, BinaryPacket};
use crate::router::RouterCommand;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

/// Builder for fluently constructing and emitting events.
///
/// Provides a fluent API for attaching binary data and choosing
/// between fire-and-forget and acknowledgement-based emission.
#[derive(Debug)]
pub struct EventBuilder {
    /// The base packet data.
    packet: BasePacket,
    /// Binary attachments (if any).
    attachments: Attachments,
    /// Router command sender.
    router_tx: mpsc::Sender<RouterCommand>,
}

impl EventBuilder {
    /// Create a new EventBuilder.
    ///
    /// # Arguments
    /// * `router_tx` - The router command sender
    /// * `packet` - The base packet data
    pub fn new(router_tx: mpsc::Sender<RouterCommand>, packet: BasePacket) -> Self {
        Self {
            packet,
            attachments: Attachments::new(),
            router_tx,
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

    /// Emit this event without expecting an acknowledgement.
    ///
    /// Sends the event through the router for transmission.
    pub async fn emit(self) -> Result<()> {
        let cmd = if self.attachments.is_empty() {
            // Standard event (Type 2)
            RouterCommand::SendEvent(self.packet)
        } else {
            // Binary event (Type 5)
            let header = BinaryPacket::new(self.packet, self.attachments.len() as u64);
            RouterCommand::SendBinaryEvent {
                header,
                payload: self.attachments,
            }
        };

        self.router_tx.send(cmd).await.map_err(|_| Error::Closed)
    }

    /// Emit this event expecting an acknowledgement.
    ///
    /// Sends the event through the router with ID assignment and ack registration.
    ///
    /// # Returns
    /// A oneshot receiver for the acknowledgement reply.
    pub async fn ack(self) -> Result<oneshot::Receiver<crate::router::SioMessage>> {
        let (tx, rx) = oneshot::channel();

        let cmd = if self.attachments.is_empty() {
            // Standard event with ack (Type 2 + ID)
            RouterCommand::SendEventWithAck {
                data: self.packet,
                ack: tx,
            }
        } else {
            // Binary event with ack (Type 5 + ID)
            let header = BinaryPacket::new(self.packet, self.attachments.len() as u64);
            RouterCommand::SendBinaryWithAck {
                header,
                payload: self.attachments,
                ack: tx,
            }
        };

        self.router_tx.send(cmd).await.map_err(|_| Error::Closed)?;

        Ok(rx)
    }
}
