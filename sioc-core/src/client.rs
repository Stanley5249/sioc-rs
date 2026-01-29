//! Client actor implementation for sioc-core.
//!
//! This module implements the "Router-Centric" architecture where:
//! - `SocketSender` provides dual-path emission (Fast vs Safe)
//! - `SocketReceiver` streams incoming packets
//! - `Client` is the public handle for connection management

use crate::builder::EventBuilder;
use crate::error::Result;
use crate::event::Event;
use crate::packet::{EventPayload, Packet, Payload};
use crate::router::{RouterCommand, router_loop};
use sioc_engine::prelude::EngineSender;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// Public Client Handle.
///
/// Connect to the server and spawn the background router task.
///
/// # Arguments
/// * `url` - The server URL (e.g., "http://localhost:3000")
///
/// # Returns
/// A tuple of `((SocketSender, SocketReceiver), JoinHandle)` where the handle can be used to
/// wait for the router task to complete.
pub async fn connect(url: String) -> Result<((SocketSender, SocketReceiver), JoinHandle<()>)> {
    // 1. Setup Engine (Active Actor)
    let (engine_tx, engine_rx) = sioc_engine::builder::ClientBuilder::new(url.parse()?)
        .build()
        .await?;

    // 2. Setup Router channels
    let (router_cmd_tx, router_cmd_rx) = mpsc::channel(32);
    let (user_event_tx, user_event_rx) = mpsc::channel(32);

    // 3. Spawn Router task
    let router_handle = tokio::spawn(router_loop(
        engine_rx,
        engine_tx.clone(),
        router_cmd_rx,
        user_event_tx,
    ));

    let sender = SocketSender {
        engine_tx,
        router_tx: router_cmd_tx,
    };

    let receiver = user_event_rx;

    Ok(((sender, receiver), router_handle))
}

/// Creates an event builder for fluent event emission.
///
/// # Arguments
/// * `sender` - The socket sender
/// * `event` - The event to emit
///
/// # Returns
/// An `EventBuilder` for chaining attachments and choosing emission path.
pub fn event<E: Event>(sender: &SocketSender, event: E) -> EventBuilder<'_, E> {
    EventBuilder::new(sender, "/".to_string(), event)
}

/// Emits a packet directly to the server (Fast Path).
///
/// This bypasses the Router and sends the packet immediately.
/// Use for packets that don't require acknowledgements.
pub async fn emit(sender: &SocketSender, packet: Packet) -> Result<()> {
    sender.emit(packet).await
}

/// Dual-path sender for Socket.IO packets.
///
/// Provides both Fast Path (direct to Engine) and Safe Path (via Router)
/// emission strategies.
#[derive(Clone, Debug)]
pub struct SocketSender {
    /// Direct channel to the Engine for Fast Path.
    engine_tx: EngineSender,

    /// Channel to the Router for Safe Path.
    router_tx: mpsc::Sender<RouterCommand>,
}

impl SocketSender {
    /// Emit a packet via the Fast Path (bypasses Router).
    ///
    /// Use this for events that don't expect acknowledgements.
    /// The packet goes directly to the Engine for immediate network write.
    pub async fn emit(&self, packet: Packet) -> Result<()> {
        // CHANGED: Use strict conversion
        let engine_packet = packet.to_engine_packet();
        self.engine_tx
            .send(engine_packet)
            .await
            .map_err(|_| crate::error::Error::Closed)
    }

    /// Emit an event payload expecting an acknowledgement (Safe Path).
    ///
    /// The Router will assign an ID, register the reply channel,
    /// and ensure proper sequencing before network write.
    ///
    /// # Returns
    /// A oneshot receiver for the acknowledgement reply.
    pub async fn emit_with_ack(
        &self,
        ns: String,
        payload: EventPayload,
    ) -> Result<oneshot::Receiver<Payload>> {
        let (tx, rx) = oneshot::channel();
        let cmd = RouterCommand::EmitWithAck {
            ns,
            data: payload,
            tx,
        };
        self.router_tx
            .send(cmd)
            .await
            .map_err(|_| crate::error::Error::Closed)?;
        Ok(rx)
    }
}

/// Receiver for incoming Socket.IO packets.
///
/// This is a type alias for the packet stream from the Router.
pub type SocketReceiver = mpsc::Receiver<Packet>;
