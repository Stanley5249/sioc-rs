//! A request to run one engine, and the handles the client-packet loop keeps.

use bytestring::ByteString;
use eioc::prelude::Message;
use tokio::sync::{mpsc, oneshot};

use crate::error::ManagerError;

/// What the client-packet loop hears about its engine, like the events
/// socket.io-client's `Manager` subscribes to. These are the manager's own
/// notifications, not Socket.IO events.
///
/// The engine channel carries only `Open` and `Connect`;
/// [`ReadyState::next_event`](crate::manager::engine::ReadyState::next_event)
/// adds `Close` and `Reconnect`.
#[derive(Debug)]
pub enum EngineEvent {
    /// The server accepted the Engine.IO handshake, like engine.io-client's
    /// `"open"` event.
    Open,
    /// The server confirmed this namespace, like socket.io-client's
    /// `"connect"` event.
    Connect(ByteString),
    /// The engine finished, like engine.io-client's `"close"` event.
    Close(Result<(), ManagerError>),
    /// The backoff delay is over, like the timer that calls socket.io-client's
    /// `Manager.reconnect`.
    Reconnect,
}

/// An engine for [`run_engines`](crate::manager::engine::run_engines) to run,
/// like socket.io-client's `Manager.open`.
#[derive(Debug)]
pub struct OpenRequest {
    /// The reconnection attempt that opens this engine, or 0 for a first one.
    pub attempt: u32,
    /// What the client-packet loop sends. Its end closes the engine.
    pub client_message_rx: mpsc::Receiver<Message>,
    /// Tells the client-packet loop that the engine opened and which
    /// namespaces the server confirmed.
    pub engine_event_tx: mpsc::UnboundedSender<EngineEvent>,
    /// Reports the engine's result once it finishes.
    pub engine_result_tx: oneshot::Sender<Result<(), ManagerError>>,
}

/// The ends of an [`OpenRequest`]'s channels that the client-packet loop keeps.
#[derive(Debug)]
pub struct OpenHandles {
    /// Carries client messages to the engine. Dropping it closes the engine.
    pub client_message_tx: mpsc::Sender<Message>,
    /// Reports that the engine opened and which namespaces the server
    /// confirmed, and ends once the engine stops delivering server messages.
    pub engine_event_rx: mpsc::UnboundedReceiver<EngineEvent>,
    /// Receives the engine's result.
    pub engine_result_rx: oneshot::Receiver<Result<(), ManagerError>>,
}

impl OpenRequest {
    /// Builds the request for one engine, with the channel ends the
    /// client-packet loop keeps.
    pub fn new(attempt: u32, client_message_capacity: usize) -> (Self, OpenHandles) {
        let (client_message_tx, client_message_rx) = mpsc::channel(client_message_capacity);

        // The channel is unbounded so that delivering server packets never
        // waits on the client's sending direction, which waits on the engine,
        // which waits on server packet delivery. Its length stays below the
        // number of namespaces opened in this engine, because each opening
        // reports `Connect` at most once, plus one `Open`, so the server cannot
        // grow it.
        let (engine_event_tx, engine_event_rx) = mpsc::unbounded_channel();

        let (engine_result_tx, engine_result_rx) = oneshot::channel();

        let request = Self {
            attempt,
            client_message_rx,
            engine_event_tx,
            engine_result_tx,
        };
        let handles = OpenHandles {
            client_message_tx,
            engine_event_rx,
            engine_result_rx,
        };

        (request, handles)
    }
}
