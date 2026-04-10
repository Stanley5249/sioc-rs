//! Engine.IO transport coordination.
//!
//! Manages the transport lifecycle: HTTP long-polling handshake, optional
//! upgrade to WebSocket, and clean shutdown via cancellation.

use crate::engine::EngineSender;
use crate::error::Result;
use crate::packet::{Frame, Handshake};
use crate::polling::polling_transport;
use crate::websocket::{WebSocketConnector, websocket_transport};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Debug, Default)]
pub enum TransportStrategy {
    #[default]
    Polling,
    WebSocket,
}

impl TransportStrategy {
    /// Spawns the transport coordination task and returns its handle.
    #[allow(clippy::too_many_arguments)]
    pub fn connect<C>(
        &self,
        base_url: Url,
        http_client: reqwest::Client,
        connector: C,
        handshake_tx: oneshot::Sender<Handshake>,
        engine_tx: EngineSender,
        transport_rx: mpsc::Receiver<Frame>,
        token: CancellationToken,
    ) -> JoinHandle<Result<()>>
    where
        C: WebSocketConnector,
    {
        match self {
            TransportStrategy::Polling => tokio::spawn(polling_transport(
                base_url,
                http_client,
                connector,
                handshake_tx,
                engine_tx,
                transport_rx,
                token,
            )),
            TransportStrategy::WebSocket => tokio::spawn(websocket_transport(
                base_url,
                connector,
                handshake_tx,
                engine_tx,
                transport_rx,
                token,
            )),
        }
    }
}
