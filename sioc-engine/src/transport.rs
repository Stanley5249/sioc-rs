//! Engine.IO transport coordination.
//!
//! Manages the transport lifecycle: HTTP long-polling handshake, optional
//! upgrade to WebSocket, and clean shutdown via cancellation.

use crate::engine::EngineSender;
use crate::error::TransportError;
use crate::packet::{Frame, Handshake};
use crate::polling::PollingClient;
use crate::websocket::{WebSocketConnector, WebSocketStream};
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
    ) -> JoinHandle<Result<(), TransportError>>
    where
        C: WebSocketConnector,
    {
        match self {
            TransportStrategy::Polling => tokio::spawn(PollingClient(http_client).transport(
                base_url,
                connector,
                handshake_tx,
                engine_tx,
                transport_rx,
                token,
            )),
            TransportStrategy::WebSocket => tokio::spawn({
                async {
                    let stream = WebSocketStream::connect(base_url, None, connector).await?;

                    stream
                        .transport(Some(handshake_tx), engine_tx, transport_rx, token)
                        .await?;

                    Ok(())
                }
            }),
        }
    }
}
