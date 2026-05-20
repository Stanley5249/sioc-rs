//! Engine.IO transport coordination.
//!
//! Manages the transport lifecycle: HTTP long-polling handshake, optional
//! upgrade to WebSocket, and clean shutdown via cancellation.

use crate::engine::FrameSender;
use crate::error::TransportError;
use crate::packet::{Frame, Handshake};
use crate::polling::PollingClient;
use crate::websocket::{WebSocketConnector, WebSocketStream};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use url::Url;

/// Selects which transport to use when opening an Engine.IO connection.
#[derive(Debug, Default)]
pub enum TransportStrategy {
    /// Start with HTTP long-polling, then upgrade to WebSocket when the server offers it.
    #[default]
    Polling,
    /// Connect directly over WebSocket, skipping the polling handshake.
    WebSocket,
}

impl TransportStrategy {
    /// Runs the transport lifecycle to completion.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport encounters a protocol or I/O failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn run<C>(
        self,
        base_url: Url,
        http_client: reqwest::Client,
        connector: C,
        handshake_tx: oneshot::Sender<Handshake>,
        frame_tx: FrameSender,
        transport_rx: mpsc::Receiver<Frame>,
        token: CancellationToken,
    ) -> Result<(), TransportError>
    where
        C: WebSocketConnector + Send + 'static,
    {
        match self {
            TransportStrategy::Polling => {
                PollingClient(http_client)
                    .transport(
                        base_url,
                        connector,
                        handshake_tx,
                        frame_tx,
                        transport_rx,
                        token,
                    )
                    .await
            }
            TransportStrategy::WebSocket => {
                let stream = WebSocketStream::connect(base_url, None, connector).await?;

                stream
                    .transport(Some(handshake_tx), frame_tx, transport_rx)
                    .await?;

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_strategy_default_is_polling() {
        assert!(matches!(
            TransportStrategy::default(),
            TransportStrategy::Polling
        ));
    }
}
