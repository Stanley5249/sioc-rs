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
    use crate::engine::FrameSender;
    use crate::error::TransportError;
    use tokio::sync::{mpsc, oneshot};
    use tokio_tungstenite::tungstenite::Error as TungsteniteError;
    use url::Url;

    #[test]
    fn transport_strategy_default_is_polling() {
        assert!(matches!(
            TransportStrategy::default(),
            TransportStrategy::Polling
        ));
    }

    #[tokio::test]
    async fn websocket_strategy_propagates_connector_error() {
        let base_url = Url::parse("ws://127.0.0.1:1/").unwrap();
        let http_client = reqwest::Client::new();
        let connector = async |_| Err(TungsteniteError::ConnectionClosed);
        let (handshake_tx, _handshake_rx) = oneshot::channel();
        let (engine_tx, _engine_rx) = mpsc::channel(1);
        let frame_tx = FrameSender(engine_tx);
        let (_transport_tx, transport_rx) = mpsc::channel(1);
        let token = CancellationToken::new();

        let result = TransportStrategy::WebSocket
            .run(
                base_url,
                http_client,
                connector,
                handshake_tx,
                frame_tx,
                transport_rx,
                token,
            )
            .await;

        assert!(matches!(result, Err(TransportError::WebSocket(_))));
    }
}
