//! Engine.IO transport coordination.
//!
//! Manages the transport lifecycle: HTTP long-polling handshake, optional
//! upgrade to WebSocket, and shutdown once the engine closes the transport channel.

use crate::error::TransportError;
use crate::packet::{Frame, Handshake};
use crate::polling;
use crate::websocket::{self, WebSocketConnector};
use tokio::sync::{mpsc, oneshot};
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
        server_frame_tx: mpsc::Sender<Frame>,
        client_frame_rx: mpsc::Receiver<Frame>,
    ) -> Result<(), TransportError>
    where
        C: WebSocketConnector + Send + 'static,
    {
        match self {
            TransportStrategy::Polling => {
                polling::transport(
                    http_client,
                    base_url,
                    connector,
                    handshake_tx,
                    server_frame_tx,
                    client_frame_rx,
                )
                .await
            }
            TransportStrategy::WebSocket => {
                let stream = websocket::connect(base_url, None, connector).await?;

                websocket::transport(stream, Some(handshake_tx), server_frame_tx, client_frame_rx)
                    .await?;

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let (server_frame_tx, _frame_rx) = mpsc::channel(1);
        let (_transport_tx, client_frame_rx) = mpsc::channel(1);

        let result = TransportStrategy::WebSocket
            .run(
                base_url,
                http_client,
                connector,
                handshake_tx,
                server_frame_tx,
                client_frame_rx,
            )
            .await;

        assert!(matches!(result, Err(TransportError::WebSocket(_))));
    }
}
