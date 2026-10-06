//! WebSocket connections that the transports open.

use std::future::Future;

use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::{MaybeTlsStream, connect_async};
use url::Url;

/// A WebSocket connector that can open a stream from a [`Url`].
pub trait WebSocketConnector: Send + 'static {
    /// Opens a WebSocket connection for the provided [`Url`].
    fn connect(
        self,
        url: Url,
    ) -> impl Future<Output = Result<WebSocketStream, TungsteniteError>> + Send;
}

impl<F, Fut> WebSocketConnector for F
where
    F: FnOnce(Url) -> Fut + Send + 'static,
    Fut: Future<Output = Result<WebSocketStream, TungsteniteError>> + Send + 'static,
{
    fn connect(
        self,
        url: Url,
    ) -> impl Future<Output = Result<WebSocketStream, TungsteniteError>> + Send {
        self(url)
    }
}

/// Opens a plain `tokio-tungstenite` WebSocket connection with no custom TLS
/// or header configuration.
impl WebSocketConnector for () {
    async fn connect(self, url: Url) -> Result<WebSocketStream, TungsteniteError> {
        let (stream, _) = connect_async(url).await?;
        Ok(stream)
    }
}

/// An open WebSocket connection that carries Engine.IO [`Frame`]s.
pub type WebSocketStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;
