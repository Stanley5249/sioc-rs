//! WebSocket transport for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::error::{Error, Result};
use crate::packet::{EioPacket, Frame, PROBE};
use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use std::ops::ControlFlow;
use tokio::net::TcpStream;
pub use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::{MaybeTlsStream, connect_async};
use tracing::debug;
use url::Url;

pub type WebSocketStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

fn upgrade_schema(mut url: Url) -> Url {
    match url.scheme() {
        "http" => url.set_scheme("ws").expect("ws is a valid scheme"),
        "https" => url.set_scheme("wss").expect("wss is a valid scheme"),
        _ => unreachable!("URL scheme should have been validated by the caller"),
    }
    url
}

fn encode_frame(frame: Frame) -> Result<WebSocketMessage> {
    match frame {
        Frame::Packet(data) => Ok(WebSocketMessage::Text(data.encode().try_into()?)),
        Frame::Binary(data) => Ok(WebSocketMessage::Binary(data)),
    }
}

/// A boxed WebSocket connector: an owned async function from [`Url`] to a
/// [`WebSocketStream`]. Construct one via `ClientBuilder::ws_connector` in the
/// `sioc` crate, which accepts any
/// `async fn(Url) -> Result<WebSocketStream, WebSocketError>` and handles
/// boxing automatically.
pub type WebSocketConnector =
    Box<dyn FnOnce(Url) -> BoxFuture<'static, Result<WebSocketStream, WebSocketError>> + Send>;

/// Opens a plain `tokio-tungstenite` WebSocket connection with no custom TLS
/// or header configuration.
pub async fn default_connect(url: Url) -> Result<WebSocketStream, WebSocketError> {
    let (stream, _) = connect_async(url.as_str()).await?;
    Ok(stream)
}

/// Stateful WebSocket transport for Engine.IO v4.
#[derive(Debug)]
pub struct WebsocketTransport {
    pub url: Url,
    pub stream: Box<WebSocketStream>,
}

impl WebsocketTransport {
    /// Open a WebSocket connection (wire only, no handshake).
    pub async fn connect<F, Fut>(base_url: Url, connect: F) -> Result<Self>
    where
        F: FnOnce(Url) -> Fut,
        Fut: Future<Output = Result<WebSocketStream>>,
    {
        let mut url = base_url;

        url.query_pairs_mut()
            .append_pair("EIO", &ENGINE_IO_VERSION.to_string())
            .append_pair("transport", "websocket");

        let stream = Box::new(connect(url.clone()).await?);

        Ok(Self { url, stream })
    }

    /// Open a WebSocket on an existing session, perform the upgrade probe,
    /// and return the ready-to-use transport.
    pub async fn connect_for_upgrade(
        base_url: Url,
        sid: String,
        connect: WebSocketConnector,
    ) -> Result<Self> {
        let mut url = upgrade_schema(base_url);

        url.query_pairs_mut()
            .append_pair("EIO", &ENGINE_IO_VERSION.to_string())
            .append_pair("transport", "websocket")
            .append_pair("sid", &sid);

        let stream = Box::new(connect(url.clone()).await?);

        let mut transport = Self { url, stream };

        transport.probe().await?;

        Ok(transport)
    }

    async fn probe(&mut self) -> Result<(), Error> {
        self.send(EioPacket::Ping(PROBE).into()).await?;

        loop {
            match self.recv().await? {
                Some(Frame::Packet(EioPacket::Pong(bytes))) if bytes == PROBE => break,
                Some(Frame::Packet(EioPacket::Noop)) => continue,
                Some(frame) => {
                    return Err(frame.unexpected("expected probe Pong in response to probe Ping"));
                }
                None => return Err(Error::Close),
            }
        }

        self.send(EioPacket::Upgrade.into()).await?;

        Ok(())
    }

    /// Receives the next frame, filtering WebSocket control frames.
    ///
    /// Returns `Ok(None)` on a clean WebSocket close handshake
    /// (`ConnectionClosed` / `AlreadyClosed`); propagates all other errors.
    pub async fn recv(&mut self) -> Result<Option<Frame>> {
        loop {
            match self.stream.next().await {
                None
                | Some(Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed)) => {
                    return Ok(None);
                }
                Some(Err(e)) => return Err(Error::Websocket(e)),
                Some(Ok(message)) => {
                    if let ControlFlow::Break(frame) = decode_message(message) {
                        return frame.map(Some);
                    }
                }
            }
        }
    }

    /// Sends a frame, flushing immediately.
    pub async fn send(&mut self, frame: Frame) -> Result<()> {
        Ok(self.stream.send(encode_frame(frame)?).await?)
    }

    /// Buffers a frame without flushing.
    pub async fn feed(&mut self, frame: Frame) -> Result<()> {
        Ok(self.stream.feed(encode_frame(frame)?).await?)
    }

    /// Flushes buffered frames.
    pub async fn flush(&mut self) -> Result<()> {
        Ok(self.stream.flush().await?)
    }

    /// Closes the WebSocket connection.
    pub async fn close(mut self) -> Result<()> {
        Ok(self.stream.close().await?)
    }
}

fn decode_message(message: WebSocketMessage) -> ControlFlow<Result<Frame>> {
    match message {
        WebSocketMessage::Text(bytes) => {
            ControlFlow::Break(match EioPacket::decode(bytes.into()) {
                Ok(packet) => Ok(packet.into()),
                Err(e) => Err(e.into()),
            })
        }
        WebSocketMessage::Binary(bytes) => ControlFlow::Break(Ok(Frame::Binary(bytes))),
        _ => ControlFlow::Continue(()),
    }
}

impl Drop for WebsocketTransport {
    fn drop(&mut self) {
        debug!(target: "sioc::eio::ws", "websocket transport dropped");
    }
}
