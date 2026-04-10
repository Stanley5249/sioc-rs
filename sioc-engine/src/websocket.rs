//! WebSocket transport for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::engine::EngineSender;
use crate::error::{Error, Result};
use crate::packet::{Frame, Handshake, PROBE, Packet};
use futures_util::{SinkExt, StreamExt};
use std::future::Future;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
pub use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::{MaybeTlsStream, connect_async};
use tokio_util::sync::CancellationToken;
use url::Url;

pub type WebSocketStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A WebSocket connector that can open a stream from a [`Url`].
pub trait WebSocketConnector: Send + 'static {
    /// Opens a WebSocket connection for the provided [`Url`].
    fn connect(
        self,
        url: Url,
    ) -> impl Future<Output = Result<WebSocketStream, WebSocketError>> + Send;
}

impl<F, Fut> WebSocketConnector for F
where
    F: FnOnce(Url) -> Fut + Send + 'static,
    Fut: Future<Output = Result<WebSocketStream, WebSocketError>> + Send + 'static,
{
    fn connect(
        self,
        url: Url,
    ) -> impl Future<Output = Result<WebSocketStream, WebSocketError>> + Send {
        self(url)
    }
}

/// Opens a plain `tokio-tungstenite` WebSocket connection with no custom TLS
/// or header configuration.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultWebSocketConnector;

impl WebSocketConnector for DefaultWebSocketConnector {
    async fn connect(self, url: Url) -> Result<WebSocketStream, WebSocketError> {
        let (stream, _) = connect_async(url.as_str()).await?;
        Ok(stream)
    }
}

fn encode_frame(frame: Frame) -> Result<WebSocketMessage> {
    Ok(match frame {
        Frame::Packet(packet) => WebSocketMessage::Text(packet.encode().try_into()?),
        Frame::Binary(bytes) => WebSocketMessage::Binary(bytes),
    })
}

async fn next_frame(stream: &mut WebSocketStream) -> Result<Frame> {
    while let Some(message) = stream.next().await.transpose()? {
        let frame = match message {
            WebSocketMessage::Text(text) => Packet::decode(text.into())?.into(),
            WebSocketMessage::Binary(bytes) => bytes.into(),
            _ => continue,
        };
        return Ok(frame);
    }
    Err(Error::Close)
}

fn websocket_url(mut url: Url, sid: Option<&str>) -> Url {
    if sid.is_some() {
        let scheme = match url.scheme() {
            "http" => Some("ws"),
            "https" => Some("wss"),
            _ => None,
        };
        if let Some(scheme) = scheme {
            let _ = url.set_scheme(scheme);
        }
    }

    {
        let mut query = url.query_pairs_mut();

        query
            .append_pair("EIO", &ENGINE_IO_VERSION.to_string())
            .append_pair("transport", "websocket");

        if let Some(sid) = sid {
            query.append_pair("sid", sid);
        }
    }

    url
}

async fn websocket_probe(stream: &mut WebSocketStream) -> Result<()> {
    tracing::debug!("sending probe Ping");

    let packet = Packet::Ping(PROBE);

    stream
        .send(WebSocketMessage::Text(packet.encode().try_into()?))
        .await?;

    match next_frame(stream).await? {
        Frame::Packet(Packet::Pong(bytes)) if bytes == PROBE => {
            tracing::debug!("received probe Pong");
        }
        other => {
            return Err(other.unexpected("expected probe Pong in response to probe Ping"));
        }
    }

    Ok(())
}

pub async fn websocket_connect<C>(
    base_url: Url,
    sid: Option<String>,
    connector: C,
) -> Result<WebSocketStream, Error>
where
    C: WebSocketConnector,
{
    let url = websocket_url(base_url, sid.as_deref());

    tracing::debug!(%url, "connecting");

    let mut stream = connector.connect(url).await?;

    if sid.is_some() {
        websocket_probe(&mut stream).await?;
    }

    Ok(stream)
}

#[tracing::instrument(skip_all, err)]
pub async fn websocket_loop(
    mut stream: WebSocketStream,
    handshake_tx: Option<oneshot::Sender<Handshake>>,
    engine_tx: EngineSender,
    mut transport_rx: mpsc::Receiver<Frame>,
    token: CancellationToken,
) -> Result<()> {
    match handshake_tx {
        Some(handshake_tx) => {
            let handshake = match next_frame(&mut stream).await? {
                Frame::Packet(Packet::Open(handshake)) => handshake,
                other => return Err(other.unexpected("expected Open packet as first frame")),
            };

            tracing::debug!(?handshake, "received OPEN");

            handshake_tx.send(handshake).map_err(Error::SendHandshake)?;
        }
        None => {
            tracing::debug!("sending UPGRADE");

            let message = WebSocketMessage::Text(Packet::Upgrade.encode().try_into()?);

            stream.send(message).await?;
        }
    };

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::debug!("cancelling websocket");
                stream.close(None).await?;
                break;
            },

            item = stream.next() => {
                let Some(message) = item.transpose()? else {
                    tracing::debug!("websocket stream closed");
                    break;
                };

                tracing::trace!(frame = %message, "received message");

                let frame: Frame = match message {
                    WebSocketMessage::Text(text) => Packet::decode(text.into())?.into(),
                    WebSocketMessage::Binary(bytes) => bytes.into(),
                    _ => continue,
                };

                engine_tx.send(frame).await?;
            }

            item = transport_rx.recv() => {
                let Some(frame) = item else {
                    tracing::debug!("transport channel closed");
                    break;
                };
                let message = encode_frame(frame)?;

                tracing::trace!(frame = %message, "sending message");

                stream.send(message).await?;
            }
        };
    }

    Ok(())
}

#[tracing::instrument(skip_all, err)]
pub async fn websocket_transport<C>(
    base_url: Url,
    connector: C,
    handshake_tx: oneshot::Sender<Handshake>,
    engine_tx: EngineSender,
    transport_rx: mpsc::Receiver<Frame>,
    token: CancellationToken,
) -> Result<()>
where
    C: WebSocketConnector,
{
    let stream = websocket_connect(base_url, None, connector).await?;

    websocket_loop(stream, Some(handshake_tx), engine_tx, transport_rx, token).await?;

    Ok(())
}
