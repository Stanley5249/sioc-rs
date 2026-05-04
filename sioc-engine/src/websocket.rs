//! WebSocket transport for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::engine::EngineSender;
use crate::error::{TransportError, WebSocketError};
use crate::packet::{Frame, Handshake, PROBE, Packet};
use bytestring::ByteString;
use futures_util::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::{MaybeTlsStream, connect_async};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
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
        let (stream, _) = connect_async(url.as_str()).await?;
        Ok(WebSocketStream(stream))
    }
}

/// Framed WebSocket stream that speaks [`Frame`] instead of raw [`WebSocketMessage`].
pub struct WebSocketStream(pub tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>);

/// Converts an inbound WebSocket text frame into a [`ByteString`] without copying.
fn bytestring_from_utf8_bytes(utf8: tokio_tungstenite::tungstenite::Utf8Bytes) -> ByteString {
    // SAFETY: `tungstenite::Utf8Bytes` guarantees the inner `Bytes` is valid UTF-8.
    unsafe { ByteString::from_bytes_unchecked(utf8.into()) }
}

impl Stream for WebSocketStream {
    type Item = Result<Frame, WebSocketError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let frame = match ready!(self.0.poll_next_unpin(cx)) {
                Some(result) => match result {
                    Ok(message) => match message {
                        WebSocketMessage::Text(text) => {
                            tracing::trace!(bytes = text.len(), "<- TEXT");

                            let bytes = bytestring_from_utf8_bytes(text);
                            let packet = Packet::decode(bytes)?;

                            Some(Ok(Frame::Packet(packet)))
                        }
                        WebSocketMessage::Binary(binary) => {
                            tracing::trace!(bytes = binary.len(), "<- BINARY");

                            Some(Ok(Frame::Binary(binary)))
                        }
                        _ => continue,
                    },
                    Err(e) => Some(Err(e.into())),
                },
                None => None,
            };

            return Poll::Ready(frame);
        }
    }
}

impl Sink<Frame> for WebSocketStream {
    type Error = WebSocketError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_ready_unpin(cx)
            .map_err(WebSocketError::Tungstenite)
    }

    fn start_send(mut self: Pin<&mut Self>, frame: Frame) -> Result<(), Self::Error> {
        let message = match frame {
            Frame::Packet(packet) => {
                let text = packet.encode();

                tracing::trace!(bytes = text.len(), "-> TEXT");

                WebSocketMessage::text(text)
            }
            Frame::Binary(bytes) => {
                tracing::trace!(bytes = bytes.len(), "-> BINARY");

                WebSocketMessage::binary(bytes)
            }
        };
        self.0
            .start_send_unpin(message)
            .map_err(WebSocketError::Tungstenite)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_flush_unpin(cx)
            .map_err(WebSocketError::Tungstenite)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_close_unpin(cx)
            .map_err(WebSocketError::Tungstenite)
    }
}

/// Builds the WebSocket URL by converting the scheme and appending EIO/transport/sid parameters.
fn websocket_url(mut url: Url, sid: Option<&str>) -> Url {
    let scheme = match url.scheme() {
        "http" => Some("ws"),
        "https" => Some("wss"),
        _ => None,
    };

    if let Some(scheme) = scheme {
        let _ = url.set_scheme(scheme);
    }

    {
        let mut query = url.query_pairs_mut();

        query
            .append_pair("EIO", ENGINE_IO_VERSION)
            .append_pair("transport", "websocket");

        if let Some(sid) = sid {
            query.append_pair("sid", sid);
        }
    }

    url
}

impl WebSocketStream {
    /// Opens a [`WebSocketStream`], running the upgrade probe when `sid` is present.
    pub async fn connect<C>(
        base_url: Url,
        sid: Option<&str>,
        connector: C,
    ) -> Result<Self, WebSocketError>
    where
        C: WebSocketConnector,
    {
        let url = websocket_url(base_url, sid);

        let span = tracing::debug_span!("connect", %url);

        let mut stream = connector.connect(url).instrument(span).await?;

        if sid.is_some() {
            stream.probe().await?;
        }

        Ok(stream)
    }

    /// Waits for the next frame, returning an error if the stream is closed.
    async fn required_next(&mut self) -> Result<Frame, WebSocketError> {
        self.try_next().await?.ok_or(WebSocketError::Closed)
    }

    /// Sends a probe `Ping` and expects a matching `Pong`, confirming the WebSocket path is live.
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn probe(&mut self) -> Result<(), WebSocketError> {
        tracing::debug!("-> PING probe");

        self.send(Packet::Ping(PROBE).into()).await?;

        match self.required_next().await? {
            Frame::Packet(Packet::Pong(payload)) if payload == PROBE => {
                tracing::debug!("<- PONG probe")
            }

            frame => return Err(WebSocketError::Probe(frame)),
        }

        Ok(())
    }

    /// Drives the WebSocket I/O loop until cancelled, the stream closes, or a channel closes.
    ///
    /// When `handshake_tx` is `Some`, reads the first `Open` frame and forwards the handshake
    /// (direct WebSocket transport). When `None`, sends `Upgrade` immediately (polling upgrade path).
    /// Drains any queued outbound frames before closing the stream.
    #[tracing::instrument(skip_all, err)]
    pub async fn transport(
        mut self,
        handshake_tx: Option<oneshot::Sender<Handshake>>,
        engine_tx: EngineSender,
        mut transport_rx: mpsc::Receiver<Frame>,
        token: CancellationToken,
    ) -> Result<(), TransportError> {
        match handshake_tx {
            Some(handshake_tx) => {
                let handshake = match self.required_next().await? {
                    Frame::Packet(Packet::Open(handshake)) => handshake,
                    frame => return Err(TransportError::Open(frame)),
                };

                tracing::debug!(sid = %handshake.sid, "<- OPEN");

                handshake_tx
                    .send(handshake)
                    .map_err(TransportError::SendHandshake)?;
            }
            None => {
                tracing::debug!("-> UPGRADE");

                self.send(Packet::Upgrade.into()).await?;
            }
        };

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    tracing::debug!("cancelled websocket");
                    break;
                },

                result = self.try_next() => {
                    let Some(frame) = result? else {
                        tracing::debug!("websocket stream closed");
                        break;
                    };

                    engine_tx.send(frame).await?;
                }

                option = transport_rx.recv() => {
                    let Some(frame) = option else {
                        tracing::debug!("transport channel closed");
                        break;
                    };

                    self.send(frame).await?;
                }
            };
        }

        // Drain frames queued before the engine exited so the disconnect
        // packet is not silently dropped by a concurrent cancellation.
        while let Some(frame) = transport_rx.recv().await {
            self.send(frame).await?;
        }

        self.close().await?;

        Ok(())
    }
}
