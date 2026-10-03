//! WebSocket transport for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::error::{TransportError, WebSocketError};
use crate::packet::{Frame, Handshake, PROBE, Packet};
use bytestring::ByteString;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, Stream, StreamExt, TryStreamExt};
use std::future::Future;
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
        let (stream, _) = connect_async(url).await?;
        Ok(WebSocketStream(stream))
    }
}

type Connection = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// An open WebSocket connection that carries Engine.IO [`Frame`]s.
pub struct WebSocketStream(pub tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>);

/// Converts an inbound WebSocket text frame into a [`ByteString`] without copying.
fn bytestring_from_utf8_bytes(utf8: tokio_tungstenite::tungstenite::Utf8Bytes) -> ByteString {
    // SAFETY: `tungstenite::Utf8Bytes` guarantees the inner `Bytes` is valid UTF-8.
    unsafe { ByteString::from_bytes_unchecked(utf8.into()) }
}

/// Reads the next frame, skipping WebSocket control messages.
///
/// Returns `None` at the peer's close frame, so the transport stops before it
/// sends into a closing socket.
async fn next_frame<S>(stream: &mut S) -> Result<Option<Frame>, WebSocketError>
where
    S: Stream<Item = Result<WebSocketMessage, TungsteniteError>> + Unpin,
{
    while let Some(message) = stream.try_next().await? {
        match message {
            WebSocketMessage::Text(text) => {
                tracing::trace!(bytes = text.len(), "<- TEXT");

                let bytes = bytestring_from_utf8_bytes(text);

                return Ok(Some(Frame::Packet(Packet::decode(&bytes)?)));
            }
            WebSocketMessage::Binary(binary) => {
                tracing::trace!(bytes = binary.len(), "<- BINARY");

                return Ok(Some(Frame::Binary(binary)));
            }
            WebSocketMessage::Close(_) => {
                tracing::trace!("<- CLOSE");

                return Ok(None);
            }
            _ => {}
        }
    }

    Ok(None)
}

/// Encodes a frame as a WebSocket message.
fn encode(frame: Frame) -> WebSocketMessage {
    match frame {
        Frame::Packet(packet) => {
            let text = packet.encode();

            tracing::trace!(bytes = text.len(), "-> TEXT");

            WebSocketMessage::text(text)
        }
        Frame::Binary(bytes) => {
            tracing::trace!(bytes = bytes.len(), "-> BINARY");

            WebSocketMessage::binary(bytes)
        }
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
    ///
    /// # Errors
    ///
    /// Returns an error if the connection or probe fails.
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
    async fn recv(&mut self) -> Result<Frame, WebSocketError> {
        next_frame(&mut self.0).await?.ok_or(WebSocketError::Closed)
    }

    /// Sends one frame.
    async fn send(&mut self, frame: Frame) -> Result<(), WebSocketError> {
        Ok(self.0.send(encode(frame)).await?)
    }

    /// Sends a probe `Ping` and expects a matching `Pong`, confirming the WebSocket path is live.
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn probe(&mut self) -> Result<(), WebSocketError> {
        tracing::debug!("-> PING probe");

        self.send(Packet::Ping(PROBE).into()).await?;

        match self.recv().await? {
            Frame::Packet(Packet::Pong(payload)) if payload == PROBE => {
                tracing::debug!("<- PONG probe");
            }

            frame => return Err(WebSocketError::Probe(frame)),
        }

        Ok(())
    }

    /// Drives the WebSocket I/O until the engine closes `transport_rx` and the stream ends.
    ///
    /// When `handshake_tx` is `Some`, reads the first `Open` frame and forwards the handshake
    /// (direct WebSocket transport). When `None`, sends `Upgrade` immediately (polling upgrade path).
    ///
    /// Inbound and outbound frames flow independently. When the engine closes
    /// `transport_rx`, the socket closes and inbound frames flow until the server
    /// answers. If the server ends the stream first, drops `frame_tx` and discards
    /// outbound frames until the engine closes `transport_rx`.
    ///
    /// # Errors
    ///
    /// Returns an error if a transport or protocol failure occurs.
    #[tracing::instrument(skip_all, err)]
    pub async fn transport(
        mut self,
        handshake_tx: Option<oneshot::Sender<Handshake>>,
        frame_tx: mpsc::Sender<Frame>,
        transport_rx: mpsc::Receiver<Frame>,
    ) -> Result<(), TransportError> {
        if let Some(handshake_tx) = handshake_tx {
            let handshake = match self.recv().await? {
                Frame::Packet(Packet::Open(handshake)) => handshake,
                frame => return Err(TransportError::Open(frame)),
            };

            tracing::debug!(sid = %handshake.sid, "<- OPEN");

            handshake_tx
                .send(handshake)
                .map_err(TransportError::SendHandshake)?;
        } else {
            tracing::debug!("-> UPGRADE");

            self.send(Packet::Upgrade.into()).await?;
        }

        let (sink, stream) = self.0.split();
        let stream_closed = CancellationToken::new();

        // Each direction runs on its own, so a slow engine never stalls
        // outbound frames and a slow socket never stalls inbound ones.
        tokio::try_join!(
            inbound(stream, frame_tx, stream_closed.clone()),
            outbound(sink, transport_rx, stream_closed),
        )?;

        Ok(())
    }
}

/// Forwards server frames to the engine until the stream ends.
///
/// Returning drops `frame_tx`, which tells the engine the transport has finished.
async fn inbound(
    mut stream: SplitStream<Connection>,
    frame_tx: mpsc::Sender<Frame>,
    stream_closed: CancellationToken,
) -> Result<(), TransportError> {
    let _guard = stream_closed.drop_guard();

    while let Some(frame) = next_frame(&mut stream).await? {
        frame_tx.send(frame).await?;
    }

    tracing::debug!("websocket stream closed");

    Ok(())
}

/// Sends engine frames until the engine closes `transport_rx`, then closes the socket.
///
/// If the stream ends first, discards frames until the engine closes `transport_rx`,
/// because the closed socket cannot send them.
async fn outbound(
    mut sink: SplitSink<Connection, WebSocketMessage>,
    mut transport_rx: mpsc::Receiver<Frame>,
    stream_closed: CancellationToken,
) -> Result<(), TransportError> {
    loop {
        tokio::select! {
            frame = transport_rx.recv() => {
                let Some(frame) = frame else {
                    tracing::debug!("transport channel closed");
                    break;
                };

                sink.send(encode(frame))
                    .await
                    .map_err(WebSocketError::from)?;
            }

            () = stream_closed.cancelled() => {
                while transport_rx.recv().await.is_some() {}
                break;
            }
        }
    }

    sink.close().await.map_err(WebSocketError::from)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{mpsc, oneshot};
    use tokio_tungstenite::tungstenite::Message as WsMsg;
    use tokio_tungstenite::{MaybeTlsStream, accept_async, client_async};

    async fn ws_pair() -> (
        WebSocketStream,
        tokio_tungstenite::WebSocketStream<TcpStream>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            accept_async(tcp).await.unwrap()
        });
        let tcp = TcpStream::connect(addr).await.unwrap();
        let (ws, _) = client_async("ws://127.0.0.1/", MaybeTlsStream::Plain(tcp))
            .await
            .unwrap();
        let client = WebSocketStream(ws);
        let server = server_task.await.unwrap();
        (client, server)
    }

    #[test]
    fn websocket_url_http_becomes_ws() {
        let url = Url::parse("http://localhost:3000/socket.io/").unwrap();
        let ws = websocket_url(url, None);
        assert_eq!(ws.scheme(), "ws");
        let q = ws.query().unwrap();
        assert!(q.contains("EIO=4"));
        assert!(q.contains("transport=websocket"));
    }

    #[test]
    fn websocket_url_https_becomes_wss() {
        let url = Url::parse("https://example.com/socket.io/").unwrap();
        let ws = websocket_url(url, None);
        assert_eq!(ws.scheme(), "wss");
    }

    #[test]
    fn websocket_url_appends_sid() {
        let url = Url::parse("http://localhost/socket.io/").unwrap();
        let ws = websocket_url(url, Some("abc123"));
        assert!(ws.query().unwrap().contains("sid=abc123"));
    }

    #[test]
    fn websocket_url_no_sid_when_none() {
        let url = Url::parse("http://localhost/socket.io/").unwrap();
        let ws = websocket_url(url, None);
        assert!(!ws.query().unwrap().contains("sid="));
    }

    #[test]
    fn websocket_url_unknown_scheme_unchanged() {
        let url = Url::parse("ws://localhost/socket.io/").unwrap();
        let ws = websocket_url(url, None);
        assert_eq!(ws.scheme(), "ws");
    }

    #[test]
    fn bytestring_from_utf8_bytes_preserves_content() {
        use tokio_tungstenite::tungstenite::Utf8Bytes;
        let text = "hello world";
        let utf8 = Utf8Bytes::from(text);
        let bs = bytestring_from_utf8_bytes(utf8);
        assert_eq!(&*bs, text);
    }

    #[tokio::test]
    async fn stream_decodes_text_frame_as_packet() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::text("4hello")).await.unwrap();
        let frame = client.recv().await.unwrap();
        assert!(matches!(frame, Frame::Packet(Packet::Message(m)) if m == "hello"));
    }

    #[tokio::test]
    async fn stream_ends_at_peer_close_frame() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::Close(None)).await.unwrap();
        assert!(next_frame(&mut client.0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stream_decodes_binary_frame() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::binary(b"data".as_ref())).await.unwrap();
        let frame = client.recv().await.unwrap();
        assert!(matches!(frame, Frame::Binary(b) if b.as_ref() == b"data"));
    }

    #[tokio::test]
    async fn stream_invalid_packet_id_is_error() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::text("9bad")).await.unwrap();
        client.recv().await.unwrap_err();
    }

    #[tokio::test]
    async fn sink_sends_packet_frame_as_text() {
        let (mut client, mut server) = ws_pair().await;
        client
            .send(Frame::Packet(Packet::Pong("probe".into())))
            .await
            .unwrap();
        let msg = server.next().await.unwrap().unwrap();
        assert_eq!(msg.to_text().unwrap(), "3probe");
    }

    #[tokio::test]
    async fn sink_sends_binary_frame() {
        let (mut client, mut server) = ws_pair().await;
        client
            .send(Frame::Binary(Bytes::from_static(b"raw")))
            .await
            .unwrap();
        let msg = server.next().await.unwrap().unwrap();
        assert!(msg.is_binary());
        assert_eq!(msg.into_data().as_ref(), b"raw");
    }

    #[tokio::test]
    async fn probe_succeeds_on_matching_pong() {
        let (mut client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let msg = server.next().await.unwrap().unwrap();
            assert_eq!(msg.to_text().unwrap(), "2probe");
            server.send(WsMsg::text("3probe")).await.unwrap();
        });
        client.probe().await.unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn probe_fails_on_wrong_frame() {
        let (mut client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let _ = server.next().await.unwrap().unwrap();
            server.send(WsMsg::text("4unexpected")).await.unwrap();
        });
        assert!(matches!(
            client.probe().await,
            Err(WebSocketError::Probe(_))
        ));
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn transport_no_handshake_sends_upgrade() {
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let msg = server.next().await.unwrap().unwrap();
            assert_eq!(msg.to_text().unwrap(), "5");
            while let Some(Ok(_)) = server.next().await {}
        });
        let (frame_tx, _) = mpsc::channel(4);
        let (_, transport_rx) = mpsc::channel::<Frame>(4);
        client
            .transport(None, frame_tx, transport_rx)
            .await
            .unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn transport_with_handshake_reads_open_packet() {
        let open_text = r#"0{"sid":"abc","upgrades":[],"pingInterval":25000,"pingTimeout":5000,"maxPayload":1000000}"#;
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            server.send(WsMsg::text(open_text)).await.unwrap();
            while let Some(Ok(_)) = server.next().await {}
        });
        let (handshake_tx, handshake_rx) = oneshot::channel();
        let (frame_tx, _) = mpsc::channel(4);
        let (transport_tx, transport_rx) = mpsc::channel::<Frame>(4);
        drop(transport_tx);
        client
            .transport(Some(handshake_tx), frame_tx, transport_rx)
            .await
            .unwrap();
        assert_eq!(&*handshake_rx.await.unwrap().sid, "abc");
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn transport_with_handshake_non_open_is_error() {
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            server.send(WsMsg::text("4hello")).await.unwrap();
            while let Some(Ok(_)) = server.next().await {}
        });
        let (handshake_tx, _) = oneshot::channel();
        let (frame_tx, _) = mpsc::channel(4);
        let (_, transport_rx) = mpsc::channel::<Frame>(4);
        let result = client
            .transport(Some(handshake_tx), frame_tx, transport_rx)
            .await;
        assert!(matches!(result, Err(TransportError::Open(_))));
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn transport_with_handshake_dropped_receiver_is_error() {
        let open_text = r#"0{"sid":"abc","upgrades":[],"pingInterval":25000,"pingTimeout":5000,"maxPayload":1000000}"#;
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            server.send(WsMsg::text(open_text)).await.unwrap();
            while let Some(Ok(_)) = server.next().await {}
        });
        let (handshake_tx, handshake_rx) = oneshot::channel::<Handshake>();
        drop(handshake_rx);
        let (frame_tx, _) = mpsc::channel(4);
        let (_, transport_rx) = mpsc::channel::<Frame>(4);
        let result = client
            .transport(Some(handshake_tx), frame_tx, transport_rx)
            .await;
        assert!(matches!(result, Err(TransportError::SendHandshake(_))));
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn transport_forwards_server_frame_to_engine() {
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let _ = server.next().await; // consume Upgrade
            server.send(WsMsg::text("4data")).await.unwrap();
            let _ = server.close(None).await;
            while let Some(Ok(_)) = server.next().await {}
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(4);
        let (transport_tx, transport_rx) = mpsc::channel::<Frame>(4);
        let transport = tokio::spawn(client.transport(None, frame_tx, transport_rx));
        let action = frame_rx.recv().await.unwrap();
        assert!(matches!(action, Frame::Packet(Packet::Message(m)) if m == "data"));
        drop(transport_tx);
        transport.await.unwrap().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn transport_discards_outbound_after_server_close() {
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let _ = server.next().await; // consume Upgrade
            server.close(None).await.unwrap();
            while let Some(Ok(_)) = server.next().await {}
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(4);
        let (transport_tx, transport_rx) = mpsc::channel::<Frame>(4);
        let transport = tokio::spawn(client.transport(None, frame_tx, transport_rx));
        assert!(frame_rx.recv().await.is_none());
        transport_tx
            .send(Frame::Packet(Packet::Close))
            .await
            .unwrap();
        drop(transport_tx);
        transport.await.unwrap().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn transport_sends_outbound_while_engine_is_full() {
        let (client, mut server) = ws_pair().await;
        let (frame_tx, _frame_rx) = mpsc::channel(1);
        let (transport_tx, transport_rx) = mpsc::channel::<Frame>(4);
        tokio::spawn(client.transport(None, frame_tx, transport_rx));
        let _ = server.next().await; // consume Upgrade
        for text in ["4fills", "4blocks"] {
            server.send(WsMsg::text(text)).await.unwrap();
        }
        transport_tx
            .send(Frame::Packet(Packet::Message("out".into())))
            .await
            .unwrap();
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), server.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.to_text().unwrap(), "4out");
    }

    #[tokio::test]
    async fn transport_forwards_outbound_frame_to_server() {
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let msg = server.next().await.unwrap().unwrap();
            assert_eq!(msg.to_text().unwrap(), "5");
            let msg = server.next().await.unwrap().unwrap();
            assert_eq!(msg.to_text().unwrap(), "4out");
            while let Some(Ok(_)) = server.next().await {}
        });
        let (frame_tx, _) = mpsc::channel(4);
        let (transport_tx, transport_rx) = mpsc::channel::<Frame>(4);
        transport_tx
            .send(Frame::Packet(Packet::Message("out".into())))
            .await
            .unwrap();
        drop(transport_tx);
        client
            .transport(None, frame_tx, transport_rx)
            .await
            .unwrap();
        server_task.await.unwrap();
    }
}
