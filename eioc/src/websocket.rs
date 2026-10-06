//! WebSocket transport for Engine.IO v4.

use std::future::Future;

use bytestring::ByteString;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, Stream, StreamExt, TryStreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::{Error as TungsteniteError, Message as WebSocketMessage};
use tokio_tungstenite::{MaybeTlsStream, connect_async};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use url::Url;

use crate::ENGINE_IO_VERSION;
use crate::error::{TransportError, WebSocketError};
use crate::packet::{Frame, Handshake, PROBE, Packet};

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

/// Converts a WebSocket text frame from the server into a [`ByteString`]
/// without copying.
fn bytestring_from_utf8_bytes(utf8: tokio_tungstenite::tungstenite::Utf8Bytes) -> ByteString {
    // SAFETY: `tungstenite::Utf8Bytes` guarantees the inner `Bytes` is valid UTF-8.
    unsafe { ByteString::from_bytes_unchecked(utf8.into()) }
}

/// Reads the next frame, skipping WebSocket control messages.
///
/// Returns `None` at the server's `Close` packet or the peer's close frame,
/// so the transport stops before it sends into a closing socket.
async fn next_frame<S>(stream: &mut S) -> Result<Option<Frame>, WebSocketError>
where
    S: Stream<Item = Result<WebSocketMessage, TungsteniteError>> + Unpin,
{
    while let Some(message) = stream.try_next().await? {
        match message {
            WebSocketMessage::Text(text) => {
                tracing::trace!(bytes = text.len(), "received text frame");

                let bytes = bytestring_from_utf8_bytes(text);

                return match Packet::decode(&bytes)? {
                    Packet::Close => {
                        tracing::debug!("server closed");

                        Ok(None)
                    }
                    packet => Ok(Some(Frame::Packet(packet))),
                };
            }
            WebSocketMessage::Binary(binary) => {
                tracing::trace!(bytes = binary.len(), "received binary frame");

                return Ok(Some(Frame::Binary(binary)));
            }
            WebSocketMessage::Close(_) => {
                tracing::trace!("received close frame");

                return Ok(None);
            }
            _ => {}
        }
    }

    Ok(None)
}

/// Encodes a frame as a WebSocket message.
fn encode_frame(frame: Frame) -> WebSocketMessage {
    match frame {
        Frame::Packet(packet) => {
            let text = packet.encode();

            tracing::trace!(bytes = text.len(), "sent text frame");

            WebSocketMessage::text(text)
        }
        Frame::Binary(bytes) => {
            tracing::trace!(bytes = bytes.len(), "sent binary frame");

            WebSocketMessage::binary(bytes)
        }
    }
}

/// Builds the WebSocket URL by converting the scheme and appending
/// EIO/transport/sid parameters.
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

/// Opens a [`WebSocketStream`], running the upgrade probe when `sid` is
/// present.
///
/// # Errors
///
/// Returns an error if the connection or probe fails.
pub async fn connect<C>(
    base_url: Url,
    sid: Option<&str>,
    connector: C,
) -> Result<WebSocketStream, WebSocketError>
where
    C: WebSocketConnector,
{
    let url = websocket_url(base_url, sid);

    let span = tracing::debug_span!("connect", %url);

    let mut stream = connector.connect(url).instrument(span).await?;

    if sid.is_some() {
        probe_upgrade(&mut stream).await?;
    }

    Ok(stream)
}

/// Waits for the next frame, returning an error if the stream is closed.
async fn recv_frame(stream: &mut WebSocketStream) -> Result<Frame, WebSocketError> {
    next_frame(stream).await?.ok_or(WebSocketError::Closed)
}

/// Sends one frame.
async fn send_frame(stream: &mut WebSocketStream, frame: Frame) -> Result<(), WebSocketError> {
    Ok(stream.send(encode_frame(frame)).await?)
}

/// Sends a probe `Ping` and expects a matching `Pong`, confirming the WebSocket
/// path is live.
#[tracing::instrument(level = "debug", skip_all)]
async fn probe_upgrade(stream: &mut WebSocketStream) -> Result<(), WebSocketError> {
    tracing::debug!("sent probe ping");

    send_frame(stream, Packet::Ping(PROBE).into()).await?;

    match recv_frame(stream).await? {
        Frame::Packet(Packet::Pong(payload)) if payload == PROBE => {
            tracing::debug!("received probe pong");
        }

        frame => return Err(WebSocketError::Probe(frame)),
    }

    Ok(())
}

/// Drives the WebSocket I/O until the engine closes `client_frame_rx` and the
/// stream ends.
///
/// When `handshake_tx` is `Some`, reads the first `Open` frame and forwards the
/// handshake (direct WebSocket transport). When `None`, sends `Upgrade`
/// immediately (polling upgrade path).
///
/// Server and client frames flow independently. When the engine closes
/// `client_frame_rx`, the socket closes and server frames flow until the server
/// answers. If the server ends the stream first, drops `server_frame_tx` and
/// discards client frames until the engine closes `client_frame_rx`.
///
/// # Errors
///
/// Returns an error if a transport or protocol failure occurs.
#[tracing::instrument(skip_all)]
pub async fn transport(
    mut stream: WebSocketStream,
    handshake_tx: Option<oneshot::Sender<Handshake>>,
    server_frame_tx: mpsc::Sender<Frame>,
    client_frame_rx: mpsc::Receiver<Frame>,
) -> Result<(), TransportError> {
    if let Some(handshake_tx) = handshake_tx {
        let handshake = match recv_frame(&mut stream).await? {
            Frame::Packet(Packet::Open(handshake)) => handshake,
            frame => return Err(TransportError::Open(frame)),
        };

        tracing::debug!(sid = %handshake.sid, "received handshake");

        handshake_tx
            .send(handshake)
            .map_err(TransportError::Handshake)?;
    } else {
        tracing::debug!("sent upgrade packet");

        send_frame(&mut stream, Packet::Upgrade.into()).await?;
    }

    let (sink, stream) = stream.split();
    let stream_closed = CancellationToken::new();

    // Each direction runs on its own, so a slow engine never stalls
    // client frames and a slow socket never stalls server ones.
    tokio::try_join!(
        forward_server_frames(stream, server_frame_tx, stream_closed.clone()),
        forward_client_frames(sink, client_frame_rx, stream_closed),
    )?;

    Ok(())
}
/// Forwards server frames to the engine until the stream ends.
///
/// Returning drops `server_frame_tx`, which tells the engine the transport has
/// finished.
async fn forward_server_frames(
    mut stream: SplitStream<WebSocketStream>,
    server_frame_tx: mpsc::Sender<Frame>,
    stream_closed: CancellationToken,
) -> Result<(), TransportError> {
    let _guard = stream_closed.drop_guard();

    while let Some(frame) = next_frame(&mut stream).await? {
        server_frame_tx.send(frame).await?;
    }

    tracing::debug!("websocket stream closed");

    Ok(())
}

/// Sends engine frames until the engine closes `client_frame_rx`, then sends
/// the server a `Close` packet and closes the socket.
///
/// If the stream ends first, discards frames until the engine closes
/// `client_frame_rx`, because the closed socket cannot send them.
async fn forward_client_frames(
    mut sink: SplitSink<WebSocketStream, WebSocketMessage>,
    mut client_frame_rx: mpsc::Receiver<Frame>,
    stream_closed: CancellationToken,
) -> Result<(), TransportError> {
    loop {
        tokio::select! {
            frame = client_frame_rx.recv() => {
                let Some(frame) = frame else {
                    tracing::debug!("client frame channel closed");

                    finish_write(sink.send(encode_frame(Packet::Close.into())).await)?;

                    break;
                };

                if !finish_write(sink.send(encode_frame(frame)).await)? {
                    break;
                }
            }

            () = stream_closed.cancelled() => {
                break;
            }
        }
    }

    // Each terminal write outcome follows the same half-close path.
    while client_frame_rx.recv().await.is_some() {}
    finish_write(sink.close().await)?;

    Ok(())
}

/// Returns whether another write is possible after tungstenite completes one.
fn finish_write(result: Result<(), TungsteniteError>) -> Result<bool, WebSocketError> {
    use tokio_tungstenite::tungstenite::error::ProtocolError;

    match result {
        Ok(()) => Ok(true),
        // These errors report the peer's close state, just like stream EOF.
        Err(
            TungsteniteError::ConnectionClosed
            | TungsteniteError::AlreadyClosed
            | TungsteniteError::Protocol(ProtocolError::SendAfterClosing),
        ) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{mpsc, oneshot};
    use tokio_tungstenite::tungstenite::Message as WsMsg;
    use tokio_tungstenite::{MaybeTlsStream, accept_async, client_async};

    use super::*;

    #[tokio::test]
    async fn forward_client_frames_drains_after_peer_close() {
        let (client, mut server) = ws_pair().await;
        let (sink, mut stream) = client.split();
        server.close(None).await.unwrap();
        assert!(next_frame(&mut stream).await.unwrap().is_none());

        // The reader has changed tungstenite's state before its stop signal
        // reaches the writer. A queued frame must still finish gracefully.
        let (client_frame_tx, client_frame_rx) = mpsc::channel(1);
        client_frame_tx
            .send(Packet::Message("late".into()).into())
            .await
            .unwrap();
        drop(client_frame_tx);
        forward_client_frames(sink, client_frame_rx, CancellationToken::new())
            .await
            .unwrap();
    }

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
        let client = ws;
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
    async fn recv_frame_decodes_text_frame_as_packet() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::text("4hello")).await.unwrap();
        let frame = recv_frame(&mut client).await.unwrap();
        assert!(matches!(frame, Frame::Packet(Packet::Message(m)) if m == "hello"));
    }

    #[tokio::test]
    async fn next_frame_ends_at_peer_close_frame() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::Close(None)).await.unwrap();
        assert!(next_frame(&mut client).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn next_frame_ends_at_close_packet() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::text("1")).await.unwrap();
        assert!(next_frame(&mut client).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recv_frame_decodes_binary_frame() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::binary(b"data".as_ref())).await.unwrap();
        let frame = recv_frame(&mut client).await.unwrap();
        assert!(matches!(frame, Frame::Binary(b) if b.as_ref() == b"data"));
    }

    #[tokio::test]
    async fn recv_frame_invalid_packet_id_is_error() {
        let (mut client, mut server) = ws_pair().await;
        server.send(WsMsg::text("9bad")).await.unwrap();
        recv_frame(&mut client).await.unwrap_err();
    }

    #[tokio::test]
    async fn send_frame_encodes_packet_as_text() {
        let (mut client, mut server) = ws_pair().await;
        send_frame(&mut client, Frame::Packet(Packet::Pong("probe".into())))
            .await
            .unwrap();
        let msg = server.next().await.unwrap().unwrap();
        assert_eq!(msg.to_text().unwrap(), "3probe");
    }

    #[tokio::test]
    async fn send_frame_encodes_binary() {
        let (mut client, mut server) = ws_pair().await;
        send_frame(&mut client, Frame::Binary(Bytes::from_static(b"raw")))
            .await
            .unwrap();
        let msg = server.next().await.unwrap().unwrap();
        assert!(msg.is_binary());
        assert_eq!(msg.into_data().as_ref(), b"raw");
    }

    #[tokio::test]
    async fn probe_upgrade_succeeds_on_matching_pong() {
        let (mut client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let msg = server.next().await.unwrap().unwrap();
            assert_eq!(msg.to_text().unwrap(), "2probe");
            server.send(WsMsg::text("3probe")).await.unwrap();
        });
        probe_upgrade(&mut client).await.unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn probe_upgrade_fails_on_wrong_frame() {
        let (mut client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let _ = server.next().await.unwrap().unwrap();
            server.send(WsMsg::text("4unexpected")).await.unwrap();
        });
        assert!(matches!(
            probe_upgrade(&mut client).await,
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
        let (server_frame_tx, _) = mpsc::channel(4);
        let (_, client_frame_rx) = mpsc::channel::<Frame>(4);
        transport(client, None, server_frame_tx, client_frame_rx)
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
        let (server_frame_tx, _) = mpsc::channel(4);
        let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
        drop(client_frame_tx);
        transport(client, Some(handshake_tx), server_frame_tx, client_frame_rx)
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
        let (server_frame_tx, _) = mpsc::channel(4);
        let (_, client_frame_rx) = mpsc::channel::<Frame>(4);
        let result = transport(client, Some(handshake_tx), server_frame_tx, client_frame_rx).await;
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
        let (server_frame_tx, _) = mpsc::channel(4);
        let (_, client_frame_rx) = mpsc::channel::<Frame>(4);
        let result = transport(client, Some(handshake_tx), server_frame_tx, client_frame_rx).await;
        assert!(matches!(result, Err(TransportError::Handshake(_))));
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
        let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
        let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
        let transport = tokio::spawn(transport(client, None, server_frame_tx, client_frame_rx));
        let action = server_frame_rx.recv().await.unwrap();
        assert!(matches!(action, Frame::Packet(Packet::Message(m)) if m == "data"));
        drop(client_frame_tx);
        transport.await.unwrap().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn transport_discards_client_frames_after_server_close() {
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let _ = server.next().await; // consume Upgrade
            server.close(None).await.unwrap();
            while let Some(Ok(_)) = server.next().await {}
        });
        let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
        let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
        let transport = tokio::spawn(transport(client, None, server_frame_tx, client_frame_rx));
        assert!(server_frame_rx.recv().await.is_none());
        client_frame_tx
            .send(Frame::Packet(Packet::Close))
            .await
            .unwrap();
        drop(client_frame_tx);
        transport.await.unwrap().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn transport_sends_client_frames_while_engine_is_full() {
        let (client, mut server) = ws_pair().await;
        let (server_frame_tx, _frame_rx) = mpsc::channel(1);
        let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
        tokio::spawn(transport(client, None, server_frame_tx, client_frame_rx));
        let _ = server.next().await; // consume Upgrade
        for text in ["4fills", "4blocks"] {
            server.send(WsMsg::text(text)).await.unwrap();
        }
        client_frame_tx
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
    async fn transport_forwards_client_frame_to_server() {
        let (client, mut server) = ws_pair().await;
        let server_task = tokio::spawn(async move {
            let msg = server.next().await.unwrap().unwrap();
            assert_eq!(msg.to_text().unwrap(), "5");
            let msg = server.next().await.unwrap().unwrap();
            assert_eq!(msg.to_text().unwrap(), "4out");
            let msg = server.next().await.unwrap().unwrap();
            assert_eq!(msg.to_text().unwrap(), "1");
            while let Some(Ok(_)) = server.next().await {}
        });
        let (server_frame_tx, _) = mpsc::channel(4);
        let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
        client_frame_tx
            .send(Frame::Packet(Packet::Message("out".into())))
            .await
            .unwrap();
        drop(client_frame_tx);
        transport(client, None, server_frame_tx, client_frame_rx)
            .await
            .unwrap();
        server_task.await.unwrap();
    }
}
