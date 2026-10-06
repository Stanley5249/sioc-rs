//! One WebSocket session, from the handshake or upgrade to the end.

use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::connector::WebSocketStream;
use crate::error::TransportError;
use crate::packet::{Frame, Handshake, Packet};

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
pub async fn run(
    mut stream: WebSocketStream,
    handshake_tx: Option<oneshot::Sender<Handshake>>,
    server_frame_tx: mpsc::Sender<Frame>,
    client_frame_rx: mpsc::Receiver<Frame>,
) -> Result<(), TransportError> {
    if let Some(handshake_tx) = handshake_tx {
        let handshake = match crate::websocket::stream::recv_frame(&mut stream).await? {
            Frame::Packet(Packet::Open(handshake)) => handshake,
            frame => return Err(TransportError::Open(frame)),
        };

        tracing::debug!(sid = %handshake.sid, "received handshake");

        handshake_tx
            .send(handshake)
            .map_err(TransportError::Handshake)?;
    } else {
        tracing::debug!("sent upgrade packet");

        crate::websocket::stream::send_frame(&mut stream, Packet::Upgrade.into()).await?;
    }

    let (sink, stream) = stream.split();
    let stream_closed = CancellationToken::new();

    // Each direction runs on its own, so a slow engine never stalls
    // client frames and a slow socket never stalls server ones.
    tokio::try_join!(
        crate::websocket::forward::forward_server_frames(
            stream,
            server_frame_tx,
            stream_closed.clone()
        ),
        crate::websocket::forward::forward_client_frames(sink, client_frame_rx, stream_closed),
    )?;

    Ok(())
}
