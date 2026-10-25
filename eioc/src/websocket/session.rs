//! One WebSocket session, from the handshake or upgrade to the end.

use std::pin::pin;

use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::connector::WebSocketStream;
use crate::error::TransportError;
use crate::packet::{Frame, Packet};
use crate::transport::{ClientFrame, ServerFrame};

/// Drives the WebSocket I/O until the session ends.
///
/// When `direct` is true, reads the server's Open frame first and forwards
/// it (direct WebSocket transport). Otherwise sends `Upgrade` at once
/// (polling upgrade path).
///
/// Server and client frames flow independently. When the engine closes
/// `client_frame_rx`, sends the Close packet and a close frame, then stops
/// without waiting for the server's reply, like the JS `WebSocket.close`, so
/// a silent server cannot hold the close open. If the server ends the stream
/// first, drops `server_frame_tx` and discards client frames until the engine
/// closes `client_frame_rx`.
///
/// # Errors
///
/// Returns an error if a transport or protocol failure occurs.
#[tracing::instrument(skip_all)]
pub async fn run(
    mut stream: WebSocketStream,
    direct: bool,
    server_frame_tx: mpsc::Sender<ServerFrame>,
    client_frame_rx: mpsc::Receiver<ClientFrame>,
) -> Result<(), TransportError> {
    if direct {
        let frame = crate::websocket::stream::recv_frame(&mut stream).await?;
        let Frame::Packet(Packet::Open(handshake)) = frame else {
            return Err(TransportError::Open(frame));
        };

        server_frame_tx.send(ServerFrame::Open(handshake)).await?;
    } else {
        tracing::debug!("sent upgrade packet");

        crate::websocket::stream::send_frame(&mut stream, Packet::Upgrade.into()).await?;
    }

    let (sink, stream) = stream.split();
    let stream_closed = CancellationToken::new();

    // Each direction runs on its own, so a slow engine never stalls client
    // frames and a slow socket never stalls server ones.
    let mut server = pin!(crate::websocket::forward::forward_server_frames(
        stream,
        server_frame_tx,
        stream_closed.clone()
    ));
    let mut client = pin!(crate::websocket::forward::forward_client_frames(
        sink,
        client_frame_rx,
        stream_closed
    ));

    tokio::select! {
        // The client direction ends only after the engine closed it, so the
        // session is over and unread server frames have no destination.
        result = &mut client => result,
        result = &mut server => {
            result?;
            client.await
        }
    }
}
