//! The loops that forward frames between the socket and the engine.

use futures_util::SinkExt;
use futures_util::stream::{SplitSink, SplitStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::{Error as TungsteniteError, Message as WebSocketMessage};
use tokio_util::sync::CancellationToken;

use crate::connector::WebSocketStream;
use crate::error::{TransportError, WebSocketError};
use crate::packet::{Frame, Packet};

/// Forwards server frames to the engine until the stream ends.
///
/// Returning drops `server_frame_tx`, which tells the engine the transport has
/// finished.
pub async fn forward_server_frames(
    mut stream: SplitStream<WebSocketStream>,
    server_frame_tx: mpsc::Sender<Frame>,
    stream_closed: CancellationToken,
) -> Result<(), TransportError> {
    let _guard = stream_closed.drop_guard();

    while let Some(frame) = crate::websocket::message::next_frame(&mut stream).await? {
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
pub async fn forward_client_frames(
    mut sink: SplitSink<WebSocketStream, WebSocketMessage>,
    mut client_frame_rx: mpsc::Receiver<Frame>,
    stream_closed: CancellationToken,
) -> Result<(), TransportError> {
    loop {
        tokio::select! {
            frame = client_frame_rx.recv() => {
                let Some(frame) = frame else {
                    tracing::debug!("client frame channel closed");

                    let close = crate::websocket::message::encode_frame(Packet::Close.into());
                    finish_write(sink.send(close).await)?;

                    break;
                };

                let message = crate::websocket::message::encode_frame(frame);
                if !finish_write(sink.send(message).await)? {
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
pub fn finish_write(result: Result<(), TungsteniteError>) -> Result<bool, WebSocketError> {
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
