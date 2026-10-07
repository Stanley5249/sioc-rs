//! The protocol loops: server frames to messages, and client messages to
//! frames.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::engine::heartbeat::Heartbeat;
use crate::error::EngineError;
use crate::packet::{Frame, Handshake, Message, Packet};

#[tracing::instrument(skip_all)]
pub async fn run_protocol(
    server_frame_rx: mpsc::Receiver<Frame>,
    server_message_tx: mpsc::Sender<Message>,
    client_message_rx: mpsc::Receiver<Message>,
    client_frame_tx: mpsc::Sender<Frame>,
    handshake_rx: oneshot::Receiver<Handshake>,
) -> Result<(), EngineError> {
    let handshake = handshake_rx.await?;
    tracing::debug!(sid = %handshake.sid, "received handshake");

    let (pong_tx, pong_rx) = mpsc::channel(1);

    // Each direction runs on its own, so a slow consumer on one side never
    // stalls the other.
    tokio::try_join!(
        server_frames_to_messages(
            server_frame_rx,
            server_message_tx,
            pong_tx,
            handshake.ping_window()
        ),
        client_messages_to_frames(client_message_rx, pong_rx, client_frame_tx),
    )?;

    Ok(())
}

/// Forwards server frames as messages until the transport finishes.
///
/// The transport ends `server_frame_rx` at the server's `Close` packet too, so
/// the channel's end is the one end of the session, whichever side closed.
/// Returning drops `server_message_tx`, which ends the message stream.
pub async fn server_frames_to_messages(
    mut server_frame_rx: mpsc::Receiver<Frame>,
    server_message_tx: mpsc::Sender<Message>,
    pong_tx: mpsc::Sender<Frame>,
    ping_window: Duration,
) -> Result<(), EngineError> {
    let mut heartbeat = Heartbeat::new(ping_window);

    while let Some(frame) = heartbeat.next_server_frame(&mut server_frame_rx).await? {
        match frame {
            Frame::Packet(packet) => {
                tracing::trace!(%packet, "received packet");

                match packet {
                    Packet::Ping(payload) => {
                        tracing::trace!("sent pong");

                        pong_tx.send(Packet::Pong(payload).into()).await?;

                        heartbeat.reset();
                    }
                    Packet::Message(payload) => {
                        server_message_tx.send(Message::Text(payload)).await?;
                    }
                    Packet::Noop => {}

                    // Like the JS client, which ignores them, so a quirky
                    // server cannot end the session.
                    packet => tracing::warn!(%packet, "ignored out-of-place packet"),
                }
            }
            Frame::Binary(payload) => {
                tracing::trace!(bytes = payload.len(), "received binary frame");

                server_message_tx.send(Message::Binary(payload)).await?;
            }
        }
    }

    tracing::debug!("transport finished");

    // Dropping `pong_tx` ends the client-message loop if it is still running.
    Ok(())
}

/// Forwards client messages and pongs as frames until either side ends the
/// session.
///
/// Then drops `client_frame_tx`, which tells the transport to send the server
/// a `Close` packet, and drains both inputs until their senders hang up.
pub async fn client_messages_to_frames(
    mut client_message_rx: mpsc::Receiver<Message>,
    mut pong_rx: mpsc::Receiver<Frame>,
    client_frame_tx: mpsc::Sender<Frame>,
) -> Result<(), EngineError> {
    loop {
        // Both handlers feed `client_frame_tx`, so waiting on it holds up only
        // this direction, which no other arm could serve anyway.
        let frame = tokio::select! {
            pong = pong_rx.recv() => {
                let Some(pong) = pong else {
                    break;
                };

                pong
            }

            message = client_message_rx.recv() => match message {
                Some(Message::Text(bytes)) => {
                    let packet = Packet::Message(bytes);

                    tracing::trace!(%packet, "sent packet");

                    packet.into()
                }
                Some(Message::Binary(bytes)) => {
                    tracing::trace!(bytes = bytes.len(), "sent binary frame");

                    bytes.into()
                }
                None => {
                    tracing::debug!("client closed");

                    break;
                }
            },
        };

        client_frame_tx.send(frame).await?;
    }

    drop(client_frame_tx);

    // Drain both inputs together, because the server-frame loop may be waiting
    // to send a pong while the upper layer is still finishing.
    tokio::join!(drain(&mut client_message_rx), drain(&mut pong_rx));

    Ok(())
}

/// Receives and discards items until every sender hangs up.
pub async fn drain<T>(rx: &mut mpsc::Receiver<T>) {
    while rx.recv().await.is_some() {}
}
