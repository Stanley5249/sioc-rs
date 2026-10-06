//! Engine.IO protocol task.

use futures_util::TryFutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use url::Url;

use crate::error::{EngineError, Error};
use crate::packet::{Frame, Handshake, Message, Packet};
use crate::transport::TransportStrategy;
use crate::websocket::WebSocketConnector;

/// Drives the engine protocol and transport concurrently until the session
/// ends.
///
/// `server_message_tx` receives what the server sends, and `client_message_rx`
/// carries what the client sends. Dropping the sender of `client_message_rx`
/// closes the session, and the engine drops `server_message_tx` once the
/// session has ended, whichever side closed it.
///
/// Returns once the transport has finished and the sender of
/// `client_message_rx` is dropped.
///
/// # Errors
///
/// Returns an error if either the engine or transport task fails.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a separate channel end or connection setting; an argument struct waits for the transport API review"
)]
pub async fn connect<C>(
    url: Url,
    http_client: reqwest::Client,
    websocket_connector: C,
    strategy: TransportStrategy,
    server_message_tx: mpsc::Sender<Message>,
    client_message_rx: mpsc::Receiver<Message>,
    server_frame_capacity: usize,
    client_frame_capacity: usize,
) -> Result<(), Error>
where
    C: WebSocketConnector,
{
    let (server_frame_tx, server_frame_rx) = mpsc::channel(server_frame_capacity);

    let (client_frame_tx, client_frame_rx) = mpsc::channel(client_frame_capacity);

    let (handshake_tx, handshake_rx) = oneshot::channel();

    let protocol_future = run_protocol(
        server_frame_rx,
        server_message_tx,
        client_message_rx,
        client_frame_tx,
        handshake_rx,
    );

    let transport_future = crate::transport::open(
        strategy,
        url,
        http_client,
        websocket_connector,
        handshake_tx,
        server_frame_tx,
        client_frame_rx,
    );

    tokio::try_join!(
        protocol_future.map_err(Error::Engine),
        transport_future.map_err(Error::Transport),
    )?;

    Ok(())
}

struct Heartbeat {
    deadline: Instant,
    ping_window: std::time::Duration,
}

impl Heartbeat {
    fn new(ping_window: std::time::Duration) -> Self {
        Self {
            deadline: Instant::now() + ping_window,
            ping_window,
        }
    }

    fn reset(&mut self) {
        self.deadline = Instant::now() + self.ping_window;
    }

    /// Receives the next frame, failing once the server misses its ping window.
    async fn next_server_frame(
        &self,
        server_frame_rx: &mut mpsc::Receiver<Frame>,
    ) -> Result<Option<Frame>, EngineError> {
        tokio::time::timeout_at(self.deadline, server_frame_rx.recv())
            .await
            .map_err(|_| EngineError::HeartbeatTimeout)
    }
}

#[tracing::instrument(skip_all)]
async fn run_protocol(
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
async fn server_frames_to_messages(
    mut server_frame_rx: mpsc::Receiver<Frame>,
    server_message_tx: mpsc::Sender<Message>,
    pong_tx: mpsc::Sender<Frame>,
    ping_window: std::time::Duration,
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

                    packet => return Err(EngineError::UnexpectedPacket(packet)),
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
async fn client_messages_to_frames(
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
async fn drain<T>(rx: &mut mpsc::Receiver<T>) {
    while rx.recv().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinHandle;

    use super::*;

    fn make_handshake() -> Handshake {
        Handshake {
            sid: "sid".into(),
            upgrades: vec![],
            ping_interval: 25_000,
            ping_timeout: 5_000,
            max_payload: 1_000_000,
        }
    }

    /// A running engine with the transport and upper-layer ends of its
    /// channels.
    struct Harness {
        server_frame_tx: mpsc::Sender<Frame>,
        server_message_rx: mpsc::Receiver<Message>,
        client_message_tx: mpsc::Sender<Message>,
        client_frame_rx: mpsc::Receiver<Frame>,
        engine: JoinHandle<Result<(), EngineError>>,
    }

    fn spawn(handshake: Handshake, server_message_capacity: usize) -> Harness {
        let (server_frame_tx, server_frame_rx) = mpsc::channel(4);
        let (server_message_tx, server_message_rx) = mpsc::channel(server_message_capacity);
        let (client_message_tx, client_message_rx) = mpsc::channel(4);
        let (client_frame_tx, client_frame_rx) = mpsc::channel(4);
        let (handshake_tx, handshake_rx) = oneshot::channel();
        handshake_tx.send(handshake).unwrap();
        let engine = tokio::spawn(run_protocol(
            server_frame_rx,
            server_message_tx,
            client_message_rx,
            client_frame_tx,
            handshake_rx,
        ));
        Harness {
            server_frame_tx,
            server_message_rx,
            client_message_tx,
            client_frame_rx,
            engine,
        }
    }

    impl Harness {
        /// Ends the session from the transport, then hangs up the upper layer.
        ///
        /// Returns the engine result, the frames sent to the transport, and the
        /// messages sent to the upper layer.
        async fn finish(mut self) -> (Result<(), EngineError>, Vec<Frame>, Vec<Message>) {
            drop(self.server_frame_tx);
            let mut frames = Vec::new();
            while let Some(frame) = self.client_frame_rx.recv().await {
                frames.push(frame);
            }
            drop(self.client_message_tx);
            let mut messages = Vec::new();
            while let Some(message) = self.server_message_rx.recv().await {
                messages.push(message);
            }
            (self.engine.await.unwrap(), frames, messages)
        }
    }

    #[tokio::test]
    async fn protocol_handshake_dropped_is_error() {
        let (_server_frame_tx, server_frame_rx) = mpsc::channel(4);
        let (server_message_tx, _server_message_rx) = mpsc::channel(4);
        let (_client_message_tx, client_message_rx) = mpsc::channel(4);
        let (client_frame_tx, _) = mpsc::channel(4);
        let (handshake_tx, handshake_rx) = oneshot::channel::<Handshake>();
        drop(handshake_tx);
        let result = run_protocol(
            server_frame_rx,
            server_message_tx,
            client_message_rx,
            client_frame_tx,
            handshake_rx,
        )
        .await;
        assert!(matches!(result, Err(EngineError::Handshake(_))));
    }

    #[tokio::test]
    async fn protocol_transport_closed_ends_message_stream() {
        let Harness {
            server_frame_tx,
            mut server_message_rx,
            client_message_tx,
            mut client_frame_rx,
            engine,
        } = spawn(make_handshake(), 4);
        drop(server_frame_tx);
        assert!(server_message_rx.recv().await.is_none());
        assert!(client_frame_rx.recv().await.is_none());
        drop(client_message_tx);
        engine.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn protocol_client_close_ends_frame_stream() {
        let Harness {
            server_frame_tx,
            mut server_message_rx,
            client_message_tx,
            mut client_frame_rx,
            engine,
        } = spawn(make_handshake(), 4);
        drop(client_message_tx);
        assert!(client_frame_rx.recv().await.is_none());

        // Frames keep arriving until the transport finishes.
        server_frame_tx
            .send(Packet::Message("late".into()).into())
            .await
            .unwrap();
        drop(server_frame_tx);
        assert!(matches!(
            server_message_rx.recv().await,
            Some(Message::Text(t)) if t == "late"
        ));
        assert!(server_message_rx.recv().await.is_none());
        engine.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn protocol_accepts_client_messages_until_sender_dropped() {
        let Harness {
            server_frame_tx,
            mut server_message_rx,
            client_message_tx,
            mut client_frame_rx,
            engine,
        } = spawn(make_handshake(), 4);
        drop(server_frame_tx);
        assert!(server_message_rx.recv().await.is_none());
        assert!(client_frame_rx.recv().await.is_none());
        client_message_tx
            .send(Message::Text("late".into()))
            .await
            .unwrap();
        drop(client_message_tx);
        engine.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn protocol_client_messages_flow_while_upper_layer_is_full() {
        let h = spawn(make_handshake(), 1);
        for text in ["fills", "blocks"] {
            h.server_frame_tx
                .send(Packet::Message(text.into()).into())
                .await
                .unwrap();
        }
        h.client_message_tx
            .send(Message::Text("out".into()))
            .await
            .unwrap();
        let Harness {
            mut client_frame_rx,
            ..
        } = h;
        let frame = tokio::time::timeout(Duration::from_secs(5), client_frame_rx.recv())
            .await
            .unwrap();
        assert!(matches!(frame, Some(Frame::Packet(Packet::Message(m))) if m == "out"));
    }

    #[tokio::test]
    async fn protocol_ping_triggers_pong() {
        let h = spawn(make_handshake(), 4);
        h.server_frame_tx
            .send(Packet::Ping("probe".into()).into())
            .await
            .unwrap();
        let (result, frames, _) = h.finish().await;
        result.unwrap();
        assert!(matches!(&frames[..], [Frame::Packet(Packet::Pong(p))] if p == "probe"));
    }

    #[tokio::test]
    async fn protocol_noop_is_ignored() {
        let h = spawn(make_handshake(), 4);
        h.server_frame_tx.send(Packet::Noop.into()).await.unwrap();
        let (result, frames, messages) = h.finish().await;
        result.unwrap();
        assert!(frames.is_empty());
        assert!(messages.is_empty());
    }

    #[tokio::test]
    async fn protocol_unexpected_packet_is_server_error() {
        let h = spawn(make_handshake(), 4);
        h.server_frame_tx
            .send(Packet::Upgrade.into())
            .await
            .unwrap();
        let (result, _, _) = h.finish().await;
        assert!(matches!(
            result,
            Err(EngineError::UnexpectedPacket(Packet::Upgrade))
        ));
    }

    #[tokio::test]
    async fn protocol_server_messages_forwarded() {
        let h = spawn(make_handshake(), 4);
        h.server_frame_tx
            .send(Packet::Message("hello".into()).into())
            .await
            .unwrap();
        h.server_frame_tx
            .send(Frame::Binary(Bytes::from_static(b"bin")))
            .await
            .unwrap();
        let (result, _, messages) = h.finish().await;
        result.unwrap();
        assert!(matches!(
            &messages[..],
            [Message::Text(t), Message::Binary(b)] if t == "hello" && b.as_ref() == b"bin"
        ));
    }

    #[tokio::test]
    async fn protocol_client_messages_sent_to_transport() {
        let Harness {
            server_frame_tx,
            server_message_rx: _server_message_rx,
            client_message_tx,
            mut client_frame_rx,
            engine,
        } = spawn(make_handshake(), 4);
        client_message_tx
            .send(Message::Text("out".into()))
            .await
            .unwrap();
        client_message_tx
            .send(Message::Binary(Bytes::from_static(b"out")))
            .await
            .unwrap();
        drop(client_message_tx);
        let mut frames = Vec::new();
        while let Some(frame) = client_frame_rx.recv().await {
            frames.push(frame);
        }
        assert!(matches!(
            &frames[..],
            [Frame::Packet(Packet::Message(m)), Frame::Binary(b)]
                if m == "out" && b.as_ref() == b"out"
        ));
        drop(server_frame_tx);
        engine.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn protocol_heartbeat_timeout_fires() {
        let handshake = Handshake {
            ping_interval: 1,
            ping_timeout: 1,
            ..make_handshake()
        };
        let h = spawn(handshake, 4);
        assert!(matches!(
            h.engine.await.unwrap(),
            Err(EngineError::HeartbeatTimeout)
        ));
    }

    #[tokio::test]
    async fn protocol_closed_message_receiver_is_error() {
        let Harness {
            server_frame_tx,
            server_message_rx,
            client_message_tx: _client_message_tx,
            client_frame_rx: _client_frame_rx,
            engine,
        } = spawn(make_handshake(), 4);
        drop(server_message_rx);
        server_frame_tx
            .send(Packet::Message("x".into()).into())
            .await
            .unwrap();
        assert!(matches!(
            engine.await.unwrap(),
            Err(EngineError::ServerMessage(_))
        ));
    }
}
