//! Engine.IO protocol task.

use crate::error::{EngineError, Error};
use crate::packet::{Frame, Handshake, Message, Packet};
use crate::transport::TransportStrategy;
use crate::websocket::WebSocketConnector;
use futures_util::{Sink, SinkExt, TryFutureExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use url::Url;

/// Drives the engine protocol and transport concurrently until the session ends.
///
/// `sink` receives inbound [`Message`]s, and `message_rx` carries outbound ones.
/// Sending [`Message::Close`] or dropping the sender closes the session. When the
/// server closes it, `sink` receives [`Message::Close`].
///
/// Returns once the transport has finished and the sender of `message_rx` is dropped.
///
/// # Errors
///
/// Returns an error if either the engine or transport task fails.
#[allow(clippy::too_many_arguments)]
pub async fn connect<C, S>(
    url: Url,
    http_client: reqwest::Client,
    websocket_connector: C,
    strategy: TransportStrategy,
    sink: S,
    message_rx: mpsc::Receiver<Message>,
    frame_capacity: usize,
    transport_capacity: usize,
) -> Result<(), Error>
where
    C: WebSocketConnector,
    S: Sink<Message> + Unpin + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let (frame_tx, frame_rx) = mpsc::channel(frame_capacity);

    let (transport_tx, transport_rx) = mpsc::channel(transport_capacity);

    let (handshake_tx, handshake_rx) = oneshot::channel();

    let eio_future = engine_io(sink, frame_rx, message_rx, transport_tx, handshake_rx);

    let transport_future = strategy.run(
        url,
        http_client,
        websocket_connector,
        handshake_tx,
        frame_tx,
        transport_rx,
    );

    tokio::try_join!(
        eio_future.map_err(Error::Engine),
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
}

/// The side that ended the session.
#[derive(Debug, PartialEq, Eq)]
enum Closed {
    ByClient,
    ByServer,
}

#[tracing::instrument(skip_all, err)]
async fn engine_io<S>(
    mut sink: S,
    mut frame_rx: mpsc::Receiver<Frame>,
    mut message_rx: mpsc::Receiver<Message>,
    transport_tx: mpsc::Sender<Frame>,
    handshake_rx: oneshot::Receiver<Handshake>,
) -> Result<(), EngineError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let handshake = handshake_rx.await?;
    tracing::debug!(sid = %handshake.sid, "<- OPEN");

    let mut heartbeat = Heartbeat::new(handshake.ping_window());

    // The session takes `transport_tx` and drops it on return, which tells the
    // transport to finish.
    let closed = session(
        &mut sink,
        &mut frame_rx,
        &mut message_rx,
        transport_tx,
        &mut heartbeat,
    )
    .await?;

    if closed == Closed::ByServer {
        // The server may end the transport without a Socket.IO disconnect.
        send_sink(&mut sink, Message::Close).await?;
    }

    // Wait for both producers to hang up, so neither sends into a closed
    // channel during shutdown.
    let transport_finished = async { while frame_rx.recv().await.is_some() {} };
    tokio::time::timeout_at(heartbeat.deadline, transport_finished)
        .await
        .map_err(|_| EngineError::HeartbeatTimeout)?;
    while message_rx.recv().await.is_some() {}

    Ok(())
}

/// Exchanges frames and messages until either side closes the session.
async fn session<S>(
    sink: &mut S,
    frame_rx: &mut mpsc::Receiver<Frame>,
    message_rx: &mut mpsc::Receiver<Message>,
    transport_tx: mpsc::Sender<Frame>,
    heartbeat: &mut Heartbeat,
) -> Result<Closed, EngineError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            () = tokio::time::sleep_until(heartbeat.deadline) => {
                return Err(EngineError::HeartbeatTimeout);
            }

            frame = frame_rx.recv() => match frame {
                Some(Frame::Packet(packet)) => {
                    tracing::trace!(%packet, "<- packet");

                    match packet {
                        Packet::Close => {
                            tracing::debug!("server closed");

                            return Ok(Closed::ByServer);
                        }
                        Packet::Ping(payload) => {
                            tracing::trace!("-> PONG");

                            transport_tx.send(Packet::Pong(payload).into()).await?;

                            heartbeat.reset();
                        }
                        Packet::Message(payload) => {
                            send_sink(sink, Message::Text(payload)).await?;
                        }
                        Packet::Noop => {}

                        packet => return Err(EngineError::Server(packet)),
                    }
                }
                Some(Frame::Binary(payload)) => {
                    tracing::trace!(bytes = payload.len(), "<- binary");

                    send_sink(sink, Message::Binary(payload)).await?;
                }
                None => {
                    tracing::debug!("transport closed");

                    return Ok(Closed::ByServer);
                }
            },

            message = message_rx.recv() => match message {
                Some(Message::Text(bytes)) => {
                    let packet = Packet::Message(bytes);

                    tracing::trace!(%packet, "-> MESSAGE");

                    transport_tx.send(packet.into()).await?;
                }
                Some(Message::Binary(bytes)) => {
                    tracing::trace!(bytes = bytes.len(), "-> binary");

                    transport_tx.send(bytes.into()).await?;
                }
                Some(Message::Close) | None => {
                    tracing::debug!("client closed");
                    tracing::trace!("-> CLOSE");

                    transport_tx.send(Packet::Close.into()).await?;

                    return Ok(Closed::ByClient);
                }
            },
        }
    }
}

async fn send_sink<S>(sink: &mut S, message: Message) -> Result<(), EngineError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    sink.send(message)
        .await
        .map_err(|e| EngineError::SendSink(e.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::{Sink, sink};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinHandle;

    struct FailingSink;

    impl Sink<Message> for FailingSink {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn make_handshake() -> Handshake {
        Handshake {
            sid: "sid".into(),
            upgrades: vec![],
            ping_interval: 25_000,
            ping_timeout: 5_000,
            max_payload: 1_000_000,
        }
    }

    /// A running engine with the transport and upper-layer ends of its channels.
    struct Harness {
        frame_tx: mpsc::Sender<Frame>,
        message_tx: mpsc::Sender<Message>,
        transport_rx: mpsc::Receiver<Frame>,
        engine: JoinHandle<Result<(), EngineError>>,
    }

    fn spawn<S>(sink: S, handshake: Handshake) -> Harness
    where
        S: Sink<Message> + Unpin + Send + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let (frame_tx, frame_rx) = mpsc::channel(4);
        let (message_tx, message_rx) = mpsc::channel(4);
        let (transport_tx, transport_rx) = mpsc::channel(4);
        let (handshake_tx, handshake_rx) = oneshot::channel();
        handshake_tx.send(handshake).unwrap();
        let engine = tokio::spawn(engine_io(
            sink,
            frame_rx,
            message_rx,
            transport_tx,
            handshake_rx,
        ));
        Harness {
            frame_tx,
            message_tx,
            transport_rx,
            engine,
        }
    }

    impl Harness {
        /// Waits for the session to end, then hangs up both producers.
        ///
        /// Returns the engine result and the frames sent to the transport.
        async fn finish(mut self) -> (Result<(), EngineError>, Vec<Frame>) {
            let mut sent = Vec::new();
            while let Some(frame) = self.transport_rx.recv().await {
                sent.push(frame);
            }
            drop(self.frame_tx);
            drop(self.message_tx);
            (self.engine.await.unwrap(), sent)
        }
    }

    type CapturingSink = Pin<Box<dyn Sink<Message, Error = std::convert::Infallible> + Send>>;

    fn capturing_sink() -> (CapturingSink, Arc<Mutex<Vec<Message>>>) {
        let store = Arc::new(Mutex::new(Vec::<Message>::new()));
        let s = store.clone();
        let sink = Box::pin(sink::unfold(s, |s, msg: Message| async move {
            s.lock().unwrap().push(msg);
            Ok::<_, std::convert::Infallible>(s)
        }));
        (sink, store)
    }

    #[tokio::test]
    async fn engine_io_handshake_dropped_is_error() {
        let (_frame_tx, frame_rx) = mpsc::channel(4);
        let (_message_tx, message_rx) = mpsc::channel(4);
        let (transport_tx, _) = mpsc::channel(4);
        let (handshake_tx, handshake_rx) = oneshot::channel::<Handshake>();
        drop(handshake_tx);
        let result = engine_io(
            sink::drain(),
            frame_rx,
            message_rx,
            transport_tx,
            handshake_rx,
        )
        .await;
        assert!(matches!(result, Err(EngineError::RecvHandshake(_))));
    }

    #[tokio::test]
    async fn engine_io_server_close_notifies_sink() {
        let (sink, store) = capturing_sink();
        let h = spawn(sink, make_handshake());
        h.frame_tx.send(Packet::Close.into()).await.unwrap();
        let (result, sent) = h.finish().await;
        result.unwrap();
        assert!(sent.is_empty());
        assert!(matches!(store.lock().unwrap()[..], [Message::Close]));
    }

    #[tokio::test]
    async fn engine_io_transport_closed_notifies_sink() {
        let (sink, store) = capturing_sink();
        let Harness {
            frame_tx,
            message_tx,
            mut transport_rx,
            engine,
        } = spawn(sink, make_handshake());
        drop(frame_tx);
        assert!(transport_rx.recv().await.is_none());
        drop(message_tx);
        engine.await.unwrap().unwrap();
        assert!(matches!(store.lock().unwrap()[..], [Message::Close]));
    }

    #[tokio::test]
    async fn engine_io_client_close_sends_close_packet() {
        let (sink, store) = capturing_sink();
        let h = spawn(sink, make_handshake());
        h.message_tx.send(Message::Close).await.unwrap();
        let (result, sent) = h.finish().await;
        result.unwrap();
        assert!(matches!(sent[..], [Frame::Packet(Packet::Close)]));
        assert!(store.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn engine_io_message_sender_dropped_sends_close_packet() {
        let Harness {
            frame_tx,
            message_tx,
            mut transport_rx,
            engine,
        } = spawn(sink::drain(), make_handshake());
        drop(message_tx);
        assert!(matches!(
            transport_rx.recv().await.unwrap(),
            Frame::Packet(Packet::Close)
        ));
        drop(frame_tx);
        engine.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn engine_io_accepts_messages_until_sender_dropped() {
        let Harness {
            frame_tx,
            message_tx,
            mut transport_rx,
            engine,
        } = spawn(sink::drain(), make_handshake());
        frame_tx.send(Packet::Close.into()).await.unwrap();
        assert!(transport_rx.recv().await.is_none());
        message_tx.send(Message::Close).await.unwrap();
        drop(frame_tx);
        drop(message_tx);
        engine.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn engine_io_accepts_frames_until_transport_finishes() {
        let Harness {
            frame_tx,
            message_tx,
            mut transport_rx,
            engine,
        } = spawn(sink::drain(), make_handshake());
        message_tx.send(Message::Close).await.unwrap();
        assert!(matches!(
            transport_rx.recv().await.unwrap(),
            Frame::Packet(Packet::Close)
        ));
        assert!(transport_rx.recv().await.is_none());
        frame_tx
            .send(Packet::Message("late".into()).into())
            .await
            .unwrap();
        drop(frame_tx);
        drop(message_tx);
        engine.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn engine_io_ping_triggers_pong() {
        let h = spawn(sink::drain(), make_handshake());
        h.frame_tx
            .send(Packet::Ping("probe".into()).into())
            .await
            .unwrap();
        h.frame_tx.send(Packet::Close.into()).await.unwrap();
        let (result, sent) = h.finish().await;
        result.unwrap();
        assert!(matches!(&sent[..], [Frame::Packet(Packet::Pong(p))] if p == "probe"));
    }

    #[tokio::test]
    async fn engine_io_noop_is_ignored() {
        let h = spawn(sink::drain(), make_handshake());
        h.frame_tx.send(Packet::Noop.into()).await.unwrap();
        h.frame_tx.send(Packet::Close.into()).await.unwrap();
        let (result, sent) = h.finish().await;
        result.unwrap();
        assert!(sent.is_empty());
    }

    #[tokio::test]
    async fn engine_io_unexpected_packet_is_server_error() {
        let h = spawn(sink::drain(), make_handshake());
        h.frame_tx.send(Packet::Upgrade.into()).await.unwrap();
        let (result, _) = h.finish().await;
        assert!(matches!(result, Err(EngineError::Server(Packet::Upgrade))));
    }

    #[tokio::test]
    async fn engine_io_text_message_forwarded_to_sink() {
        let (sink, store) = capturing_sink();
        let h = spawn(sink, make_handshake());
        h.frame_tx
            .send(Packet::Message("hello".into()).into())
            .await
            .unwrap();
        h.frame_tx.send(Packet::Close.into()).await.unwrap();
        let (result, _) = h.finish().await;
        result.unwrap();
        assert!(
            matches!(&store.lock().unwrap()[..], [Message::Text(t), Message::Close] if t == "hello")
        );
    }

    #[tokio::test]
    async fn engine_io_binary_frame_forwarded_to_sink() {
        let (sink, store) = capturing_sink();
        let h = spawn(sink, make_handshake());
        h.frame_tx
            .send(Frame::Binary(Bytes::from_static(b"bin")))
            .await
            .unwrap();
        h.frame_tx.send(Packet::Close.into()).await.unwrap();
        let (result, _) = h.finish().await;
        result.unwrap();
        assert!(
            matches!(&store.lock().unwrap()[..], [Message::Binary(b), Message::Close] if b.as_ref() == b"bin")
        );
    }

    #[tokio::test]
    async fn engine_io_outbound_text_sent_to_transport() {
        let h = spawn(sink::drain(), make_handshake());
        h.message_tx
            .send(Message::Text("out".into()))
            .await
            .unwrap();
        h.message_tx.send(Message::Close).await.unwrap();
        let (result, sent) = h.finish().await;
        result.unwrap();
        assert!(matches!(
            &sent[..],
            [Frame::Packet(Packet::Message(m)), Frame::Packet(Packet::Close)] if m == "out"
        ));
    }

    #[tokio::test]
    async fn engine_io_outbound_binary_sent_to_transport() {
        let h = spawn(sink::drain(), make_handshake());
        h.message_tx
            .send(Message::Binary(Bytes::from_static(b"out")))
            .await
            .unwrap();
        h.message_tx.send(Message::Close).await.unwrap();
        let (result, sent) = h.finish().await;
        result.unwrap();
        assert!(matches!(
            &sent[..],
            [Frame::Binary(b), Frame::Packet(Packet::Close)] if b.as_ref() == b"out"
        ));
    }

    #[tokio::test]
    async fn engine_io_heartbeat_timeout_fires() {
        let handshake = Handshake {
            ping_interval: 1,
            ping_timeout: 1,
            ..make_handshake()
        };
        let h = spawn(sink::drain(), handshake);
        let (result, _) = h.finish().await;
        assert!(matches!(result, Err(EngineError::HeartbeatTimeout)));
    }

    #[tokio::test]
    async fn engine_io_message_sink_error() {
        let h = spawn(FailingSink, make_handshake());
        h.frame_tx
            .send(Packet::Message("x".into()).into())
            .await
            .unwrap();
        let (result, _) = h.finish().await;
        assert!(matches!(result, Err(EngineError::SendSink(_))));
    }
}
