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
/// Sending [`Message::Close`] or dropping the sender closes the session. Either
/// way, `sink` receives [`Message::Close`] as the end of the stream.
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

    /// Receives the next frame, failing once the server misses its ping window.
    async fn recv(
        &self,
        frame_rx: &mut mpsc::Receiver<Frame>,
    ) -> Result<Option<Frame>, EngineError> {
        tokio::time::timeout_at(self.deadline, frame_rx.recv())
            .await
            .map_err(|_| EngineError::HeartbeatTimeout)
    }
}

#[tracing::instrument(skip_all, err)]
async fn engine_io<S>(
    sink: S,
    frame_rx: mpsc::Receiver<Frame>,
    message_rx: mpsc::Receiver<Message>,
    transport_tx: mpsc::Sender<Frame>,
    handshake_rx: oneshot::Receiver<Handshake>,
) -> Result<(), EngineError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let handshake = handshake_rx.await?;
    tracing::debug!(sid = %handshake.sid, "<- OPEN");

    let (pong_tx, pong_rx) = mpsc::channel(1);

    // Each direction runs on its own, so a slow consumer on one side never
    // stalls the other.
    tokio::try_join!(
        inbound(sink, frame_rx, pong_tx, handshake.ping_window()),
        outbound(message_rx, pong_rx, transport_tx),
    )?;

    Ok(())
}

/// Forwards server frames to `sink` until the server or the transport ends the session.
///
/// Sends [`Message::Close`] to `sink` as the end of the stream, whichever side
/// closed, then waits for the transport to finish.
async fn inbound<S>(
    mut sink: S,
    mut frame_rx: mpsc::Receiver<Frame>,
    pong_tx: mpsc::Sender<Frame>,
    ping_window: std::time::Duration,
) -> Result<(), EngineError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let mut heartbeat = Heartbeat::new(ping_window);

    while let Some(frame) = heartbeat.recv(&mut frame_rx).await? {
        match frame {
            Frame::Packet(packet) => {
                tracing::trace!(%packet, "<- packet");

                match packet {
                    Packet::Close => {
                        tracing::debug!("server closed");

                        break;
                    }
                    Packet::Ping(payload) => {
                        tracing::trace!("-> PONG");

                        pong_tx.send(Packet::Pong(payload).into()).await?;

                        heartbeat.reset();
                    }
                    Packet::Message(payload) => {
                        send_sink(&mut sink, Message::Text(payload)).await?;
                    }
                    Packet::Noop => {}

                    packet => return Err(EngineError::Server(packet)),
                }
            }
            Frame::Binary(payload) => {
                tracing::trace!(bytes = payload.len(), "<- binary");

                send_sink(&mut sink, Message::Binary(payload)).await?;
            }
        }
    }

    // Closing the pong channel ends the outbound loop if it is still running.
    drop(pong_tx);

    send_sink(&mut sink, Message::Close).await?;

    while heartbeat.recv(&mut frame_rx).await?.is_some() {}

    Ok(())
}

/// Forwards outbound messages and pongs to the transport until either side ends the session.
///
/// Then closes the transport channel and drains both inputs until their senders hang up.
async fn outbound(
    mut message_rx: mpsc::Receiver<Message>,
    mut pong_rx: mpsc::Receiver<Frame>,
    transport_tx: mpsc::Sender<Frame>,
) -> Result<(), EngineError> {
    loop {
        // Both branches feed `transport_tx`, so waiting on it only applies
        // backpressure to this direction.
        let frame = tokio::select! {
            pong = pong_rx.recv() => {
                let Some(pong) = pong else {
                    break;
                };

                pong
            }

            message = message_rx.recv() => match message {
                Some(Message::Text(bytes)) => {
                    let packet = Packet::Message(bytes);

                    tracing::trace!(%packet, "-> MESSAGE");

                    packet.into()
                }
                Some(Message::Binary(bytes)) => {
                    tracing::trace!(bytes = bytes.len(), "-> binary");

                    bytes.into()
                }
                Some(Message::Close) | None => {
                    tracing::debug!("client closed");
                    tracing::trace!("-> CLOSE");

                    transport_tx.send(Packet::Close.into()).await?;

                    break;
                }
            },
        };

        transport_tx.send(frame).await?;
    }

    drop(transport_tx);

    // Drain both inputs together, because the inbound loop may be waiting to
    // send a pong while the upper layer is still finishing.
    tokio::join!(drain(&mut message_rx), drain(&mut pong_rx));

    Ok(())
}

/// Receives and discards items until every sender hangs up.
async fn drain<T>(rx: &mut mpsc::Receiver<T>) {
    while rx.recv().await.is_some() {}
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
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinHandle;
    use tokio_util::sync::PollSender;

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
        assert!(matches!(store.lock().unwrap()[..], [Message::Close]));
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
    async fn engine_io_outbound_flows_while_sink_is_full() {
        let (sink_tx, _sink_rx) = mpsc::channel(1);
        let h = spawn(PollSender::new(sink_tx), make_handshake());
        for text in ["fills", "blocks"] {
            h.frame_tx
                .send(Packet::Message(text.into()).into())
                .await
                .unwrap();
        }
        h.message_tx
            .send(Message::Text("out".into()))
            .await
            .unwrap();
        let Harness {
            mut transport_rx, ..
        } = h;
        let frame = tokio::time::timeout(Duration::from_secs(5), transport_rx.recv())
            .await
            .unwrap();
        assert!(matches!(frame, Some(Frame::Packet(Packet::Message(m))) if m == "out"));
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
