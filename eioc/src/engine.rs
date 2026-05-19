//! Engine.IO protocol task.

use crate::error::{EngineError, Error};
use crate::packet::{Frame, Handshake, Message, Packet};
use crate::transport::TransportStrategy;
use crate::websocket::WebSocketConnector;
use futures_util::{Sink, SinkExt, TryFutureExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Data exchanged between the engine task and its producers
#[derive(Debug)]
pub enum EngineAction {
    /// An inbound frame from the transport layer.
    Transport(Frame),
    /// An outbound message from the upper layer.
    Sink(Message),
}

impl From<Frame> for EngineAction {
    fn from(frame: Frame) -> Self {
        Self::Transport(frame)
    }
}

impl From<Message> for EngineAction {
    fn from(message: Message) -> Self {
        Self::Sink(message)
    }
}

/// Sends [`Frame`]s to the engine task (transport layer use).
#[derive(Clone, Debug)]
pub struct FrameSender(pub mpsc::Sender<EngineAction>);

impl FrameSender {
    pub async fn send(&self, frame: Frame) -> Result<(), mpsc::error::SendError<EngineAction>> {
        self.0.send(EngineAction::Transport(frame)).await
    }
}

/// Sends [`Message`]s to the engine task (Socket.IO layer use).
#[derive(Clone, Debug)]
pub struct MessageSender(pub mpsc::Sender<EngineAction>);

impl MessageSender {
    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<EngineAction>> {
        self.0.send(EngineAction::Sink(message)).await
    }
}

/// Drives the engine protocol and transport concurrently until the session ends.
///
/// `sink` receives decoded inbound [`Message`]s from the transport.
/// The caller creates the engine channel, retains the [`MessageSender`] half,
/// and passes the [`FrameSender`] and receiver here.
#[allow(clippy::too_many_arguments)]
pub async fn connect<C, S>(
    url: Url,
    http_client: reqwest::Client,
    websocket_connector: C,
    strategy: TransportStrategy,
    sink: S,
    engine_rx: mpsc::Receiver<EngineAction>,
    frame_tx: FrameSender,
    transport_capacity: usize,
) -> Result<(), Error>
where
    C: WebSocketConnector,
    S: Sink<Message> + Unpin + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let (transport_tx, transport_rx) = mpsc::channel(transport_capacity);

    let (handshake_tx, handshake_rx) = oneshot::channel();

    let token = CancellationToken::new();

    let eio_future = engine_io(sink, engine_rx, transport_tx, handshake_rx, token.clone());

    let transport_future = strategy.run(
        url,
        http_client,
        websocket_connector,
        handshake_tx,
        frame_tx,
        transport_rx,
        token,
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

#[tracing::instrument(skip_all, err)]
async fn engine_io<S>(
    mut sink: S,
    mut engine_rx: mpsc::Receiver<EngineAction>,
    transport_tx: mpsc::Sender<Frame>,
    handshake_rx: oneshot::Receiver<Handshake>,
    token: CancellationToken,
) -> Result<(), EngineError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    // Ensure the transport shuts down whenever the engine exits, regardless of the reason.
    let _guard = token.drop_guard();

    let handshake = handshake_rx.await?;
    tracing::debug!(sid = %handshake.sid, "<- OPEN");

    let mut heartbeat = Heartbeat::new(handshake.ping_window());

    while let Some(action) = tokio::time::timeout_at(heartbeat.deadline, engine_rx.recv()).await? {
        match action {
            EngineAction::Transport(frame) => match frame {
                Frame::Packet(packet) => {
                    tracing::trace!(%packet, "<- packet");

                    match packet {
                        Packet::Close => {
                            tracing::debug!("server closed");

                            break;
                        }
                        Packet::Ping(payload) => {
                            tracing::trace!("-> PONG");

                            transport_tx.send(Packet::Pong(payload).into()).await?;

                            heartbeat.reset();
                        }
                        Packet::Message(payload) => {
                            sink.send(Message::Text(payload))
                                .await
                                .map_err(|e| EngineError::SendSink(e.into()))?;
                        }
                        Packet::Noop => {}

                        packet => return Err(EngineError::Server(packet)),
                    }
                }
                Frame::Binary(payload) => {
                    tracing::trace!(bytes = payload.len(), "<- binary");

                    sink.send(Message::Binary(payload))
                        .await
                        .map_err(|e| EngineError::SendSink(e.into()))?;
                }
            },
            EngineAction::Sink(message) => match message {
                Message::Text(bytes) => {
                    let packet = Packet::Message(bytes);

                    tracing::trace!(%packet, "-> MESSAGE");

                    transport_tx.send(packet.into()).await?;
                }
                Message::Binary(bytes) => {
                    tracing::trace!(bytes = bytes.len(), "-> binary");

                    transport_tx.send(bytes.into()).await?;
                }
                Message::Close => {
                    tracing::debug!("client closed");
                    tracing::trace!("-> CLOSE");

                    transport_tx.send(Packet::Close.into()).await?;
                    break;
                }
            },
        };
    }

    Ok(())
}
