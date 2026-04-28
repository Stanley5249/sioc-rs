//! Engine.IO protocol task.

use crate::error::{BoxedError, EngineError, Error, TransportError};
use crate::packet::{Frame, Handshake, Message, Packet};
use crate::transport::TransportStrategy;
use crate::websocket::WebSocketConnector;
use futures_util::{Sink, SinkExt};
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Sleep;
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

/// Sends [`EngineAction`]s to the engine task.
#[derive(Clone, Debug)]
pub struct EngineSender(pub mpsc::Sender<EngineAction>);

impl EngineSender {
    pub async fn send<T>(&self, action: T) -> Result<(), mpsc::error::SendError<EngineAction>>
    where
        T: Into<EngineAction>,
    {
        self.0.send(action.into()).await
    }
}

/// Channel-based handles to the Engine.IO protocol and transport tasks.
pub struct Engine {
    /// Sender for delivering outbound messages from the Socket.IO layer to the engine.
    pub tx: EngineSender,
    /// Handle for the engine protocol task.
    pub engine_handle: JoinHandle<Result<(), EngineError>>,
    /// Handle for the transport coordination task.
    pub transport_handle: JoinHandle<Result<(), TransportError>>,
}

impl Engine {
    /// Spawns the engine and transport tasks and returns handles to both.
    ///
    /// `sink` receives decoded inbound [`Message`]s from the transport.
    pub fn connect<C, S>(
        url: Url,
        http_client: reqwest::Client,
        websocket_connector: C,
        strategy: TransportStrategy,
        sink: S,
    ) -> Self
    where
        C: WebSocketConnector,
        S: Sink<Message, Error = BoxedError> + Unpin + Send + 'static,
    {
        let (engine_tx, engine_rx) = mpsc::channel(32);

        let (transport_tx, transport_rx) = mpsc::channel(32);

        let (handshake_tx, handshake_rx) = oneshot::channel();

        let engine_tx = EngineSender(engine_tx);

        let token = CancellationToken::new();

        let transport_handle = strategy.connect(
            url,
            http_client,
            websocket_connector,
            handshake_tx,
            engine_tx.clone(),
            transport_rx,
            token.clone(),
        );

        let engine_handle = tokio::spawn(engine_io(
            sink,
            engine_rx,
            transport_tx,
            handshake_rx,
            token,
        ));

        Self {
            tx: engine_tx,
            engine_handle,
            transport_handle,
        }
    }

    /// Awaits the engine and transport tasks.
    ///
    /// The caller must send [`Message::Close`] before calling this so the engine
    /// exits naturally and the drop guard propagates cancellation to the transport.
    pub async fn join(self) -> Result<(), Error> {
        let (engine_result, transport_result) =
            tokio::join!(self.engine_handle, self.transport_handle);
        engine_result.map_err(EngineError::Join)??;
        transport_result.map_err(TransportError::Join)??;
        Ok(())
    }
}

struct Heartbeat {
    timer: Pin<Box<Sleep>>,
    ping_window: std::time::Duration,
}

impl Heartbeat {
    fn new(ping_window: std::time::Duration) -> Self {
        Self {
            timer: Box::pin(tokio::time::sleep(ping_window)),
            ping_window,
        }
    }

    fn reset(&mut self) {
        self.timer = Box::pin(tokio::time::sleep(self.ping_window));
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
    S: Sink<Message, Error = BoxedError> + Unpin,
{
    // Ensure the transport shuts down whenever the engine exits, regardless of the reason.
    let _guard = token.drop_guard();

    let handshake = handshake_rx.await?;
    tracing::debug!(%handshake.sid, "received handshake");

    let mut heartbeat = Heartbeat::new(handshake.ping_window());

    loop {
        let item = tokio::select! {
            _ = &mut heartbeat.timer => return Err(EngineError::HeartbeatTimeout),
            item = engine_rx.recv() => item,
        };

        let Some(action) = item else {
            tracing::debug!("engine channel closed");
            break;
        };

        match action {
            EngineAction::Transport(frame) => match frame {
                Frame::Packet(packet) => {
                    tracing::trace!(?packet, "received packet");

                    match packet {
                        Packet::Close => {
                            tracing::debug!("server closed");
                            break;
                        }
                        Packet::Ping(data) => {
                            tracing::trace!("sending PONG");
                            transport_tx.send(Packet::Pong(data).into()).await?;
                            heartbeat.reset();
                        }
                        Packet::Message(data) => {
                            sink.send(Message::Text(data))
                                .await
                                .map_err(EngineError::SendSink)?;
                        }
                        Packet::Noop => {}

                        other => {
                            return Err(EngineError::packet(
                                other,
                                "engine did not expect this packet",
                            ));
                        }
                    }
                }
                Frame::Binary(data) => {
                    tracing::trace!(len = data.len(), "received binary");

                    sink.send(Message::Binary(data))
                        .await
                        .map_err(EngineError::SendSink)?;
                }
            },
            EngineAction::Sink(message) => match message {
                Message::Text(bytes) => {
                    let packet = Packet::Message(bytes);
                    tracing::trace!(?packet, "sending packet");
                    transport_tx.send(packet.into()).await?;
                }
                Message::Binary(bytes) => {
                    tracing::trace!(len = bytes.len(), "sending binary");
                    transport_tx.send(bytes.into()).await?;
                }
                Message::Close => {
                    tracing::debug!("client closed");
                    break;
                }
            },
        };
    }

    Ok(())
}
