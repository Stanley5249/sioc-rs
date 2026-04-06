//! Engine.IO protocol task.

use crate::error::{BoxedError, Error, Result};
use crate::packet::{Frame, Handshake, Packet, Transit};
use crate::transport::TransportStrategy;
use crate::utils::join_tasks;
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
    /// A decoded frame received from the transport.
    Frame(Frame),
    /// An outbound message from the Socket.IO layer.
    Transit(Transit),
}

/// Restricted sender: can only deliver [`Frame`]s to the engine (used by transport tasks).
#[derive(Clone, Debug)]
pub struct FrameSender(mpsc::Sender<EngineAction>);

impl FrameSender {
    pub fn new(sender: mpsc::Sender<EngineAction>) -> Self {
        Self(sender)
    }

    pub async fn send(&self, frame: Frame) -> Result<()> {
        Ok(self.0.send(EngineAction::Frame(frame)).await?)
    }
}

/// Sends outbound [`Message`]s to the engine (used by the socket router).
#[derive(Clone, Debug)]
pub struct TransitSender(mpsc::Sender<EngineAction>);

impl TransitSender {
    pub fn new(sender: mpsc::Sender<EngineAction>) -> Self {
        Self(sender)
    }

    pub async fn send(&self, message: Transit) -> Result<()> {
        Ok(self.0.send(EngineAction::Transit(message)).await?)
    }
}

/// Channel-based handles to the Engine.IO protocol and transport tasks.
pub struct Engine {
    /// Sender for delivering outbound messages from the Socket.IO layer to the engine.
    pub tx: TransitSender,
    /// Handle for the engine protocol task.
    pub engine_handle: JoinHandle<Result<()>>,
    /// Handle for the transport coordination task.
    pub transport_handle: JoinHandle<Result<()>>,
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
        S: Sink<Transit, Error = BoxedError> + Unpin + Send + 'static,
    {
        let (engine_tx, engine_rx) = mpsc::channel(32);
        let (transport_tx, transport_rx) = mpsc::channel(32);
        let (handshake_tx, handshake_rx) = oneshot::channel();

        let token = CancellationToken::new();

        let transport_handle = strategy.connect(
            url,
            http_client,
            websocket_connector,
            handshake_tx,
            FrameSender::new(engine_tx.clone()),
            transport_rx,
            token.clone(),
        );

        let engine_handle = tokio::spawn(engine_loop(
            sink,
            engine_rx,
            transport_tx,
            handshake_rx,
            token,
        ));

        Self {
            tx: TransitSender::new(engine_tx),
            engine_handle,
            transport_handle,
        }
    }

    pub async fn join(self) -> Result<()> {
        let _ = join_tasks(self.engine_handle, self.transport_handle).await?;
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
async fn engine_loop<S>(
    mut sink: S,
    mut engine_rx: mpsc::Receiver<EngineAction>,
    transport_tx: mpsc::Sender<Frame>,
    handshake_rx: oneshot::Receiver<Handshake>,
    token: CancellationToken,
) -> Result<()>
where
    S: Sink<Transit, Error = BoxedError> + Unpin,
{
    // Cancels the transport when this task exits for any reason (error, close, or drop).
    let _guard = token.drop_guard();

    let handshake = handshake_rx.await?;
    tracing::debug!(?handshake, "received handshake");

    let mut heartbeat = Heartbeat::new(handshake.ping_window());

    loop {
        let item = tokio::select! {
            _ = &mut heartbeat.timer => return Err(Error::HeartbeatTimeout),
            item = engine_rx.recv() => item,
        };

        let Some(action) = item else {
            tracing::debug!("engine channel closed");
            break;
        };

        match action {
            EngineAction::Frame(frame) => match frame {
                Frame::Packet(packet) => {
                    tracing::trace!(?packet, "received packet");

                    match packet {
                        Packet::Close => {
                            tracing::debug!("server closed");
                            break;
                        }
                        Packet::Ping(data) => {
                            transport_tx.send(Packet::Pong(data).into()).await?;
                            heartbeat.reset();
                        }
                        Packet::Message(data) => {
                            sink.send(Transit::Text(data))
                                .await
                                .map_err(Error::SendTransit)?;
                        }
                        Packet::Noop => {}

                        other => {
                            return Err(other.unexpected("client did not expect this packet"));
                        }
                    }
                }
                Frame::Binary(data) => {
                    tracing::trace!(len = data.len(), "received binary");

                    sink.send(Transit::Binary(data))
                        .await
                        .map_err(Error::SendTransit)?;
                }
            },
            EngineAction::Transit(message) => match message {
                Transit::Text(bytes) => {
                    let packet = Packet::Message(bytes);
                    tracing::trace!(?packet, "sending packet");
                    transport_tx.send(packet.into()).await?;
                }
                Transit::Binary(bytes) => {
                    tracing::trace!(len = bytes.len(), "sending binary");
                    transport_tx.send(bytes.into()).await?;
                }
                Transit::Close => {
                    tracing::debug!("client closed");
                    break;
                }
            },
        };
    }

    Ok(())
}
