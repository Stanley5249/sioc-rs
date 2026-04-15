//! Engine.IO protocol task.

use crate::error::{BoxedError, EngineError, Error, TransportError};
use crate::packet::{Frame, Handshake, Packet, Transit};
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
    Sink(Transit),
}

impl From<Frame> for EngineAction {
    fn from(frame: Frame) -> Self {
        Self::Transport(frame)
    }
}

impl From<Transit> for EngineAction {
    fn from(transit: Transit) -> Self {
        Self::Sink(transit)
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
    /// `sink` receives decoded inbound [`Transit`]s from the transport.
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

        let engine_handle = tokio::spawn(engine_loop(
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

    pub async fn join(self) -> Result<(), Error> {
        let (engine_result, transport_result) =
            tokio::join!(self.engine_handle, self.transport_handle);
        engine_result.map_err(EngineError::Task)??;
        transport_result.map_err(TransportError::Task)??;
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
) -> Result<(), EngineError>
where
    S: Sink<Transit, Error = BoxedError> + Unpin,
{
    // Ensure the transport shuts down whenever the engine exits, regardless of the reason.
    let _guard = token.drop_guard();

    let handshake = handshake_rx.await?;
    tracing::debug!(?handshake, "received handshake");

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
                            transport_tx.send(Packet::Pong(data).into()).await?;
                            heartbeat.reset();
                        }
                        Packet::Message(data) => {
                            sink.send(Transit::Text(data))
                                .await
                                .map_err(EngineError::SendTransit)?;
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

                    sink.send(Transit::Binary(data))
                        .await
                        .map_err(EngineError::SendTransit)?;
                }
            },
            EngineAction::Sink(message) => match message {
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
