//! Engine.IO protocol task.

use crate::error::{Error, Result};
use crate::packet::{EioPacket, Frame, Handshake, Message};
use crate::transport::TransportStrategy;
use crate::utils::join_tasks;
use crate::websocket::WebSocketConnector;
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
    Message(Message),
}

/// Restricted sender: can only deliver [`Frame`]s to the engine (used by transport tasks).
#[derive(Clone, Debug)]
pub struct FrameSender(pub mpsc::Sender<EngineAction>);

impl FrameSender {
    pub async fn send(&self, frame: Frame) -> Result<(), mpsc::error::SendError<EngineAction>> {
        self.0.send(EngineAction::Frame(frame)).await
    }
}

/// Restricted sender: can only deliver outbound [`Message`]s to the engine (used by the Socket.IO manager).
#[derive(Clone, Debug)]
pub struct MessageSender(pub mpsc::Sender<EngineAction>);

impl MessageSender {
    pub async fn send(&self, message: Message) -> Result<()> {
        Ok(self.0.send(EngineAction::Message(message)).await?)
    }
}

/// Channel-based handles to the Engine.IO protocol and transport tasks.
pub struct Engine {
    /// Sender for delivering outbound messages from the Socket.IO layer to the engine.
    pub tx: MessageSender,
    /// Handle for the engine protocol task.
    pub engine_handle: JoinHandle<Result<()>>,
    /// Handle for the transport coordination task.
    pub transport_handle: JoinHandle<Result<()>>,
}

impl Engine {
    /// Spawns the engine and transport tasks and returns handles to both.
    ///
    /// `inbound_tx` receives decoded inbound [`Message`]s. The channel type `T`
    /// must be constructible from a `Message` (e.g. `ManagerAction::from`).
    pub fn connect<C, T>(
        url: Url,
        http_client: reqwest::Client,
        websocket_connector: C,
        strategy: TransportStrategy,
        inbound_tx: mpsc::Sender<T>,
    ) -> Self
    where
        C: WebSocketConnector,
        T: From<Message> + Send + Sync + 'static,
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
            FrameSender(engine_tx.clone()),
            transport_rx,
            token.clone(),
        );

        let engine_handle = tokio::spawn(engine_loop(
            engine_rx,
            inbound_tx,
            transport_tx,
            handshake_rx,
            token,
        ));

        Self {
            tx: MessageSender(engine_tx),
            engine_handle,
            transport_handle,
        }
    }

    #[tracing::instrument(skip_all, err)]
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
async fn engine_loop<T>(
    mut engine_rx: mpsc::Receiver<EngineAction>,
    inbound_tx: mpsc::Sender<T>,
    transport_tx: mpsc::Sender<Frame>,
    handshake_rx: oneshot::Receiver<Handshake>,
    token: CancellationToken,
) -> Result<()>
where
    T: From<Message> + Send + Sync + 'static,
{
    // Cancels the transport when this task exits for any reason (error, close, or drop).
    let _guard = token.drop_guard();

    let handshake = handshake_rx.await?;
    let mut heartbeat = Heartbeat::new(handshake.ping_window());

    loop {
        tokio::select! {
            _ = &mut heartbeat.timer => return Err(Error::HeartbeatTimeout),

            item = engine_rx.recv() => {
                let Some(action) = item else {
                    break;
                };

                match action {
                    EngineAction::Frame(frame) => match frame {
                        Frame::Packet(EioPacket::Ping(data)) => {
                            transport_tx.send(EioPacket::Pong(data).into()).await?;
                            heartbeat.reset();
                        }
                        Frame::Packet(EioPacket::Message(data)) => {
                            inbound_tx
                                .send(Message::Text(data).into())
                                .await.map_err(|e| Error::SendMessage(Box::new(e)))?;
                        }
                        Frame::Binary(data) => {
                            inbound_tx
                                .send(Message::Binary(data).into())
                                .await.map_err(|e| Error::SendMessage(Box::new(e)))?;
                        }
                        Frame::Packet(EioPacket::Close) => {break;},

                        Frame::Packet(EioPacket::Noop) => {}
                        Frame::Packet(packet) => {
                            return Err(packet.unexpected("client did not expect this packet"));
                        }
                    }
                    EngineAction::Message(message) => match message {
                        Message::Text(bytes) => {
                            transport_tx.send(EioPacket::Message(bytes).into()).await?;
                        }
                        Message::Binary(bytes) => {
                            transport_tx.send(bytes.into()).await?;

                        }
                        Message::Close => break,
                    }
                };
            }
        }
    }

    Ok(())
}
