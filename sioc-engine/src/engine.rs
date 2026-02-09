use crate::error::{Error, Result};
use crate::packet::{EioPacket, Frame, Handshake, Message};
use crate::prelude::Transport;
use crate::websocket::WebsocketTransport;
use futures_util::future::OptionFuture;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Sleep;

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

/// Channel-based handle to the Engine.IO protocol task.
///
/// The background task owns the [`Transport`] and handles ping/pong
/// heartbeats, upgrade negotiation, and message framing.
pub struct Engine {
    pub tx: mpsc::Sender<Message>,
    pub rx: mpsc::Receiver<Message>,
    pub handle: JoinHandle<Result<()>>,
}

impl Engine {
    /// Spawns the engine protocol task and returns a channel-based handle.
    pub fn spawn(
        transport: Transport,
        handshake: Handshake,
        upgrade_handle: Option<JoinHandle<Result<WebsocketTransport>>>,
    ) -> Self {
        let (inbound_tx, inbound_rx) = mpsc::channel(32);
        let (outbound_tx, outbound_rx) = mpsc::channel(32);

        let handle = tokio::spawn(engine_task(
            transport,
            handshake,
            outbound_rx,
            inbound_tx,
            upgrade_handle,
        ));

        Self {
            tx: outbound_tx,
            rx: inbound_rx,
            handle,
        }
    }
}

async fn engine_task(
    mut transport: Transport,
    handshake: Handshake,
    mut outbound_rx: mpsc::Receiver<Message>,
    inbound_tx: mpsc::Sender<Message>,
    upgrade_handle: Option<JoinHandle<Result<WebsocketTransport>>>,
) -> Result<()> {
    let mut heartbeat = Heartbeat::new(handshake.ping_window());
    let mut upgrading = upgrade_handle.is_some();
    let mut upgrade_fut = OptionFuture::from(upgrade_handle);

    loop {
        tokio::select! {
            _ = &mut heartbeat.timer => {
                return Err(Error::HeartbeatTimeout);
            }
            frame = transport.recv() => {
                match frame? {
                    Some(Frame::Packet(EioPacket::Ping(data))) => {
                        transport.send(Frame::from(EioPacket::Pong(data))).await?;
                        heartbeat.reset();
                    }
                    Some(Frame::Packet(EioPacket::Message(data))) => {
                        inbound_tx.send(Message::Text(data)).await?;
                    }
                    Some(Frame::Binary(data)) => {
                        inbound_tx.send(Message::Binary(data)).await?;
                    }
                    Some(Frame::Packet(EioPacket::Close)) => {
                        transport.close().await?;
                        break;
                    }
                    Some(Frame::Packet(EioPacket::Noop)) => continue,
                    Some(Frame::Packet(packet)) => {
                        return Err(packet.unexpected("client not expected"));
                    }
                    None => break,
                }
            }
            Some(msg) = outbound_rx.recv() => {
                let mut should_close = matches!(msg, Message::Close);
                transport.feed(Frame::from(msg)).await?;

                while let Ok(msg) = outbound_rx.try_recv() {
                    if matches!(msg, Message::Close) { should_close = true; }
                    transport.feed(Frame::from(msg)).await?;
                }

                transport.flush().await?;

                if should_close {
                    transport.close().await?;
                    break;
                }
            }
            result = &mut upgrade_fut, if upgrading => {
                upgrading = false;
                match result {
                    Some(Ok(Ok(ws))) => transport = Transport::Websocket(ws),
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => return Err(Error::UpgradeFailed(e)),
                    None => {}
                }
            }
        }
    }

    Ok(())
}
