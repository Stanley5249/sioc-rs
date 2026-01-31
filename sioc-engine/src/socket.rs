use crate::{
    error::{Error, Result},
    packet::{Handshake, Packet},
    transport::TransportSender,
};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

/// The Engine.IO socket - holds the write half and connection metadata
pub struct EngineSocket {
    /// The transport sender (internal use only).
    pub(crate) transport: TransportSender,
    /// Handshake data (public readonly).
    pub connection_data: Handshake,
    /// Connection state.
    pub connected: bool,
}

impl EngineSocket {
    /// Sends a packet to the server.
    pub async fn send(&mut self, packet: Packet) -> Result<()> {
        if !self.connected {
            return Err(Error::IllegalActionBeforeOpen);
        }

        let msg = packet.into_message();
        self.transport.send(msg).await
    }

    /// Closes the connection.
    pub async fn close(&mut self) -> Result<()> {
        self.send(Packet::Close).await?;
        self.connected = false;
        Ok(())
    }
}

/// Background task for sending packets to the transport.
pub async fn send_loop(mut transport: TransportSender, mut rx: mpsc::Receiver<Packet>) {
    while let Some(packet) = rx.recv().await {
        let is_close = matches!(packet, Packet::Close);
        let msg = packet.into_message();
        if transport.send(msg).await.is_err() {
            break;
        }
        if is_close {
            break;
        }
    }
}

/// Background task for sending periodic ping packets.
pub async fn heartbeat_loop(interval_ms: u64, tx: mpsc::Sender<Packet>) {
    let mut timer = interval(Duration::from_millis(interval_ms));
    loop {
        timer.tick().await;
        if tx.send(Packet::Ping(Bytes::from("probe"))).await.is_err() {
            break;
        }
    }
}
