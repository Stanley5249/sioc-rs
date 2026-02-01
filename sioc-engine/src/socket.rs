use crate::{
    packet::{Handshake, MessagePacket, Packet},
    transport::{TransportReceiver, TransportSender},
};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

/// The Engine.IO client - an active actor that manages the transport layer and protocol logic.
///
/// This struct encapsulates the Engine.IO connection, handling heartbeats, ping/pong responses,
/// and providing a narrowed interface for sending and receiving MessagePackets (Text and Binary only).
/// It spawns a background task upon creation to manage the protocol internally.
pub struct EioClient {
    /// Sender for outgoing MessagePackets.
    eio_tx: mpsc::Sender<MessagePacket>,
    /// Receiver for incoming MessagePackets (Text/Binary only).
    eio_rx: mpsc::Receiver<MessagePacket>,
    /// Handshake data.
    pub connection_data: Handshake,
}

impl EioClient {
    /// Create a new EngineClient with the given transport components and handshake data.
    ///
    /// This spawns a background task to handle the Engine.IO protocol, including heartbeats and ping/pong.
    ///
    /// # Arguments
    /// * `transport_tx` - The transport sender for outgoing packets.
    /// * `transport_rx` - The transport receiver for incoming packets.
    /// * `handshake` - The handshake data from the server.
    pub fn new(
        transport_tx: TransportSender,
        transport_rx: TransportReceiver,
        handshake: Handshake,
    ) -> Self {
        let (eio_tx, eio_out_rx) = mpsc::channel(32);
        let (eio_in_tx, eio_rx) = mpsc::channel(32);

        // Spawn background task
        tokio::spawn(client_loop(
            transport_tx,
            transport_rx,
            eio_out_rx,
            eio_in_tx,
            handshake.ping_interval,
        ));

        Self {
            eio_tx,
            eio_rx,
            connection_data: handshake,
        }
    }

    /// Get a clonable sender for outgoing MessagePackets.
    ///
    /// This sender restricts the API to sending `MessagePacket` types only.
    pub fn sender(&self) -> mpsc::Sender<MessagePacket> {
        self.eio_tx.clone()
    }

    /// Get a mutable reference to the receiver for incoming MessagePackets.
    ///
    /// This receiver yields only `Text` and `Binary` messages, as protocol handling is internalized.
    pub fn receiver(&mut self) -> &mut mpsc::Receiver<MessagePacket> {
        &mut self.eio_rx
    }
}

/// Background task for the EioClient: handles protocol and heartbeats.
async fn client_loop(
    mut transport_tx: TransportSender,
    mut transport_rx: TransportReceiver,
    mut eio_rx: mpsc::Receiver<MessagePacket>,
    eio_tx: mpsc::Sender<MessagePacket>,
    ping_interval: u64,
) {
    let mut heartbeat_timer = interval(Duration::from_millis(ping_interval));
    loop {
        tokio::select! {
            // Send heartbeat
            _ = heartbeat_timer.tick() => {
                let ping_packet = Packet::Ping(Bytes::from("probe"));
                let msg = ping_packet.into_message();
                if transport_tx.send(msg).await.is_err() {
                    break;
                }
            }

            // Receive from transport
            result = transport_rx.recv() => {
                match result {
                    Ok(Packet::Message(msg)) => {
                        // Forward Text/Binary to receiver
                        if eio_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Ok(Packet::Ping(data)) => {
                        // Respond with Pong
                        let pong_packet = Packet::Pong(data);
                        let msg = pong_packet.into_message();
                        if transport_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Ok(Packet::Close) => {
                        // Close connection
                        break;
                    }
                    Ok(_) => {} // Ignore other packets
                    Err(_) => break,
                }
            }

            // Send outgoing messages
            Some(msg) = eio_rx.recv() => {
                if transport_tx.send(msg).await.is_err() {
                    break;
                }
            }
        }
    }
}
