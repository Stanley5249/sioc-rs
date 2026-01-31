//! Router task for handling Safe Path events with acknowledgements.

use crate::error::Result;
use crate::packet::{EventPacket, Packet};
use sioc_engine::prelude::{
    EngineReceiver, EngineSender, Message as EngineMessage, Packet as EnginePacket,
};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

/// Commands sent to the Router task for Safe Path processing.
#[derive(Debug)]
pub enum RouterCommand {
    /// Emit an event that expects an acknowledgement.
    EmitWithAck {
        /// Packet to emit.
        packet: EventPacket,
        /// Sender for acknowledgement.
        tx: oneshot::Sender<Packet>,
    },
}

/// The main router loop that manages Safe Path events and acknowledgements.
pub async fn router_loop(
    mut engine_rx: EngineReceiver,
    engine_tx: EngineSender,
    mut cmd_rx: mpsc::Receiver<RouterCommand>,
    event_tx: mpsc::Sender<Packet>,
) {
    let mut acks: HashMap<u64, oneshot::Sender<Packet>> = HashMap::new();
    let mut next_id = 0u64;

    loop {
        tokio::select! {
            // Handle Safe Path commands
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    RouterCommand::EmitWithAck { mut packet, tx } => {
                        packet.id = Some(next_id);
                        acks.insert(next_id, tx);
                        next_id += 1;

                        let full_packet = Packet::Event(packet);
                        if send_packet_with_attachments(&engine_tx, &full_packet).await.is_err() {
                            acks.remove(&(next_id - 1));
                        }
                    }
                }
            }

            // Handle network packets
            result = engine_rx.recv() => {
                match result {
                    Ok(engine_packet) => {
                        if !handle_engine_packet(
                            engine_packet,
                            &engine_tx,
                            &mut engine_rx,
                            &mut acks,
                            &event_tx,
                        ).await.unwrap_or(false) {
                            break; // Connection closed
                        }
                    }
                    Err(_) => break, // Engine error
                }
            }
        }
    }
}

/// Send a Socket.IO packet to the Engine.
async fn send_packet_with_attachments(engine_tx: &EngineSender, packet: &Packet) -> Result<()> {
    let engine_packet = packet.to_engine_packet();
    engine_tx.send(engine_packet).await?;

    let attachments = match packet {
        Packet::Event(p) => &p.attachments,
        Packet::Ack(p) => &p.attachments,
        _ => return Ok(()),
    };

    for attachment in attachments {
        let binary_msg = EnginePacket::Message(EngineMessage::Binary(attachment.clone()));
        engine_tx.send(binary_msg).await?;
    }
    Ok(())
}

/// Handle an incoming Engine packet.
async fn handle_engine_packet(
    engine_packet: EnginePacket,
    engine_tx: &EngineSender,
    engine_rx: &mut EngineReceiver,
    acks: &mut HashMap<u64, oneshot::Sender<Packet>>,
    event_tx: &mpsc::Sender<Packet>,
) -> Result<bool> {
    match engine_packet {
        EnginePacket::Message(msg) => {
            let bytes = match msg {
                EngineMessage::Text(b) => b,
                EngineMessage::Binary(_) => return Ok(true), // Ignore standalone binary
            };

            // Try to parse Socket.IO packet
            if let Ok(mut packet) =
                Packet::try_from(EnginePacket::Message(EngineMessage::Text(bytes)))
            {
                // Assemble attachments if needed
                match &mut packet {
                    Packet::Event(p) => {
                        for _ in 0..p.attachment_count {
                            loop {
                                match engine_rx.recv().await {
                                    Ok(EnginePacket::Message(EngineMessage::Binary(bin))) => {
                                        p.attachments.push(bin);
                                        break;
                                    }
                                    Ok(EnginePacket::Ping(d)) => {
                                        engine_tx.send(EnginePacket::Pong(d)).await?;
                                    }
                                    Ok(EnginePacket::Close) => return Ok(false),
                                    _ => {} // Ignore unexpected
                                }
                            }
                        }
                    }
                    Packet::Ack(p) => {
                        for _ in 0..p.attachment_count {
                            loop {
                                match engine_rx.recv().await {
                                    Ok(EnginePacket::Message(EngineMessage::Binary(bin))) => {
                                        p.attachments.push(bin);
                                        break;
                                    }
                                    Ok(EnginePacket::Ping(d)) => {
                                        engine_tx.send(EnginePacket::Pong(d)).await?;
                                    }
                                    Ok(EnginePacket::Close) => return Ok(false),
                                    _ => {} // Ignore unexpected
                                }
                            }
                        }
                    }
                    _ => {}
                }

                // Route the packet
                match packet {
                    Packet::Ack(p) => {
                        if let Some(tx) = acks.remove(&p.ack_id) {
                            let _ = tx.send(Packet::Ack(p));
                        }
                    }
                    Packet::Event(_)
                    | Packet::Connect { .. }
                    | Packet::Disconnect { .. }
                    | Packet::ConnectError { .. } => {
                        let _ = event_tx.send(packet).await;
                    }
                }
            }
        }
        EnginePacket::Close => return Ok(false),
        EnginePacket::Ping(data) => {
            engine_tx.send(EnginePacket::Pong(data)).await?;
        }
        _ => {} // Ignore other packets
    }

    Ok(true)
}
