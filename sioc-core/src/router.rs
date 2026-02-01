//! Active Router: Centralized State Machine for Socket.IO Protocol
//!
//! The Router is the only component that manages:
//! - Protocol State: ID assignment for acknowledgements
//! - Memory: Binary attachment reassembly
//!
//! It exposes strictly typed commands for Safe Path operations and handles
//! the stateful reassembly of binary packets.

use crate::packet::{
    AckPacket, Attachments, BasePacket, BinaryAckPacket, BinaryEventPacket, BinaryPacket,
    EventPacket, Packet,
};
use sioc_engine::prelude::{EioClient, MessagePacket};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

/// Commands sent to the Router for Safe Path processing.
///
/// These commands provide a strictly typed API that prevents invalid states:
/// - Events with/without acks are separate variants
/// - Binary attachments are explicitly separated from headers
#[derive(Debug)]
pub enum RouterCommand {
    /// Send a standard event (Type 2) without expecting an acknowledgement.
    SendEvent(BasePacket),

    /// Send an event (Type 2) requesting an acknowledgement.
    ///
    /// The router will assign an ID and register the ack handler.
    SendEventWithAck {
        /// The event data.
        data: BasePacket,
        /// Channel to receive the acknowledgement response.
        ack: oneshot::Sender<Packet>,
    },

    /// Send an acknowledgement response (Type 3).
    SendAck(AckPacket),

    /// Send a binary event (Type 5) without expecting an acknowledgement.
    ///
    /// The router will send the header (text) followed by each attachment (binary).
    SendBinaryEvent {
        /// The event header with attachment count.
        header: BinaryPacket,
        /// The binary attachments.
        payload: Attachments,
    },

    /// Send a binary event (Type 5) requesting an acknowledgement.
    ///
    /// The router will assign an ID, register the ack handler, and send the
    /// header followed by attachments.
    SendBinaryWithAck {
        /// The event header with attachment count.
        header: BinaryPacket,
        /// The binary attachments.
        payload: Attachments,
        /// Channel to receive the acknowledgement response.
        ack: oneshot::Sender<Packet>,
    },

    /// Send a binary acknowledgement response (Type 6).
    ///
    /// The router will send the header (text) followed by each attachment (binary).
    SendBinaryAck {
        /// The ack header with attachment count.
        header: BinaryAckPacket,
        /// The binary attachments.
        payload: Attachments,
    },
}

/// Router state for binary reassembly.
///
/// The router is either idle (waiting for a text header) or buffering
/// (accumulating binary attachments directly into the packet).
#[derive(Debug)]
enum RouterState {
    /// Idle state: waiting for the next text message.
    Idle,

    /// Buffering state: accumulating binary attachments.
    ///
    /// The packet contains empty attachments initially, which are filled
    /// as binary chunks arrive. Attachments are mutated in place for efficiency.
    Buffering {
        /// The packet being assembled with attachments.
        packet: Packet,
    },
}

/// The main router loop that manages Safe Path events and acknowledgements.
///
/// This is the centralized state machine that:
/// 1. Assigns IDs to outgoing requests
/// 2. Registers ack handlers
/// 3. Reassembles binary packets
/// 4. Dispatches complete messages to the user
///
/// # Arguments
/// * `eio` - The Engine.IO client for transport
/// * `router_rx` - Channel for receiving router commands
/// * `sio_tx` - Channel for sending complete messages to the user
pub async fn router_loop(
    mut eio: EioClient,
    mut router_rx: mpsc::Receiver<RouterCommand>,
    sio_tx: mpsc::Sender<Packet>,
) {
    let mut acks: HashMap<u64, oneshot::Sender<Packet>> = HashMap::new();
    let mut next_id: u64 = 0;
    let mut state = RouterState::Idle;

    // Get sender/receiver from EioClient
    let eio_tx = eio.sender();
    let eio_rx = eio.receiver();

    loop {
        tokio::select! {
            // ================================================================
            // Outgoing: Handle Router Commands
            // ================================================================
            Some(cmd) = router_rx.recv() => {
                match cmd {
                    RouterCommand::SendEvent(base) => {
                        let packet = Packet::Event(EventPacket::NoAck(base));
                        let msg = packet.to_message();
                        let _ = eio_tx.send(msg).await;
                    }

                    RouterCommand::SendEventWithAck { data, ack } => {
                        // Assign ID and register ack handler
                        let id = next_id;
                        next_id += 1;
                        acks.insert(id, ack);

                        // Upgrade BasePacket -> AckPacket
                        let ack_packet = AckPacket::new(data, id);
                        let packet = Packet::Event(EventPacket::Ack(ack_packet));
                        let msg = packet.to_message();
                        let _ = eio_tx.send(msg).await;
                    }

                    RouterCommand::SendAck(ack) => {
                        let packet = Packet::Ack(ack);
                        let msg = packet.to_message();
                        let _ = eio_tx.send(msg).await;
                    }

                    RouterCommand::SendBinaryEvent { header, payload } => {
                        // Send header (text) - we temporarily wrap payload but to_message ignores it
                        let packet = Packet::BinaryEvent(BinaryEventPacket::NoAck(header), Attachments::new());
                        let msg = packet.to_message();
                        let _ = eio_tx.send(msg).await;

                        // Send attachments (binary)
                        for blob in payload {
                            let _ = eio_tx.send(MessagePacket::Binary(blob)).await;
                        }
                    }

                    RouterCommand::SendBinaryWithAck { header, payload, ack } => {
                        // Assign ID and register ack handler
                        let id = next_id;
                        next_id += 1;
                        acks.insert(id, ack);

                        // Upgrade BinaryPacket -> BinaryAckPacket
                        let ack_packet = AckPacket::new(header.inner, id);
                        let bin_ack = BinaryAckPacket::new(ack_packet, header.attachments);

                        let packet = Packet::BinaryEvent(BinaryEventPacket::Ack(bin_ack), Attachments::new());
                        let msg = packet.to_message();
                        let _ = eio_tx.send(msg).await;

                        // Send attachments (binary)
                        for blob in payload {
                            let _ = eio_tx.send(MessagePacket::Binary(blob)).await;
                        }
                    }

                    RouterCommand::SendBinaryAck { header, payload } => {
                        // Send header (text)
                        let packet = Packet::BinaryAck(header, Attachments::new());
                        let msg = packet.to_message();
                        let _ = eio_tx.send(msg).await;

                        // Send attachments (binary)
                        for blob in payload {
                            let _ = eio_tx.send(MessagePacket::Binary(blob)).await;
                        }
                    }
                }
            }

            // ================================================================
            // Incoming: Handle Network Messages
            // ================================================================
            Some(msg) = eio_rx.recv() => {
                // Handle binary attachments (stateful reassembly)
                if let MessagePacket::Binary(chunk) = msg {
                    match &mut state {
                        RouterState::Buffering { packet } => {
                            // Mutate attachments in place and check if complete
                            let complete = match packet {
                                Packet::BinaryEvent(inner, atts) => {
                                    atts.push(chunk);
                                    let expected = match inner {
                                        BinaryEventPacket::NoAck(p) => p.attachments,
                                        BinaryEventPacket::Ack(p) => p.attachments,
                                    };
                                    atts.len() as u64 == expected
                                }
                                Packet::BinaryAck(inner, atts) => {
                                    atts.push(chunk);
                                    atts.len() as u64 == inner.attachments
                                }
                                _ => unreachable!("Buffering state should only contain binary packets"),
                            };

                            if complete {
                                // Take ownership of the packet and reset state to Idle
                                if let RouterState::Buffering { packet } = std::mem::replace(&mut state, RouterState::Idle) {
                                    dispatch(packet, &mut acks, &sio_tx).await;
                                }
                            }
                        }
                        RouterState::Idle => {
                            eprintln!("Router error: unexpected binary packet in Idle state");
                        }
                    }
                    continue;
                }

                // Handle text headers (stateless parsing)
                if let Ok(packet) = Packet::try_from_message(msg) {
                    // Determine expected attachment count by inspecting strict variants
                    let expected = match &packet {
                        Packet::BinaryEvent(BinaryEventPacket::NoAck(p), _) => p.attachments,
                        Packet::BinaryEvent(BinaryEventPacket::Ack(p), _) => p.attachments,
                        Packet::BinaryAck(p, _) => p.attachments,
                        _ => 0,
                    };

                    if expected > 0 {
                        // Enter buffering state - packet already has empty attachments allocated
                        state = RouterState::Buffering { packet };
                    } else {
                        // No attachments: dispatch immediately
                        dispatch(packet, &mut acks, &sio_tx).await;
                    }
                } else {
                    eprintln!("Router error: failed to parse packet");
                }
            }
        }
    }
}

/// Dispatch a complete message to the appropriate destination.
///
/// If the packet is an acknowledgement, it is sent to the registered ack handler.
/// Otherwise, it is sent to the user via the sio_tx channel.
///
/// # Arguments
/// * `packet` - The complete packet (including attachments if any)
/// * `acks` - The acknowledgement handler registry
/// * `sio_tx` - The channel for sending messages to the user
async fn dispatch(
    packet: Packet,
    acks: &mut HashMap<u64, oneshot::Sender<Packet>>,
    sio_tx: &mpsc::Sender<Packet>,
) {
    // Check if this is an acknowledgement response
    match &packet {
        Packet::Ack(p) => {
            if let Some(tx) = acks.remove(&p.ack_id) {
                let _ = tx.send(packet);
                return;
            }
        }
        Packet::BinaryAck(p, _) => {
            if let Some(tx) = acks.remove(&p.inner.ack_id) {
                let _ = tx.send(packet);
                return;
            }
        }
        _ => {}
    }

    // Not an ack: send to user
    let _ = sio_tx.send(packet).await;
}
