//! Sends what the namespace handles ask for.

use super::{ConnectRequest, Routes};
use crate::error::ManagerError;
use crate::packet::{ClientPacket, Packet};
use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// The client-packet loop's view of one namespace generation.
///
/// It lives until the generation's client packets end, which can be after
/// the namespace closed and was opened again.
struct Namespace {
    ns: ByteString,
    /// Set by the server's CONNECT response; events wait in `send_buffer` until then.
    connected: bool,
    send_buffer: Vec<Message>,
    next_ack_id: u64,
}

/// Waits for the next client packet of one namespace generation.
///
/// Hands the receiver back, so the caller decides whether to keep listening.
async fn recv_client_packet(
    generation: u64,
    mut client_packet_rx: mpsc::Receiver<ClientPacket>,
) -> (u64, Option<ClientPacket>, mpsc::Receiver<ClientPacket>) {
    let client_packet = client_packet_rx.recv().await;

    (generation, client_packet, client_packet_rx)
}

/// Sends what the namespace handles ask for until the session closes.
///
/// Closes the session once the client handle and every namespace are gone, so
/// having no namespace at startup or between namespaces keeps it open.
pub(super) async fn client_packets_to_messages(
    mut connect_request_rx: mpsc::Receiver<ConnectRequest>,
    mut connected_generation_rx: mpsc::UnboundedReceiver<u64>,
    routes: &Routes,
    client_message_tx: mpsc::Sender<Message>,
) -> Result<(), ManagerError> {
    let mut namespaces = HashMap::<u64, Namespace>::new();
    let mut client_packets = FuturesUnordered::new();
    let mut generations = 0..;
    let mut client_open = true;

    while client_open || !namespaces.is_empty() {
        // Every handler sends only to `client_message_tx`, so waiting there holds
        // up only this direction, which no other arm could serve anyway.
        tokio::select! {
            connect_request = connect_request_rx.recv(), if client_open => {
                let Some(ConnectRequest { ns, payload, client_packet_rx, client_packet_tx, server_packet_tx }) = connect_request else {
                    client_open = false;
                    continue;
                };

                let generation = generations.next().unwrap_or_default();

                if !routes.insert(ns.clone(), generation, server_packet_tx, client_packet_tx) {
                    return Err(ManagerError::NamespaceConflict { ns });
                }

                let namespace = Namespace {
                    ns: ns.clone(),
                    connected: false,
                    send_buffer: Vec::new(),
                    next_ack_id: 0,
                };

                namespaces.insert(generation, namespace);
                client_packets.push(recv_client_packet(generation, client_packet_rx));

                send_wire_packet(&client_message_tx, &ns, Packet::Connect(payload), None).await?;
            }

            Some((generation, client_packet, client_packet_rx)) = client_packets.next() => {
                let Some(client_packet) = client_packet else {
                    let namespace = namespaces
                        .remove(&generation)
                        .expect("a namespace generation lives until its client packets end");

                    // Only a namespace the client closed still has its route.
                    if routes.close_generation(&namespace.ns, generation) {
                        tracing::debug!(ns = %namespace.ns, "client closed");

                        send_wire_packet(&client_message_tx, &namespace.ns, Packet::Disconnect, None).await?;
                    }

                    continue;
                };

                let namespace = namespaces
                    .get_mut(&generation)
                    .expect("a namespace generation lives until its client packets end");

                send_client_packet(namespace, generation, routes, &client_message_tx, client_packet).await?;
                client_packets.push(recv_client_packet(generation, client_packet_rx));
            }

            generation = connected_generation_rx.recv() => {
                // The server-message loop ended, so the engine closed the session.
                let Some(generation) = generation else {
                    return Ok(());
                };

                if let Some(namespace) = namespaces.get_mut(&generation) {
                    namespace.connected = true;

                    if !namespace.send_buffer.is_empty() {
                        tracing::trace!(ns = %namespace.ns, count = namespace.send_buffer.len(), "flushed send buffer");
                    }

                    for message in namespace.send_buffer.drain(..) {
                        client_message_tx.send(message).await?;
                    }
                }
            }
        }
    }

    tracing::debug!("client handle and namespaces closed");

    // Dropping `client_message_tx` tells the engine to close the session.
    drop(client_message_tx);
    drop(client_packets);

    // Wait for the server-message loop to end, so it never sends into a
    // closed channel.
    while connected_generation_rx.recv().await.is_some() {}

    Ok(())
}

/// Encodes one event or ack, holding events until the server confirms the namespace.
///
/// Discards packets the handles sent before the server closed the namespace,
/// because the server no longer accepts them.
async fn send_client_packet(
    namespace: &mut Namespace,
    generation: u64,
    routes: &Routes,
    client_message_tx: &mpsc::Sender<Message>,
    client_packet: ClientPacket,
) -> Result<(), ManagerError> {
    let ns = &namespace.ns;

    if !routes.is_open(ns, generation) {
        tracing::debug!(%ns, "discarded client packet for a closed namespace");
        return Ok(());
    }

    match client_packet {
        ClientPacket::Event {
            payload,
            ack_tx,
            attachments,
        } => {
            // Register before sending, so the server's answer always finds it.
            let id = ack_tx.map(|ack_tx| {
                let id = namespace.next_ack_id;
                namespace.next_ack_id += 1;
                routes.register_ack(ns, generation, id, ack_tx);
                id
            });

            let packet = match &attachments {
                None => Packet::Event { payload, id },
                Some(attachments) => Packet::BinaryEvent {
                    payload,
                    id,
                    count: attachments.len(),
                },
            };

            if namespace.connected {
                send_wire_packet(client_message_tx, ns, packet, attachments).await
            } else {
                tracing::trace!(%ns, %packet, "buffering messages");

                let messages = encode_packet(ns, &packet, attachments);
                namespace.send_buffer.extend(messages);

                Ok(())
            }
        }
        ClientPacket::Ack {
            payload,
            id,
            attachments,
        } => {
            let packet = match &attachments {
                None => Packet::Ack { payload, id },
                Some(attachments) => Packet::BinaryAck {
                    payload,
                    id,
                    count: attachments.len(),
                },
            };

            send_wire_packet(client_message_tx, ns, packet, attachments).await
        }
    }
}

/// Encodes a packet and sends its messages to the engine.
async fn send_wire_packet(
    client_message_tx: &mpsc::Sender<Message>,
    ns: &ByteString,
    packet: Packet,
    attachments: Option<Vec<Bytes>>,
) -> Result<(), ManagerError> {
    tracing::trace!(%ns, %packet, "-> packet");

    for message in encode_packet(ns, &packet, attachments) {
        client_message_tx.send(message).await?;
    }

    Ok(())
}

/// Encodes a packet as one text message followed by its binary attachments.
fn encode_packet(
    ns: &ByteString,
    packet: &Packet,
    attachments: Option<Vec<Bytes>>,
) -> impl Iterator<Item = Message> {
    let text = Message::Text(packet.encode(ns).into());
    let binaries = attachments.into_iter().flatten().map(Message::Binary);

    std::iter::once(text).chain(binaries)
}
