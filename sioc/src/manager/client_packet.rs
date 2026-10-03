//! Sends what the namespace handles ask for.

use super::{ConnectRequest, NamespaceStatus, Routes};
use crate::error::ManagerError;
use crate::packet::{ClientPacket, Packet};
use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// The client-packet loop's view of an open namespace.
struct Namespace {
    /// Tells a reopened namespace apart from the handles of the one before it.
    generation: u64,
    /// Set by the server's CONNECT response; events wait in `send_buffer` until then.
    connected: bool,
    send_buffer: Vec<Message>,
    next_ack_id: u64,
}

/// Waits for the next client packet from one namespace's handles.
///
/// Hands the receiver back, so the caller decides whether to keep listening.
async fn recv_client_packet(
    generation: u64,
    ns: ByteString,
    mut client_packet_rx: mpsc::Receiver<ClientPacket>,
) -> (
    u64,
    ByteString,
    Option<ClientPacket>,
    mpsc::Receiver<ClientPacket>,
) {
    let client_packet = client_packet_rx.recv().await;

    (generation, ns, client_packet, client_packet_rx)
}

/// Sends what the namespace handles ask for until the session closes.
///
/// Closes the session once the client handle and every namespace are gone, so
/// having no namespace at startup or between namespaces keeps it open.
pub(super) async fn client_packets_to_messages(
    mut connect_request_rx: mpsc::Receiver<ConnectRequest>,
    mut namespace_status_rx: mpsc::UnboundedReceiver<NamespaceStatus>,
    routes: &Routes,
    client_message_tx: mpsc::Sender<Message>,
) -> Result<(), ManagerError> {
    let mut namespaces = HashMap::<ByteString, Namespace>::new();
    let mut client_packets = FuturesUnordered::new();
    let mut generations = 0..;
    let mut client_open = true;

    while client_open || !namespaces.is_empty() {
        // Every handler sends only to `client_message_tx`, so waiting there holds
        // up only this direction, which no other arm could serve anyway.
        tokio::select! {
            connect_request = connect_request_rx.recv(), if client_open => {
                let Some(ConnectRequest { ns, payload, client_packet_rx, server_packet_tx }) = connect_request else {
                    client_open = false;
                    continue;
                };

                if !routes.insert(ns.clone(), server_packet_tx) {
                    return Err(ManagerError::NamespaceConflict { ns });
                }

                let generation = generations.next().unwrap_or_default();
                let namespace = Namespace {
                    generation,
                    connected: false,
                    send_buffer: Vec::new(),
                    next_ack_id: 0,
                };

                namespaces.insert(ns.clone(), namespace);
                client_packets.push(recv_client_packet(generation, ns.clone(), client_packet_rx));

                send_wire_packet(&client_message_tx, &ns, Packet::Connect(payload), None).await?;
            }

            Some((generation, ns, client_packet, client_packet_rx)) = client_packets.next() => {
                // Handles of a namespace that closed, or closed and reopened, no
                // longer apply, and dropping their receiver tells them so.
                let Some(namespace) = namespaces.get_mut(&ns).filter(|n| n.generation == generation) else {
                    continue;
                };

                match client_packet {
                    Some(ClientPacket::Disconnect) => {
                        close_namespace(&mut namespaces, routes, &client_message_tx, ns).await?;
                    }
                    Some(client_packet) => {
                        send_client_packet(namespace, routes, &client_message_tx, &ns, client_packet).await?;
                        client_packets.push(recv_client_packet(generation, ns, client_packet_rx));
                    }
                    None => {
                        tracing::warn!(%ns, "dropped while connected");
                        close_namespace(&mut namespaces, routes, &client_message_tx, ns).await?;
                    }
                }
            }

            namespace_status = namespace_status_rx.recv() => match namespace_status {
                Some(NamespaceStatus::Connected(ns)) => {
                    if let Some(namespace) = namespaces.get_mut(&ns) {
                        namespace.connected = true;

                        if !namespace.send_buffer.is_empty() {
                            tracing::trace!(%ns, count = namespace.send_buffer.len(), "flushed send buffer");
                        }

                        for message in namespace.send_buffer.drain(..) {
                            client_message_tx.send(message).await?;
                        }
                    }
                }
                Some(NamespaceStatus::Disconnected(ns)) => {
                    namespaces.remove(&ns);
                }
                // The server-message loop ended, so the engine closed the session.
                None => return Ok(()),
            },
        }
    }

    tracing::debug!("client handle and namespaces closed");

    // Dropping `client_message_tx` tells the engine to close the session.
    drop(client_message_tx);
    drop(client_packets);

    // Wait for the server-message loop to end, so it never sends into a
    // closed namespace status channel.
    while namespace_status_rx.recv().await.is_some() {}

    Ok(())
}

async fn close_namespace(
    namespaces: &mut HashMap<ByteString, Namespace>,
    routes: &Routes,
    client_message_tx: &mpsc::Sender<Message>,
    ns: ByteString,
) -> Result<(), ManagerError> {
    namespaces.remove(&ns);
    routes.remove(&ns);

    send_wire_packet(client_message_tx, &ns, Packet::Disconnect, None).await
}

/// Encodes one event or ack, holding events until the server confirms the namespace.
async fn send_client_packet(
    namespace: &mut Namespace,
    routes: &Routes,
    client_message_tx: &mpsc::Sender<Message>,
    ns: &ByteString,
    client_packet: ClientPacket,
) -> Result<(), ManagerError> {
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
                routes.register_ack(ns, id, ack_tx);
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

                namespace
                    .send_buffer
                    .extend(encode_packet(ns, &packet, attachments));

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
        // The caller closes the namespace instead.
        ClientPacket::Disconnect => Ok(()),
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
