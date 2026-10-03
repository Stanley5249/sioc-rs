//! Sends what the namespace handles ask for.

use super::{Control, NewSocket, Routes};
use crate::error::ManagerError;
use crate::packet::{Directive, Packet};
use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// The directive loop's view of an open namespace.
struct Namespace {
    /// Tells a reopened namespace apart from the handles of the one before it.
    id: u64,
    /// Set by the server's CONNECT response; events wait in `buffer` until then.
    connected: bool,
    buffer: Vec<Message>,
    next_ack_id: u64,
}

/// Waits for the next directive from one namespace's handles.
///
/// Hands the receiver back, so the caller decides whether to keep listening.
async fn recv_directive(
    id: u64,
    ns: ByteString,
    mut directive_rx: mpsc::Receiver<Directive>,
) -> (
    u64,
    ByteString,
    Option<Directive>,
    mpsc::Receiver<Directive>,
) {
    let directive = directive_rx.recv().await;

    (id, ns, directive, directive_rx)
}

/// Sends what the namespace handles ask for until the session closes.
///
/// Closes the session once the client handle and every namespace are gone, so
/// having no namespace at startup or between namespaces keeps it open.
pub(super) async fn route_directives(
    mut new_socket_rx: mpsc::Receiver<NewSocket>,
    mut control_rx: mpsc::UnboundedReceiver<Control>,
    routes: &Routes,
    client_message_tx: mpsc::Sender<Message>,
) -> Result<(), ManagerError> {
    let mut namespaces = HashMap::<ByteString, Namespace>::new();
    let mut directives = FuturesUnordered::new();
    let mut ids = 0..;
    let mut client_open = true;

    while client_open || !namespaces.is_empty() {
        // Every handler sends only to `client_message_tx`, so waiting there holds
        // up only this direction, which no other arm could serve anyway.
        tokio::select! {
            new_socket = new_socket_rx.recv(), if client_open => {
                let Some(NewSocket { ns, payload, directive_rx, signal_tx }) = new_socket else {
                    client_open = false;
                    continue;
                };

                if !routes.insert(ns.clone(), signal_tx) {
                    return Err(ManagerError::NamespaceConflict { ns });
                }

                let id = ids.next().unwrap_or_default();
                let namespace = Namespace {
                    id,
                    connected: false,
                    buffer: Vec::new(),
                    next_ack_id: 0,
                };

                namespaces.insert(ns.clone(), namespace);
                directives.push(recv_directive(id, ns.clone(), directive_rx));

                send_packet(&client_message_tx, &ns, Packet::Connect(payload), None).await?;
            }

            Some((id, ns, directive, directive_rx)) = directives.next() => {
                // Handles of a namespace that closed, or closed and reopened, no
                // longer apply, and dropping their receiver tells them so.
                let Some(namespace) = namespaces.get_mut(&ns).filter(|n| n.id == id) else {
                    continue;
                };

                match directive {
                    Some(Directive::Disconnect) => {
                        close_namespace(&mut namespaces, routes, &client_message_tx, ns).await?;
                    }
                    Some(directive) => {
                        send_directive(namespace, routes, &client_message_tx, &ns, directive).await?;
                        directives.push(recv_directive(id, ns, directive_rx));
                    }
                    None => {
                        tracing::warn!(%ns, "dropped while connected");
                        close_namespace(&mut namespaces, routes, &client_message_tx, ns).await?;
                    }
                }
            }

            control = control_rx.recv() => match control {
                Some(Control::Connected(ns)) => {
                    if let Some(namespace) = namespaces.get_mut(&ns) {
                        namespace.connected = true;

                        if !namespace.buffer.is_empty() {
                            tracing::trace!(%ns, count = namespace.buffer.len(), "flushed buffer");
                        }

                        for message in namespace.buffer.drain(..) {
                            client_message_tx.send(message).await?;
                        }
                    }
                }
                Some(Control::Disconnected(ns)) => {
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
    drop(directives);

    // Wait for the server-message loop to end, so it never sends into a
    // closed control channel.
    while control_rx.recv().await.is_some() {}

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

    send_packet(client_message_tx, &ns, Packet::Disconnect, None).await
}

/// Encodes one event or ack, holding events until the server confirms the namespace.
async fn send_directive(
    namespace: &mut Namespace,
    routes: &Routes,
    client_message_tx: &mpsc::Sender<Message>,
    ns: &ByteString,
    directive: Directive,
) -> Result<(), ManagerError> {
    match directive {
        Directive::Event {
            payload,
            tx: ack_tx,
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
                send_packet(client_message_tx, ns, packet, attachments).await
            } else {
                tracing::trace!(%ns, %packet, "buffering messages");

                namespace.buffer.extend(encode(ns, &packet, attachments));

                Ok(())
            }
        }
        Directive::Ack {
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

            send_packet(client_message_tx, ns, packet, attachments).await
        }
        // The caller closes the namespace instead.
        Directive::Disconnect => Ok(()),
    }
}

async fn send_packet(
    client_message_tx: &mpsc::Sender<Message>,
    ns: &ByteString,
    packet: Packet,
    attachments: Option<Vec<Bytes>>,
) -> Result<(), ManagerError> {
    tracing::trace!(%ns, %packet, "-> packet");

    for message in encode(ns, &packet, attachments) {
        client_message_tx.send(message).await?;
    }

    Ok(())
}

/// Encodes a packet as one text message followed by its binary attachments.
fn encode(
    ns: &ByteString,
    packet: &Packet,
    attachments: Option<Vec<Bytes>>,
) -> impl Iterator<Item = Message> {
    let text = Message::Text(packet.encode(ns).into());
    let binaries = attachments.into_iter().flatten().map(Message::Binary);

    std::iter::once(text).chain(binaries)
}
