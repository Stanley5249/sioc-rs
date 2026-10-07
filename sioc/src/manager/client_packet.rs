//! Sends what the namespace handles ask for.

use std::collections::HashMap;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::ManagerError;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::routes::Routes;
use crate::manager::supervisor::{Session, SessionRequest};
use crate::packet::{ClientPacket, Packet};

/// The client-packet loop's view of one namespace generation.
///
/// It lives until the generation's client packets end, which can be after
/// the namespace closed and was opened again.
struct Namespace {
    ns: ByteString,
    /// The JSON auth payload, sent with the CONNECT packet in every session.
    auth: ByteString,
    /// Set by the server's CONNECT response in the open session; events wait
    /// in `send_buffer` until then.
    connected: bool,
    send_buffer: Vec<Message>,
    next_ack_id: u64,
}

/// Waits for the next client packet of one namespace generation.
///
/// Once `closed` is cancelled, closes the channel, so the packets sent before
/// still arrive and then the channel ends, as it does when every handle drops.
/// Hands the receiver back, so the caller decides whether to keep listening.
async fn recv_client_packet(
    generation: u64,
    mut client_packet_rx: mpsc::Receiver<ClientPacket>,
    closed: CancellationToken,
) -> (
    u64,
    Option<ClientPacket>,
    mpsc::Receiver<ClientPacket>,
    CancellationToken,
) {
    // Prefer the channel, so buffered packets drain before the close takes effect.
    let client_packet = tokio::select! {
        biased;
        client_packet = client_packet_rx.recv() => client_packet,
        () = closed.cancelled() => {
            client_packet_rx.close();
            client_packet_rx.recv().await
        }
    };

    (generation, client_packet, client_packet_rx, closed)
}

/// The client-packet loop's link to the supervisor's sessions.
enum Link {
    /// No session is open, and none is requested.
    Idle,
    /// Waiting for the supervisor's next session.
    Requested,
    /// A session is open.
    Open(Session),
}

/// Sends what the namespace handles ask for, across sessions, until the
/// client ends.
///
/// Asks the supervisor for the first session at once, and for another one
/// whenever a session ends while namespaces are open. Without a session, holds
/// events in each namespace's send buffer and discards acks.
///
/// Ends once the client handle and every namespace are gone, so having no
/// namespace at startup or between namespaces keeps it open, or once the
/// supervisor stops. Then closes every namespace and the open session, drops
/// `session_request_tx`, and waits until `session_rx` ends.
///
/// # Errors
///
/// Returns an error if a channel to the engine or the supervisor closes early.
///
/// # Panics
///
/// Panics if a client packet arrives for a generation this loop never opened,
/// which the generation counter rules out, or if the supervisor opens a
/// session nobody asked for.
pub async fn client_packets_to_messages(
    mut connect_request_rx: mpsc::Receiver<ConnectRequest>,
    session_request_tx: mpsc::Sender<SessionRequest>,
    mut session_rx: mpsc::Receiver<Session>,
    routes: &Routes,
) -> Result<(), ManagerError> {
    let mut namespaces = HashMap::<u64, Namespace>::new();
    let mut client_packets = FuturesUnordered::new();
    let mut generations = 0..;
    let mut client_open = true;
    let mut link = Link::Idle;

    // Open the first session at once, like socket.io-client's `Manager`.
    request_session(&mut link, &session_request_tx, SessionRequest::Open).await?;

    while client_open || !namespaces.is_empty() {
        // Every handler sends only to the open session or the supervisor, so
        // waiting there holds up only this direction, which no other arm
        // could serve anyway.
        tokio::select! {
            connect_request = connect_request_rx.recv(), if client_open => {
                let Some(ConnectRequest { ns, payload, client_packet_rx, closed, server_packet_tx, reply_tx }) = connect_request else {
                    client_open = false;
                    continue;
                };

                let generation = generations.next().unwrap_or_default();

                if !routes.insert(ns.clone(), generation, server_packet_tx, closed.clone()) {
                    // A cancelled connect drops its reply receiver.
                    let _ = reply_tx.send(Err(crate::error::SocketError::NamespaceConflict { ns }));
                    continue;
                }

                if let Link::Open(session) = &link {
                    send_wire_packet(&session.client_message_tx, &ns, Packet::Connect(payload.clone()), None).await?;
                }

                let namespace = Namespace {
                    ns,
                    auth: payload,
                    connected: false,
                    send_buffer: Vec::new(),
                    next_ack_id: 0,
                };

                namespaces.insert(generation, namespace);
                client_packets.push(recv_client_packet(generation, client_packet_rx, closed));

                request_session(&mut link, &session_request_tx, SessionRequest::Open).await?;

                // The caller may have cancelled connect while the request was queued.
                let _ = reply_tx.send(Ok(()));
            }

            Some((generation, client_packet, client_packet_rx, closed)) = client_packets.next() => {
                let session = match &link {
                    Link::Open(session) => Some(session),
                    Link::Idle | Link::Requested => None,
                };

                let Some(client_packet) = client_packet else {
                    let namespace = namespaces
                        .remove(&generation)
                        .expect("a namespace generation lives until its client packets end");

                    close_namespace(&namespace.ns, generation, routes, session).await?;

                    continue;
                };

                let namespace = namespaces
                    .get_mut(&generation)
                    .expect("a namespace generation lives until its client packets end");

                send_client_packet(namespace, generation, routes, session, client_packet).await?;
                client_packets.push(recv_client_packet(generation, client_packet_rx, closed));
            }

            session = session_rx.recv() => {
                let Some(session) = session else {
                    tracing::debug!("supervisor stopped");

                    break;
                };

                assert!(
                    matches!(link, Link::Requested),
                    "the supervisor opens a session only on request"
                );

                tracing::debug!(session = session.number, "session opened");

                resend_connects(&namespaces, routes, &session).await?;
                link = Link::Open(session);
            }

            generation = recv_connected_generation(&mut link) => {
                let Link::Open(session) = &link else {
                    unreachable!("only an open session reports generations");
                };

                // The server-message loop ended, so the session is over.
                let Some(generation) = generation else {
                    tracing::debug!(session = session.number, "session closed");

                    // Dropping the session lets its engine finish.
                    link = Link::Idle;
                    end_session(&mut namespaces, routes);

                    if !routes.is_empty() {
                        request_session(&mut link, &session_request_tx, SessionRequest::Reconnect).await?;
                    }

                    continue;
                };

                if let Some(namespace) = namespaces.get_mut(&generation) {
                    flush_send_buffer(namespace, generation, routes, session).await?;
                }
            }
        }
    }

    tracing::debug!("client ended");

    // Closing the namespaces ends their receivers and fails their pending acks.
    routes.clear();
    drop(client_packets);
    drop(session_request_tx);

    if let Link::Open(session) = link {
        close_session(session).await;
    }

    // Wait for the supervisor to hang up, so it never sends into a closed
    // channel.
    while let Some(session) = session_rx.recv().await {
        close_session(session).await;
    }

    Ok(())
}

/// Asks the supervisor for a session, unless one is open or requested.
async fn request_session(
    link: &mut Link,
    session_request_tx: &mpsc::Sender<SessionRequest>,
    request: SessionRequest,
) -> Result<(), ManagerError> {
    if !matches!(link, Link::Idle) {
        return Ok(());
    }

    session_request_tx
        .send(request)
        .await
        .map_err(|_| ManagerError::Session)?;

    *link = Link::Requested;

    Ok(())
}

/// Waits for the next namespace generation the server confirmed in the open
/// session, and never resolves without one.
async fn recv_connected_generation(link: &mut Link) -> Option<u64> {
    match link {
        Link::Open(session) => session.connected_generation_rx.recv().await,
        Link::Idle | Link::Requested => std::future::pending().await,
    }
}

/// Sends CONNECT with the stored auth payload for every open namespace, at
/// the start of a session.
///
/// Namespaces closed by the server stay closed.
async fn resend_connects(
    namespaces: &HashMap<u64, Namespace>,
    routes: &Routes,
    session: &Session,
) -> Result<(), ManagerError> {
    for (generation, namespace) in namespaces {
        if routes.is_open(&namespace.ns, *generation) {
            let packet = Packet::Connect(namespace.auth.clone());
            send_wire_packet(&session.client_message_tx, &namespace.ns, packet, None).await?;
        }
    }

    Ok(())
}

/// Marks every namespace unconfirmed and fails the acks of sent events, after
/// a session ended. Buffered events wait for the next session.
fn end_session(namespaces: &mut HashMap<u64, Namespace>, routes: &Routes) {
    routes.end_session();

    for namespace in namespaces.values_mut() {
        namespace.connected = false;
    }
}

/// Closes a session the loop no longer uses, and waits until it ends.
async fn close_session(session: Session) {
    let Session {
        client_message_tx,
        mut connected_generation_rx,
        ..
    } = session;

    drop(client_message_tx);

    // Wait for the server-message loop to end, so it never sends into a closed
    // channel.
    while connected_generation_rx.recv().await.is_some() {}
}

/// Closes one namespace generation after its client packets end.
///
/// Only a namespace the client closed still has its route, and only an open
/// session has seen its CONNECT.
async fn close_namespace(
    ns: &ByteString,
    generation: u64,
    routes: &Routes,
    session: Option<&Session>,
) -> Result<(), ManagerError> {
    if !routes.close_generation(ns, generation) {
        return Ok(());
    }

    tracing::debug!(%ns, "client closed");

    if let Some(session) = session {
        send_wire_packet(&session.client_message_tx, ns, Packet::Disconnect, None).await?;
    }

    Ok(())
}

/// Sends the events a namespace buffered until the server confirmed it.
async fn flush_send_buffer(
    namespace: &mut Namespace,
    generation: u64,
    routes: &Routes,
    session: &Session,
) -> Result<(), ManagerError> {
    namespace.connected = true;

    if !namespace.send_buffer.is_empty() {
        tracing::trace!(ns = %namespace.ns, count = namespace.send_buffer.len(), "flushed send buffer");
    }

    // Register before sending, so the server's answers always find them.
    routes.flush_acks(&namespace.ns, generation);

    for message in namespace.send_buffer.drain(..) {
        session.client_message_tx.send(message).await?;
    }

    Ok(())
}

/// Encodes one event or ack, holding events until the server confirms the
/// namespace in the open session.
///
/// Discards packets the handles sent before the server closed the namespace,
/// because the server no longer accepts them, and acks while no session is
/// open, because there is no channel to send them on.
async fn send_client_packet(
    namespace: &mut Namespace,
    generation: u64,
    routes: &Routes,
    session: Option<&Session>,
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
            let ack = ack_tx.map(|ack_tx| {
                let id = namespace.next_ack_id;
                namespace.next_ack_id += 1;
                (id, ack_tx)
            });

            let id = ack.as_ref().map(|(id, _)| *id);

            let packet = match &attachments {
                None => Packet::Event { payload, id },
                Some(attachments) => Packet::BinaryEvent {
                    payload,
                    id,
                    count: attachments.len(),
                },
            };

            let Some(session) = session.filter(|_| namespace.connected) else {
                if let Some((id, ack_tx)) = ack {
                    routes.buffer_ack(ns, generation, id, ack_tx);
                }

                tracing::trace!(%ns, %packet, "buffered packet");

                let messages = encode_packet(ns, &packet, attachments);
                namespace.send_buffer.extend(messages);

                return Ok(());
            };

            // Register before sending, so the server's answer always finds it.
            if let Some((id, ack_tx)) = ack {
                routes.register_ack(ns, generation, id, ack_tx);
            }

            send_wire_packet(&session.client_message_tx, ns, packet, attachments).await
        }
        ClientPacket::Ack {
            payload,
            id,
            attachments,
        } => {
            let Some(session) = session else {
                tracing::debug!(%ns, id, "discarded ack while no session is open");
                return Ok(());
            };

            let packet = match &attachments {
                None => Packet::Ack { payload, id },
                Some(attachments) => Packet::BinaryAck {
                    payload,
                    id,
                    count: attachments.len(),
                },
            };

            send_wire_packet(&session.client_message_tx, ns, packet, attachments).await
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
    tracing::trace!(%ns, %packet, "sent packet");

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
