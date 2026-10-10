//! Delivers what the server sends to each namespace.

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use tokio::sync::mpsc;

use crate::error::{ManagerError, PacketError};
use crate::manager::routes::Routes;
use crate::packet::{Connect, ConnectError, DynAck, DynEvent, Ns, Packet, ServerPacket};

/// Delivers server packets to the namespace receivers until the engine closes
/// `server_message_rx`.
///
/// Waiting on a full receiver holds up only this direction. After an error,
/// drops `connected_ns_tx`, which tells the client-packet loop to
/// close the engine, and discards server messages until the engine hangs up,
/// so the engine never sees a failed send.
///
/// # Errors
///
/// Returns an error if the server breaks the protocol, or if the client-packet
/// loop stopped while a namespace was still connecting.
pub async fn server_messages_to_packets(
    mut server_message_rx: mpsc::Receiver<Message>,
    routes: &Routes,
    connected_ns_tx: mpsc::UnboundedSender<ByteString>,
) -> Result<(), ManagerError> {
    let result = deliver_server_messages(&mut server_message_rx, routes, &connected_ns_tx).await;

    drop(connected_ns_tx);

    while server_message_rx.recv().await.is_some() {}

    tracing::debug!("server message channel closed");

    result
}

/// Delivers server packets until the engine closes `server_message_rx` or the
/// server breaks the protocol.
async fn deliver_server_messages(
    server_message_rx: &mut mpsc::Receiver<Message>,
    routes: &Routes,
    connected_ns_tx: &mpsc::UnboundedSender<ByteString>,
) -> Result<(), ManagerError> {
    let mut reconstructor = None;

    while let Some(message) = server_message_rx.recv().await {
        match message {
            Message::Text(text) => {
                route_text(text, routes, connected_ns_tx, &mut reconstructor).await?;
            }
            Message::Binary(attachment) => {
                route_binary(attachment, routes, &mut reconstructor).await?;
            }
        }
    }

    Ok(())
}

async fn route_text(
    text: ByteString,
    routes: &Routes,
    connected_ns_tx: &mpsc::UnboundedSender<ByteString>,
    reconstructor: &mut Option<Ns<BinaryPacket>>,
) -> Result<(), ManagerError> {
    if reconstructor.is_some() {
        return Err(ManagerError::UnexpectedText(text));
    }

    let Ns(ns, packet) = text.try_into()?;

    tracing::trace!(%ns, %packet, "received packet");

    match packet {
        Packet::Connect(payload) => {
            let connect: Connect = serde_json::from_str(&payload).map_err(PacketError::Json)?;

            tracing::debug!(%ns, sid = %connect.sid, "connected");

            if routes.mark_connected(&ns) {
                connected_ns_tx
                    .send(ns.clone())
                    .map_err(|_| ManagerError::NamespaceStatus)?;
            }
            send_server_packet(
                routes.server_packet_tx(&ns),
                &ns,
                ServerPacket::Connect(connect),
            )
            .await;
        }
        Packet::Disconnect => {
            tracing::debug!(%ns, "server closed");

            send_server_packet(routes.close(&ns), &ns, ServerPacket::Disconnect).await;
        }
        Packet::Event { payload, id } => {
            let event = DynEvent::new(payload, id);
            let server_packet = ServerPacket::Event(event);
            send_server_packet(routes.server_packet_tx(&ns), &ns, server_packet).await;
        }
        Packet::Ack { payload, id } => {
            resolve_ack(routes, &ns, id, DynAck::new(payload));
        }
        Packet::ConnectError(payload) => {
            let error: ConnectError = serde_json::from_str(&payload).map_err(PacketError::Json)?;

            tracing::debug!(%ns, %error, "namespace connection refused");

            // The server refused the namespace, so it closes like a DISCONNECT.
            send_server_packet(routes.close(&ns), &ns, ServerPacket::ConnectError(error)).await;
        }
        Packet::BinaryEvent { payload, id, count } => {
            *reconstructor = Some(Ns(ns, BinaryPacket::event(payload, id, count)));
        }
        Packet::BinaryAck { payload, id, count } => {
            *reconstructor = Some(Ns(ns, BinaryPacket::ack(payload, id, count)));
        }
    }

    Ok(())
}

async fn route_binary(
    attachment: Bytes,
    routes: &Routes,
    reconstructor: &mut Option<Ns<BinaryPacket>>,
) -> Result<(), ManagerError> {
    let bytes = attachment.len();

    let Some(Ns(ns, packet)) = attach_and_take(reconstructor, attachment)? else {
        tracing::trace!(bytes, status = "pending", "received attachment");
        return Ok(());
    };

    tracing::trace!(%ns, bytes, status = "complete", "received attachment");

    match packet {
        BinaryPacket::Event {
            payload,
            id,
            attachments,
            ..
        } => {
            let event = DynEvent {
                id,
                ..DynEvent::new(payload, None)
            }
            .with_attachments(attachments);
            send_server_packet(
                routes.server_packet_tx(&ns),
                &ns,
                ServerPacket::Event(event),
            )
            .await;
        }
        BinaryPacket::Ack {
            payload,
            id,
            attachments,
            ..
        } => {
            let ack = DynAck::new(payload).with_attachments(attachments);
            resolve_ack(routes, &ns, id, ack);
        }
    }

    Ok(())
}

/// Sends a server packet to a namespace receiver.
///
/// The server may still send packets for a namespace the client already left,
/// and the caller may drop a receiver it no longer reads, so both are
/// discarded.
async fn send_server_packet(
    server_packet_tx: Option<mpsc::Sender<ServerPacket>>,
    ns: &ByteString,
    server_packet: ServerPacket,
) {
    let Some(server_packet_tx) = server_packet_tx else {
        tracing::debug!(%ns, "discarded server packet for a closed namespace");
        return;
    };

    if server_packet_tx.send(server_packet).await.is_err() {
        tracing::debug!(%ns, "discarded server packet for a dropped receiver");
    }
}

/// Resolves a pending ack, discarding acks nobody waits for.
fn resolve_ack(routes: &Routes, ns: &ByteString, id: u64, ack: DynAck) {
    let Some(ack_tx) = routes.take_ack(ns, id) else {
        tracing::debug!(%ns, id, "discarded ack with no pending handle");
        return;
    };

    if ack_tx.send(ack).is_err() {
        tracing::debug!(%ns, id, "discarded ack for a dropped handle");
    }
}

enum BinaryPacket {
    Event {
        payload: ByteString,
        id: Option<u64>,
        attachments: Vec<Bytes>,
        count: usize,
    },
    Ack {
        payload: ByteString,
        id: u64,
        attachments: Vec<Bytes>,
        count: usize,
    },
}

impl BinaryPacket {
    fn event(payload: ByteString, id: Option<u64>, count: usize) -> Self {
        Self::Event {
            payload,
            id,
            attachments: Vec::new(),
            count,
        }
    }

    fn ack(payload: ByteString, id: u64, count: usize) -> Self {
        Self::Ack {
            payload,
            id,
            attachments: Vec::new(),
            count,
        }
    }

    fn attach(&mut self, bytes: Bytes) {
        match self {
            Self::Event { attachments, .. } | Self::Ack { attachments, .. } => {
                attachments.push(bytes);
            }
        }
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::Event {
                attachments, count, ..
            }
            | Self::Ack {
                attachments, count, ..
            } => attachments.len() == *count,
        }
    }
}

fn attach_and_take(
    pending: &mut Option<Ns<BinaryPacket>>,
    bytes: Bytes,
) -> Result<Option<Ns<BinaryPacket>>, ManagerError> {
    let Some(Ns(ns, mut packet)) = pending.take() else {
        return Err(ManagerError::UnexpectedBinary(bytes));
    };
    packet.attach(bytes);
    if packet.is_complete() {
        Ok(Some(Ns(ns, packet)))
    } else {
        *pending = Some(Ns(ns, packet));
        Ok(None)
    }
}
