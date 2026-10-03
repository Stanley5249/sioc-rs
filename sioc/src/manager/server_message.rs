//! Delivers what the server sends to each namespace.

use super::{Control, Routes};
use crate::error::{ManagerError, PacketError};
use crate::packet::{Connect, ConnectError, DynAck, DynEvent, Ns, Packet, Signal};
use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use tokio::sync::mpsc;

/// Delivers server packets to the namespace receivers until the engine closes `server_message_rx`.
///
/// Waiting on a full receiver holds up only this direction.
pub(super) async fn route_server_messages(
    mut server_message_rx: mpsc::Receiver<Message>,
    routes: &Routes,
    control_tx: mpsc::UnboundedSender<Control>,
) -> Result<(), ManagerError> {
    let mut reconstructor = Reconstructor::new();

    while let Some(message) = server_message_rx.recv().await {
        match message {
            Message::Text(text) => {
                route_text(text, routes, &control_tx, &mut reconstructor).await?;
            }
            Message::Binary(attachment) => {
                route_binary(attachment, routes, &mut reconstructor).await?;
            }
        }
    }

    tracing::debug!("closing all namespaces");

    routes.clear();

    Ok(())
}

async fn route_text(
    text: ByteString,
    routes: &Routes,
    control_tx: &mpsc::UnboundedSender<Control>,
    reconstructor: &mut Reconstructor,
) -> Result<(), ManagerError> {
    if reconstructor.is_pending() {
        return Err(ManagerError::UnexpectedText(text));
    }

    let Ns(ns, packet) = text.try_into()?;

    tracing::trace!(%ns, %packet, "<- packet");

    match packet {
        Packet::Connect(payload) => {
            let connect: Connect = serde_json::from_str(&payload).map_err(PacketError::Json)?;

            tracing::debug!(%ns, sid = %connect.sid, "connected");

            if routes.connect(&ns) {
                send_control(control_tx, Control::Connected(ns.clone()))?;
            }
            deliver(routes.signal_tx(&ns), &ns, Signal::Connect(connect)).await;
        }
        Packet::Disconnect => {
            tracing::debug!(%ns, "disconnected");

            let signal_tx = routes.remove(&ns);
            if signal_tx.is_some() {
                send_control(control_tx, Control::Disconnected(ns.clone()))?;
            }
            deliver(signal_tx, &ns, Signal::Disconnect).await;
        }
        Packet::Event { payload, id } => {
            let signal = Signal::Event(DynEvent::new(payload, id));
            deliver(routes.signal_tx(&ns), &ns, signal).await;
        }
        Packet::Ack { payload, id } => {
            resolve_ack(routes, &ns, id, DynAck::new(payload));
        }
        Packet::ConnectError(payload) => {
            let error: ConnectError = serde_json::from_str(&payload).map_err(PacketError::Json)?;

            tracing::error!(%ns, %error, "connect error");

            deliver(routes.signal_tx(&ns), &ns, Signal::ConnectError(error)).await;
        }
        Packet::BinaryEvent { payload, id, count } => {
            reconstructor.insert(ns, BinaryPacket::event(payload, id, count));
        }
        Packet::BinaryAck { payload, id, count } => {
            reconstructor.insert(ns, BinaryPacket::ack(payload, id, count));
        }
    }

    Ok(())
}

async fn route_binary(
    attachment: Bytes,
    routes: &Routes,
    reconstructor: &mut Reconstructor,
) -> Result<(), ManagerError> {
    let bytes = attachment.len();

    let Some(Ns(ns, packet)) = reconstructor.attach_and_take(attachment)? else {
        tracing::trace!(bytes, status = "pending", "<- attachment");
        return Ok(());
    };

    tracing::trace!(%ns, bytes, status = "complete", "<- attachment");

    match packet {
        BinaryPacket::Event {
            payload,
            id,
            attachments,
            ..
        } => {
            let event = DynEvent::new(payload, id).with_attachments(attachments);
            deliver(routes.signal_tx(&ns), &ns, Signal::Event(event)).await;
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

/// Delivers a signal to a namespace receiver.
///
/// The server may still send packets for a namespace the client already left,
/// and the caller may drop a receiver it no longer reads, so both are discarded.
async fn deliver(signal_tx: Option<mpsc::Sender<Signal>>, ns: &ByteString, signal: Signal) {
    let Some(signal_tx) = signal_tx else {
        tracing::debug!(%ns, "discarded signal for a closed namespace");
        return;
    };

    if signal_tx.send(signal).await.is_err() {
        tracing::debug!(%ns, "discarded signal for a dropped receiver");
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

fn send_control(
    control_tx: &mpsc::UnboundedSender<Control>,
    control: Control,
) -> Result<(), ManagerError> {
    control_tx
        .send(control)
        .map_err(|_| ManagerError::ControlClosed)
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

struct Reconstructor {
    pending: Option<Ns<BinaryPacket>>,
}

impl Reconstructor {
    fn new() -> Self {
        Self { pending: None }
    }

    fn is_pending(&self) -> bool {
        self.pending.is_some()
    }

    fn insert(&mut self, ns: ByteString, packet: BinaryPacket) {
        self.pending = Some(Ns(ns, packet));
    }

    fn attach_and_take(&mut self, bytes: Bytes) -> Result<Option<Ns<BinaryPacket>>, ManagerError> {
        match std::mem::take(&mut self.pending) {
            Some(Ns(ns, mut packet)) => {
                packet.attach(bytes);

                if packet.is_complete() {
                    Ok(Some(Ns(ns, packet)))
                } else {
                    self.pending = Some(Ns(ns, packet));

                    Ok(None)
                }
            }
            None => Err(ManagerError::UnexpectedBinary(bytes)),
        }
    }
}
