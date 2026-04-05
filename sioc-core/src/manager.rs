//! Internal routing manager for Socket.IO packet flow.
//!
//! [`Manager`] bridges the engine.IO transport to the typed Socket.IO API.
//! Spawn it with [`Manager::run`].

use crate::error::{Error, Result};
use crate::packet::{Command, DynAck, DynEvent, Ns, Packet, RawPacket};
use bytes::Bytes;
use sioc_engine::engine::Engine;
use sioc_engine::prelude::{Message, MessageSender};
use std::collections::BTreeMap;
use std::collections::hash_map::{Entry, HashMap};
use tokio::sync::{mpsc, oneshot};

pub enum ManagerAction {
    // outbound from client
    Command(Ns<Command>),
    // inbound from engine
    Message(Message),
}

impl From<Ns<Command>> for ManagerAction {
    fn from(command: Ns<Command>) -> Self {
        ManagerAction::Command(command)
    }
}

impl From<Message> for ManagerAction {
    fn from(message: Message) -> Self {
        ManagerAction::Message(message)
    }
}

/// Restricted sender: can only send outbound [`Command`]s to the manager (used by client).
#[derive(Clone, Debug)]
pub struct CommandSender(pub mpsc::Sender<ManagerAction>);

impl CommandSender {
    pub async fn send(
        &self,
        command: Ns<Command>,
    ) -> Result<(), mpsc::error::SendError<ManagerAction>> {
        self.0.send(command.into()).await
    }
}

impl Socket {
    fn new(tx: mpsc::Sender<Packet>) -> Self {
        Self {
            tx,
            acks: BTreeMap::new(),
            ids: 0,
            connected: false,
            buffer: Vec::new(),
        }
    }

    /// Allocate the next ack ID and store the responder.
    fn register_ack(&mut self, sender: oneshot::Sender<DynAck>) -> u64 {
        let id = self.ids;
        self.ids += 1;
        self.acks.insert(id, sender);
        id
    }

    fn send_ack(&mut self, ns: String, id: u64, ack: DynAck) -> Result<String> {
        match self.acks.remove(&id) {
            Some(sender) => match sender.send(ack) {
                Ok(()) => Ok(ns),
                Err(ack) => Err(Error::SendAck { ns, ack }),
            },
            None => Err(Error::UnknownAckId { ns, id }),
        }
    }

    async fn send_packet(&mut self, ns: String, packet: Packet) -> Result<String> {
        match self.tx.send(packet).await {
            Ok(()) => Ok(ns),
            Err(source) => Err(Error::SendPacket { ns, source }),
        }
    }

    async fn send_binary_packet(&mut self, ns: String, packet: BinaryPacket) -> Result<String> {
        match packet {
            BinaryPacket::Event {
                data,
                id,
                attachments,
                ..
            } => {
                let packet = Packet::Event(DynEvent::new(data, id).with_attachments(attachments));

                self.send_packet(ns, packet).await
            }
            BinaryPacket::Ack {
                data,
                id,
                attachments,
                ..
            } => {
                let ack = DynAck::new(data).with_attachments(attachments);

                self.send_ack(ns, id, ack)
            }
        }
    }
}

struct SocketsMap(HashMap<String, Socket>);

impl SocketsMap {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn get_mut(&mut self, ns: String) -> Result<Ns<&mut Socket>> {
        match self.0.get_mut(&ns) {
            Some(socket) => Ok(Ns(ns, socket)),
            None => Err(Error::UnknownNamespace { ns }),
        }
    }

    fn connect(&mut self, ns: String, socket: Socket) -> Result<Ns<&mut Socket>> {
        match self.0.entry(ns) {
            Entry::Occupied(e) => {
                let ns = e.key().clone();
                Err(Error::NamespaceConflict { ns })
            }
            Entry::Vacant(e) => {
                let ns = e.key().clone();
                let socket = e.insert(socket);
                Ok(Ns(ns, socket))
            }
        }
    }

    fn disconnect(&mut self, ns: String) -> Result<Ns<Socket>> {
        match self.0.remove(&ns) {
            Some(socket) => Ok(Ns(ns, socket)),
            None => Err(Error::UnknownNamespace { ns }),
        }
    }

    fn require(&self, ns: String) -> Result<String> {
        if self.0.contains_key(&ns) {
            Ok(ns)
        } else {
            Err(Error::UnknownNamespace { ns })
        }
    }

    fn close(&mut self) {
        self.0.clear();
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

enum BinaryPacket {
    Event {
        data: Bytes,
        id: Option<u64>,
        attachments: Vec<Bytes>,
        count: usize,
    },
    Ack {
        data: Bytes,
        id: u64,
        attachments: Vec<Bytes>,
        count: usize,
    },
}

impl BinaryPacket {
    fn event(data: Bytes, id: Option<u64>, count: usize) -> Self {
        Self::Event {
            data,
            id,
            attachments: Vec::new(),
            count,
        }
    }

    fn ack(data: Bytes, id: u64, count: usize) -> Self {
        Self::Ack {
            data,
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
            } => attachments.len() >= *count,
        }
    }

    fn as_raw(&self) -> RawPacket {
        match self {
            Self::Event {
                data, id, count, ..
            } => RawPacket::BinaryEvent {
                data: data.clone(),
                id: *id,
                count: *count,
            },
            Self::Ack {
                data, id, count, ..
            } => RawPacket::BinaryAck {
                data: data.clone(),
                id: *id,
                count: *count,
            },
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

    fn insert(&mut self, ns: String, packet: BinaryPacket) {
        self.pending = Some(Ns(ns, packet));
    }

    fn attach_and_take(&mut self, bytes: Bytes) -> Result<Option<Ns<BinaryPacket>>> {
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
            None => Err(Error::UnexpectedBinary(bytes)),
        }
    }
}

struct Socket {
    /// Channel to the state handler for delivering inbound packets.
    tx: mpsc::Sender<Packet>,
    /// Pending ack responders keyed by their wire ID.
    acks: BTreeMap<u64, oneshot::Sender<DynAck>>,
    /// Monotonically increasing counter used to generate ack IDs.
    ids: u64,
    /// True once the server has sent a CONNECT response for this namespace.
    connected: bool,
    /// Events encoded before CONNECT was confirmed; flushed on CONNECT response.
    buffer: Vec<Message>,
}

/// Routes packets between the Socket.IO API and the engine.IO transport.
pub struct Manager {
    rx: mpsc::Receiver<ManagerAction>,
    sockets: SocketsMap,
    reconstructor: Reconstructor,
}

impl Manager {
    pub fn new(rx: mpsc::Receiver<ManagerAction>) -> Self {
        Self {
            rx,
            sockets: SocketsMap::new(),
            reconstructor: Reconstructor::new(),
        }
    }

    /// Drive the Socket.IO protocol loop until the connection closes.
    ///
    /// Interleaves outbound encoding (local → wire) with inbound decoding
    /// (wire → namespace channels). Exits when the transport closes or all
    /// namespace senders are dropped. Joins the engine and transport tasks
    /// before returning.
    #[tracing::instrument(skip_all, err)]
    pub async fn run(mut self, engine: Engine) -> Result<()> {
        while let Some(command) = self.rx.recv().await {
            match command {
                ManagerAction::Command(Ns(ns, packet)) => {
                    self.dispatch_command(&engine.tx, ns, packet).await?;
                }
                ManagerAction::Message(message) => {
                    self.route_message(&engine.tx, message).await?;
                }
            }
            if self.sockets.is_empty() {
                engine.tx.send(Message::Close).await?;
            }
        }

        engine.join().await?;

        Ok(())
    }

    /// Encodes and sends (or buffers) one outbound packet.
    ///
    /// Events sent before the server confirms namespace connection are held in
    /// `socket.buffer` and flushed when the CONNECT response arrives.
    async fn dispatch_command(
        &mut self,
        engine_tx: &MessageSender,
        ns: String,
        command: Command,
    ) -> Result<()> {
        let mut socket_buffer = None;

        let (ns, packet, attachments) = match command {
            Command::Connect { tx, data } => {
                let socket = Socket::new(tx);
                let Ns(ns, _) = self.sockets.connect(ns, socket)?;

                (ns, RawPacket::Connect(data), None)
            }
            Command::Disconnect => {
                let Ns(ns, _) = self.sockets.disconnect(ns)?;

                (ns, RawPacket::Disconnect, None)
            }
            Command::Event {
                data,
                tx,
                attachments,
            } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                let id = tx.map(|tx| socket.register_ack(tx));

                if !socket.connected {
                    socket_buffer = Some(&mut socket.buffer);
                }

                let packet = match &attachments {
                    None => RawPacket::Event { data, id },
                    Some(attachments) => RawPacket::BinaryEvent {
                        data,
                        id,
                        count: attachments.len(),
                    },
                };

                (ns, packet, attachments)
            }
            Command::Ack {
                data,
                id,
                attachments,
            } => {
                let ns = self.sockets.require(ns)?;

                let packet = match &attachments {
                    None => RawPacket::Ack { data, id },
                    Some(attachments) => RawPacket::BinaryAck {
                        data,
                        id,
                        count: attachments.len(),
                    },
                };

                (ns, packet, attachments)
            }
        };

        tracing::trace!(ns, ?packet, "sending packet");

        let text = Message::Text(packet.encode(&ns));
        let binaries = attachments.into_iter().flatten().map(Message::Binary);
        let messages = std::iter::once(text).chain(binaries);

        match socket_buffer {
            Some(buffer) => {
                tracing::trace!(ns, "buffering packets");
                buffer.extend(messages);
            }
            None => {
                for message in messages {
                    engine_tx.send(message).await?;
                }
            }
        }

        Ok(())
    }

    async fn route_message(&mut self, engine_tx: &MessageSender, message: Message) -> Result<()> {
        match message {
            Message::Text(bytes) => {
                self.route_text_message(bytes, engine_tx).await?;
            }

            Message::Binary(attachment) => {
                self.route_binary_message(attachment).await?;
            }

            Message::Close => {
                tracing::trace!("closing all namespaces");
                self.sockets.close();
            }
        }

        Ok(())
    }

    async fn route_text_message(&mut self, bytes: Bytes, engine_tx: &MessageSender) -> Result<()> {
        if self.reconstructor.is_pending() {
            return Err(Error::UnexpectedText(bytes));
        }

        let Ns(ns, packet) = bytes.try_into()?;

        tracing::trace!(ns, ?packet, "received packet");

        match packet {
            RawPacket::Connect(data) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket.connected = true;

                let len = socket.buffer.len();

                if len > 0 {
                    tracing::trace!(ns, len, "sending buffered packets");

                    for message in socket.buffer.drain(..) {
                        engine_tx.send(message).await?;
                    }
                }

                let connect = serde_json::from_slice(&data)
                    .expect("CONNECT packet data should be valid JSON");

                socket.send_packet(ns, Packet::Connect(connect)).await?;
            }
            RawPacket::Disconnect => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket.send_packet(ns, Packet::Disconnect).await?;
            }
            RawPacket::Event { data, id } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket
                    .send_packet(ns, Packet::Event(DynEvent::new(data, id)))
                    .await?;
            }
            RawPacket::Ack { data, id } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket.send_ack(ns, id, DynAck::new(data))?;
            }
            RawPacket::ConnectError(data) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                let error = serde_json::from_slice(&data)
                    .expect("CONNECT_ERROR packet data should be valid JSON");

                socket.send_packet(ns, Packet::ConnectError(error)).await?;
            }
            RawPacket::BinaryEvent { data, id, count } => {
                let ns = self.sockets.require(ns)?;

                self.reconstructor
                    .insert(ns, BinaryPacket::event(data, id, count));
            }
            RawPacket::BinaryAck { data, id, count } => {
                let ns = self.sockets.require(ns)?;

                self.reconstructor
                    .insert(ns, BinaryPacket::ack(data, id, count));
            }
        };

        Ok(())
    }

    async fn route_binary_message(&mut self, bytes: Bytes) -> Result<()> {
        let len = bytes.len();

        match self.reconstructor.attach_and_take(bytes)? {
            Some(Ns(ns, packet)) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                tracing::trace!(
                    len,
                    status = "complete",
                    ns,
                    packet = ?packet.as_raw(),
                    "received binary attachment"
                );

                socket.send_binary_packet(ns, packet).await?;
            }
            None => {
                tracing::trace!(len, status = "pending", "received binary attachment");
            }
        }
        Ok(())
    }
}
