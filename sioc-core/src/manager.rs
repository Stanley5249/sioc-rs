//! Internal routing manager for Socket.IO packet flow.
//!
//! [`Manager`] bridges the engine.IO transport to the typed Socket.IO API.
//! Spawn it with [`Manager::run`].

use crate::error::{Error, Result};
use crate::packet::{Command, DynAck, DynEvent, Ns, Packet, SioPacket};
use crate::parse::write_packet;
use bytes::{Bytes, BytesMut};
use sioc_engine::prelude::Message;
use std::collections::BTreeMap;
use std::collections::hash_map::{Entry, HashMap};
use tokio::sync::{mpsc, oneshot};

impl Socket {
    fn new(sender: mpsc::Sender<Packet>) -> Self {
        Self {
            tx: sender,
            acks: BTreeMap::new(),
            ids: 0,
        }
    }

    /// Allocate the next ack ID and store the responder.
    fn register_ack(&mut self, sender: oneshot::Sender<DynAck>) -> u64 {
        let id = self.ids;
        self.ids += 1;
        self.acks.insert(id, sender);
        id
    }

    fn acknowledge(&mut self, ns: String, id: u64, ack: DynAck) -> Result<String> {
        match self.acks.remove(&id) {
            Some(sender) => match sender.send(ack) {
                Ok(()) => Ok(ns),
                Err(ack) => Err(Error::AckClosed { ns, ack }),
            },
            None => Err(Error::UnknownAckId { ns, id }),
        }
    }

    async fn on_packet(&mut self, ns: String, packet: Packet) -> Result<String> {
        match self.tx.send(packet).await {
            Ok(()) => Ok(ns),
            Err(source) => Err(Error::SendPacket { ns, source }),
        }
    }

    async fn on_binary(&mut self, ns: String, packet: BinaryPacket) -> Result<String> {
        match packet {
            BinaryPacket::Event {
                data,
                id,
                attachments,
                ..
            } => {
                let packet = Packet::Event(DynEvent::new(data, id).with_attachments(attachments));

                self.on_packet(ns, packet).await
            }
            BinaryPacket::Ack {
                data,
                id,
                attachments,
                ..
            } => {
                let ack = DynAck::new(data).with_attachments(attachments);

                self.acknowledge(ns, id, ack)
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

    fn attach(&mut self, bytes: Bytes) -> bool {
        match self {
            Self::Event {
                attachments, count, ..
            }
            | Self::Ack {
                attachments, count, ..
            } => {
                attachments.push(bytes);
                attachments.len() >= *count
            }
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

    fn insert(&mut self, packet: Ns<BinaryPacket>) {
        self.pending = Some(packet);
    }

    fn attach_and_take(&mut self, bytes: Bytes) -> Result<Option<Ns<BinaryPacket>>> {
        match std::mem::take(&mut self.pending) {
            Some(Ns(ns, mut packet)) => Ok(if packet.attach(bytes) {
                Some(Ns(ns, packet))
            } else {
                self.pending = Some(Ns(ns, packet));
                None
            }),
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
}

/// Routes packets between the Socket.IO API and the engine.IO transport.
pub struct Manager {
    rx: mpsc::Receiver<Ns<Command>>,
    sockets: SocketsMap,
    reconstructor: Reconstructor,
}

impl Manager {
    pub fn new(local_rx: mpsc::Receiver<Ns<Command>>) -> Self {
        Self {
            rx: local_rx,
            sockets: SocketsMap::new(),
            reconstructor: Reconstructor::new(),
        }
    }

    /// Drive the Socket.IO protocol loop until the connection closes.
    ///
    /// Interleaves outbound encoding (local → wire) with inbound decoding
    /// (wire → namespace channels) via `select!`. Exits when the transport
    /// closes or all namespace senders are dropped.
    pub async fn run(
        mut self,
        mut engine_rx: mpsc::Receiver<Message>,
        engine_tx: mpsc::Sender<Message>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                packet = self.rx.recv() => {
                    match packet {
                        Some(Ns(ns, packet)) => {
                            for message in self.handle_outbound(ns, packet)? {
                                if engine_tx.send(message).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                        None => return Ok(()),
                    }
                }
                message = engine_rx.recv() => {
                    match message {
                        Some(message) => self.handle_inbound(message).await?,
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    /// Encodes one outbound packet into wire messages (text frame + binary attachments).
    ///
    /// Separated from the `rx.recv()` await so `run` can use `self.rx.recv()`
    /// as the select future (partial field borrow) without conflicting with the
    /// full `&mut self` needed in branch bodies.
    fn handle_outbound(
        &mut self,
        ns: String,
        command: Command,
    ) -> Result<impl Iterator<Item = Message>> {
        let type_id = command.type_id();
        let mut buffer = BytesMut::with_capacity(command.size_hint(&ns));

        let attachments = match command {
            Command::Connect { sender, data } => {
                let Ns(ns, _) = self.sockets.connect(ns, Socket::new(sender))?;
                write_packet(&mut buffer, type_id, None, &ns, None, data.as_deref());
                None
            }
            Command::Disconnect => {
                let Ns(ns, _) = self.sockets.disconnect(ns)?;
                write_packet(&mut buffer, type_id, None, &ns, None, None);
                None
            }
            Command::Event {
                data,
                sender,
                attachments,
            } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;
                let id = sender.map(|sender| socket.register_ack(sender));
                write_packet(&mut buffer, type_id, None, &ns, id, Some(&data));
                attachments
            }
            Command::Ack {
                data,
                id,
                attachments,
            } => {
                let ns = self.sockets.require(ns)?;
                write_packet(&mut buffer, type_id, None, &ns, Some(id), Some(&data));
                attachments
            }
        };

        let text = Message::Text(buffer.freeze());
        let binaries = attachments.into_iter().flatten().map(Message::Binary);

        Ok(std::iter::once(text).chain(binaries))
    }

    async fn handle_inbound(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Text(bytes) => self.handle_text(bytes).await,
            Message::Binary(attachment) => self.handle_binary(attachment).await,
            // Engine closed; the run loop will exit when engine.recv() returns None.
            Message::Close => Ok(()),
        }
    }

    async fn handle_text(&mut self, bytes: Bytes) -> Result<()> {
        if self.reconstructor.is_pending() {
            return Err(Error::UnexpectedText(bytes));
        }
        let Ns(ns, packet) = bytes.try_into()?;
        match packet {
            SioPacket::Connect(connection) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;
                let packet = Packet::Connect(connection);
                socket.on_packet(ns, packet).await?;
            }
            SioPacket::Disconnect => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;
                let packet = Packet::Disconnect;
                socket.on_packet(ns, packet).await?;
            }
            SioPacket::ConnectError(error) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;
                let packet = Packet::ConnectError(error);
                socket.on_packet(ns, packet).await?;
            }
            SioPacket::Event { data, id, count } => match count {
                Some(count) => {
                    let ns = self.sockets.require(ns)?;
                    let packet = BinaryPacket::event(data, id, count);
                    self.reconstructor.insert(Ns(ns, packet));
                }
                None => {
                    let Ns(ns, socket) = self.sockets.get_mut(ns)?;
                    let packet = Packet::Event(DynEvent::new(data, id));
                    socket.on_packet(ns, packet).await?;
                }
            },
            SioPacket::Ack { data, id, count } => match count {
                Some(count) => {
                    let ns = self.sockets.require(ns)?;
                    let ack = BinaryPacket::ack(data, id, count);
                    self.reconstructor.insert(Ns(ns, ack));
                }
                None => {
                    let Ns(ns, socket) = self.sockets.get_mut(ns)?;
                    let ack = DynAck::new(data);
                    socket.acknowledge(ns, id, ack)?;
                }
            },
        };
        Ok(())
    }

    async fn handle_binary(&mut self, bytes: Bytes) -> Result<()> {
        if let Some(Ns(ns, packet)) = self.reconstructor.attach_and_take(bytes)? {
            let Ns(ns, socket) = self.sockets.get_mut(ns)?;
            socket.on_binary(ns, packet).await?;
        }
        Ok(())
    }
}
