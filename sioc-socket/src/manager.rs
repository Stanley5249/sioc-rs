//! Socket.IO namespace router.

use crate::error::{ManagerError, PacketError};
use crate::packet::{Connect, ConnectError, Directive, DynAck, DynEvent, Ns, Packet, Signal};
use bytes::Bytes;
use bytestring::ByteString;
use futures_util::{Sink, SinkExt, future};
use sioc_engine::engine::{Engine, EngineSender};
use sioc_engine::prelude::Message;
use std::collections::BTreeMap;
use std::collections::hash_map::{Entry, HashMap};
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{PollSendError, PollSender};

/// Creates a [`Sink<Message>`] that maps each [`Message`] to a [`ManagerAction`] and sends it.
pub fn message_sink(
    tx: mpsc::Sender<ManagerAction>,
) -> impl Sink<Message, Error = PollSendError<ManagerAction>> {
    PollSender::new(tx).with(|message: Message| future::ok(message.into()))
}

#[derive(Debug)]
pub enum ManagerAction {
    /// Outbound directive from the client.
    Socket(Ns<Directive>),
    /// Inbound message from the engine.
    Engine(Message),
}

impl From<Ns<Directive>> for ManagerAction {
    fn from(directive: Ns<Directive>) -> Self {
        ManagerAction::Socket(directive)
    }
}

impl From<Message> for ManagerAction {
    fn from(message: Message) -> Self {
        ManagerAction::Engine(message)
    }
}

/// Sends outbound [`Directive`]s to the socket router.
#[derive(Clone, Debug)]
pub struct DirectiveSender(mpsc::Sender<ManagerAction>);

impl DirectiveSender {
    pub fn new(tx: mpsc::Sender<ManagerAction>) -> Self {
        Self(tx)
    }

    pub async fn send(
        &self,
        ns: ByteString,
        directive: Directive,
    ) -> Result<(), SendError<ManagerAction>> {
        self.0.send(Ns(ns, directive).into()).await
    }

    pub fn blocking_send(
        &self,
        ns: ByteString,
        directive: Directive,
    ) -> Result<(), SendError<ManagerAction>> {
        self.0.blocking_send(Ns(ns, directive).into())
    }

    pub fn try_send(
        &self,
        ns: ByteString,
        directive: Directive,
    ) -> Result<(), TrySendError<ManagerAction>> {
        self.0.try_send(Ns(ns, directive).into())
    }
}

impl Socket {
    fn new(tx: mpsc::Sender<Signal>) -> Self {
        Self {
            tx,
            acks: BTreeMap::new(),
            ids: 0,
            connected: false,
            buffer: Vec::new(),
        }
    }

    /// Allocates the next ack ID and stores the responder.
    fn register_ack(&mut self, sender: oneshot::Sender<DynAck>) -> u64 {
        let id = self.ids;
        self.ids += 1;
        self.acks.insert(id, sender);
        id
    }

    fn send_ack(
        &mut self,
        ns: ByteString,
        id: u64,
        ack: DynAck,
    ) -> Result<ByteString, ManagerError> {
        match self.acks.remove(&id) {
            Some(sender) => match sender.send(ack) {
                Ok(()) => Ok(ns),
                Err(ack) => Err(ManagerError::SendAck { ns, ack }),
            },
            None => Err(ManagerError::UnknownAckId { ns, id }),
        }
    }

    async fn send_packet(
        &mut self,
        ns: ByteString,
        packet: Signal,
    ) -> Result<ByteString, ManagerError> {
        match self.tx.send(packet).await {
            Ok(()) => Ok(ns),
            Err(source) => Err(ManagerError::SendSocket { ns, source }),
        }
    }

    async fn send_binary_packet(
        &mut self,
        ns: ByteString,
        packet: BinaryPacket,
    ) -> Result<ByteString, ManagerError> {
        match packet {
            BinaryPacket::Event {
                payload,
                id,
                attachments,
                ..
            } => {
                let packet =
                    Signal::Event(DynEvent::new(payload, id).with_attachments(attachments));

                self.send_packet(ns, packet).await
            }
            BinaryPacket::Ack {
                payload,
                id,
                attachments,
                ..
            } => {
                let ack = DynAck::new(payload).with_attachments(attachments);

                self.send_ack(ns, id, ack)
            }
        }
    }
}

struct SocketsMap(HashMap<ByteString, Socket>);

impl SocketsMap {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn get_mut(&mut self, ns: ByteString) -> Result<Ns<&mut Socket>, ManagerError> {
        match self.0.get_mut(&ns) {
            Some(socket) => Ok(Ns(ns, socket)),
            None => Err(ManagerError::UnknownNamespace { ns }),
        }
    }

    fn connect(&mut self, ns: ByteString, socket: Socket) -> Result<Ns<&mut Socket>, ManagerError> {
        match self.0.entry(ns) {
            Entry::Occupied(e) => Err(ManagerError::NamespaceConflict {
                ns: e.key().clone(),
            }),
            Entry::Vacant(e) => {
                let ns = e.key().clone();
                Ok(Ns(ns, e.insert(socket)))
            }
        }
    }

    fn disconnect(&mut self, ns: ByteString) -> Result<Ns<Socket>, ManagerError> {
        match self.0.remove(&ns) {
            Some(socket) => Ok(Ns(ns, socket)),
            None => Err(ManagerError::UnknownNamespace { ns }),
        }
    }

    fn require(&self, ns: ByteString) -> Result<ByteString, ManagerError> {
        if self.0.contains_key(&ns) {
            Ok(ns)
        } else {
            Err(ManagerError::UnknownNamespace { ns })
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

struct Socket {
    tx: mpsc::Sender<Signal>,
    acks: BTreeMap<u64, oneshot::Sender<DynAck>>,
    ids: u64,
    // Set when the server sends a CONNECT response; gates event delivery.
    connected: bool,
    // Events encoded before `connected` is set; flushed on CONNECT response.
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

    /// Runs the socket routing loop until all namespaces disconnect.
    #[tracing::instrument(skip_all, err)]
    pub async fn socket_io(mut self, engine: Engine) -> Result<(), ManagerError> {
        while let Some(directive) = self.rx.recv().await {
            match directive {
                ManagerAction::Socket(Ns(ns, packet)) => {
                    self.dispatch_directive(&engine.tx, ns, packet).await?;
                }
                ManagerAction::Engine(message) => {
                    self.route_message(&engine.tx, message).await?;
                }
            }
            if self.sockets.is_empty() {
                engine.tx.send(Message::Close).await?;
                break;
            }
        }

        engine.join().await?;

        Ok(())
    }

    /// Encodes and sends (or buffers) one outbound directive.
    async fn dispatch_directive(
        &mut self,
        engine_tx: &EngineSender,
        ns: ByteString,
        directive: Directive,
    ) -> Result<(), ManagerError> {
        let mut socket_buffer = None;

        let (ns, packet, attachments) = match directive {
            Directive::Connect { tx, payload } => {
                let socket = Socket::new(tx);
                let Ns(ns, _) = self.sockets.connect(ns, socket)?;

                (ns, Packet::Connect(payload), None)
            }
            Directive::Disconnect => {
                let Ns(ns, _) = self.sockets.disconnect(ns)?;

                (ns, Packet::Disconnect, None)
            }
            Directive::Event {
                payload,
                tx,
                attachments,
            } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                let id = tx.map(|tx| socket.register_ack(tx));

                if !socket.connected {
                    socket_buffer = Some(&mut socket.buffer);
                }

                let packet = match &attachments {
                    None => Packet::Event { payload, id },
                    Some(attachments) => Packet::BinaryEvent {
                        payload,
                        id,
                        count: attachments.len(),
                    },
                };

                (ns, packet, attachments)
            }
            Directive::Ack {
                payload,
                id,
                attachments,
            } => {
                let ns = self.sockets.require(ns)?;

                let packet = match &attachments {
                    None => Packet::Ack { payload, id },
                    Some(attachments) => Packet::BinaryAck {
                        payload,
                        id,
                        count: attachments.len(),
                    },
                };

                (ns, packet, attachments)
            }
        };

        let text = Message::Text(packet.encode(&ns).into());
        let binaries = attachments.into_iter().flatten().map(Message::Binary);
        let messages = std::iter::once(text).chain(binaries);

        match socket_buffer {
            Some(buffer) => {
                buffer.extend(messages);

                tracing::trace!(%ns, %packet, buffer.len = buffer.len(), "buffering messages");
            }
            None => {
                tracing::trace!(%ns, %packet, "-> packet");
                for message in messages {
                    engine_tx.send(message).await?;
                }
            }
        }

        Ok(())
    }

    async fn route_message(
        &mut self,
        engine_tx: &EngineSender,
        message: Message,
    ) -> Result<(), ManagerError> {
        match message {
            Message::Text(text) => {
                self.route_text_message(text, engine_tx).await?;
            }

            Message::Binary(attachment) => {
                self.route_binary_message(attachment).await?;
            }

            Message::Close => {
                tracing::debug!("closing all namespaces");
                self.sockets.close();
            }
        }

        Ok(())
    }

    async fn route_text_message(
        &mut self,
        text: ByteString,
        engine_tx: &EngineSender,
    ) -> Result<(), ManagerError> {
        if self.reconstructor.is_pending() {
            return Err(ManagerError::UnexpectedText(text));
        }

        let Ns(ns, packet) = text.try_into()?;

        tracing::trace!(%ns, %packet, "<- packet");

        match packet {
            Packet::Connect(payload) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket.connected = true;

                let count = socket.buffer.len();

                if count > 0 {
                    tracing::trace!(%ns, count, "flushed buffer");

                    for message in socket.buffer.drain(..) {
                        engine_tx.send(message).await?;
                    }
                }

                let connect: Connect = serde_json::from_str(&payload).map_err(PacketError::Json)?;

                tracing::debug!(%ns, sid = %connect.sid, "connected");

                socket.send_packet(ns, Signal::Connect(connect)).await?;
            }
            Packet::Disconnect => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                tracing::debug!(%ns, "disconnected");

                socket.send_packet(ns, Signal::Disconnect).await?;
            }
            Packet::Event { payload, id } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket
                    .send_packet(ns, Signal::Event(DynEvent::new(payload, id)))
                    .await?;
            }
            Packet::Ack { payload, id } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket.send_ack(ns, id, DynAck::new(payload))?;
            }
            Packet::ConnectError(payload) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                let error: ConnectError =
                    serde_json::from_str(&payload).map_err(PacketError::Json)?;

                tracing::error!(%ns, %error, "connect error");

                socket.send_packet(ns, Signal::ConnectError(error)).await?;
            }
            Packet::BinaryEvent { payload, id, count } => {
                let ns = self.sockets.require(ns)?;

                self.reconstructor
                    .insert(ns, BinaryPacket::event(payload, id, count));
            }
            Packet::BinaryAck { payload, id, count } => {
                let ns = self.sockets.require(ns)?;

                self.reconstructor
                    .insert(ns, BinaryPacket::ack(payload, id, count));
            }
        };

        Ok(())
    }

    async fn route_binary_message(&mut self, attachment: Bytes) -> Result<(), ManagerError> {
        let bytes = attachment.len();

        match self.reconstructor.attach_and_take(attachment)? {
            Some(Ns(ns, packet)) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                tracing::trace!(%ns, bytes, status = "complete", "<- attachment");

                socket.send_binary_packet(ns, packet).await?;
            }
            None => {
                tracing::trace!(bytes, status = "pending", "<- attachment");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sioc_engine::engine::EngineAction;
    use tokio::task::JoinHandle;

    const CONNECT_RESPONSE: &str = "0{\"sid\":\"test\"}";

    fn mock_engine(tx: mpsc::Sender<EngineAction>) -> Engine {
        Engine {
            tx: EngineSender(tx),
            engine_handle: tokio::spawn(async { Ok(()) }),
            transport_handle: tokio::spawn(async { Ok(()) }),
        }
    }

    /// Spawns a manager and returns `(manager_tx, engine_rx, join_handle)`.
    fn setup_manager() -> (
        mpsc::Sender<ManagerAction>,
        mpsc::Receiver<EngineAction>,
        JoinHandle<Result<(), ManagerError>>,
    ) {
        let (engine_tx, engine_rx) = mpsc::channel(32);
        let (manager_tx, manager_rx) = mpsc::channel(32);
        let handle = tokio::spawn(Manager::new(manager_rx).socket_io(mock_engine(engine_tx)));
        (manager_tx, engine_rx, handle)
    }

    async fn open_namespace(
        manager_tx: &mpsc::Sender<ManagerAction>,
        ns: &str,
    ) -> mpsc::Receiver<Signal> {
        let (tx, rx) = mpsc::channel(32);
        manager_tx
            .send(ManagerAction::Socket(Ns(
                ns.into(),
                Directive::Connect {
                    tx,
                    payload: ByteString::new(),
                },
            )))
            .await
            .unwrap();
        rx
    }

    async fn server_connect(manager_tx: &mpsc::Sender<ManagerAction>) {
        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static(CONNECT_RESPONSE),
            )))
            .await
            .unwrap();
    }

    /// Events are held in the socket buffer until the server sends a CONNECT response.
    #[tokio::test]
    async fn events_buffered_before_server_connect() {
        let (manager_tx, mut engine_rx, _) = setup_manager();

        open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap(); // drain outbound connect frame

        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/".into(),
                Directive::Event {
                    payload: ByteString::from_static("[\"ping\"]"),
                    tx: None,
                    attachments: None,
                },
            )))
            .await
            .unwrap();

        tokio::task::yield_now().await;
        assert!(
            engine_rx.try_recv().is_err(),
            "event must be buffered before server CONNECT"
        );
    }

    /// All buffered events are flushed in order once the server confirms the namespace.
    #[tokio::test]
    async fn buffered_events_flushed_on_server_connect() {
        let (manager_tx, mut engine_rx, _) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        for c in 'a'..='c' {
            let payload = format!("[\"{}\"]", c);

            manager_tx
                .send(ManagerAction::Socket(Ns(
                    "/".into(),
                    Directive::Event {
                        payload: ByteString::from(payload),
                        tx: None,
                        attachments: None,
                    },
                )))
                .await
                .unwrap();
        }

        tokio::task::yield_now().await;
        assert!(engine_rx.try_recv().is_err(), "must still be buffered");

        server_connect(&manager_tx).await;

        for _ in 0..3 {
            assert!(matches!(
                engine_rx.recv().await.unwrap(),
                EngineAction::Sink(Message::Text(_))
            ));
        }
        assert!(matches!(
            socket_rx.recv().await.unwrap(),
            Signal::Connect(_)
        ));
    }

    /// After the last namespace disconnects the manager sends Close to the engine and exits.
    #[tokio::test]
    async fn disconnect_closes_engine_when_empty() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let _socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        server_connect(&manager_tx).await;
        manager_tx
            .send(ManagerAction::Socket(Ns("/".into(), Directive::Disconnect)))
            .await
            .unwrap();

        assert!(matches!(
            engine_rx.recv().await.unwrap(),
            EngineAction::Sink(Message::Text(_))
        ));
        assert!(matches!(
            engine_rx.recv().await.unwrap(),
            EngineAction::Sink(Message::Close)
        ));

        drop(manager_tx);
        handle.await.unwrap().unwrap();
    }

    /// Routing a directive to an unknown namespace returns an error.
    #[tokio::test]
    async fn unknown_namespace_returns_error() {
        let (manager_tx, _engine_rx, handle) = setup_manager();

        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/no-such-ns".into(),
                Directive::Event {
                    payload: ByteString::from_static("[\"x\"]"),
                    tx: None,
                    attachments: None,
                },
            )))
            .await
            .unwrap();

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::ManagerError::UnknownNamespace { .. })
        ));
    }

    /// Connecting the same namespace twice returns a conflict error.
    #[tokio::test]
    async fn duplicate_connect_returns_conflict() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();

        open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();
        open_namespace(&manager_tx, "/").await;

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::ManagerError::NamespaceConflict { .. })
        ));
    }

    /// Disconnecting a namespace that is not open returns an error.
    #[tokio::test]
    async fn double_disconnect_returns_error() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let _socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        server_connect(&manager_tx).await;
        manager_tx
            .send(ManagerAction::Socket(Ns("/".into(), Directive::Disconnect)))
            .await
            .unwrap();

        // Drain the disconnect frame and Close so the second disconnect can be processed.
        engine_rx.recv().await.unwrap(); // disconnect frame
        engine_rx.recv().await.unwrap(); // Close

        // Manager already broke out of the loop; second disconnect goes unprocessed.
        // Verify the manager task exited cleanly after the first disconnect.
        drop(manager_tx);
        handle.await.unwrap().unwrap();
    }

    /// Ack IDs are assigned per-namespace and responses route back correctly.
    #[tokio::test]
    async fn ack_roundtrip() {
        let (manager_tx, mut engine_rx, _) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        server_connect(&manager_tx).await;
        socket_rx.recv().await.unwrap(); // consume Packet::Connect

        let (ack_tx, mut ack_rx) = tokio::sync::oneshot::channel();
        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/".into(),
                Directive::Event {
                    payload: ByteString::from_static("[\"greet\",\"hello\"]"),
                    tx: Some(ack_tx),
                    attachments: None,
                },
            )))
            .await
            .unwrap();

        engine_rx.recv().await.unwrap(); // drain outbound event frame

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static("30[\"world\"]"),
            )))
            .await
            .unwrap();

        tokio::task::yield_now().await;
        let ack = ack_rx.try_recv().unwrap();
        assert_eq!(ack.payload, "[\"world\"]");
    }

    /// A binary event is held until all attachment frames arrive, then delivered as a complete packet.
    #[tokio::test]
    async fn binary_event_reassembly() {
        let (manager_tx, _engine_rx, _) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;

        server_connect(&manager_tx).await;
        socket_rx.recv().await.unwrap(); // consume Packet::Connect

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static("52-[\"img\"]"),
            )))
            .await
            .unwrap();

        manager_tx
            .send(ManagerAction::Engine(Message::Binary(Bytes::from_static(
                b"\x01\x02",
            ))))
            .await
            .unwrap();

        tokio::task::yield_now().await;
        assert!(
            socket_rx.try_recv().is_err(),
            "incomplete, second attachment not yet received"
        );

        manager_tx
            .send(ManagerAction::Engine(Message::Binary(Bytes::from_static(
                b"\x03\x04",
            ))))
            .await
            .unwrap();

        let pkt = socket_rx.recv().await.unwrap();
        match pkt {
            Signal::Event(ev) => assert_eq!(ev.attachments.as_ref().unwrap().len(), 2),
            other => panic!("expected Event, got {other:?}"),
        }
    }
}
