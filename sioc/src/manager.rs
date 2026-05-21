//! Socket.IO namespace router.

use crate::error::{ManagerError, PacketError};
use crate::packet::{Connect, ConnectError, Directive, DynAck, DynEvent, Ns, Packet, Signal};
use bytes::Bytes;
use bytestring::ByteString;
use eioc::engine::MessageSender;
use eioc::prelude::Message;
use futures_util::{Sink, SinkExt, future};
use std::collections::BTreeMap;
use std::collections::hash_map::{Entry, HashMap};
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{PollSendError, PollSender};

/// Creates a [`Sink<Message>`] that maps each [`Message`] to a [`ManagerAction`] and sends it.
pub(crate) fn message_sink(
    tx: mpsc::Sender<ManagerAction>,
) -> impl Sink<Message, Error = PollSendError<ManagerAction>> {
    PollSender::new(tx).with(|message: Message| future::ok(message.into()))
}

/// Internal dispatch action sent between the socket layer and the engine task.
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
pub(crate) struct DirectiveSender(mpsc::Sender<ManagerAction>);

impl DirectiveSender {
    /// Wraps a manager channel sender.
    pub fn new(tx: mpsc::Sender<ManagerAction>) -> Self {
        Self(tx)
    }

    /// Sends `directive` to the manager asynchronously.
    ///
    /// # Errors
    ///
    /// Returns an error if the manager channel is closed.
    pub async fn send(
        &self,
        ns: ByteString,
        directive: Directive,
    ) -> Result<(), SendError<ManagerAction>> {
        self.0.send(Ns(ns, directive).into()).await
    }

    /// Attempts to send `directive` without blocking.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is closed or full.
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

    fn take(&mut self) -> HashMap<ByteString, Socket> {
        std::mem::take(&mut self.0)
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
    /// Creates a manager that reads from `rx`.
    #[must_use]
    pub fn new(rx: mpsc::Receiver<ManagerAction>) -> Self {
        Self {
            rx,
            sockets: SocketsMap::new(),
            reconstructor: Reconstructor::new(),
        }
    }

    /// Runs the socket routing loop until all namespaces disconnect.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine or a namespace channel fails.
    #[tracing::instrument(skip_all, err)]
    pub async fn socket_io(mut self, tx: MessageSender) -> Result<(), ManagerError> {
        let result = self.run(&tx).await;

        if !self.sockets.is_empty() {
            for (ns, _socket) in self.sockets.take() {
                tracing::warn!(%ns, "namespace still connected after closing manager");

                tx.send(Message::Text(Packet::Disconnect.encode(&ns).into()))
                    .await?;
            }
            tx.send(Message::Close).await?;
        }

        result
    }

    async fn run(&mut self, tx: &MessageSender) -> Result<(), ManagerError> {
        while let Some(directive) = self.rx.recv().await {
            match directive {
                ManagerAction::Socket(Ns(ns, packet)) => {
                    self.dispatch_directive(tx, ns, packet).await?;
                }
                ManagerAction::Engine(message) => {
                    self.route_message(tx, message).await?;
                }
            }
            if self.sockets.is_empty() {
                tx.send(Message::Close).await?;
                break;
            }
        }
        Ok(())
    }

    /// Encodes and sends (or buffers) one outbound directive.
    async fn dispatch_directive(
        &mut self,
        message_tx: &MessageSender,
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
            Directive::Dropped => match self.sockets.disconnect(ns) {
                Ok(Ns(ns, _)) => {
                    tracing::warn!(%ns, "dropped while connected");
                    (ns, Packet::Disconnect, None)
                }
                Err(_) => return Ok(()),
            },
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

        if let Some(buffer) = socket_buffer {
            buffer.extend(messages);

            tracing::trace!(%ns, %packet, buffer.len = buffer.len(), "buffering messages");
        } else {
            tracing::trace!(%ns, %packet, "-> packet");
            for message in messages {
                message_tx.send(message).await?;
            }
        }

        Ok(())
    }

    async fn route_message(
        &mut self,
        message_tx: &MessageSender,
        message: Message,
    ) -> Result<(), ManagerError> {
        match message {
            Message::Text(text) => {
                self.route_text_message(text, message_tx).await?;
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
        message_tx: &MessageSender,
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
                        message_tx.send(message).await?;
                    }
                }

                let connect: Connect = serde_json::from_str(&payload).map_err(PacketError::Json)?;

                tracing::debug!(%ns, sid = %connect.sid, "connected");

                socket.send_packet(ns, Signal::Connect(connect)).await?;
            }
            Packet::Disconnect => {
                let Ns(ns, mut socket) = self.sockets.disconnect(ns)?;

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
        }

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
    use eioc::engine::EngineAction;
    use tokio::task::JoinHandle;

    const CONNECT_RESPONSE: &str = "0{\"sid\":\"test\"}";

    /// Spawns a manager and returns `(manager_tx, engine_rx, join_handle)`.
    fn setup_manager() -> (
        mpsc::Sender<ManagerAction>,
        mpsc::Receiver<EngineAction>,
        JoinHandle<Result<(), ManagerError>>,
    ) {
        let (engine_tx, engine_rx) = mpsc::channel(32);

        let (manager_tx, manager_rx) = mpsc::channel(32);

        let handle = tokio::spawn(Manager::new(manager_rx).socket_io(MessageSender(engine_tx)));

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
            let payload = format!("[\"{c}\"]");

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

        let expected = ["2[\"a\"]", "2[\"b\"]", "2[\"c\"]"];
        for exp in &expected {
            match engine_rx.recv().await.unwrap() {
                EngineAction::Sink(Message::Text(text)) => assert_eq!(&*text, *exp),
                other => panic!("expected Text, got {other:?}"),
            }
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
        let _other_rx = open_namespace(&manager_tx, "/other").await;
        engine_rx.recv().await.unwrap(); // drain `/` connect frame
        engine_rx.recv().await.unwrap(); // drain `/other` connect frame

        // First disconnect succeeds; manager stays alive because `/other` is still open.
        manager_tx
            .send(ManagerAction::Socket(Ns("/".into(), Directive::Disconnect)))
            .await
            .unwrap();
        engine_rx.recv().await.unwrap(); // drain disconnect frame

        // Second disconnect on the same namespace must return UnknownNamespace.
        manager_tx
            .send(ManagerAction::Socket(Ns("/".into(), Directive::Disconnect)))
            .await
            .unwrap();

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::ManagerError::UnknownNamespace { .. })
        ));
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

    /// Server sends `Disconnect`; the socket receives `Signal::Disconnect`.
    #[tokio::test]
    async fn server_disconnect_delivery() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();
        server_connect(&manager_tx).await;
        socket_rx.recv().await.unwrap(); // consume Connect signal

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static("1"),
            )))
            .await
            .unwrap();

        assert!(matches!(
            socket_rx.recv().await.unwrap(),
            Signal::Disconnect
        ));
        assert!(matches!(
            engine_rx.recv().await.unwrap(),
            EngineAction::Sink(Message::Close)
        ));
        handle.await.unwrap().unwrap();
    }

    /// Server sends `ConnectError`; the socket receives `Signal::ConnectError`.
    #[tokio::test]
    async fn server_connect_error_delivery() {
        let (manager_tx, mut engine_rx, _) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static(r#"4{"message":"forbidden"}"#),
            )))
            .await
            .unwrap();

        assert!(matches!(
            socket_rx.recv().await.unwrap(),
            Signal::ConnectError(_)
        ));
    }

    /// Server sends `BinaryAck` followed by binary frames; assembled ack is delivered.
    #[tokio::test]
    async fn binary_ack_reassembly() {
        let (manager_tx, mut engine_rx, _) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();
        server_connect(&manager_tx).await;
        socket_rx.recv().await.unwrap();

        let (ack_tx, mut ack_rx) = oneshot::channel();
        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/".into(),
                Directive::Event {
                    payload: ByteString::from_static(r#"["greet"]"#),
                    tx: Some(ack_tx),
                    attachments: None,
                },
            )))
            .await
            .unwrap();
        engine_rx.recv().await.unwrap();

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static(r#"61-0["ok"]"#),
            )))
            .await
            .unwrap();
        manager_tx
            .send(ManagerAction::Engine(Message::Binary(Bytes::from_static(
                b"\xAB\xCD",
            ))))
            .await
            .unwrap();

        tokio::task::yield_now().await;
        let ack = ack_rx.try_recv().unwrap();
        assert_eq!(&*ack.payload, r#"["ok"]"#);
        let att = ack.attachments.as_ref().unwrap();
        assert_eq!(att.len(), 1);
        assert_eq!(att[0], Bytes::from_static(b"\xAB\xCD"));
    }

    /// Client sends a binary Event; the manager encodes `BinaryEvent` + binary frames.
    #[tokio::test]
    async fn directive_event_binary() {
        let (manager_tx, mut engine_rx, _) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();
        server_connect(&manager_tx).await;
        socket_rx.recv().await.unwrap();

        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/".into(),
                Directive::Event {
                    payload: ByteString::from_static(r#"["img"]"#),
                    tx: None,
                    attachments: Some(vec![Bytes::from_static(b"\x01\x02")]),
                },
            )))
            .await
            .unwrap();

        match engine_rx.recv().await.unwrap() {
            EngineAction::Sink(Message::Text(text)) => {
                assert_eq!(&*text, r#"51-["img"]"#);
            }
            other => panic!("expected Text, got {other:?}"),
        }
        match engine_rx.recv().await.unwrap() {
            EngineAction::Sink(Message::Binary(bytes)) => {
                assert_eq!(bytes, Bytes::from_static(b"\x01\x02"));
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    /// Client sends a plain Ack; the manager encodes and forwards it.
    #[tokio::test]
    async fn directive_ack_no_binary() {
        let (manager_tx, mut engine_rx, _) = setup_manager();
        open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/".into(),
                Directive::Ack {
                    payload: ByteString::from_static("[true]"),
                    id: 42,
                    attachments: None,
                },
            )))
            .await
            .unwrap();

        match engine_rx.recv().await.unwrap() {
            EngineAction::Sink(Message::Text(text)) => {
                assert_eq!(&*text, "342[true]");
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    /// Client sends a binary Ack; the manager encodes `BinaryAck` + binary frames.
    #[tokio::test]
    async fn directive_ack_binary() {
        let (manager_tx, mut engine_rx, _) = setup_manager();
        open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/".into(),
                Directive::Ack {
                    payload: ByteString::from_static("[true]"),
                    id: 7,
                    attachments: Some(vec![Bytes::from_static(b"\xCA\xFE")]),
                },
            )))
            .await
            .unwrap();

        match engine_rx.recv().await.unwrap() {
            EngineAction::Sink(Message::Text(text)) => {
                assert_eq!(&*text, "61-7[true]");
            }
            other => panic!("expected Text, got {other:?}"),
        }
        match engine_rx.recv().await.unwrap() {
            EngineAction::Sink(Message::Binary(bytes)) => {
                assert_eq!(bytes, Bytes::from_static(b"\xCA\xFE"));
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    /// `Directive::Dropped` for a live namespace logs a warning and sends Disconnect.
    #[tokio::test]
    async fn directive_dropped_when_connected() {
        let (manager_tx, mut engine_rx, _) = setup_manager();
        let _socket_rx = open_namespace(&manager_tx, "/").await;
        let _other_rx = open_namespace(&manager_tx, "/other").await;
        engine_rx.recv().await.unwrap();
        engine_rx.recv().await.unwrap();

        manager_tx
            .send(ManagerAction::Socket(Ns("/".into(), Directive::Dropped)))
            .await
            .unwrap();

        match engine_rx.recv().await.unwrap() {
            EngineAction::Sink(Message::Text(text)) => {
                assert_eq!(&*text, "1");
            }
            other => panic!("expected Disconnect text, got {other:?}"),
        }
    }

    /// `Directive::Dropped` for an unknown namespace is a silent no-op.
    #[tokio::test]
    async fn directive_dropped_when_not_connected() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let _socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/unknown".into(),
                Directive::Dropped,
            )))
            .await
            .unwrap();

        tokio::task::yield_now().await;
        assert!(
            engine_rx.try_recv().is_err(),
            "Dropped for unknown ns must not produce engine messages"
        );
        drop(manager_tx);
        let _ = handle.await;
    }

    /// An unexpected binary frame with no pending reassembly is an error.
    #[tokio::test]
    async fn unexpected_binary_frame_is_error() {
        let (manager_tx, _engine_rx, handle) = setup_manager();
        let _socket_rx = open_namespace(&manager_tx, "/").await;

        manager_tx
            .send(ManagerAction::Engine(Message::Binary(Bytes::from_static(
                b"\x01",
            ))))
            .await
            .unwrap();

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::ManagerError::UnexpectedBinary(_))
        ));
    }

    /// A text frame arriving while binary reassembly is pending is an error.
    #[tokio::test]
    async fn unexpected_text_during_binary_is_error() {
        let (manager_tx, _engine_rx, handle) = setup_manager();
        let _socket_rx = open_namespace(&manager_tx, "/").await;

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static(r#"52-["img"]"#),
            )))
            .await
            .unwrap();

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static(r#"2["ping"]"#),
            )))
            .await
            .unwrap();

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::ManagerError::UnexpectedText(_))
        ));
    }

    /// Ack arrives after the caller has dropped its receiver.
    #[tokio::test]
    async fn ack_receiver_dropped_returns_error() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();
        server_connect(&manager_tx).await;
        socket_rx.recv().await.unwrap();

        let (ack_tx, ack_rx) = oneshot::channel();
        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/".into(),
                Directive::Event {
                    payload: ByteString::from_static(r#"["greet"]"#),
                    tx: Some(ack_tx),
                    attachments: None,
                },
            )))
            .await
            .unwrap();
        engine_rx.recv().await.unwrap();
        drop(ack_rx);

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static(r#"30["ok"]"#),
            )))
            .await
            .unwrap();

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::ManagerError::SendAck { .. })
        ));
    }

    /// `Message::Close` clears all sockets and the manager exits cleanly.
    #[tokio::test]
    async fn server_close_message_clears_sockets() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let _socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();

        manager_tx
            .send(ManagerAction::Engine(Message::Close))
            .await
            .unwrap();

        assert!(matches!(
            engine_rx.recv().await.unwrap(),
            EngineAction::Sink(Message::Close)
        ));
        drop(manager_tx);
        handle.await.unwrap().unwrap();
    }

    /// Manager cleans up still-connected namespaces when the channel is dropped.
    #[tokio::test]
    async fn socket_io_cleanup_on_channel_close() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let _socket_rx = open_namespace(&manager_tx, "/other").await;
        engine_rx.recv().await.unwrap();

        drop(manager_tx);

        match engine_rx.recv().await.unwrap() {
            EngineAction::Sink(Message::Text(text)) => {
                assert_eq!(&*text, "1/other,");
            }
            other => panic!("expected Disconnect text, got {other:?}"),
        }
        assert!(matches!(
            engine_rx.recv().await.unwrap(),
            EngineAction::Sink(Message::Close)
        ));
        handle.await.unwrap().unwrap();
    }

    /// Sending to a closed socket channel returns `SendSocket`.
    #[tokio::test]
    async fn send_packet_closed_channel_is_error() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();
        drop(socket_rx);

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static(r#"2["ping"]"#),
            )))
            .await
            .unwrap();

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::ManagerError::SendSocket { .. })
        ));
    }

    #[test]
    fn manager_action_from_directive() {
        let a = ManagerAction::from(Ns("/".into(), Directive::Disconnect));
        assert!(matches!(a, ManagerAction::Socket(_)));
    }

    #[test]
    fn manager_action_from_message() {
        let a = ManagerAction::from(Message::Close);
        assert!(matches!(a, ManagerAction::Engine(Message::Close)));
    }

    #[tokio::test]
    async fn directive_sender_send() {
        let (tx, mut rx) = mpsc::channel(1);
        let sender = DirectiveSender::new(tx);
        sender
            .send("/".into(), Directive::Disconnect)
            .await
            .unwrap();
        assert!(rx.recv().await.is_some());
    }

    #[test]
    fn directive_sender_try_send() {
        let (tx, mut rx) = mpsc::channel(1);
        let sender = DirectiveSender::new(tx);
        sender.try_send("/".into(), Directive::Disconnect).unwrap();
        rx.try_recv().unwrap();
    }

    #[tokio::test]
    async fn message_sink_wraps_messages() {
        use futures_util::SinkExt;
        let (tx, mut rx) = mpsc::channel(1);
        let mut sink = message_sink(tx);
        sink.send(Message::Close).await.unwrap();
        assert!(matches!(
            rx.recv().await.unwrap(),
            ManagerAction::Engine(Message::Close)
        ));
    }

    #[tokio::test]
    async fn unknown_ack_id_returns_error() {
        let (manager_tx, mut engine_rx, handle) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;
        engine_rx.recv().await.unwrap();
        server_connect(&manager_tx).await;
        socket_rx.recv().await.unwrap(); // consume Connect signal

        manager_tx
            .send(ManagerAction::Engine(Message::Text(
                ByteString::from_static("399[]"),
            )))
            .await
            .unwrap();

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::ManagerError::UnknownAckId { .. })
        ));
    }

    #[tokio::test]
    async fn ack_directive_to_unknown_namespace_returns_error() {
        let (manager_tx, _engine_rx, handle) = setup_manager();

        manager_tx
            .send(ManagerAction::Socket(Ns(
                "/no-such-ns".into(),
                Directive::Ack {
                    payload: ByteString::from_static("[]"),
                    id: 0,
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
            Signal::Event(ev) => {
                let attachments = ev.attachments.as_ref().unwrap();
                assert_eq!(attachments.len(), 2);
                assert_eq!(attachments[0], Bytes::from_static(b"\x01\x02"));
                assert_eq!(attachments[1], Bytes::from_static(b"\x03\x04"));
            }
            other => panic!("expected Event, got {other:?}"),
        }
    }
}
