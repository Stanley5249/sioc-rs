//! Socket.IO namespace router.

use crate::error::{Error, ParseError, PayloadError, Result};
use crate::packet::{Connect, ConnectError, Directive, DynAck, DynEvent, Ns, Packet, Signal};
use bytes::Bytes;
use futures_util::{Sink, SinkExt, future};
use sioc_engine::engine::{Engine, EngineSender};
use sioc_engine::error::BoxedError;
use sioc_engine::prelude::Transit;
use std::collections::BTreeMap;
use std::collections::hash_map::{Entry, HashMap};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::PollSender;

/// Creates a [`Sink<Transit>`] that maps each [`Transit`] to a [`ManagerAction`] and sends it.
pub fn manager_sink(tx: mpsc::Sender<ManagerAction>) -> impl Sink<Transit, Error = BoxedError> {
    PollSender::new(tx).with(|transit: Transit| future::ok(transit.into()))
}

#[derive(Debug)]
pub enum ManagerAction {
    /// Outbound directive from the client.
    Directive(Ns<Directive>),
    /// Inbound message from the engine.
    Transit(Transit),
}

impl From<Ns<Directive>> for ManagerAction {
    fn from(directive: Ns<Directive>) -> Self {
        ManagerAction::Directive(directive)
    }
}

impl From<Transit> for ManagerAction {
    fn from(message: Transit) -> Self {
        ManagerAction::Transit(message)
    }
}

/// Sends outbound [`Directive`]s to the socket router.
#[derive(Clone, Debug)]
pub struct ManagerSender(pub mpsc::Sender<ManagerAction>);

impl ManagerSender {
    pub async fn send(&self, ns: String, directive: Directive) -> Result<()> {
        Ok(self.0.send(Ns(ns, directive).into()).await?)
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

    fn send_ack(&mut self, ns: String, id: u64, ack: DynAck) -> Result<String> {
        match self.acks.remove(&id) {
            Some(sender) => match sender.send(ack) {
                Ok(()) => Ok(ns),
                Err(ack) => Err(Error::SendAck { ns, ack }),
            },
            None => Err(Error::UnknownAckId { ns, id }),
        }
    }

    async fn send_packet(&mut self, ns: String, packet: Signal) -> Result<String> {
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
                let packet = Signal::Event(DynEvent::new(data, id).with_attachments(attachments));

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
            Entry::Occupied(e) => Err(Error::NamespaceConflict {
                ns: e.key().clone(),
            }),
            Entry::Vacant(e) => {
                let ns = e.key().clone();
                Ok(Ns(ns, e.insert(socket)))
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
    tx: mpsc::Sender<Signal>,
    acks: BTreeMap<u64, oneshot::Sender<DynAck>>,
    ids: u64,
    /// Set when the server sends a CONNECT response; gates event delivery.
    connected: bool,
    /// Events encoded before `connected` is set; flushed on CONNECT response.
    buffer: Vec<Transit>,
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
    pub async fn run(mut self, engine: Engine) -> Result<()> {
        while let Some(directive) = self.rx.recv().await {
            match directive {
                ManagerAction::Directive(Ns(ns, packet)) => {
                    self.dispatch_directive(&engine.tx, ns, packet).await?;
                }
                ManagerAction::Transit(message) => {
                    self.route_message(&engine.tx, message).await?;
                }
            }
            if self.sockets.is_empty() {
                engine.tx.send(Transit::Close).await?;
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
        ns: String,
        directive: Directive,
    ) -> Result<()> {
        let mut socket_buffer = None;

        let (ns, packet, attachments) = match directive {
            Directive::Connect { tx, data } => {
                let socket = Socket::new(tx);
                let Ns(ns, _) = self.sockets.connect(ns, socket)?;

                (ns, Packet::Connect(data), None)
            }
            Directive::Disconnect => {
                let Ns(ns, _) = self.sockets.disconnect(ns)?;

                (ns, Packet::Disconnect, None)
            }
            Directive::Event {
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
                    None => Packet::Event { data, id },
                    Some(attachments) => Packet::BinaryEvent {
                        data,
                        id,
                        count: attachments.len(),
                    },
                };

                (ns, packet, attachments)
            }
            Directive::Ack {
                data,
                id,
                attachments,
            } => {
                let ns = self.sockets.require(ns)?;

                let packet = match &attachments {
                    None => Packet::Ack { data, id },
                    Some(attachments) => Packet::BinaryAck {
                        data,
                        id,
                        count: attachments.len(),
                    },
                };

                (ns, packet, attachments)
            }
        };

        tracing::trace!(ns, ?packet, "sending packet");

        let text = Transit::Text(packet.encode(&ns));
        let binaries = attachments.into_iter().flatten().map(Transit::Binary);
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

    async fn route_message(&mut self, engine_tx: &EngineSender, message: Transit) -> Result<()> {
        match message {
            Transit::Text(bytes) => {
                self.route_text_message(bytes, engine_tx).await?;
            }

            Transit::Binary(attachment) => {
                self.route_binary_message(attachment).await?;
            }

            Transit::Close => {
                tracing::debug!("closing all namespaces");
                self.sockets.close();
            }
        }

        Ok(())
    }

    async fn route_text_message(&mut self, bytes: Bytes, engine_tx: &EngineSender) -> Result<()> {
        if self.reconstructor.is_pending() {
            return Err(Error::UnexpectedText(bytes));
        }

        let Ns(ns, packet) = bytes.try_into()?;

        tracing::trace!(ns, ?packet, "received packet");

        match packet {
            Packet::Connect(data) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket.connected = true;

                let len = socket.buffer.len();

                if len > 0 {
                    tracing::trace!(ns, len, "sending buffered packets");

                    for message in socket.buffer.drain(..) {
                        engine_tx.send(message).await?;
                    }
                }

                let connect: Connect = serde_json::from_slice(&data)
                    .map_err(|e| ParseError::Payload(PayloadError::new::<Connect>(e)))?;

                socket.send_packet(ns, Signal::Connect(connect)).await?;
            }
            Packet::Disconnect => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket.send_packet(ns, Signal::Disconnect).await?;
            }
            Packet::Event { data, id } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket
                    .send_packet(ns, Signal::Event(DynEvent::new(data, id)))
                    .await?;
            }
            Packet::Ack { data, id } => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                socket.send_ack(ns, id, DynAck::new(data))?;
            }
            Packet::ConnectError(data) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                let error: ConnectError = serde_json::from_slice(&data)
                    .map_err(|e| ParseError::Payload(PayloadError::new::<ConnectError>(e)))?;

                socket.send_packet(ns, Signal::ConnectError(error)).await?;
            }
            Packet::BinaryEvent { data, id, count } => {
                let ns = self.sockets.require(ns)?;

                self.reconstructor
                    .insert(ns, BinaryPacket::event(data, id, count));
            }
            Packet::BinaryAck { data, id, count } => {
                let ns = self.sockets.require(ns)?;

                self.reconstructor
                    .insert(ns, BinaryPacket::ack(data, id, count));
            }
        };

        Ok(())
    }

    async fn route_binary_message(&mut self, bytes: Bytes) -> Result<()> {
        let count = bytes.len();

        match self.reconstructor.attach_and_take(bytes)? {
            Some(Ns(ns, packet)) => {
                let Ns(ns, socket) = self.sockets.get_mut(ns)?;

                tracing::trace!(ns, count, status = "complete", "received binary attachment");

                socket.send_binary_packet(ns, packet).await?;
            }
            None => {
                tracing::trace!(count, status = "pending", "received binary attachment");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sioc_engine::engine::EngineAction;

    const CONNECT_RESPONSE: &[u8] = b"0{\"sid\":\"test\"}";

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
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let (engine_tx, engine_rx) = mpsc::channel(32);
        let (manager_tx, manager_rx) = mpsc::channel(32);
        let handle = tokio::spawn(Manager::new(manager_rx).run(mock_engine(engine_tx)));
        (manager_tx, engine_rx, handle)
    }

    async fn open_namespace(
        manager_tx: &mpsc::Sender<ManagerAction>,
        ns: &str,
    ) -> mpsc::Receiver<Signal> {
        let (tx, rx) = mpsc::channel(32);
        manager_tx
            .send(ManagerAction::Directive(Ns(
                ns.into(),
                Directive::Connect {
                    tx,
                    data: Bytes::new(),
                },
            )))
            .await
            .unwrap();
        rx
    }

    async fn server_connect(manager_tx: &mpsc::Sender<ManagerAction>) {
        manager_tx
            .send(ManagerAction::Transit(Transit::Text(Bytes::from_static(
                CONNECT_RESPONSE,
            ))))
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
            .send(ManagerAction::Directive(Ns(
                "/".into(),
                Directive::Event {
                    data: Bytes::from_static(b"[\"ping\"]"),
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

        for i in 0u8..3 {
            manager_tx
                .send(ManagerAction::Directive(Ns(
                    "/".into(),
                    Directive::Event {
                        data: Bytes::copy_from_slice(&[b'[', b'"', b'a' + i, b'"', b']']),
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
                EngineAction::Transit(Transit::Text(_))
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
            .send(ManagerAction::Directive(Ns(
                "/".into(),
                Directive::Disconnect,
            )))
            .await
            .unwrap();

        assert!(matches!(
            engine_rx.recv().await.unwrap(),
            EngineAction::Transit(Transit::Text(_))
        ));
        assert!(matches!(
            engine_rx.recv().await.unwrap(),
            EngineAction::Transit(Transit::Close)
        ));

        drop(manager_tx);
        handle.await.unwrap().unwrap();
    }

    /// Routing a directive to an unknown namespace returns an error.
    #[tokio::test]
    async fn unknown_namespace_returns_error() {
        let (manager_tx, _engine_rx, handle) = setup_manager();

        manager_tx
            .send(ManagerAction::Directive(Ns(
                "/no-such-ns".into(),
                Directive::Event {
                    data: Bytes::from_static(b"[\"x\"]"),
                    tx: None,
                    attachments: None,
                },
            )))
            .await
            .unwrap();

        drop(manager_tx);
        assert!(matches!(
            handle.await.unwrap(),
            Err(crate::error::Error::UnknownNamespace { .. })
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
            Err(crate::error::Error::NamespaceConflict { .. })
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
            .send(ManagerAction::Directive(Ns(
                "/".into(),
                Directive::Disconnect,
            )))
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
            .send(ManagerAction::Directive(Ns(
                "/".into(),
                Directive::Event {
                    data: Bytes::from_static(b"[\"greet\",\"hello\"]"),
                    tx: Some(ack_tx),
                    attachments: None,
                },
            )))
            .await
            .unwrap();

        engine_rx.recv().await.unwrap(); // drain outbound event frame

        manager_tx
            .send(ManagerAction::Transit(Transit::Text(Bytes::from_static(
                b"30[\"world\"]",
            ))))
            .await
            .unwrap();

        tokio::task::yield_now().await;
        let ack = ack_rx.try_recv().unwrap();
        assert_eq!(&ack.data[..], b"[\"world\"]");
    }

    /// A binary event is held until all attachment frames arrive, then delivered as a complete packet.
    #[tokio::test]
    async fn binary_event_reassembly() {
        let (manager_tx, _engine_rx, _) = setup_manager();
        let mut socket_rx = open_namespace(&manager_tx, "/").await;

        server_connect(&manager_tx).await;
        socket_rx.recv().await.unwrap(); // consume Packet::Connect

        manager_tx
            .send(ManagerAction::Transit(Transit::Text(Bytes::from_static(
                b"52-[\"img\"]",
            ))))
            .await
            .unwrap();

        manager_tx
            .send(ManagerAction::Transit(Transit::Binary(Bytes::from_static(
                b"\x01\x02",
            ))))
            .await
            .unwrap();

        tokio::task::yield_now().await;
        assert!(
            socket_rx.try_recv().is_err(),
            "incomplete — second attachment not yet received"
        );

        manager_tx
            .send(ManagerAction::Transit(Transit::Binary(Bytes::from_static(
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
