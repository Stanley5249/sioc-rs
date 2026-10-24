//! A fake engine and a spawned manager that the actor tests drive.

use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{Handshake, Message, ServerMessage};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::client::{SocketReceiver, SocketSender};
use crate::config::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::packet::{ClientPacket, DynAck, ServerPacket};

pub const CONNECT_RESPONSE: &str = "0{\"sid\":\"test\"}";

/// The engine end of one session, which a test plays.
pub struct TestSession {
    pub server_message_tx: mpsc::Sender<ServerMessage>,
    pub client_message_rx: mpsc::Receiver<Message>,
    /// Finishes the engine; dropping it finishes the engine without an error.
    pub result_tx: oneshot::Sender<Result<(), eioc::error::Error>>,
}

impl TestSession {
    /// Ends the session as the engine does: closes the server direction,
    /// takes client messages until the manager hangs up, then finishes.
    ///
    /// Returns the client messages that were still queued.
    pub async fn end(self) -> Vec<Message> {
        let Self {
            server_message_tx,
            mut client_message_rx,
            result_tx,
        } = self;
        drop(server_message_tx);
        let mut messages = Vec::new();
        while let Some(message) = client_message_rx.recv().await {
            messages.push(message);
        }
        result_tx.send(Ok(())).unwrap();
        messages
    }
}

/// A running manager with the client handle and the engine end of its open
/// session.
pub struct TestManager {
    pub connect_request_tx: mpsc::Sender<ConnectRequest>,
    pub session: Option<TestSession>,
    /// Receives each session the manager opens after the first.
    pub session_rx: mpsc::Receiver<TestSession>,
    pub task: JoinHandle<Result<(), ManagerError>>,
}

/// Asserts that nothing reaches the engine for a short while.
pub async fn assert_quiet(client_message_rx: &mut mpsc::Receiver<Message>) {
    let result = tokio::time::timeout(Duration::from_millis(50), client_message_rx.recv()).await;
    assert!(result.is_err(), "unexpected message {result:?}");
}

pub fn event(payload: &'static str, ack_tx: Option<oneshot::Sender<DynAck>>) -> ClientPacket {
    ClientPacket::Event {
        payload: ByteString::from_static(payload),
        ack_tx,
        attachments: None,
    }
}

impl TestManager {
    /// Spawns a manager that stops instead of reconnecting, and takes its first
    /// session.
    pub async fn spawn() -> Self {
        Self::spawn_with(None).await
    }

    /// Spawns a manager that reconnects at once, and takes its first session.
    pub async fn spawn_reconnecting() -> Self {
        Self::spawn_with(Some(Backoff::new(
            Duration::ZERO,
            Duration::ZERO,
            0.0,
            None,
        )))
        .await
    }

    pub async fn spawn_with(backoff: Option<Backoff>) -> Self {
        Self::spawn_with_channels(backoff, ChannelConfig::from(32)).await
    }

    pub async fn spawn_with_channels(backoff: Option<Backoff>, channels: ChannelConfig) -> Self {
        let (connect_request_tx, connect_request_rx) = mpsc::channel(32);
        let (session_tx, mut session_rx) = mpsc::channel(1);
        let connect_engine = move |server_message_tx, client_message_rx| {
            let session_tx = session_tx.clone();
            async move {
                let (result_tx, result_rx) = oneshot::channel();
                let session = TestSession {
                    server_message_tx,
                    client_message_rx,
                    result_tx,
                };
                // A finished test takes no more sessions.
                if session_tx.send(session).await.is_err() {
                    return Ok(());
                }
                result_rx.await.unwrap_or(Ok(()))
            }
        };
        let task = tokio::spawn(crate::manager::actor::run(
            connect_request_rx,
            connect_engine,
            channels,
            backoff,
        ));
        let session = session_rx.recv().await;
        Self {
            connect_request_tx,
            session,
            session_rx,
            task,
        }
    }

    /// Returns the engine end of the open session.
    pub fn session(&mut self) -> &mut TestSession {
        self.session.as_mut().expect("a session is open")
    }

    /// Ends the open session from the engine side and takes the next one.
    ///
    /// Returns the client messages that were still queued for the old session.
    pub async fn reconnect(&mut self) -> Vec<Message> {
        let messages = self.session.take().expect("a session is open").end().await;
        self.session = Some(
            self.session_rx
                .recv()
                .await
                .expect("the manager reconnects"),
        );
        messages
    }

    /// Opens a namespace as `Client::connect` does and consumes its CONNECT
    /// packet.
    pub async fn open(&mut self, ns: &str) -> (SocketSender, SocketReceiver) {
        self.open_with(ns, ByteString::new(), 32).await
    }

    pub async fn open_with(
        &mut self,
        ns: &str,
        auth: ByteString,
        server_packet_capacity: usize,
    ) -> (SocketSender, SocketReceiver) {
        let (connect_request, handles) =
            ConnectRequest::new(ns.into(), auth, 32, server_packet_capacity);
        self.connect_request_tx.send(connect_request).await.unwrap();
        assert!(self.recv_client_text().await.starts_with('0'));
        let (tx, rx, reply_rx) = handles.into_socket();
        reply_rx.await.unwrap().unwrap();
        (tx, rx)
    }

    /// Reports a successful handshake, as the engine does first.
    pub async fn send_open(&mut self) {
        let handshake = Handshake {
            sid: "engine".into(),
            upgrades: Vec::new(),
            ping_interval: 25_000,
            ping_timeout: 20_000,
            max_payload: 1_000_000,
        };
        self.session()
            .server_message_tx
            .send(ServerMessage::Open(handshake))
            .await
            .unwrap();
    }

    pub async fn send_server_message(&mut self, text: &'static str) {
        self.session()
            .server_message_tx
            .send(ServerMessage::Text(ByteString::from_static(text)))
            .await
            .unwrap();
    }

    pub async fn send_server_binary(&mut self, bytes: &'static [u8]) {
        self.session()
            .server_message_tx
            .send(ServerMessage::Binary(Bytes::from_static(bytes)))
            .await
            .unwrap();
    }

    pub async fn recv_client_text(&mut self) -> ByteString {
        match self.session().client_message_rx.recv().await {
            Some(Message::Text(text)) => text,
            other => panic!("expected text, got {other:?}"),
        }
    }

    pub async fn recv_client_binary(&mut self) -> Bytes {
        match self.session().client_message_rx.recv().await {
            Some(Message::Binary(bytes)) => bytes,
            other => panic!("expected binary, got {other:?}"),
        }
    }

    /// Drops the client handle, ends the open session from the engine side,
    /// and returns the manager's result.
    ///
    /// A manager that stops instead of reconnecting ends with the session; one
    /// that reconnects ends once its namespaces are gone.
    pub async fn finish(self) -> Result<(), ManagerError> {
        let Self {
            connect_request_tx,
            session,
            session_rx: _session_rx,
            task,
        } = self;
        drop(connect_request_tx);
        if let Some(session) = session {
            session.end().await;
        }
        task.await.unwrap()
    }
}

/// Sends a connect request as `Client::connect` does, without reading its
/// CONNECT packet, and returns the handles once the manager accepted it.
pub async fn request_namespace(
    connect_request_tx: &mpsc::Sender<ConnectRequest>,
    ns: &str,
) -> (SocketSender, SocketReceiver) {
    let (connect_request, handles) = ConnectRequest::new(ns.into(), ByteString::new(), 32, 32);
    connect_request_tx.send(connect_request).await.unwrap();
    let (tx, rx, reply_rx) = handles.into_socket();
    reply_rx.await.unwrap().unwrap();
    (tx, rx)
}

pub fn ack(id: u64) -> ClientPacket {
    ClientPacket::Ack {
        payload: ByteString::from_static("[true]"),
        id,
        attachments: None,
    }
}

/// Breaks the protocol in an open session, then checks that the manager closes
/// the session, keeps accepting server messages until the engine hangs up, and
/// reconnects.
pub async fn assert_protocol_error_reconnects(breach: &[ServerMessage]) {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    let TestSession {
        server_message_tx,
        mut client_message_rx,
        result_tx,
    } = manager.session.take().unwrap();
    for server_message in breach {
        server_message_tx
            .send(server_message.clone())
            .await
            .unwrap();
    }
    assert!(client_message_rx.recv().await.is_none());
    server_message_tx
        .send(ServerMessage::Text(ByteString::from_static(r#"2["late"]"#)))
        .await
        .unwrap();
    drop(server_message_tx);
    result_tx.send(Ok(())).unwrap();

    manager.session = manager.session_rx.recv().await;
    assert_eq!(&*manager.recv_client_text().await, "0");
    manager.send_server_message(CONNECT_RESPONSE).await;
    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Connect(_))
    ));

    drop(client_packet_tx);
    assert_eq!(&*manager.recv_client_text().await, "1");
    manager.finish().await.unwrap();
}
