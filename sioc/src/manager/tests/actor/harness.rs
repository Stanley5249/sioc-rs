//! A fake engine and a spawned manager that the actor tests drive.

use std::time::Duration;

use bytestring::ByteString;
use eioc::prelude::{ClientMessage, Handshake, ServerMessage};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::task::AbortOnDropHandle;

use crate::client::{SocketReceiver, SocketSender};
use crate::config::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;

pub const CONNECT_RESPONSE: &str = "0{\"sid\":\"test\"}";

/// The engine end of one session, which a test plays.
pub struct TestSession {
    pub server_message_tx: mpsc::Sender<ServerMessage>,
    pub client_message_rx: mpsc::Receiver<ClientMessage>,
    /// Finishes the engine; dropping it finishes the engine without an error.
    pub result_tx: oneshot::Sender<Result<(), eioc::error::Error>>,
}

impl TestSession {
    /// Ends the session as the engine does: closes the server direction,
    /// takes client messages until the manager hangs up, then finishes.
    ///
    /// Returns the client messages that were still queued.
    pub async fn close(self) -> Vec<ClientMessage> {
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
        let connect_engine = move || spawn_session(channels, session_tx.clone());
        let task = tokio::spawn(crate::manager::actor::run(
            connect_request_rx,
            connect_engine,
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

    pub async fn send_server_message(&mut self, text: &'static str) {
        self.session()
            .server_message_tx
            .send(ServerMessage::Text(ByteString::from_static(text)))
            .await
            .unwrap();
    }

    pub async fn recv_client_text(&mut self) -> ByteString {
        match self.session().client_message_rx.recv().await {
            Some(ClientMessage::Text(text)) => text,
            other => panic!("expected text, got {other:?}"),
        }
    }

    /// Drops the client handle, ends the open session from the engine side,
    /// and returns the manager's result.
    ///
    /// A manager that stops instead of reconnecting ends with the session; one
    /// that reconnects ends once its namespaces are gone.
    pub async fn join(self) -> Result<(), ManagerError> {
        let Self {
            connect_request_tx,
            session,
            session_rx: _session_rx,
            task,
        } = self;
        drop(connect_request_tx);
        if let Some(session) = session {
            session.close().await;
        }
        task.await.unwrap()
    }
}

/// Starts a fake engine task with the channel capacities the client gives a
/// real one.
pub fn spawn_session(
    channels: ChannelConfig,
    session_tx: mpsc::Sender<TestSession>,
) -> eioc::engine::Engine {
    let (server_message_tx, server_message_rx) = mpsc::channel(channels.manager);
    let (client_message_tx, client_message_rx) = mpsc::channel(channels.engine);
    let (closed_tx, closed_rx) = watch::channel(());
    let task = tokio::spawn(run_session(
        server_message_tx,
        client_message_rx,
        closed_tx,
        session_tx,
    ));
    eioc::engine::Engine {
        client_message_tx,
        server_message_rx,
        closed_rx,
        task: AbortOnDropHandle::new(task),
    }
}

/// Models an engine's owned server direction separately from peer injection.
/// Closing the peer input is observed while the engine future is polled, just
/// as the real protocol ends the session and drops its server sender before
/// draining client input. Like a real engine, it sees that end only after it
/// forwarded every earlier message.
pub async fn run_session(
    server_message_tx: mpsc::Sender<ServerMessage>,
    client_message_rx: mpsc::Receiver<ClientMessage>,
    closed_tx: watch::Sender<()>,
    session_tx: mpsc::Sender<TestSession>,
) -> Result<(), eioc::error::Error> {
    let (peer_tx, mut peer_rx) = mpsc::channel(32);
    let (result_tx, result_rx) = oneshot::channel();
    server_message_tx
        .send(ServerMessage::Open(Handshake {
            sid: "engine".into(),
            upgrades: Vec::new(),
            ping_interval: 25_000,
            ping_timeout: 20_000,
            max_payload: 1_000_000,
        }))
        .await
        .unwrap();
    let session = TestSession {
        server_message_tx: peer_tx,
        client_message_rx,
        result_tx,
    };
    if session_tx.send(session).await.is_err() {
        return Ok(());
    }
    while let Some(message) = peer_rx.recv().await {
        server_message_tx.send(message).await.unwrap();
    }
    drop(closed_tx);
    drop(server_message_tx);
    result_rx.await.unwrap_or(Ok(()))
}
