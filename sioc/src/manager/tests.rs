use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{Event, Handshake, Message};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::client::{ChannelConfig, SocketSender};
use crate::error::{ManagerError, SocketError};
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::packet::{ClientPacket, DynAck, ServerPacket};

const CONNECT_RESPONSE: &str = "0{\"sid\":\"test\"}";

/// The engine end of one session, which a test plays.
struct TestSession {
    event_tx: mpsc::Sender<Event>,
    client_message_rx: mpsc::Receiver<Message>,
    /// Finishes the engine; dropping it finishes the engine without an error.
    result_tx: oneshot::Sender<Result<(), eioc::error::Error>>,
}

impl TestSession {
    /// Ends the session as the engine does: closes the server direction,
    /// takes client messages until the manager hangs up, then finishes.
    ///
    /// Returns the client messages that were still queued.
    async fn end(self) -> Vec<Message> {
        let Self {
            event_tx,
            mut client_message_rx,
            result_tx,
        } = self;
        drop(event_tx);
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
struct TestManager {
    connect_request_tx: mpsc::Sender<ConnectRequest>,
    session: Option<TestSession>,
    /// Receives each session the manager opens after the first.
    session_rx: mpsc::Receiver<TestSession>,
    task: JoinHandle<Result<(), ManagerError>>,
}

/// Asserts that nothing reaches the engine for a short while.
async fn assert_quiet(client_message_rx: &mut mpsc::Receiver<Message>) {
    let result = tokio::time::timeout(Duration::from_millis(50), client_message_rx.recv()).await;
    assert!(result.is_err(), "unexpected message {result:?}");
}

fn event(payload: &'static str, ack_tx: Option<oneshot::Sender<DynAck>>) -> ClientPacket {
    ClientPacket::Event {
        payload: ByteString::from_static(payload),
        ack_tx,
        attachments: None,
    }
}

impl TestManager {
    /// Spawns a manager that stops instead of reconnecting, and takes its first
    /// session.
    async fn spawn() -> Self {
        Self::spawn_with(None).await
    }

    /// Spawns a manager that reconnects at once, and takes its first session.
    async fn spawn_reconnecting() -> Self {
        Self::spawn_with(Some(Backoff::new(
            Duration::ZERO,
            Duration::ZERO,
            0.0,
            None,
        )))
        .await
    }

    async fn spawn_with(backoff: Option<Backoff>) -> Self {
        let (connect_request_tx, connect_request_rx) = mpsc::channel(32);
        let (session_tx, mut session_rx) = mpsc::channel(1);
        let connect_engine = move |event_tx, client_message_rx| {
            let session_tx = session_tx.clone();
            async move {
                let (result_tx, result_rx) = oneshot::channel();
                let session = TestSession {
                    event_tx,
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
        let task = tokio::spawn(crate::manager::client_packet::client_packets_to_messages(
            connect_request_rx,
            connect_engine,
            ChannelConfig::from(32),
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
    fn session(&mut self) -> &mut TestSession {
        self.session.as_mut().expect("a session is open")
    }

    /// Ends the open session from the engine side and takes the next one.
    ///
    /// Returns the client messages that were still queued for the old session.
    async fn reconnect(&mut self) -> Vec<Message> {
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
    async fn open(&mut self, ns: &str) -> (SocketSender, mpsc::Receiver<ServerPacket>) {
        self.open_with(ns, ByteString::new(), 32).await
    }

    async fn open_with(
        &mut self,
        ns: &str,
        auth: ByteString,
        server_packet_capacity: usize,
    ) -> (SocketSender, mpsc::Receiver<ServerPacket>) {
        let (connect_request, handles) =
            ConnectRequest::new(ns.into(), auth, 32, server_packet_capacity);
        self.connect_request_tx.send(connect_request).await.unwrap();
        assert!(self.recv_client_text().await.starts_with('0'));
        handles.reply_rx.await.unwrap().unwrap();
        let client_packet_tx = SocketSender::new(handles.client_packet_tx, handles.closed);
        (client_packet_tx, handles.server_packet_rx)
    }

    /// Reports a successful handshake, as the engine does first.
    async fn send_open(&mut self) {
        let handshake = Handshake {
            sid: "engine".into(),
            upgrades: Vec::new(),
            ping_interval: 25_000,
            ping_timeout: 20_000,
            max_payload: 1_000_000,
        };
        self.session()
            .event_tx
            .send(Event::Open(handshake))
            .await
            .unwrap();
    }

    async fn send_server_message(&mut self, text: &'static str) {
        let message = Message::Text(ByteString::from_static(text));
        self.session()
            .event_tx
            .send(Event::Message(message))
            .await
            .unwrap();
    }

    async fn send_server_binary(&mut self, bytes: &'static [u8]) {
        let message = Message::Binary(Bytes::from_static(bytes));
        self.session()
            .event_tx
            .send(Event::Message(message))
            .await
            .unwrap();
    }

    async fn recv_client_text(&mut self) -> ByteString {
        match self.session().client_message_rx.recv().await {
            Some(Message::Text(text)) => text,
            other => panic!("expected text, got {other:?}"),
        }
    }

    async fn recv_client_binary(&mut self) -> Bytes {
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
    async fn finish(self) -> Result<(), ManagerError> {
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

#[tokio::test]
async fn stays_open_with_no_namespace() {
    let mut manager = TestManager::spawn().await;
    assert_quiet(&mut manager.session().client_message_rx).await;
    assert!(matches!(
        manager.session().client_message_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn closes_when_client_handle_drops_with_no_namespace() {
    let TestManager {
        connect_request_tx,
        session,
        task,
        ..
    } = TestManager::spawn().await;
    let mut session = session.unwrap();
    drop(connect_request_tx);
    assert!(session.client_message_rx.recv().await.is_none());
    session.end().await;
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn stays_open_after_last_namespace_while_client_handle_lives() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    client_packet_tx.disconnect();
    assert_eq!(&*manager.recv_client_text().await, "1");
    assert_quiet(&mut manager.session().client_message_rx).await;
    assert!(matches!(
        manager.session().client_message_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));

    let TestManager {
        connect_request_tx,
        session,
        task,
        ..
    } = manager;
    let mut session = session.unwrap();
    drop(connect_request_tx);
    assert!(session.client_message_rx.recv().await.is_none());
    session.end().await;
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn closes_after_client_handle_and_last_namespace_drop() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    let TestManager {
        connect_request_tx,
        session,
        task,
        ..
    } = manager;
    let mut session = session.unwrap();
    drop(connect_request_tx);
    assert_quiet(&mut session.client_message_rx).await;

    drop(client_packet_tx);
    assert!(matches!(
        session.client_message_rx.recv().await,
        Some(Message::Text(text)) if text == "1"
    ));
    assert!(session.client_message_rx.recv().await.is_none());
    session.end().await;
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn dropping_handles_disconnects_only_that_namespace() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    let (_other_tx, _other_rx) = manager.open("/other").await;
    drop(client_packet_tx);
    assert_eq!(&*manager.recv_client_text().await, "1");
    assert_quiet(&mut manager.session().client_message_rx).await;
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn events_wait_for_server_connect_in_order() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    client_packet_tx
        .send(event(r#"["a"]"#, None))
        .await
        .unwrap();
    client_packet_tx
        .send(event(r#"["b"]"#, None))
        .await
        .unwrap();
    assert_quiet(&mut manager.session().client_message_rx).await;

    manager.send_server_message(CONNECT_RESPONSE).await;
    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Connect(_))
    ));
    assert_eq!(&*manager.recv_client_text().await, r#"2["a"]"#);
    assert_eq!(&*manager.recv_client_text().await, r#"2["b"]"#);

    client_packet_tx
        .send(event(r#"["c"]"#, None))
        .await
        .unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2["c"]"#);
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn ack_roundtrip() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    let (ack_tx, ack_rx) = oneshot::channel();
    client_packet_tx
        .send(event(r#"["greet"]"#, Some(ack_tx)))
        .await
        .unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"20["greet"]"#);

    manager.send_server_message(r#"30["ok"]"#).await;
    assert_eq!(&*ack_rx.await.unwrap().payload, r#"["ok"]"#);
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn binary_ack_reassembly() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    let (ack_tx, ack_rx) = oneshot::channel();
    client_packet_tx
        .send(event(r#"["greet"]"#, Some(ack_tx)))
        .await
        .unwrap();
    manager.recv_client_text().await;

    manager.send_server_message(r#"61-0["ok"]"#).await;
    manager.send_server_binary(b"\xAB\xCD").await;
    let ack = ack_rx.await.unwrap();
    assert_eq!(&*ack.payload, r#"["ok"]"#);
    assert_eq!(
        ack.attachments.unwrap(),
        vec![Bytes::from_static(b"\xAB\xCD")]
    );
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn binary_event_waits_for_every_attachment() {
    let mut manager = TestManager::spawn().await;
    let (_client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    manager.send_server_message(r#"52-["img"]"#).await;
    manager.send_server_binary(b"\x01\x02").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_packet_rx.try_recv().unwrap_err();

    manager.send_server_binary(b"\x03\x04").await;
    let Some(ServerPacket::Event(event)) = server_packet_rx.recv().await else {
        panic!("expected an event");
    };
    assert_eq!(event.attachments.unwrap().len(), 2);
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn binary_event_sends_attachments() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    let client_packet = ClientPacket::Event {
        payload: ByteString::from_static(r#"["img"]"#),
        ack_tx: None,
        attachments: Some(vec![Bytes::from_static(b"\x01\x02")]),
    };
    client_packet_tx.send(client_packet).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"51-["img"]"#);
    assert_eq!(
        manager.recv_client_binary().await,
        Bytes::from_static(b"\x01\x02")
    );
    manager.finish().await.unwrap();
}

fn ack(id: u64) -> ClientPacket {
    ClientPacket::Ack {
        payload: ByteString::from_static("[true]"),
        id,
        attachments: None,
    }
}

#[tokio::test]
async fn ack_waits_for_nothing_once_confirmed() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    client_packet_tx
        .send(event(r#"["held"]"#, None))
        .await
        .unwrap();
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2["held"]"#);
    client_packet_tx.send(ack(42)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, "342[true]");
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn ack_before_server_connect_is_discarded() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    client_packet_tx.send(ack(42)).await.unwrap();
    assert_quiet(&mut manager.session().client_message_rx).await;

    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    client_packet_tx.send(ack(43)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, "343[true]");
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn disconnect_before_server_connect_sends_nothing() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
    let (_other_tx, mut other_rx) = manager.open("/other").await;
    client_packet_tx.disconnect();
    assert_quiet(&mut manager.session().client_message_rx).await;

    // A late CONNECT for the closed namespace is discarded.
    manager.send_server_message(CONNECT_RESPONSE).await;
    manager.send_server_message(r#"0/other,{"sid":"o"}"#).await;
    assert!(matches!(
        other_rx.recv().await,
        Some(ServerPacket::Connect(_))
    ));
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn binary_ack_sends_attachments() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    let client_packet = ClientPacket::Ack {
        payload: ByteString::from_static("[true]"),
        id: 7,
        attachments: Some(vec![Bytes::from_static(b"\xCA\xFE")]),
    };
    client_packet_tx.send(client_packet).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, "61-7[true]");
    assert_eq!(
        manager.recv_client_binary().await,
        Bytes::from_static(b"\xCA\xFE")
    );
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn server_disconnect_ends_receiver_and_handles() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    manager.send_server_message("1").await;
    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Disconnect)
    ));
    assert!(server_packet_rx.recv().await.is_none());

    // The handles closed before the receiver saw the DISCONNECT.
    client_packet_tx.closed().await;
    assert!(matches!(
        client_packet_tx.send(event(r#"["late"]"#, None)).await,
        Err(SocketError::Closed)
    ));

    // The server closed the namespace, so dropping the handles sends nothing.
    drop(client_packet_tx);
    assert_quiet(&mut manager.session().client_message_rx).await;
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn connect_error_closes_namespace() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager
        .send_server_message(r#"4{"message":"denied"}"#)
        .await;
    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::ConnectError(_))
    ));
    assert!(server_packet_rx.recv().await.is_none());
    assert!(matches!(
        client_packet_tx.send(event(r#"["late"]"#, None)).await,
        Err(SocketError::Closed)
    ));

    // The refused namespace can open again.
    let (_client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Connect(_))
    ));
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn client_close_sends_earlier_packets_first() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    client_packet_tx
        .send(event(r#"["a"]"#, None))
        .await
        .unwrap();
    client_packet_tx.disconnect();
    assert_eq!(&*manager.recv_client_text().await, r#"2["a"]"#);
    assert_eq!(&*manager.recv_client_text().await, "1");
    assert!(server_packet_rx.recv().await.is_none());
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn reopened_namespace_ignores_old_handles() {
    let mut manager = TestManager::spawn().await;
    let (old_tx, mut old_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    old_rx.recv().await.unwrap();
    manager.send_server_message("1").await;
    old_rx.recv().await.unwrap();

    let (new_tx, mut new_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    new_rx.recv().await.unwrap();

    old_tx.send(event(r#"["old"]"#, None)).await.unwrap_err();
    new_tx.send(event(r#"["new"]"#, None)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2["new"]"#);
    assert_quiet(&mut manager.session().client_message_rx).await;
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn late_ack_never_resolves_a_reopened_namespace() {
    let mut manager = TestManager::spawn().await;
    let (old_tx, mut old_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    old_rx.recv().await.unwrap();
    let (old_ack_tx, _old_ack_rx) = oneshot::channel();
    old_tx
        .send(event(r#"["old"]"#, Some(old_ack_tx)))
        .await
        .unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"20["old"]"#);
    old_tx.disconnect();
    assert_eq!(&*manager.recv_client_text().await, "1");

    let (new_tx, mut new_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    new_rx.recv().await.unwrap();
    let (new_ack_tx, mut new_ack_rx) = oneshot::channel();
    new_tx
        .send(event(r#"["new"]"#, Some(new_ack_tx)))
        .await
        .unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"21["new"]"#);

    // Another name counts on its own, from 0.
    let (other_tx, mut other_rx) = manager.open("/other").await;
    manager.send_server_message(r#"0/other,{"sid":"o"}"#).await;
    other_rx.recv().await.unwrap();
    let (other_ack_tx, _other_ack_rx) = oneshot::channel();
    other_tx
        .send(event(r#"["other"]"#, Some(other_ack_tx)))
        .await
        .unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2/other,0["other"]"#);

    // The server answers the old event after the reopen, as socket.io's
    // `Socket.ack` allows.
    manager.send_server_message(r#"30["late"]"#).await;
    manager.send_server_message(r#"2["sync"]"#).await;
    new_rx.recv().await.unwrap();
    assert!(matches!(
        new_ack_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn duplicate_namespace_is_conflict() {
    let mut manager = TestManager::spawn().await;
    let (_client_packet_tx, _server_packet_rx) = manager.open("/").await;
    let (connect_request, handles) = ConnectRequest::new("/".into(), ByteString::new(), 1, 1);
    manager
        .connect_request_tx
        .send(connect_request)
        .await
        .unwrap();
    assert!(matches!(
        handles.reply_rx.await.unwrap(),
        Err(SocketError::NamespaceConflict { .. })
    ));
    let (_other_tx, _other_rx) = manager.open("/other").await;
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn late_server_packets_are_discarded() {
    let mut manager = TestManager::spawn().await;
    manager.send_server_message(r#"2/gone,["late"]"#).await;
    manager.send_server_message(r#"30["late"]"#).await;
    manager.send_server_message("1/gone,").await;

    let (_client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Connect(_))
    ));
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn dropped_receiver_discards_events() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, server_packet_rx) = manager.open("/").await;
    drop(server_packet_rx);
    manager.send_server_message(CONNECT_RESPONSE).await;
    manager.send_server_message(r#"2["ignored"]"#).await;

    client_packet_tx
        .send(event(r#"["still"]"#, None))
        .await
        .unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2["still"]"#);
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn closed_engine_channel_is_error() {
    let TestManager {
        connect_request_tx,
        session,
        session_rx: _session_rx,
        task,
    } = TestManager::spawn().await;
    let TestSession {
        event_tx,
        client_message_rx,
        result_tx,
    } = session.unwrap();
    drop(client_message_rx);
    let (connect_request, handles) = ConnectRequest::new("/".into(), ByteString::new(), 1, 1);
    connect_request_tx.send(connect_request).await.unwrap();
    // The failed CONNECT drops the reply.
    handles.reply_rx.await.unwrap_err();

    drop(event_tx);
    drop(result_tx);
    assert!(matches!(
        task.await.unwrap(),
        Err(ManagerError::ClientMessage(_))
    ));
}

/// Breaks the protocol in an open session, then checks that the manager closes
/// the session, keeps accepting server messages until the engine hangs up, and
/// reconnects.
async fn assert_protocol_error_reconnects(breach: &[Message]) {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    let TestSession {
        event_tx,
        mut client_message_rx,
        result_tx,
    } = manager.session.take().unwrap();
    for message in breach {
        event_tx
            .send(Event::Message(message.clone()))
            .await
            .unwrap();
    }
    assert!(client_message_rx.recv().await.is_none());
    let late = Message::Text(ByteString::from_static(r#"2["late"]"#));
    event_tx.send(Event::Message(late)).await.unwrap();
    drop(event_tx);
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

#[tokio::test]
async fn unexpected_binary_reconnects() {
    assert_protocol_error_reconnects(&[Message::Binary(Bytes::from_static(b"\xFF"))]).await;
}

#[tokio::test]
async fn text_during_binary_reassembly_reconnects() {
    assert_protocol_error_reconnects(&[
        Message::Text(ByteString::from_static(r#"51-["img"]"#)),
        Message::Text(ByteString::from_static(r#"2["oops"]"#)),
    ])
    .await;
}

#[tokio::test]
async fn engine_close_ends_receivers_and_pending_acks() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    let (ack_tx, ack_rx) = oneshot::channel();
    client_packet_tx
        .send(event(r#"["greet"]"#, Some(ack_tx)))
        .await
        .unwrap();
    manager.recv_client_text().await;

    manager.finish().await.unwrap();
    assert!(server_packet_rx.recv().await.is_none());
    ack_rx.await.unwrap_err();
}

#[tokio::test]
async fn emits_flow_while_receiver_is_full() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) =
        manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    for _ in 0..4 {
        manager.send_server_message(r#"2["flood"]"#).await;
    }

    client_packet_tx
        .send(event(r#"["out"]"#, None))
        .await
        .unwrap();
    let text = tokio::time::timeout(Duration::from_secs(5), manager.recv_client_text())
        .await
        .expect("emit stalled behind a full receiver");
    assert_eq!(&*text, r#"2["out"]"#);

    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Connect(_))
    ));
}

#[tokio::test]
async fn repeated_server_connect_flushes_once() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
    client_packet_tx
        .send(event(r#"["a"]"#, None))
        .await
        .unwrap();
    for _ in 0..3 {
        manager.send_server_message(CONNECT_RESPONSE).await;
    }
    assert_eq!(&*manager.recv_client_text().await, r#"2["a"]"#);
    assert_quiet(&mut manager.session().client_message_rx).await;
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn reconnect_resends_connect_with_auth() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let auth = ByteString::from_static(r#"{"token":"t"}"#);
    let (client_packet_tx, mut server_packet_rx) = manager.open_with("/chat", auth, 32).await;
    manager.send_server_message(r#"0/chat,{"sid":"a"}"#).await;
    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Connect(connect)) if connect.sid == "a"
    ));

    assert_eq!(manager.reconnect().await, [] as [Message; 0]);
    assert_eq!(
        &*manager.recv_client_text().await,
        r#"0/chat,{"token":"t"}"#
    );
    manager.send_server_message(r#"0/chat,{"sid":"b"}"#).await;
    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Connect(connect)) if connect.sid == "b"
    ));

    drop(client_packet_tx);
    assert_eq!(&*manager.recv_client_text().await, "1/chat,");
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn reconnect_drops_queued_messages_and_buffers_events() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    client_packet_tx
        .send(event(r#"["old"]"#, None))
        .await
        .unwrap();
    while manager.session().client_message_rx.is_empty() {
        tokio::task::yield_now().await;
    }
    let messages = manager.reconnect().await;
    assert!(matches!(&messages[..], [Message::Text(text)] if text == r#"2["old"]"#));

    assert_eq!(&*manager.recv_client_text().await, "0");
    client_packet_tx
        .send(event(r#"["new"]"#, None))
        .await
        .unwrap();
    assert_quiet(&mut manager.session().client_message_rx).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    assert_eq!(&*manager.recv_client_text().await, r#"2["new"]"#);

    drop(client_packet_tx);
    assert_eq!(&*manager.recv_client_text().await, "1");
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn session_end_fails_sent_acks_and_keeps_buffered_ones() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    let (sent_tx, sent_rx) = oneshot::channel();
    client_packet_tx
        .send(event(r#"["sent"]"#, Some(sent_tx)))
        .await
        .unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"20["sent"]"#);
    manager.reconnect().await;
    sent_rx.await.unwrap_err();

    assert_eq!(&*manager.recv_client_text().await, "0");
    let (buffered_tx, mut buffered_rx) = oneshot::channel();
    client_packet_tx
        .send(event(r#"["buffered"]"#, Some(buffered_tx)))
        .await
        .unwrap();
    assert_quiet(&mut manager.session().client_message_rx).await;
    assert_eq!(manager.reconnect().await, [] as [Message; 0]);
    assert!(matches!(
        buffered_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    assert_eq!(&*manager.recv_client_text().await, "0");
    manager.send_server_message(CONNECT_RESPONSE).await;
    assert_eq!(&*manager.recv_client_text().await, r#"21["buffered"]"#);
    manager.send_server_message(r#"31["ok"]"#).await;
    assert_eq!(&*buffered_rx.await.unwrap().payload, r#"["ok"]"#);

    drop(client_packet_tx);
    assert_eq!(&*manager.recv_client_text().await, "1");
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn no_reconnection_without_namespaces_until_one_opens() {
    let mut manager = TestManager::spawn_reconnecting().await;
    manager.session.take().unwrap().end().await;
    let next = tokio::time::timeout(Duration::from_millis(50), manager.session_rx.recv()).await;
    assert!(next.is_err(), "unexpected session");

    let (connect_request, handles) = ConnectRequest::new("/".into(), ByteString::new(), 32, 32);
    manager
        .connect_request_tx
        .send(connect_request)
        .await
        .unwrap();
    manager.session = manager.session_rx.recv().await;
    assert_eq!(&*manager.recv_client_text().await, "0");
    handles.reply_rx.await.unwrap().unwrap();
    let mut server_packet_rx = handles.server_packet_rx;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    drop(handles.client_packet_tx);
    assert_eq!(&*manager.recv_client_text().await, "1");
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn opened_engine_restarts_the_attempt_count() {
    let backoff = Backoff::new(Duration::ZERO, Duration::ZERO, 0.0, Some(1));
    let mut manager = TestManager::spawn_with(Some(backoff)).await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    for _ in 0..3 {
        manager.send_open().await;
        manager.reconnect().await;
        assert_eq!(&*manager.recv_client_text().await, "0");
    }

    // The last engine never opened, so it used up the one attempt. A
    // confirmed namespace does not count, like socket.io-client, whose
    // `Manager.onopen` follows the handshake.
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    manager.session.take().unwrap().end().await;
    assert!(server_packet_rx.recv().await.is_none());

    drop(client_packet_tx);
    manager.finish().await.unwrap();
}
