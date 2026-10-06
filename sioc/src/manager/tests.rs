use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::client::SocketSender;
use crate::error::{ManagerError, SocketError};
use crate::manager::client_packet::ConnectRequest;
use crate::packet::{ClientPacket, DynAck, ServerPacket};

const CONNECT_RESPONSE: &str = "0{\"sid\":\"test\"}";

/// A running manager with the client handle and both engine ends of its
/// channels.
struct TestManager {
    connect_request_tx: mpsc::Sender<ConnectRequest>,
    server_message_tx: mpsc::Sender<Message>,
    client_message_rx: mpsc::Receiver<Message>,
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
    fn spawn() -> Self {
        let (connect_request_tx, connect_request_rx) = mpsc::channel(32);
        let (server_message_tx, server_message_rx) = mpsc::channel(32);
        let (client_message_tx, client_message_rx) = mpsc::channel(32);
        let task = tokio::spawn(crate::manager::session::run(
            connect_request_rx,
            server_message_rx,
            client_message_tx,
        ));
        Self {
            connect_request_tx,
            server_message_tx,
            client_message_rx,
            task,
        }
    }

    /// Opens a namespace as `Client::connect` does and consumes its CONNECT
    /// packet.
    async fn open(&mut self, ns: &str) -> (SocketSender, mpsc::Receiver<ServerPacket>) {
        self.open_with(ns, 32).await
    }

    async fn open_with(
        &mut self,
        ns: &str,
        server_packet_capacity: usize,
    ) -> (SocketSender, mpsc::Receiver<ServerPacket>) {
        let (connect_request, handles) =
            ConnectRequest::new(ns.into(), ByteString::new(), 32, server_packet_capacity);
        self.connect_request_tx.send(connect_request).await.unwrap();
        assert!(self.recv_client_text().await.starts_with('0'));
        handles.reply_rx.await.unwrap().unwrap();
        let client_packet_tx = SocketSender::new(handles.client_packet_tx, handles.closed);
        (client_packet_tx, handles.server_packet_rx)
    }

    async fn send_server_message(&self, text: &'static str) {
        self.server_message_tx
            .send(Message::Text(ByteString::from_static(text)))
            .await
            .unwrap();
    }

    async fn send_server_binary(&self, bytes: &'static [u8]) {
        self.server_message_tx
            .send(Message::Binary(Bytes::from_static(bytes)))
            .await
            .unwrap();
    }

    async fn recv_client_text(&mut self) -> ByteString {
        match self.client_message_rx.recv().await {
            Some(Message::Text(text)) => text,
            other => panic!("expected text, got {other:?}"),
        }
    }

    async fn recv_client_binary(&mut self) -> Bytes {
        match self.client_message_rx.recv().await {
            Some(Message::Binary(bytes)) => bytes,
            other => panic!("expected binary, got {other:?}"),
        }
    }

    /// Ends the session from the engine side and returns the manager's result.
    ///
    /// Keeps reading client messages meanwhile, as the engine drains them, so
    /// a full queue cannot stall the manager and hang the test.
    async fn finish(self) -> Result<(), ManagerError> {
        let Self {
            connect_request_tx,
            server_message_tx,
            mut client_message_rx,
            task,
        } = self;
        drop(server_message_tx);
        let drain = async move { while client_message_rx.recv().await.is_some() {} };
        let (result, ()) = tokio::join!(task, drain);
        drop(connect_request_tx);
        result.unwrap()
    }
}

#[tokio::test]
async fn stays_open_with_no_namespace() {
    let mut manager = TestManager::spawn();
    assert_quiet(&mut manager.client_message_rx).await;
    assert!(matches!(
        manager.client_message_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn closes_when_client_handle_drops_with_no_namespace() {
    let mut manager = TestManager::spawn();
    drop(manager.connect_request_tx);
    assert!(manager.client_message_rx.recv().await.is_none());
    drop(manager.server_message_tx);
    manager.task.await.unwrap().unwrap();
}

#[tokio::test]
async fn stays_open_after_last_namespace_while_client_handle_lives() {
    let mut manager = TestManager::spawn();
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
    client_packet_tx.disconnect();
    assert_eq!(&*manager.recv_client_text().await, "1");
    assert_quiet(&mut manager.client_message_rx).await;
    assert!(matches!(
        manager.client_message_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));

    drop(manager.connect_request_tx);
    assert!(manager.client_message_rx.recv().await.is_none());
    drop(manager.server_message_tx);
    manager.task.await.unwrap().unwrap();
}

#[tokio::test]
async fn closes_after_client_handle_and_last_namespace_drop() {
    let mut manager = TestManager::spawn();
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
    drop(manager.connect_request_tx);
    assert_quiet(&mut manager.client_message_rx).await;

    drop(client_packet_tx);
    assert!(matches!(
        manager.client_message_rx.recv().await,
        Some(Message::Text(text)) if text == "1"
    ));
    assert!(manager.client_message_rx.recv().await.is_none());
    drop(manager.server_message_tx);
    manager.task.await.unwrap().unwrap();
}

#[tokio::test]
async fn dropping_handles_disconnects_only_that_namespace() {
    let mut manager = TestManager::spawn();
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
    let (_other_tx, _other_rx) = manager.open("/other").await;
    drop(client_packet_tx);
    assert_eq!(&*manager.recv_client_text().await, "1");
    assert_quiet(&mut manager.client_message_rx).await;
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn events_wait_for_server_connect_in_order() {
    let mut manager = TestManager::spawn();
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    client_packet_tx
        .send(event(r#"["a"]"#, None))
        .await
        .unwrap();
    client_packet_tx
        .send(event(r#"["b"]"#, None))
        .await
        .unwrap();
    assert_quiet(&mut manager.client_message_rx).await;

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
    let mut manager = TestManager::spawn();
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
    let mut manager = TestManager::spawn();
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
    let mut manager = TestManager::spawn();
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
    let mut manager = TestManager::spawn();
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

#[tokio::test]
async fn ack_is_not_buffered() {
    let mut manager = TestManager::spawn();
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
    let client_packet = ClientPacket::Ack {
        payload: ByteString::from_static("[true]"),
        id: 42,
        attachments: None,
    };
    client_packet_tx.send(client_packet).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, "342[true]");
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn binary_ack_sends_attachments() {
    let mut manager = TestManager::spawn();
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
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
    let mut manager = TestManager::spawn();
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
    assert_quiet(&mut manager.client_message_rx).await;
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn connect_error_closes_namespace() {
    let mut manager = TestManager::spawn();
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
    let mut manager = TestManager::spawn();
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
    let mut manager = TestManager::spawn();
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
    assert_quiet(&mut manager.client_message_rx).await;
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn duplicate_namespace_is_conflict() {
    let mut manager = TestManager::spawn();
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
    let mut manager = TestManager::spawn();
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
    let mut manager = TestManager::spawn();
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
        server_message_tx: _server_message_tx,
        client_message_rx,
        task,
    } = TestManager::spawn();
    drop(client_message_rx);
    let (connect_request, _handles) = ConnectRequest::new("/".into(), ByteString::new(), 1, 1);
    connect_request_tx.send(connect_request).await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(ManagerError::ClientMessage(_))
    ));
}

#[tokio::test]
async fn unexpected_binary_is_error() {
    let manager = TestManager::spawn();
    manager.send_server_binary(b"\xFF").await;
    assert!(matches!(
        manager.task.await.unwrap(),
        Err(ManagerError::UnexpectedBinary(_))
    ));
}

#[tokio::test]
async fn text_during_binary_reassembly_is_error() {
    let mut manager = TestManager::spawn();
    let (_client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();

    manager.send_server_message(r#"51-["img"]"#).await;
    manager.send_server_message(r#"2["oops"]"#).await;
    assert!(matches!(
        manager.task.await.unwrap(),
        Err(ManagerError::UnexpectedText(_))
    ));
}

#[tokio::test]
async fn engine_close_ends_receivers_and_pending_acks() {
    let mut manager = TestManager::spawn();
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
    let mut manager = TestManager::spawn();
    let (client_packet_tx, mut server_packet_rx) = manager.open_with("/", 1).await;
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
    let mut manager = TestManager::spawn();
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
    client_packet_tx
        .send(event(r#"["a"]"#, None))
        .await
        .unwrap();
    for _ in 0..3 {
        manager.send_server_message(CONNECT_RESPONSE).await;
    }
    assert_eq!(&*manager.recv_client_text().await, r#"2["a"]"#);
    assert_quiet(&mut manager.client_message_rx).await;
    manager.finish().await.unwrap();
}
