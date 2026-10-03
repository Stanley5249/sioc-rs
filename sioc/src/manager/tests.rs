use super::*;
use crate::packet::{Directive, DynAck, Signal};
use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use std::time::Duration;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const CONNECT_RESPONSE: &str = "0{\"sid\":\"test\"}";

/// A running manager with the client handle and both engine ends of its channels.
struct Harness {
    connect_request_tx: mpsc::Sender<ConnectRequest>,
    server_message_tx: mpsc::Sender<Message>,
    client_message_rx: mpsc::Receiver<Message>,
    manager: JoinHandle<Result<(), ManagerError>>,
}

fn spawn() -> Harness {
    let (connect_request_tx, connect_request_rx) = mpsc::channel(32);
    let (server_message_tx, server_message_rx) = mpsc::channel(32);
    let (client_message_tx, client_message_rx) = mpsc::channel(32);
    let manager = tokio::spawn(run(
        connect_request_rx,
        server_message_rx,
        client_message_tx,
    ));
    Harness {
        connect_request_tx,
        server_message_tx,
        client_message_rx,
        manager,
    }
}

/// Asserts that nothing reaches the engine for a short while.
async fn assert_quiet(client_message_rx: &mut mpsc::Receiver<Message>) {
    let result = tokio::time::timeout(Duration::from_millis(50), client_message_rx.recv()).await;
    assert!(result.is_err(), "unexpected message {result:?}");
}

fn event(payload: &'static str, ack_tx: Option<oneshot::Sender<DynAck>>) -> Directive {
    Directive::Event {
        payload: ByteString::from_static(payload),
        ack_tx,
        attachments: None,
    }
}

impl Harness {
    /// Opens a namespace and consumes its CONNECT packet.
    async fn open(&mut self, ns: &str) -> (mpsc::Sender<Directive>, mpsc::Receiver<Signal>) {
        self.open_with(ns, 32).await
    }

    async fn open_with(
        &mut self,
        ns: &str,
        signal_capacity: usize,
    ) -> (mpsc::Sender<Directive>, mpsc::Receiver<Signal>) {
        let (directive_tx, directive_rx) = mpsc::channel(32);
        let (signal_tx, signal_rx) = mpsc::channel(signal_capacity);
        let connect_request = ConnectRequest {
            ns: ns.into(),
            payload: ByteString::new(),
            directive_rx,
            signal_tx,
        };
        self.connect_request_tx.send(connect_request).await.unwrap();
        assert!(self.text().await.starts_with('0'));
        (directive_tx, signal_rx)
    }

    async fn server(&self, text: &'static str) {
        self.server_message_tx
            .send(Message::Text(ByteString::from_static(text)))
            .await
            .unwrap();
    }

    async fn server_binary(&self, bytes: &'static [u8]) {
        self.server_message_tx
            .send(Message::Binary(Bytes::from_static(bytes)))
            .await
            .unwrap();
    }

    async fn text(&mut self) -> ByteString {
        match self.client_message_rx.recv().await {
            Some(Message::Text(text)) => text,
            other => panic!("expected text, got {other:?}"),
        }
    }

    async fn binary(&mut self) -> Bytes {
        match self.client_message_rx.recv().await {
            Some(Message::Binary(bytes)) => bytes,
            other => panic!("expected binary, got {other:?}"),
        }
    }

    /// Ends the session from the engine side and returns the manager's result.
    async fn close_server(self) -> Result<(), ManagerError> {
        drop(self.server_message_tx);
        self.manager.await.unwrap()
    }
}

#[tokio::test]
async fn stays_open_with_no_namespace() {
    let mut h = spawn();
    assert_quiet(&mut h.client_message_rx).await;
    assert!(matches!(
        h.client_message_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn closes_when_client_handle_drops_with_no_namespace() {
    let mut h = spawn();
    drop(h.connect_request_tx);
    assert!(h.client_message_rx.recv().await.is_none());
    drop(h.server_message_tx);
    h.manager.await.unwrap().unwrap();
}

#[tokio::test]
async fn stays_open_after_last_namespace_while_client_handle_lives() {
    let mut h = spawn();
    let (directive_tx, _signal_rx) = h.open("/").await;
    directive_tx.send(Directive::Disconnect).await.unwrap();
    assert_eq!(&*h.text().await, "1");
    assert_quiet(&mut h.client_message_rx).await;
    assert!(matches!(
        h.client_message_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));

    drop(h.connect_request_tx);
    assert!(h.client_message_rx.recv().await.is_none());
    drop(h.server_message_tx);
    h.manager.await.unwrap().unwrap();
}

#[tokio::test]
async fn closes_after_client_handle_and_last_namespace_drop() {
    let mut h = spawn();
    let (directive_tx, _signal_rx) = h.open("/").await;
    drop(h.connect_request_tx);
    assert_quiet(&mut h.client_message_rx).await;

    drop(directive_tx);
    assert!(matches!(
        h.client_message_rx.recv().await,
        Some(Message::Text(text)) if text == "1"
    ));
    assert!(h.client_message_rx.recv().await.is_none());
    drop(h.server_message_tx);
    h.manager.await.unwrap().unwrap();
}

#[tokio::test]
async fn dropping_handles_disconnects_only_that_namespace() {
    let mut h = spawn();
    let (directive_tx, _signal_rx) = h.open("/").await;
    let (_other_tx, _other_rx) = h.open("/other").await;
    drop(directive_tx);
    assert_eq!(&*h.text().await, "1");
    assert_quiet(&mut h.client_message_rx).await;
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn events_wait_for_server_connect_in_order() {
    let mut h = spawn();
    let (directive_tx, mut signal_rx) = h.open("/").await;
    directive_tx.send(event(r#"["a"]"#, None)).await.unwrap();
    directive_tx.send(event(r#"["b"]"#, None)).await.unwrap();
    assert_quiet(&mut h.client_message_rx).await;

    h.server(CONNECT_RESPONSE).await;
    assert!(matches!(signal_rx.recv().await, Some(Signal::Connect(_))));
    assert_eq!(&*h.text().await, r#"2["a"]"#);
    assert_eq!(&*h.text().await, r#"2["b"]"#);

    directive_tx.send(event(r#"["c"]"#, None)).await.unwrap();
    assert_eq!(&*h.text().await, r#"2["c"]"#);
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn ack_roundtrip() {
    let mut h = spawn();
    let (directive_tx, mut signal_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    signal_rx.recv().await.unwrap();

    let (ack_tx, ack_rx) = oneshot::channel();
    directive_tx
        .send(event(r#"["greet"]"#, Some(ack_tx)))
        .await
        .unwrap();
    assert_eq!(&*h.text().await, r#"20["greet"]"#);

    h.server(r#"30["ok"]"#).await;
    assert_eq!(&*ack_rx.await.unwrap().payload, r#"["ok"]"#);
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn binary_ack_reassembly() {
    let mut h = spawn();
    let (directive_tx, mut signal_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    signal_rx.recv().await.unwrap();

    let (ack_tx, ack_rx) = oneshot::channel();
    directive_tx
        .send(event(r#"["greet"]"#, Some(ack_tx)))
        .await
        .unwrap();
    h.text().await;

    h.server(r#"61-0["ok"]"#).await;
    h.server_binary(b"\xAB\xCD").await;
    let ack = ack_rx.await.unwrap();
    assert_eq!(&*ack.payload, r#"["ok"]"#);
    assert_eq!(
        ack.attachments.unwrap(),
        vec![Bytes::from_static(b"\xAB\xCD")]
    );
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn binary_event_waits_for_every_attachment() {
    let mut h = spawn();
    let (_directive_tx, mut signal_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    signal_rx.recv().await.unwrap();

    h.server(r#"52-["img"]"#).await;
    h.server_binary(b"\x01\x02").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    signal_rx.try_recv().unwrap_err();

    h.server_binary(b"\x03\x04").await;
    let Some(Signal::Event(event)) = signal_rx.recv().await else {
        panic!("expected an event");
    };
    assert_eq!(event.attachments.unwrap().len(), 2);
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn binary_event_directive_sends_attachments() {
    let mut h = spawn();
    let (directive_tx, mut signal_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    signal_rx.recv().await.unwrap();

    let directive = Directive::Event {
        payload: ByteString::from_static(r#"["img"]"#),
        ack_tx: None,
        attachments: Some(vec![Bytes::from_static(b"\x01\x02")]),
    };
    directive_tx.send(directive).await.unwrap();
    assert_eq!(&*h.text().await, r#"51-["img"]"#);
    assert_eq!(h.binary().await, Bytes::from_static(b"\x01\x02"));
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn ack_directive_is_not_buffered() {
    let mut h = spawn();
    let (directive_tx, _signal_rx) = h.open("/").await;
    let directive = Directive::Ack {
        payload: ByteString::from_static("[true]"),
        id: 42,
        attachments: None,
    };
    directive_tx.send(directive).await.unwrap();
    assert_eq!(&*h.text().await, "342[true]");
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn binary_ack_directive_sends_attachments() {
    let mut h = spawn();
    let (directive_tx, _signal_rx) = h.open("/").await;
    let directive = Directive::Ack {
        payload: ByteString::from_static("[true]"),
        id: 7,
        attachments: Some(vec![Bytes::from_static(b"\xCA\xFE")]),
    };
    directive_tx.send(directive).await.unwrap();
    assert_eq!(&*h.text().await, "61-7[true]");
    assert_eq!(h.binary().await, Bytes::from_static(b"\xCA\xFE"));
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn server_disconnect_ends_receiver_and_handles() {
    let mut h = spawn();
    let (directive_tx, mut signal_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    signal_rx.recv().await.unwrap();

    h.server("1").await;
    assert!(matches!(signal_rx.recv().await, Some(Signal::Disconnect)));
    assert!(signal_rx.recv().await.is_none());

    // The stale handle is dropped on its next directive, which closes it.
    directive_tx.send(event(r#"["late"]"#, None)).await.unwrap();
    directive_tx.closed().await;
    assert_quiet(&mut h.client_message_rx).await;
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn reopened_namespace_ignores_old_handles() {
    let mut h = spawn();
    let (old_tx, mut old_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    old_rx.recv().await.unwrap();
    h.server("1").await;
    old_rx.recv().await.unwrap();

    let (new_tx, mut new_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    new_rx.recv().await.unwrap();

    old_tx.send(event(r#"["old"]"#, None)).await.unwrap();
    new_tx.send(event(r#"["new"]"#, None)).await.unwrap();
    assert_eq!(&*h.text().await, r#"2["new"]"#);
    assert_quiet(&mut h.client_message_rx).await;
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn duplicate_namespace_is_conflict() {
    let mut h = spawn();
    let (_directive_tx, _signal_rx) = h.open("/").await;
    let (_, directive_rx) = mpsc::channel(1);
    let (signal_tx, _) = mpsc::channel(1);
    let connect_request = ConnectRequest {
        ns: "/".into(),
        payload: ByteString::new(),
        directive_rx,
        signal_tx,
    };
    h.connect_request_tx.send(connect_request).await.unwrap();
    assert!(matches!(
        h.manager.await.unwrap(),
        Err(ManagerError::NamespaceConflict { .. })
    ));
}

#[tokio::test]
async fn late_server_packets_are_discarded() {
    let mut h = spawn();
    h.server(r#"2/gone,["late"]"#).await;
    h.server(r#"30["late"]"#).await;
    h.server("1/gone,").await;

    let (_directive_tx, mut signal_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    assert!(matches!(signal_rx.recv().await, Some(Signal::Connect(_))));
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn dropped_receiver_discards_events() {
    let mut h = spawn();
    let (directive_tx, signal_rx) = h.open("/").await;
    drop(signal_rx);
    h.server(CONNECT_RESPONSE).await;
    h.server(r#"2["ignored"]"#).await;

    directive_tx
        .send(event(r#"["still"]"#, None))
        .await
        .unwrap();
    assert_eq!(&*h.text().await, r#"2["still"]"#);
    h.close_server().await.unwrap();
}

#[tokio::test]
async fn unexpected_binary_is_error() {
    let h = spawn();
    h.server_binary(b"\xFF").await;
    assert!(matches!(
        h.manager.await.unwrap(),
        Err(ManagerError::UnexpectedBinary(_))
    ));
}

#[tokio::test]
async fn text_during_binary_reassembly_is_error() {
    let mut h = spawn();
    let (_directive_tx, mut signal_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    signal_rx.recv().await.unwrap();

    h.server(r#"51-["img"]"#).await;
    h.server(r#"2["oops"]"#).await;
    assert!(matches!(
        h.manager.await.unwrap(),
        Err(ManagerError::UnexpectedText(_))
    ));
}

#[tokio::test]
async fn engine_close_ends_receivers_and_pending_acks() {
    let mut h = spawn();
    let (directive_tx, mut signal_rx) = h.open("/").await;
    h.server(CONNECT_RESPONSE).await;
    signal_rx.recv().await.unwrap();

    let (ack_tx, ack_rx) = oneshot::channel();
    directive_tx
        .send(event(r#"["greet"]"#, Some(ack_tx)))
        .await
        .unwrap();
    h.text().await;

    h.close_server().await.unwrap();
    assert!(signal_rx.recv().await.is_none());
    ack_rx.await.unwrap_err();
}

#[tokio::test]
async fn emits_flow_while_receiver_is_full() {
    let mut h = spawn();
    let (directive_tx, mut signal_rx) = h.open_with("/", 1).await;
    h.server(CONNECT_RESPONSE).await;
    for _ in 0..4 {
        h.server(r#"2["flood"]"#).await;
    }

    directive_tx.send(event(r#"["out"]"#, None)).await.unwrap();
    let text = tokio::time::timeout(Duration::from_secs(5), h.text())
        .await
        .expect("emit stalled behind a full receiver");
    assert_eq!(&*text, r#"2["out"]"#);

    assert!(matches!(signal_rx.recv().await, Some(Signal::Connect(_))));
}

#[tokio::test]
async fn repeated_server_connect_flushes_once() {
    let mut h = spawn();
    let (directive_tx, _signal_rx) = h.open("/").await;
    directive_tx.send(event(r#"["a"]"#, None)).await.unwrap();
    for _ in 0..3 {
        h.server(CONNECT_RESPONSE).await;
    }
    assert_eq!(&*h.text().await, r#"2["a"]"#);
    assert_quiet(&mut h.client_message_rx).await;
    h.close_server().await.unwrap();
}
