//! Engine loss, protocol errors, and reconnection.

use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{Message, ServerMessage};
use tokio::sync::oneshot;

use super::harness::*;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::packet::ServerPacket;

#[tokio::test]
async fn connect_while_closing_opens_the_next_engine_at_once() {
    let hour = Duration::from_secs(3600);
    let mut manager = TestManager::spawn_with(Some(Backoff::new(hour, hour, 0.0, None))).await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    client_packet_tx.disconnect();
    assert_eq!(&*manager.recv_client_text().await, "1");
    assert!(manager.session().client_message_rx.recv().await.is_none());

    // The request arrives before the engine finishes closing, and the next
    // engine skips the backoff delay.
    let handles = request_namespace(&manager.connect_request_tx, "/next").await;
    manager.session.take().unwrap().end().await;
    manager.session = tokio::time::timeout(Duration::from_secs(5), manager.session_rx.recv())
        .await
        .expect("the next engine opens at once");
    assert_eq!(&*manager.recv_client_text().await, "0/next,");
    drop(handles);
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn reconnection_stops_when_the_last_namespace_closes() {
    let delay = Duration::from_millis(50);
    let mut manager = TestManager::spawn_with(Some(Backoff::new(delay, delay, 0.0, None))).await;
    let (client_packet_tx, _server_packet_rx) = manager.open("/").await;
    manager.session.take().unwrap().end().await;
    drop(client_packet_tx);

    let next = tokio::time::timeout(delay * 4, manager.session_rx.recv()).await;
    assert!(next.is_err(), "unexpected session");
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn giving_up_closes_namespaces_and_keeps_the_client() {
    let mut manager = TestManager::spawn().await;
    let (_client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.session.take().unwrap().end().await;
    assert!(server_packet_rx.recv().await.is_none());

    let handles = request_namespace(&manager.connect_request_tx, "/next").await;
    manager.session = manager.session_rx.recv().await;
    assert_eq!(&*manager.recv_client_text().await, "0/next,");
    drop(handles);
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn invalid_binary_placeholders_reconnect() {
    for payload in [
        r#"51-["x",{"_placeholder":true,"num":1}]"#,
        r#"51-["x",{"nested":[{"_placeholder":true,"num":-1}]}]"#,
        r#"51-["x",{"_placeholder":true,"num":0.5}]"#,
        r#"51-["x",{"_placeholder":true}]"#,
        r#"61-0[{"_placeholder":true,"num":1}]"#,
    ] {
        assert_protocol_error_reconnects(&[ServerMessage::Text(payload.into())]).await;
    }
}

#[tokio::test]
async fn unexpected_binary_reconnects() {
    assert_protocol_error_reconnects(&[ServerMessage::Binary(Bytes::from_static(b"\xFF"))]).await;
}

#[tokio::test]
async fn text_during_binary_reassembly_reconnects() {
    assert_protocol_error_reconnects(&[
        ServerMessage::Text(ByteString::from_static(r#"51-["img"]"#)),
        ServerMessage::Text(ByteString::from_static(r#"2["oops"]"#)),
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
