//! Opening, closing, and reusing namespaces and the client.

use std::time::Duration;

use bytestring::ByteString;
use eioc::prelude::Message;
use tokio::sync::mpsc::error::TryRecvError;

use super::harness::*;
use crate::error::{ManagerError, SocketError};
use crate::manager::connect_request::ConnectRequest;
use crate::packet::ServerPacket;

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
async fn closes_engine_after_last_namespace_while_client_handle_lives() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    client_packet_tx.disconnect();
    assert_eq!(&*manager.recv_client_text().await, "1");
    assert!(manager.session().client_message_rx.recv().await.is_none());
    manager.session.take().unwrap().end().await;

    // The next namespace opens the next engine.
    let handles = request_namespace(&manager.connect_request_tx, "/next").await;
    manager.session = manager.session_rx.recv().await;
    assert_eq!(&*manager.recv_client_text().await, "0/next,");
    drop(handles);
    manager.finish().await.unwrap();
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

    // The server closed the namespace, so dropping the handles sends no
    // DISCONNECT, and with no namespace left the engine closes.
    drop(client_packet_tx);
    assert!(manager.session().client_message_rx.recv().await.is_none());
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn connect_error_closes_namespace() {
    let mut manager = TestManager::spawn().await;
    // Keeps the engine open.
    let (_keep_tx, _keep_rx) = manager.open("/keep").await;
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
    // Keeps the engine open.
    let (_keep_tx, _keep_rx) = manager.open("/keep").await;
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
        server_message_tx,
        client_message_rx,
        result_tx,
    } = session.unwrap();
    drop(client_message_rx);
    let (connect_request, handles) = ConnectRequest::new("/".into(), ByteString::new(), 1, 1);
    connect_request_tx.send(connect_request).await.unwrap();
    // The namespace opens before its CONNECT fails, so the reply succeeds and
    // the client ends with the error.
    handles.reply_rx.await.unwrap().unwrap();

    drop(server_message_tx);
    drop(result_tx);
    assert!(matches!(
        task.await.unwrap(),
        Err(ManagerError::EngineEnded)
    ));
}

#[tokio::test]
async fn internal_send_failure_releases_a_full_retained_receiver() {
    let mut manager = TestManager::spawn().await;
    let (tx, _rx) = manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    tx.send(event(r#"["ready"]"#, None)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2["ready"]"#);
    manager.send_server_message(r#"2["blocked"]"#).await;
    manager.session().client_message_rx.close();
    tx.send(event(r#"["fails"]"#, None)).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), manager.finish())
        .await
        .expect("a broken engine must release blocked delivery");
    // The manager sees the closed receiver on its next send, the engine
    // finishing while it still holds the sender, or the engine hanging up
    // first, whichever comes first. Only the first two are failures.
    if let Err(error) = result {
        assert!(error.is_internal(), "{error:?}");
    }
}

#[tokio::test]
async fn local_disconnect_finishes_with_a_full_retained_receiver() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    manager.send_server_message(r#"2["first"]"#).await;
    manager.send_server_message(r#"2["blocked"]"#).await;
    tx.disconnect();

    tokio::time::timeout(Duration::from_secs(1), manager.finish())
        .await
        .expect("disconnect must release blocked delivery")
        .unwrap();
    while rx.recv().await.is_some() {}
}

#[tokio::test]
async fn server_disconnect_finishes_and_keeps_its_terminal_packet() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    manager.send_server_message(r#"2["first"]"#).await;
    manager.send_server_message("1").await;
    tx.closed().await;

    tokio::time::timeout(Duration::from_secs(1), manager.finish())
        .await
        .expect("terminal delivery must not wait for channel capacity")
        .unwrap();
    assert!(matches!(rx.recv().await, Some(ServerPacket::Event(_))));
    assert!(matches!(rx.recv().await, Some(ServerPacket::Disconnect)));
    assert!(rx.recv().await.is_none());
}

/// Aborting the manager task ends every handle instead of leaving it waiting.
#[tokio::test]
async fn aborting_the_manager_ends_the_handles() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    assert!(matches!(rx.recv().await, Some(ServerPacket::Connect(_))));

    manager.task.abort();
    assert!(manager.task.await.unwrap_err().is_cancelled());
    assert!(rx.recv().await.is_none());
    assert!(matches!(
        tx.send(event(r#"["after"]"#, None)).await,
        Err(SocketError::Closed)
    ));
}
