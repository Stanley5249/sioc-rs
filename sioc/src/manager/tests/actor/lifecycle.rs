//! How the actor ends: the client handle and namespaces leaving, internal
//! errors, and an aborted task, none of which waits on the application.

use std::time::Duration;

use bytestring::ByteString;
use eioc::prelude::Message;

use super::harness::*;
use crate::error::{ManagerError, SocketError};
use crate::manager::connect_request::ConnectRequest;
use crate::packet::ServerPacket;

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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
async fn peer_close_drains_final_messages_without_an_application_consumer() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    // An emitted event proves the namespace was confirmed while Connect
    // remains in the full application queue.
    tx.send(event(r#"["ready"]"#, None)).await.unwrap();
    manager.recv_client_text().await;
    for message in [r#"2["held"]"#, r#"2["overflow"]"#, "1"] {
        manager.send_server_message(message).await;
    }
    tokio::time::timeout(Duration::from_secs(1), manager.finish())
        .await
        .expect("engine cleanup drains without application consumption")
        .unwrap();
    assert!(matches!(rx.recv().await, Some(ServerPacket::Connect(_))));
    // The final namespace termination still reaches its separate terminal
    // channel, even when normal event delivery was full during reclamation.
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
