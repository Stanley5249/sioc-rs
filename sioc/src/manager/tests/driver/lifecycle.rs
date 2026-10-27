//! How the driver ends: the client handle and namespaces leaving, internal
//! errors, and an aborted task, none of which waits on the application.

use std::time::Duration;

use bytestring::ByteString;
use eioc::prelude::ClientMessage;

use super::harness::*;
use crate::error::{ManagerError, SocketError};
use crate::manager::tests::rig::event;
use crate::packet::ServerPacket;

#[tokio::test]
async fn closes_when_client_handle_drops_with_no_namespace() {
    let manager = TestManager::spawn().await;
    manager.drop_client();
    let TestManager { session, task, .. } = manager;
    let mut session = session.unwrap();
    assert!(session.client_message_rx.recv().await.is_none());
    session.close().await;
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn closes_after_client_handle_and_last_namespace_drop() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    manager.drop_client();
    let TestManager { session, task, .. } = manager;
    let mut session = session.unwrap();

    drop(client_packet_tx);
    assert!(matches!(
        session.client_message_rx.recv().await,
        Some(ClientMessage::Text(text)) if text == "1"
    ));
    assert!(session.client_message_rx.recv().await.is_none());
    session.close().await;
    task.await.unwrap().unwrap();
}

/// An engine stops reading early only by returning an error, which the
/// client ends with instead of a closed channel.
#[tokio::test]
async fn an_engine_that_stops_reading_ends_the_client_with_its_error() {
    let mut manager = TestManager::spawn().await;
    let TestSession {
        server_message_tx,
        client_message_rx,
        result_tx,
    } = manager.session.take().unwrap();
    drop(client_message_rx);
    // The namespace connects before its CONNECT fails, so the client ends
    // with the error.
    let _handles = manager.connect("/", ByteString::new(), 1).unwrap();
    let TestManager {
        task,
        session_rx: _session_rx,
        ..
    } = manager;

    drop(server_message_tx);
    result_tx
        .send(Err(eioc::error::EngineError::ServerMessageClosed.into()))
        .unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(ManagerError::Engine(eioc::error::Error::Engine(
            eioc::error::EngineError::ServerMessageClosed
        )))
    ));
}

#[tokio::test(start_paused = true)]
async fn internal_send_failure_releases_a_full_retained_receiver() {
    let mut manager = TestManager::spawn().await;
    let (tx, _rx) = manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    tx.send(event(r#"["ready"]"#)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2["ready"]"#);
    manager.send_server_message(r#"2["blocked"]"#).await;
    manager.session().client_message_rx.close();
    tx.send(event(r#"["fails"]"#)).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), manager.join())
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

    tokio::time::timeout(Duration::from_secs(1), manager.join())
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
    tx.send(event(r#"["ready"]"#)).await.unwrap();
    manager.recv_client_text().await;
    for message in [r#"2["held"]"#, r#"2["overflow"]"#, "1"] {
        manager.send_server_message(message).await;
    }
    tokio::time::timeout(Duration::from_secs(1), manager.join())
        .await
        .expect("engine cleanup drains without application consumption")
        .unwrap();
    assert!(matches!(rx.recv().await, Some(ServerPacket::Connect(_))));
    // The final namespace termination still reaches its separate terminal
    // channel, even when normal event delivery was full during reclamation.
    assert!(matches!(rx.recv().await, Some(ServerPacket::Disconnect)));
    assert!(rx.recv().await.is_none());
}

/// A session that drops with no open namespace is not reopened until a
/// namespace opens, however long the clock runs.
#[tokio::test(start_paused = true)]
async fn no_session_reopens_without_namespaces_until_one_opens() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (tx, rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    tx.disconnect();
    drop(rx);
    manager.session.take().unwrap().close().await;

    let reopened = tokio::time::timeout(Duration::from_secs(3600), manager.session_rx.recv()).await;
    assert!(reopened.is_err(), "no namespace needs a new session");

    let _handles = manager.connect("/", ByteString::new(), 32).unwrap();
    let reopened = tokio::time::timeout(Duration::from_secs(1), manager.session_rx.recv()).await;
    assert!(matches!(reopened, Ok(Some(_))));
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
        tx.send(event(r#"["after"]"#)).await,
        Err(SocketError::Closed)
    ));
}
