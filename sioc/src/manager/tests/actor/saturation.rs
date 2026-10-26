//! Full queues, stalled receivers, and floods: what the actor waits for and
//! what it keeps serving meanwhile.

use std::time::Duration;

use bytestring::ByteString;
use eioc::prelude::{ClientMessage, ServerMessage};
use tokio::sync::mpsc;

use super::harness::*;
use crate::config::ChannelConfig;
use crate::error::SocketError;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::tests::rig::event;
use crate::packet::ServerPacket;

#[tokio::test(start_paused = true)]
async fn emits_flow_while_receiver_is_full() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) =
        manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    for _ in 0..4 {
        manager.send_server_message(r#"2["flood"]"#).await;
    }

    client_packet_tx.send(event(r#"["out"]"#)).await.unwrap();
    let text = tokio::time::timeout(Duration::from_secs(5), manager.recv_client_text())
        .await
        .expect("emit stalled behind a full receiver");
    assert_eq!(&*text, r#"2["out"]"#);

    assert!(matches!(
        server_packet_rx.recv().await,
        Some(ServerPacket::Connect(_))
    ));
}

/// The application emits once per server event while the server floods and
/// every queue holds two items, the deadlock that `44b0c3f` fixed.
#[tokio::test(start_paused = true)]
async fn emits_while_handling_a_server_flood() {
    const FLOOD: usize = 2000;

    let (connect_request_tx, connect_request_rx) = mpsc::channel(4);
    let (session_tx, mut session_rx) = mpsc::channel(1);
    let connect_engine = move || spawn_session(ChannelConfig::from(2), session_tx.clone());
    let task = tokio::spawn(crate::manager::actor::run(
        connect_request_rx,
        connect_engine,
        None,
    ));
    let TestSession {
        server_message_tx,
        mut client_message_rx,
        result_tx,
    } = session_rx.recv().await.unwrap();

    let (connect_request, handles) = ConnectRequest::new("/".into(), ByteString::new(), 2, 1);
    connect_request_tx.send(connect_request).await.unwrap();
    let (tx, mut rx, reply_rx) = handles.into_socket();
    reply_rx.await.unwrap().unwrap();

    // The engine reads client messages independently of the server direction.
    let reader = tokio::spawn(async move {
        let mut events = 0;
        while events < FLOOD {
            match client_message_rx.recv().await {
                Some(ClientMessage::Text(text)) if text.starts_with('2') => events += 1,
                Some(_) => {}
                None => panic!("the manager closed the engine during the flood"),
            }
        }
        client_message_rx
    });
    let flooder = tokio::spawn({
        let server_message_tx = server_message_tx.clone();
        async move {
            let connect = ServerMessage::Text(CONNECT_RESPONSE.into());
            server_message_tx.send(connect).await.unwrap();
            for _ in 0..FLOOD {
                let flood = ServerMessage::Text(r#"2["flood"]"#.into());
                server_message_tx.send(flood).await.unwrap();
            }
        }
    });
    let app = tokio::spawn(async move {
        let mut received = 0;
        while received < FLOOD {
            match rx.recv().await {
                Some(ServerPacket::Event(_)) => {
                    received += 1;
                    tx.send(event(r#"["reply"]"#)).await.unwrap();
                }
                Some(_) => {}
                None => panic!("the receiver ended during the flood"),
            }
        }
        (tx, rx)
    });

    let finished = async {
        flooder.await.unwrap();
        let client_message_rx = reader.await.unwrap();
        let handles = app.await.unwrap();
        (client_message_rx, handles)
    };
    let (mut client_message_rx, handles) = tokio::time::timeout(Duration::from_secs(20), finished)
        .await
        .expect("emitting while handling a flood never deadlocks");

    drop(handles);
    drop(connect_request_tx);
    drop(server_message_tx);
    while client_message_rx.recv().await.is_some() {}
    result_tx.send(Ok(())).unwrap();
    task.await.unwrap().unwrap();
}

/// A full receiver holds one packet, which stops reading the engine. The
/// engine's hang-up must still close it, so an event emitted afterwards waits
/// for the next engine instead of going into the session that only drains.
#[tokio::test(start_paused = true)]
async fn held_packet_keeps_later_events_for_the_next_engine() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (tx, mut rx) = manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    // The Connect packet fills the receiver, so the manager holds this one.
    manager.send_server_message(r#"2["held"]"#).await;

    let old = manager.session.take().expect("a session is open");
    drop(old.server_message_tx);

    // The old engine gets nothing more, and closes once the manager lets go.
    let mut client_message_rx = old.client_message_rx;
    assert!(client_message_rx.recv().await.is_none());
    tx.send(event(r#"["late"]"#)).await.unwrap();
    old.result_tx.send(Ok(())).unwrap();

    tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("the original held packet survives cleanup")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("the original held packet survives cleanup")
        .unwrap();
    manager.session = tokio::time::timeout(Duration::from_secs(1), manager.session_rx.recv())
        .await
        .expect("the next engine opens after cleanup");
    assert!(manager.recv_client_text().await.starts_with('0'));
    manager.send_server_message(CONNECT_RESPONSE).await;
    assert_eq!(&*manager.recv_client_text().await, r#"2["late"]"#);
}

/// A held packet stops reading the engine, so the session's end arrives behind
/// a message the manager has not read. The engine must still be released, and
/// the next one opened, without the application reading its receiver.
#[tokio::test(start_paused = true)]
async fn an_engine_end_behind_unread_messages_needs_no_application_read() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (_tx, mut rx) = manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    // The Connect packet fills the receiver, so the manager holds this one.
    manager.send_server_message(r#"2["held"]"#).await;
    manager.send_server_message(r#"2["behind"]"#).await;
    // The paused clock moves only once every task waits, so the manager has
    // taken what it reads before the session ends behind it.
    tokio::time::sleep(Duration::from_millis(1)).await;

    let old = manager.session.take().expect("a session is open");
    drop(old.server_message_tx);
    let mut client_message_rx = old.client_message_rx;
    tokio::time::timeout(Duration::from_secs(1), client_message_rx.recv())
        .await
        .expect("the manager hangs up the ended engine");
    old.result_tx.send(Ok(())).unwrap();
    manager.session = tokio::time::timeout(Duration::from_secs(1), manager.session_rx.recv())
        .await
        .expect("the next engine opens");

    // The held packet keeps its receiver, and the overflow behind it is gone.
    assert!(matches!(rx.recv().await, Some(ServerPacket::Connect(_))));
    assert!(matches!(rx.recv().await, Some(ServerPacket::Event(_))));
    assert_eq!(rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
}

/// Every queue holds one item. A receiver the application stops reading holds
/// up what the server sends to the whole connection, as the README documents,
/// but the other namespace still emits, and both flow again once it reads.
#[tokio::test]
async fn a_stalled_receiver_stops_only_incoming_data() {
    let mut manager = TestManager::spawn_with_channels(None, ChannelConfig::from(1)).await;
    let (_a_tx, mut a_rx) = manager.open_with("/a", ByteString::new(), 1).await;
    let (b_tx, mut b_rx) = manager.open_with("/b", ByteString::new(), 1).await;
    manager.send_server_message(r#"0/a,{"sid":"a"}"#).await;
    manager.send_server_message(r#"0/b,{"sid":"b"}"#).await;
    // `/a` holds its Connect, so this packet waits in the manager.
    manager.send_server_message(r#"2/a,["held"]"#).await;
    manager.send_server_message(r#"2/b,["queued"]"#).await;

    b_tx.send(event(r#"["out"]"#)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2/b,["out"]"#);

    assert!(matches!(b_rx.recv().await, Some(ServerPacket::Connect(_))));
    assert!(matches!(a_rx.recv().await, Some(ServerPacket::Connect(_))));
    assert!(matches!(a_rx.recv().await, Some(ServerPacket::Event(_))));
    assert!(matches!(b_rx.recv().await, Some(ServerPacket::Event(_))));
}

#[tokio::test(start_paused = true)]
async fn unconfirmed_namespace_applies_backpressure_and_flushes_in_order() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open("/").await;
    for _ in 0..32 {
        tx.send(event(r#"["queued"]"#)).await.unwrap();
    }
    let pending = tx.send(event(r#"["last"]"#));
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut pending)
            .await
            .is_err(),
        "the unconfirmed buffer must apply backpressure"
    );

    manager.send_server_message(CONNECT_RESPONSE).await;
    let receive = async {
        for _ in 0..32 {
            assert_eq!(&*manager.recv_client_text().await, r#"2["queued"]"#);
        }
        assert_eq!(&*manager.recv_client_text().await, r#"2["last"]"#);
    };
    let ((), sent) = tokio::join!(receive, &mut pending);
    sent.unwrap();
    assert!(matches!(rx.recv().await, Some(ServerPacket::Connect(_))));
    tx.disconnect();
    manager.join().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn full_unconfirmed_buffer_does_not_block_other_namespaces() {
    let mut manager = TestManager::spawn().await;
    let (tx, _rx) = manager.open("/").await;
    for _ in 0..32 {
        tx.send(event(r#"["queued"]"#)).await.unwrap();
    }
    let pending = tx.send(event(r#"["blocked"]"#));
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut pending)
            .await
            .is_err(),
        "the first namespace must be backpressured"
    );

    let (other_tx, mut other_rx) = manager.open("/other").await;
    manager
        .send_server_message(r#"0/other,{"sid":"other"}"#)
        .await;
    other_rx.recv().await.unwrap();
    other_tx.send(event(r#"["out"]"#)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2/other,["out"]"#);
    tx.disconnect();
    assert!(matches!(pending.await, Err(SocketError::Closed)));
    other_tx.disconnect();
    manager.join().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn dropping_last_sender_releases_a_full_unconfirmed_buffer() {
    let mut manager = TestManager::spawn().await;
    let (tx, _rx) = manager.open("/").await;
    for _ in 0..32 {
        tx.send(event(r#"["queued"]"#)).await.unwrap();
    }
    drop(tx);
    tokio::time::timeout(Duration::from_secs(1), manager.join())
        .await
        .expect("dropping the final sender releases buffered work")
        .unwrap();
}
