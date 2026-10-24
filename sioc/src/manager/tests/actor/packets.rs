//! Events, acks, and binary packets in both directions.

use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use tokio::sync::oneshot;

use super::harness::*;
use crate::error::SocketError;
use crate::packet::{ClientPacket, DynAck, ServerPacket};

#[tokio::test(start_paused = true)]
async fn unconfirmed_namespace_applies_backpressure_and_flushes_in_order() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open("/").await;
    for _ in 0..32 {
        tx.send(event(r#"["queued"]"#, None)).await.unwrap();
    }
    let pending = tx.send(event(r#"["last"]"#, None));
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
    manager.finish().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn full_unconfirmed_buffer_does_not_block_other_namespaces() {
    let mut manager = TestManager::spawn().await;
    let (tx, _rx) = manager.open("/").await;
    for _ in 0..32 {
        tx.send(event(r#"["queued"]"#, None)).await.unwrap();
    }
    let pending = tx.send(event(r#"["blocked"]"#, None));
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
    other_tx.send(event(r#"["out"]"#, None)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2/other,["out"]"#);
    tx.disconnect();
    assert!(matches!(pending.await, Err(SocketError::Closed)));
    other_tx.disconnect();
    manager.finish().await.unwrap();
}

#[tokio::test]
async fn dropping_last_sender_releases_a_full_unconfirmed_buffer() {
    let mut manager = TestManager::spawn().await;
    let (tx, _rx) = manager.open("/").await;
    for _ in 0..32 {
        tx.send(event(r#"["queued"]"#, None)).await.unwrap();
    }
    drop(tx);
    tokio::time::timeout(Duration::from_secs(1), manager.finish())
        .await
        .expect("dropping the final sender releases buffered work")
        .unwrap();
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
async fn ack_before_server_connect_waits_for_it() {
    let mut manager = TestManager::spawn().await;
    let (client_packet_tx, mut server_packet_rx) = manager.open("/").await;
    client_packet_tx.send(ack(42)).await.unwrap();
    assert_quiet(&mut manager.session().client_message_rx).await;

    // The reply waits in the channel. If its id belongs to an earlier engine,
    // the server ignores it, because its ack ids never repeat.
    manager.send_server_message(CONNECT_RESPONSE).await;
    server_packet_rx.recv().await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, "342[true]");
    client_packet_tx.send(ack(43)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, "343[true]");
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
async fn late_ack_never_resolves_a_reopened_namespace() {
    let mut manager = TestManager::spawn().await;
    // Keeps the engine open.
    let (other_tx, mut other_rx) = manager.open("/other").await;
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

/// A timed-out ack handle stops waiting, but its event still goes out once
/// the server confirms the namespace.
#[tokio::test]
async fn ack_timeout_keeps_the_event() {
    let mut manager = TestManager::spawn().await;
    let (tx, _rx) = manager.open("/").await;
    let (ack_tx, ack_rx) = oneshot::channel::<DynAck>();
    tx.send(event(r#"["slow"]"#, Some(ack_tx))).await.unwrap();

    let waited = tokio::time::timeout(Duration::from_millis(10), ack_rx).await;
    assert!(waited.is_err(), "nothing answers an unsent event");

    manager.send_server_message(CONNECT_RESPONSE).await;
    assert_eq!(&*manager.recv_client_text().await, r#"20["slow"]"#);
    manager.send_server_message(r#"30["late"]"#).await;
    manager.finish().await.unwrap();
}
