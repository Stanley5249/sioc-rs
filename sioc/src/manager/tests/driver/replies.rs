//! Replies end with their connection while unsent events survive.

use std::future::Future;

use bytestring::ByteString;
use eioc::prelude::ClientMessage;

use super::harness::*;
use crate::config::ChannelConfig;
use crate::manager::tests::rig::{ack, event};

#[tokio::test(start_paused = true)]
async fn queued_and_blocked_replies_end_with_their_engine_while_events_survive() {
    let mut manager = TestManager::spawn_with_channels(
        Some(crate::manager::backoff::Backoff::new(
            std::time::Duration::ZERO,
            std::time::Duration::ZERO,
            0.0,
            None,
        )),
        ChannelConfig {
            engine: 1,
            ..ChannelConfig::from(32)
        },
    )
    .await;
    let (tx, mut rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    within("server confirmation", rx.recv()).await.unwrap();

    // At most one packet can enter the old engine. The rest remain bounded
    // in the two namespace inputs without a consumer on the engine side.
    for _ in 0..32 {
        within("queue events", tx.send(event(r#"["queued"]"#)))
            .await
            .unwrap();
    }
    for id in 0..32 {
        within("queue ACKs", tx.send(ack(id))).await.unwrap();
    }
    let old = manager.session.take().unwrap();
    let old_messages = {
        let blocked = tx.send(ack(999));
        tokio::pin!(blocked);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(blocked.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let messages = within("old engine EOF", old.close()).await;
        within("blocked ACK released", blocked).await.unwrap();
        messages
    };

    manager.session = within("new engine opens", manager.session_rx.recv()).await;
    assert!(
        within("new engine CONNECT", manager.recv_client_text())
            .await
            .starts_with('0')
    );
    manager.send_server_message(CONNECT_RESPONSE).await;
    within("server confirmation", rx.recv()).await.unwrap();
    let old_events = old_messages
        .iter()
        .filter(|message| {
            matches!(message,
                ClientMessage::Text(text) if text == r#"2["queued"]"#
            )
        })
        .count();
    let receive = async {
        let mut events = 0;
        loop {
            let text = within("buffered events replay", manager.recv_client_text()).await;
            if text == r#"2["last"]"# {
                break;
            }
            assert_eq!(
                &*text, r#"2["queued"]"#,
                "an old ACK cannot reach the next engine"
            );
            events += 1;
        }
        events
    };
    let (sent, new_events) = tokio::join!(tx.send(event(r#"["last"]"#)), receive);
    sent.unwrap();
    assert_eq!(old_events + new_events, 32);
    tx.send(ack(1000)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, "31000[]");
    tx.disconnect();
    within("final manager cleanup", manager.join())
        .await
        .unwrap();
}

#[tokio::test]
async fn replies_while_unconfirmed_are_discarded_instead_of_buffered() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (tx, mut rx) = manager.open("/").await;
    for id in [1, 2] {
        tx.send(ack(id)).await.unwrap();
        manager.send_server_message(CONNECT_RESPONSE).await;
        rx.recv().await.unwrap();
        tx.send(event(r#"["confirmed"]"#)).await.unwrap();
        assert_eq!(&*manager.recv_client_text().await, r#"2["confirmed"]"#);
        if id == 1 {
            manager.session.take().unwrap().close().await;
            manager.session = manager.session_rx.recv().await;
            assert!(manager.recv_client_text().await.starts_with('0'));
        }
    }
    tx.disconnect();
    manager.join().await.unwrap();
}

#[tokio::test]
async fn local_disconnect_drains_replies_before_releasing_the_name() {
    let mut manager = TestManager::spawn_with_channels(
        None,
        ChannelConfig {
            engine: 1,
            ..ChannelConfig::from(32)
        },
    )
    .await;
    let (tx, mut rx) = manager.open_with("/", ByteString::new(), 1).await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    tx.send(ack(7)).await.unwrap();
    tx.send(event(r#"["last"]"#)).await.unwrap();
    tx.disconnect();
    let TestSession {
        server_message_tx,
        mut client_message_rx,
        result_tx,
    } = manager.session.take().unwrap();
    let mut messages = Vec::new();
    while let Some(message) = client_message_rx.recv().await {
        messages.push(message);
    }
    drop(server_message_tx);
    result_tx.send(Ok(())).unwrap();
    let text: Vec<_> = messages
        .into_iter()
        .map(|message| {
            let ClientMessage::Text(text) = message else {
                panic!("these packets have no binary attachments");
            };
            text
        })
        .collect();
    assert_eq!(text, ["37[]", r#"2["last"]"#, "1"]);
    manager.join().await.unwrap();
}

async fn within<T>(stage: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(3), future)
        .await
        .unwrap_or_else(|_| panic!("stalled during {stage}"))
}
