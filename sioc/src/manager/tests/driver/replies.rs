//! Replies end with their connection while unsent events survive.

use std::future::Future;

use bytestring::ByteString;
use eioc::prelude::{ClientMessage, ServerMessage};

use super::harness::*;
use crate::client::{SocketReceiver, SocketSender};
use crate::config::ChannelConfig;
use crate::error::SocketError;
use crate::manager::tests::rig::{ack, event};
use crate::marker::{AckMarker, HasAck};
use crate::packet::{DynAckId, ServerPacket};

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

    // The server asks 33 times on this connection. The 32 replies fill the
    // queue and the last one waits.
    let mut ids = Vec::new();
    for id in (0..32).chain([999]) {
        ids.push(ask(&mut manager, &mut rx, "", id).await);
    }
    let blocked_id = ids.pop().unwrap();

    // At most one packet can enter the old engine. The rest remain bounded
    // in the two namespace inputs without a consumer on the engine side.
    for _ in 0..32 {
        within("queue events", tx.send(event(r#"["queued"]"#)))
            .await
            .unwrap();
    }
    for id in ids {
        within("queue ACKs", tx.send(ack(id))).await.unwrap();
    }
    let old = manager.session.take().unwrap();
    let old_messages = {
        let blocked = tx.send(ack(blocked_id));
        tokio::pin!(blocked);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(blocked.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let messages = within("old engine EOF", old.close()).await;
        assert!(matches!(
            within("blocked ACK released", blocked).await,
            Err(SocketError::Closed)
        ));
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
    let fresh = ask(&mut manager, &mut rx, "", 1000).await;
    tx.send(ack(fresh)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, "31000[]");
    tx.disconnect();
    within("final manager cleanup", manager.join())
        .await
        .unwrap();
}

#[tokio::test]
async fn replies_while_unconfirmed_are_rejected_instead_of_buffered() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (tx, mut rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    let old = ask(&mut manager, &mut rx, "", 1).await;
    manager.session.take().unwrap().close().await;
    manager.session = manager.session_rx.recv().await;
    assert!(manager.recv_client_text().await.starts_with('0'));

    // The new engine has not confirmed the namespace, so the reply for the
    // old one is rejected, and nothing is kept for the new one.
    assert!(matches!(tx.send(ack(old)).await, Err(SocketError::Closed)));
    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    tx.send(event(r#"["confirmed"]"#)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2["confirmed"]"#);
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
    let id = ask(&mut manager, &mut rx, "", 7).await;
    tx.send(ack(id)).await.unwrap();
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

#[tokio::test(start_paused = true)]
async fn a_delayed_reply_does_not_cross_into_the_next_engine() {
    let mut manager = TestManager::spawn_reconnecting().await;
    let (tx, mut rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    within("server confirmation", rx.recv()).await.unwrap();
    let id = ask(&mut manager, &mut rx, "", 5).await;
    let id = HasAck::<()>::parse(Some(id)).unwrap();

    within("old engine EOF", manager.session.take().unwrap().close()).await;
    manager.session = within("new engine opens", manager.session_rx.recv()).await;
    assert!(
        within("new engine CONNECT", manager.recv_client_text())
            .await
            .starts_with('0')
    );
    manager.send_server_message(CONNECT_RESPONSE).await;
    within("server confirmation", rx.recv()).await.unwrap();

    // The reply was due before the reconnect, so it is rejected.
    assert!(matches!(
        within("delayed reply", tx.acknowledge(id, ())).await,
        Err(SocketError::Closed)
    ));
    within("later event", tx.send(event(r#"["later"]"#)))
        .await
        .unwrap();
    assert_eq!(
        &*within("next client text", manager.recv_client_text()).await,
        r#"2["later"]"#
    );
    tx.disconnect();
    within("final manager cleanup", manager.join())
        .await
        .unwrap();
}

/// Handles of `/` whose event and reply capacities are 1, with three ids the
/// server asked for. The first reply enters the blocked engine, the second
/// holds the only reply slot, and the third waits for it.
async fn blocked_replies(
    manager: &mut TestManager,
) -> (SocketSender, SocketReceiver, Vec<DynAckId>) {
    let (tx, mut rx) = within(
        "open",
        crate::client::connect(
            &manager.manager,
            "/".into(),
            ByteString::new(),
            ChannelConfig {
                sender: 1,
                ..ChannelConfig::from(32)
            },
        ),
    )
    .await
    .unwrap();
    assert!(manager.recv_client_text().await.starts_with('0'));
    manager.send_server_message(CONNECT_RESPONSE).await;
    within("server confirmation", rx.recv()).await.unwrap();
    let mut ids = Vec::new();
    for id in 1..=3 {
        ids.push(ask(manager, &mut rx, "", id).await);
    }
    within("first reply", tx.send(ack(ids[0].clone())))
        .await
        .unwrap();
    within("second reply", tx.send(ack(ids[1].clone())))
        .await
        .unwrap();
    (tx, rx, ids)
}

async fn is_pending<F: Future>(future: std::pin::Pin<&mut F>) -> bool {
    let mut future = future;
    std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx).is_pending())).await
}

#[tokio::test(start_paused = true)]
async fn a_reply_waiting_for_capacity_is_rejected_when_its_connection_ends() {
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
    let (tx, mut rx, ids) = blocked_replies(&mut manager).await;

    let TestSession {
        server_message_tx,
        mut client_message_rx,
        result_tx,
    } = manager.session.take().unwrap();
    let waiting = tx.send(ack(ids[2].clone()));
    tokio::pin!(waiting);
    assert!(is_pending(waiting.as_mut()).await);
    // The session fails without reading the client channel, so no reply slot
    // frees before the connection ends.
    drop(server_message_tx);
    result_tx
        .send(Err(eioc::error::EngineError::HeartbeatTimeout.into()))
        .unwrap();
    assert!(matches!(
        within("waiting reply released", waiting).await,
        Err(SocketError::Closed)
    ));
    let mut old_messages = Vec::new();
    while let Some(message) = within("old engine EOF", client_message_rx.recv()).await {
        old_messages.push(message);
    }
    assert!(
        !old_messages
            .iter()
            .any(|message| matches!(message, ClientMessage::Text(text) if text == "33[]"))
    );

    manager.session = within("new engine opens", manager.session_rx.recv()).await;
    assert!(
        within("new engine CONNECT", manager.recv_client_text())
            .await
            .starts_with('0')
    );
    manager.send_server_message(CONNECT_RESPONSE).await;
    within("server confirmation", rx.recv()).await.unwrap();
    tx.send(event(r#"["later"]"#)).await.unwrap();
    assert_eq!(
        &*within("next client text", manager.recv_client_text()).await,
        r#"2["later"]"#
    );
    tx.disconnect();
    within("final manager cleanup", manager.join())
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_reply_waiting_for_capacity_fails_when_the_handles_disconnect() {
    let mut manager = TestManager::spawn_with_channels(
        None,
        ChannelConfig {
            engine: 1,
            ..ChannelConfig::from(32)
        },
    )
    .await;
    let (tx, _rx, ids) = blocked_replies(&mut manager).await;

    let waiting = tx.send(ack(ids[2].clone()));
    tokio::pin!(waiting);
    assert!(is_pending(waiting.as_mut()).await);
    tx.disconnect();
    assert!(matches!(
        within("waiting reply released", waiting).await,
        Err(SocketError::Closed)
    ));
    within("final manager cleanup", manager.join())
        .await
        .unwrap();
}

/// The server asks for an ack with wire id `id` on `ns_prefix`'s namespace
/// (empty for `/`, else `"/ns,"`), and the receiver hands over the id as the
/// manager bound it.
pub async fn ask(
    manager: &mut TestManager,
    rx: &mut SocketReceiver,
    ns_prefix: &str,
    id: u64,
) -> DynAckId {
    let text = format!(r#"2{ns_prefix}{id}["ask"]"#);
    manager
        .session()
        .server_message_tx
        .send(ServerMessage::Text(text.into()))
        .await
        .unwrap();
    match within("asked event", rx.recv()).await {
        Some(ServerPacket::Event(event)) => event.id.expect("the event asks for a reply"),
        other => panic!("expected an event, got {other:?}"),
    }
}

async fn within<T>(stage: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(3), future)
        .await
        .unwrap_or_else(|_| panic!("stalled during {stage}"))
}
