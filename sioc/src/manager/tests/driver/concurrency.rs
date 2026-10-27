//! Handles that call the core from many tasks at once: what they accepted
//! reaches the engine in order, before DISCONNECT, and nothing after it.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use eioc::prelude::ClientMessage;
use tokio::sync::oneshot;

use super::harness::*;
use crate::client::SocketSender;
use crate::error::SocketError;
use crate::manager::tests::rig::event;
use crate::packet::ClientPacket;

const TASKS: usize = 8;

/// Emits `[task,n]` with increasing `n` until the namespace closes or `limit`
/// events went out, and returns how many were accepted.
async fn emit_until_closed(tx: SocketSender, task: usize, limit: usize) -> usize {
    let mut sent = 0;
    while sent < limit {
        let packet = ClientPacket::Event {
            payload: format!("[{task},{sent}]").into(),
            ack_tx: None,
            attachments: None,
        };
        match tx.send(packet).await {
            Ok(()) => sent += 1,
            Err(SocketError::Closed) => break,
            Err(error) => panic!("{error}"),
        }
    }
    sent
}

/// Reads the engine until the manager hangs up, and reports once `notify_at`
/// messages arrived.
fn read_engine(
    mut session: TestSession,
    notify_at: usize,
) -> (
    oneshot::Receiver<()>,
    tokio::task::JoinHandle<(Vec<String>, TestSession)>,
) {
    let (started_tx, started_rx) = oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut started_tx = Some(started_tx);
        let mut texts = Vec::new();
        while let Some(message) = session.client_message_rx.recv().await {
            let ClientMessage::Text(text) = message else {
                panic!("these events have no attachments");
            };
            texts.push(text.to_string());
            if texts.len() >= notify_at
                && let Some(started_tx) = started_tx.take()
            {
                let _ = started_tx.send(());
            }
        }
        (texts, session)
    });
    (started_rx, reader)
}

/// Checks that `texts` holds every accepted event in per-task order, then
/// exactly one DISCONNECT.
fn assert_events_then_disconnect(texts: &[String], accepted: usize) {
    let (last, events) = texts.split_last().expect("DISCONNECT arrives");
    assert_eq!(last, "1", "DISCONNECT comes last");
    assert_eq!(events.len(), accepted, "every accepted event, and no other");
    let mut next = [0; TASKS];
    for text in events {
        let numbers = text
            .strip_prefix("2[")
            .and_then(|rest| rest.strip_suffix(']'))
            .expect("an event of `/`");
        let (task, n) = numbers.split_once(',').unwrap();
        let (task, n): (usize, usize) = (task.parse().unwrap(), n.parse().unwrap());
        assert_eq!(n, next[task], "task {task} keeps its order");
        next[task] += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_emits_and_disconnect_lose_no_accepted_event() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    let (started, reader) = read_engine(manager.session.take().unwrap(), 200);

    let emitters: Vec<_> = (0..TASKS)
        .map(|task| tokio::spawn(emit_until_closed(tx.clone(), task, usize::MAX)))
        .collect();
    started.await.unwrap();
    tx.disconnect();
    let mut accepted = 0;
    for emitter in emitters {
        accepted += emitter.await.unwrap();
    }

    let (texts, session) = tokio::time::timeout(Duration::from_secs(10), reader)
        .await
        .expect("the manager closes the engine after DISCONNECT")
        .unwrap();
    assert_events_then_disconnect(&texts, accepted);
    drop(tx);
    manager.session = Some(session);
    manager.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrently_dropped_clones_disconnect_once_after_every_event() {
    const EVENTS: usize = 100;

    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    let (_started, reader) = read_engine(manager.session.take().unwrap(), usize::MAX);

    // Each task drops its clone when it is done, and the last drop
    // disconnects.
    let emitters: Vec<_> = (0..TASKS)
        .map(|task| tokio::spawn(emit_until_closed(tx.clone(), task, EVENTS)))
        .collect();
    drop(tx);
    let mut accepted = 0;
    for emitter in emitters {
        accepted += emitter.await.unwrap();
    }
    assert_eq!(accepted, TASKS * EVENTS);

    let (texts, session) = tokio::time::timeout(Duration::from_secs(10), reader)
        .await
        .expect("the manager closes the engine after DISCONNECT")
        .unwrap();
    assert_events_then_disconnect(&texts, accepted);
    manager.session = Some(session);
    manager.join().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_panicking_handle_wakes_idle_driver_and_closes_handles() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open("/").await;
    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    let manager_core = Arc::clone(&manager.manager);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            manager_core.with_core(|_| panic!("transition failed"));
        }))
        .is_err()
    );
    let TestManager { session, task, .. } = manager;
    tokio::time::timeout(Duration::from_secs(1), async {
        assert!(session.unwrap().client_message_rx.recv().await.is_none());
        task.await
    })
    .await
    .expect("poison wakes the idle driver and closes engine handles")
    .unwrap_err();
    // The shutdown guard ends the poisoned core's handles.
    tx.closed().await;
    assert!(matches!(
        tx.send(event(r#"["closed"]"#)).await,
        Err(SocketError::Closed)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_emit_queues_nothing() {
    let mut manager = TestManager::spawn().await;
    let (tx, mut rx) = manager.open("/").await;
    for _ in 0..32 {
        tx.send(event(r#"["queued"]"#)).await.unwrap();
    }
    // The unconfirmed buffer is full, so this emit waits until it is dropped.
    let waiting = tokio::time::timeout(Duration::from_secs(1), tx.send(event(r#"["cancelled"]"#)));
    waiting.await.unwrap_err();

    manager.send_server_message(CONNECT_RESPONSE).await;
    rx.recv().await.unwrap();
    for _ in 0..32 {
        assert_eq!(&*manager.recv_client_text().await, r#"2["queued"]"#);
    }
    tx.send(event(r#"["after"]"#)).await.unwrap();
    assert_eq!(&*manager.recv_client_text().await, r#"2["after"]"#);
    tx.disconnect();
    assert_eq!(&*manager.recv_client_text().await, "1");
    manager.join().await.unwrap();
}
