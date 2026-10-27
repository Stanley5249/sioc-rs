//! Outgoing packets and the shared lifetime of namespace sender clones,
//! against a manager core that no driver runs.

use std::pin::pin;
use std::sync::Arc;

use eioc::prelude::{ClientMessage, Handshake, ServerMessage};
use futures_util::FutureExt;
use tokio::sync::mpsc::error::TryRecvError;

use crate::client::{SocketReceiver, SocketSender};
use crate::config::ChannelConfig;
use crate::error::{PayloadError, SocketError};
use crate::event::Emit;
use crate::manager::driver::Manager;
use crate::manager::state::ManagerCore;
use crate::marker::{AckMarker, HasAck, NoAck, NoBinary};
use crate::packet::{ClientPacket, ServerPacket};

struct TestEmit;

impl Emit<NoAck, NoBinary> for TestEmit {
    type Output = ();

    fn prepare(self) -> Result<(ClientPacket, ()), PayloadError> {
        Ok((
            ClientPacket::Event {
                payload: r#"["test"]"#.into(),
                ack_tx: None,
                attachments: None,
            },
            (),
        ))
    }
}

/// Handles of `/`, which the server confirmed on an open engine.
fn socket_sender() -> (SocketSender, SocketReceiver, Arc<Manager>) {
    let manager = Manager::new(ManagerCore::new(None, fastrand::Rng::with_seed(1)));
    let (sender, receiver) = futures_util::FutureExt::now_or_never(crate::client::connect(
        &manager,
        "/".into(),
        "".into(),
        ChannelConfig::from(8),
    ))
    .unwrap()
    .unwrap();
    manager.with_core(|core| {
        core.receive(ServerMessage::Open(Handshake {
            sid: "engine".into(),
            upgrades: Vec::new(),
            ping_interval: 25_000,
            ping_timeout: 20_000,
            max_payload: 1_000_000,
        }));
        core.next_message().expect("CONNECT goes out");
        core.receive(ServerMessage::Text(r#"0{"sid":"s"}"#.into()));
    });
    (sender, receiver, manager)
}

/// Takes every text message the engine would accept.
fn messages(manager: &Manager) -> Vec<String> {
    manager
        .with_core(|core| {
            std::iter::from_fn(|| core.next_message())
                .map(|message| match message {
                    ClientMessage::Text(text) => text.to_string(),
                    ClientMessage::Binary(_) => panic!("no attachments here"),
                })
                .collect()
        })
        .unwrap()
}

#[tokio::test]
async fn dropping_last_clone_disconnects_the_namespace() {
    let (sender, _receiver, manager) = socket_sender();
    let clone = sender.clone();
    drop(sender);
    assert_eq!(messages(&manager), Vec::<String>::new());
    drop(clone);
    assert_eq!(messages(&manager), ["1"]);
}

#[tokio::test]
async fn disconnect_closes_every_clone_and_keeps_sent_packets() {
    let (sender, _receiver, manager) = socket_sender();
    let clone = sender.clone();
    sender.emit(TestEmit).await.unwrap();
    sender.disconnect();
    assert!(matches!(
        clone.emit(TestEmit).await,
        Err(SocketError::Closed)
    ));
    assert_eq!(messages(&manager), [r#"2["test"]"#, "1"]);
}

#[tokio::test]
async fn disconnect_after_close_returns() {
    let (sender, _receiver, manager) = socket_sender();
    manager.with_core(ManagerCore::shutdown);
    sender.disconnect();
    sender.disconnect();
    assert_eq!(messages(&manager), Vec::<String>::new());
}

#[tokio::test]
async fn closed_resolves_for_every_clone_after_disconnect() {
    let (sender, _receiver, _manager) = socket_sender();
    let clone = sender.clone();
    sender.disconnect();
    clone.closed().await;
}

#[tokio::test]
async fn rejects_invalid_binary_attachment_counts_before_enqueueing() {
    let (sender, _receiver, manager) = socket_sender();
    for count in [0, 11] {
        for is_ack in [false, true] {
            let attachments = Some(vec![bytes::Bytes::new(); count]);
            let packet = if is_ack {
                ClientPacket::Ack {
                    payload: "[]".into(),
                    id: 0,
                    attachments,
                }
            } else {
                ClientPacket::Event {
                    payload: r#"["x"]"#.into(),
                    ack_tx: None,
                    attachments,
                }
            };
            assert!(
                matches!(sender.send(packet).await, Err(SocketError::AttachmentCount { count: actual }) if actual == count),
                "{count}, ack={is_ack}"
            );
            assert_eq!(messages(&manager), Vec::<String>::new());
        }
    }
}

#[tokio::test]
async fn emit_sends_event_packet() {
    let (sender, _receiver, manager) = socket_sender();
    sender.emit(TestEmit).await.unwrap();
    assert_eq!(messages(&manager), [r#"2["test"]"#]);
}

#[tokio::test]
async fn emit_returns_error_once_the_manager_ended() {
    let (sender, _receiver, manager) = socket_sender();
    manager.with_core(ManagerCore::shutdown);
    assert!(matches!(
        sender.emit(TestEmit).await,
        Err(SocketError::Closed)
    ));
}

#[tokio::test]
async fn acknowledge_sends_ack_packet_with_correct_id() {
    let (sender, _receiver, manager) = socket_sender();
    let id = HasAck::<()>::parse(Some(5)).unwrap();
    sender.acknowledge(id, ()).await.unwrap();
    assert_eq!(messages(&manager), ["35[]"]);
}

/// The application's next `connect` of `/`, as `Client::connect` runs it.
fn reconnect(
    manager: &Arc<Manager>,
) -> impl Future<Output = Result<(SocketSender, SocketReceiver), SocketError>> + use<> {
    let manager = Arc::clone(manager);
    async move { crate::client::connect(&manager, "/".into(), "".into(), ChannelConfig::from(8)).await }
}

#[tokio::test]
async fn closed_resolves_before_the_stalled_flush_and_connect_waits_for_it() {
    let (sender, mut receiver, manager) = socket_sender();
    sender.emit(TestEmit).await.unwrap();
    sender.disconnect();
    let mut waiting = pin!(reconnect(&manager));
    assert!(waiting.as_mut().now_or_never().is_none());
    // Admission is closed although nothing reached the engine yet.
    assert!(pin!(sender.closed()).now_or_never().is_some());
    assert!(matches!(receiver.try_recv(), Ok(ServerPacket::Connect(_))));
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

    assert_eq!(messages(&manager), [r#"2["test"]"#, "1"]);
    assert!(matches!(
        receiver.try_recv(),
        Err(TryRecvError::Disconnected)
    ));
    let (new, _new_receiver) = waiting.await.unwrap();
    // The old handles cannot touch the new participation.
    sender.disconnect();
    assert!(matches!(
        sender.emit(TestEmit).await,
        Err(SocketError::Closed)
    ));
    assert!(pin!(new.closed()).now_or_never().is_none());
}

#[tokio::test]
async fn concurrent_connects_admit_exactly_one_after_the_flush() {
    let (sender, _receiver, manager) = socket_sender();
    sender.emit(TestEmit).await.unwrap();
    sender.disconnect();
    let mut waiting: Vec<_> = (0..3).map(|_| Box::pin(reconnect(&manager))).collect();
    for connect in &mut waiting {
        assert!(connect.as_mut().now_or_never().is_none());
    }
    messages(&manager);
    let results = futures_util::future::join_all(waiting).await;
    let admitted = results.iter().filter(|result| result.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|result| matches!(result, Err(SocketError::NamespaceConflict { .. })))
        .count();
    assert_eq!((admitted, conflicts), (1, 2));
}

#[tokio::test]
async fn a_cancelled_waiting_connect_leaves_no_ownership_behind() {
    let (sender, _receiver, manager) = socket_sender();
    sender.emit(TestEmit).await.unwrap();
    sender.disconnect();
    // Cancelled before the release.
    let mut early = Box::pin(reconnect(&manager));
    assert!(early.as_mut().now_or_never().is_none());
    drop(early);
    // Woken by the release, then cancelled before it retries.
    let mut woken = Box::pin(reconnect(&manager));
    assert!(woken.as_mut().now_or_never().is_none());
    assert_eq!(messages(&manager), [r#"2["test"]"#, "1"]);
    drop(woken);

    let (new, _new_receiver) = reconnect(&manager).now_or_never().unwrap().unwrap();
    new.emit(TestEmit).await.unwrap();
    assert!(pin!(new.closed()).now_or_never().is_none());
}

#[tokio::test]
async fn dropping_the_last_sender_during_stalled_output_still_flushes_then_releases() {
    let (sender, _receiver, manager) = socket_sender();
    sender.emit(TestEmit).await.unwrap();
    drop(sender);
    let mut waiting = pin!(reconnect(&manager));
    assert!(waiting.as_mut().now_or_never().is_none());
    assert_eq!(messages(&manager), [r#"2["test"]"#, "1"]);
    waiting.await.unwrap();
}

#[tokio::test]
async fn shutdown_wakes_a_waiting_connect() {
    let (sender, _receiver, manager) = socket_sender();
    sender.emit(TestEmit).await.unwrap();
    sender.disconnect();
    let mut waiting = pin!(reconnect(&manager));
    assert!(waiting.as_mut().now_or_never().is_none());
    manager.with_core(ManagerCore::shutdown);
    assert!(matches!(waiting.await, Err(SocketError::Closed)));
}
