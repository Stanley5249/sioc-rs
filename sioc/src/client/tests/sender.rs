//! Outgoing packets and the shared lifetime of namespace sender clones,
//! against a manager core that no driver runs.

use std::sync::Arc;

use eioc::prelude::{ClientMessage, Handshake, ServerMessage};

use crate::client::{SocketReceiver, SocketSender};
use crate::error::{PayloadError, SocketError};
use crate::event::Emit;
use crate::manager::driver::Manager;
use crate::manager::state::ManagerCore;
use crate::marker::{AckMarker, HasAck, NoAck, NoBinary};
use crate::packet::ClientPacket;

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
    let (sender, receiver) = crate::client::connect(&manager, "/".into(), "".into(), 8, 8).unwrap();
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
