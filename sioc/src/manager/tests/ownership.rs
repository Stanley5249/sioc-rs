//! Participation ownership while local disconnect waits for Engine capacity,
//! and how a replacement waits for the name.

use std::time::Duration;

use bytes::Bytes;
use eioc::prelude::ClientMessage;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig, ack, event, event_ack};
use crate::error::SocketError;
use crate::packet::ClientPacket;

#[test]
fn a_replacement_waits_for_the_stalled_flush_without_entering_the_core() {
    let mut rig = Rig::new();
    rig.connected("/keep");
    let old = rig.connected("/");
    for _ in 0..8 {
        rig.send("/", event("[\"old\"]"));
    }
    rig.disconnect("/");
    // Every attempt during the flush gets the old release and leaves no
    // handles, capacity, or CONNECT behind.
    for _ in 0..100 {
        let busy = rig.connect("/", "{}", 1);
        let release = busy.result.unwrap().expect("the flush owns the name");
        assert!(!release.is_cancelled());
        assert!(busy.release.is_cancelled(), "the core dropped the attempt");
        // Unexpected confirmations cannot activate anything.
        rig.confirm("/");
    }
    assert_eq!(old.capacity.available_permits(), 0);
    assert!(!old.release.is_cancelled());

    let mut expected = vec!["2[\"old\"]"; 8];
    expected.push("1");
    assert_eq!(rig.messages(), expected);
    assert!(old.release.is_cancelled());

    let new = rig.connect("/", "{}", 1);
    assert!(new.result.unwrap().is_none());
    assert!(matches!(
        rig.connect("/", "{}", 1).result,
        Err(SocketError::NamespaceConflict { .. })
    ));
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");
    rig.send("/", event("[\"new\"]"));
    assert_eq!(rig.messages(), ["2[\"new\"]"]);
}

#[test]
fn server_termination_cancels_locally_flushing_work_and_releases_the_name() {
    for terminal in ["1", "4{\"message\":\"no\"}"] {
        let mut rig = Rig::new();
        rig.connected("/keep");
        let mut old = rig.connected("/");
        let (packet, mut waiter) = event_ack("[\"old\"]");
        rig.send("/", packet);
        rig.disconnect("/");
        let release = rig.connect("/", "{}", 1).result.unwrap().unwrap();
        rig.server(terminal);
        assert!(release.is_cancelled());
        assert!(rig.messages().is_empty(), "no old EVENT or DISCONNECT");
        assert_eq!(waiter.try_recv().unwrap_err(), TryRecvError::Closed);
        old.terminal_packet_rx.try_recv().unwrap();

        assert!(rig.connect("/", "{}", 1).result.unwrap().is_none());
        assert_eq!(rig.messages(), ["0{}"]);
    }
}

#[test]
fn old_queued_ack_waiters_never_enter_the_replacement() {
    let mut rig = Rig::new();
    rig.connected("/keep");
    rig.connected("/");
    let (packet, mut old_waiter) = event_ack("[\"old\"]");
    rig.send("/", packet);
    rig.disconnect("/");
    assert_eq!(rig.messages(), ["20[\"old\"]", "1"]);
    assert_eq!(old_waiter.try_recv().unwrap_err(), TryRecvError::Closed);
    assert!(rig.connect("/", "{}", 1).result.unwrap().is_none());
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");
    let (packet, mut new_waiter) = event_ack("[\"new\"]");
    rig.send("/", packet);
    assert_eq!(rig.messages(), ["21[\"new\"]"]);
    rig.server("30[\"stale\"]");
    assert_eq!(new_waiter.try_recv().unwrap_err(), TryRecvError::Empty);
    rig.server("31[\"current\"]");
    new_waiter.try_recv().unwrap();
}

#[test]
fn engine_loss_cancels_the_flush_and_releases_the_name() {
    let mut rig = Rig::reconnecting();
    let old = rig.connected("/");
    let (packet, mut old_waiter) = event_ack("[\"old\"]");
    rig.send("/", packet);
    rig.disconnect("/");
    let release = rig.connect("/", "{}", 1).result.unwrap().unwrap();
    // The flush was the engine's only user, so nothing reconnects.
    assert_eq!(rig.close(), []);
    assert!(release.is_cancelled());
    assert!(old.release.is_cancelled());
    assert_eq!(old_waiter.try_recv().unwrap_err(), TryRecvError::Closed);

    assert!(rig.connect("/", "{}", 1).result.unwrap().is_none());
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}"]);
}

#[test]
fn engine_reconnection_does_not_release_a_live_participation() {
    let mut rig = Rig::reconnecting();
    let live = rig.connected("/");
    rig.send("/", event("[\"kept\"]"));
    assert_eq!(rig.close(), [Effect::Schedule(Duration::ZERO)]);
    assert!(!live.release.is_cancelled());
    assert!(!live.closed.is_cancelled());
    assert!(matches!(
        rig.connect("/", "{}", 1).result,
        Err(SocketError::NamespaceConflict { .. })
    ));
    rig.reconnect();
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");
    assert_eq!(rig.messages(), ["2[\"kept\"]"]);
}

#[test]
fn shutdown_releases_a_flushing_participation_and_refuses_new_handles() {
    let mut rig = Rig::new();
    rig.connected("/keep");
    rig.connected("/");
    rig.send("/", event("[\"old\"]"));
    rig.disconnect("/");
    let release = rig.connect("/", "{}", 1).result.unwrap().unwrap();
    rig.core.shutdown();
    assert!(release.is_cancelled());
    assert!(matches!(
        rig.connect("/", "{}", 1).result,
        Err(SocketError::Closed)
    ));
}

#[test]
fn slots_acquired_before_disconnect_cannot_accept_late_work() {
    let mut rig = Rig::new();
    rig.connected("/keep");
    let old = rig.connected("/");
    let event_slot = old.capacity.clone().try_acquire_owned().unwrap();
    let reply_slot = old.reply_capacity.clone().try_acquire_owned().unwrap();
    let connection = rig.core.connection("/", &old.capacity).unwrap();
    rig.disconnect("/");
    assert!(matches!(
        rig.core.emit(&"/".into(), event("[\"late\"]"), event_slot),
        Err(SocketError::Closed)
    ));
    rig.core
        .acknowledge(&"/".into(), ack(99), reply_slot, &connection);
    rig.core.disconnect(&"/".into(), &old.capacity);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(old.capacity.available_permits(), 8);
    assert_eq!(old.reply_capacity.available_permits(), 8);
}

#[test]
fn termination_after_a_binary_header_preserves_only_its_attachments() {
    for terminal in ["1", "4{\"message\":\"no\"}"] {
        let mut rig = Rig::new();
        rig.connected("/keep");
        rig.connected("/");
        rig.send(
            "/",
            ClientPacket::Event {
                payload: "[\"binary\",{\"_placeholder\":true,\"num\":0}]".into(),
                ack_tx: None,
                attachments: Some(vec![
                    Bytes::from_static(b"first"),
                    Bytes::from_static(b"second"),
                ]),
            },
        );
        rig.send("/", event("[\"cancel\"]"));
        rig.disconnect("/");
        assert!(
            matches!(rig.core.next_message(), Some(ClientMessage::Text(text)) if text.starts_with("52-"))
        );
        rig.server(terminal);
        assert!(rig.connect("/", "{}", 1).result.unwrap().is_none());
        assert_eq!(rig.messages(), ["bin:first", "bin:second", "0{}"]);
    }
}
