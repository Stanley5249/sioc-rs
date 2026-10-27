//! Participation ownership while local disconnect waits for Engine capacity.

use bytes::Bytes;
use eioc::prelude::ClientMessage;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Rig, event, event_ack};
use crate::error::SocketError;
use crate::packet::ClientPacket;

#[test]
fn server_termination_cancels_locally_flushing_work() {
    for terminal in ["1", "4{\"message\":\"no\"}"] {
        let mut rig = Rig::new();
        rig.connected("/keep");
        let mut old = rig.connected("/");
        let (packet, mut waiter) = event_ack("[\"old\"]");
        rig.send("/", packet);
        rig.disconnect("/");
        rig.server(terminal);
        assert!(rig.messages().is_empty(), "no old EVENT or DISCONNECT");
        assert_eq!(waiter.try_recv().unwrap_err(), TryRecvError::Closed);
        old.terminal_packet_rx.try_recv().unwrap();
    }
}

#[test]
fn repeated_reuse_cannot_accumulate_capacity_pools_while_flushing() {
    let mut rig = Rig::new();
    rig.connected("/keep");
    let old = rig.connected("/");
    for _ in 0..8 {
        rig.send("/", event("[\"old\"]"));
    }
    rig.disconnect("/");
    let waiting = rig.connect("/", "{}", 1);
    waiting.result.unwrap();
    for _ in 0..8 {
        rig.send("/", event("[\"waiting\"]"));
    }
    for _ in 0..100 {
        // Unexpected confirmations cannot activate the waiting replacement.
        rig.confirm("/");
        let rejected = rig.connect("/", "{}", 1);
        assert!(matches!(
            rejected.result,
            Err(SocketError::NamespaceConflict { .. })
        ));
    }
    assert_eq!(old.capacity.available_permits(), 0);
    assert_eq!(waiting.capacity.available_permits(), 0);
    let mut expected = vec!["2[\"old\"]"; 8];
    expected.extend(["1", "0{}"]);
    assert_eq!(rig.messages(), expected);
    rig.confirm("/");
    assert_eq!(rig.messages(), vec!["2[\"waiting\"]"; 8]);
}

#[test]
fn cancelling_waiting_replacements_reclaims_each_pool_without_queuing_packets() {
    let mut rig = Rig::new();
    rig.connected("/keep");
    let old = rig.connected("/");
    for _ in 0..8 {
        rig.send("/", event("[\"old\"]"));
    }
    rig.disconnect("/");
    for _ in 0..100 {
        let waiting = rig.connect("/", "{}", 1);
        waiting.result.unwrap();
        rig.confirm("/");
        for _ in 0..8 {
            rig.send("/", event("[\"cancelled\"]"));
        }
        rig.disconnect("/");
        assert!(waiting.closed.is_cancelled());
        assert_eq!(waiting.capacity.available_permits(), 8);
        assert_eq!(old.capacity.available_permits(), 0);
    }
    let mut expected = vec!["2[\"old\"]"; 8];
    expected.push("1");
    assert_eq!(rig.messages(), expected);
}

#[test]
fn old_queued_ack_waiters_never_enter_the_replacement() {
    let mut rig = Rig::new();
    rig.connected("/keep");
    rig.connected("/");
    let (packet, mut old_waiter) = event_ack("[\"old\"]");
    rig.send("/", packet);
    rig.disconnect("/");
    rig.connect("/", "{}", 1).result.unwrap();
    // An unsolicited CONNECT during the flush must not replace the owner.
    rig.confirm("/");
    let (packet, mut new_waiter) = event_ack("[\"new\"]");
    rig.send("/", packet);
    assert_eq!(rig.messages(), ["20[\"old\"]", "1", "0{}"]);
    assert_eq!(old_waiter.try_recv().unwrap_err(), TryRecvError::Closed);
    rig.confirm("/");
    assert_eq!(rig.messages(), ["21[\"new\"]"]);
    rig.server("30[\"stale\"]");
    assert_eq!(new_waiter.try_recv().unwrap_err(), TryRecvError::Empty);
    rig.server("31[\"current\"]");
    new_waiter.try_recv().unwrap();
}

#[test]
fn engine_loss_cancels_the_flush_but_preserves_the_waiting_replacement() {
    let mut rig = Rig::reconnecting();
    rig.connected("/");
    let (packet, mut old_waiter) = event_ack("[\"old\"]");
    rig.send("/", packet);
    rig.disconnect("/");
    let waiting = rig.connect("/", "{}", 1);
    waiting.result.unwrap();
    let (packet, mut new_waiter) = event_ack("[\"new\"]");
    rig.send("/", packet);
    rig.close();
    assert_eq!(old_waiter.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(new_waiter.try_recv().unwrap_err(), TryRecvError::Empty);
    assert!(!waiting.closed.is_cancelled());
    rig.reconnect();
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");
    assert_eq!(rig.messages(), ["20[\"new\"]"]);
    rig.server("30[]");
    new_waiter.try_recv().unwrap();
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
    let replacement = rig.connect("/", "{}", 1);
    replacement.result.unwrap();
    assert!(matches!(
        rig.core.emit(&"/".into(), event("[\"late\"]"), event_slot),
        Err(SocketError::Closed)
    ));
    rig.core
        .acknowledge(&"/".into(), super::rig::ack(99), reply_slot, &connection);
    rig.core.disconnect(&"/".into(), &old.capacity);
    assert_eq!(rig.messages(), ["1", "0{}"]);
    assert!(!replacement.closed.is_cancelled());
    assert_eq!(old.capacity.available_permits(), 8);
    assert_eq!(old.reply_capacity.available_permits(), 8);
}

#[test]
fn server_termination_cancels_only_the_flushing_owner_then_activates_waiting_handles() {
    for terminal in ["1", "4{\"message\":\"no\"}"] {
        let mut rig = Rig::new();
        rig.connected("/keep");
        rig.connected("/");
        let (packet, mut old_waiter) = event_ack("[\"old\"]");
        rig.send("/", packet);
        rig.disconnect("/");
        let waiting = rig.connect("/", "{}", 1);
        waiting.result.unwrap();
        rig.send("/", event("[\"new\"]"));
        rig.server(terminal);
        assert_eq!(old_waiter.try_recv().unwrap_err(), TryRecvError::Closed);
        assert!(!waiting.closed.is_cancelled());
        assert_eq!(rig.messages(), ["0{}"]);
        rig.confirm("/");
        assert_eq!(rig.messages(), ["2[\"new\"]"]);
    }
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
        rig.connect("/", "{}", 1).result.unwrap();
        assert_eq!(rig.messages(), ["bin:first", "bin:second", "0{}"]);
    }
}
