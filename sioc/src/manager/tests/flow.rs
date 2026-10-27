//! What leaves for the engine and what reaches the handles, in what order,
//! and when each waits.

use super::rig::{Effect, Rig, event};
use crate::packet::ServerPacket;

#[test]
fn connect_goes_first_and_events_keep_order_per_namespace() {
    let mut rig = Rig::new();
    rig.connect("/", "{}", 32);
    rig.connect("/a", "{\"t\":1}", 32);
    assert_eq!(rig.effects(), [Effect::Open]);

    rig.handshake();
    rig.send("/", event("[\"e1\"]"));
    rig.send("/a", event("[\"e3\"]"));
    // Nothing but CONNECT may reach a namespace the server has not confirmed.
    assert_eq!(rig.messages(), ["0{}", "0/a,{\"t\":1}"]);

    rig.confirm("/");
    assert_eq!(rig.messages(), ["2[\"e1\"]"]);
    rig.send("/", event("[\"e2\"]"));
    assert_eq!(rig.messages(), ["2[\"e2\"]"]);

    rig.confirm("/a");
    assert_eq!(rig.messages(), ["2/a,[\"e3\"]"]);
}

#[test]
fn connect_waits_for_the_engine_handshake() {
    let mut rig = Rig::new();
    rig.connect("/", "{}", 32);
    rig.effects();

    // Like `Socket.connect` waiting for the manager's "open" event, nothing
    // reaches an engine whose handshake has not completed.
    assert_eq!(rig.messages(), Vec::<String>::new());
    rig.connect("/a", "{}", 32);
    assert_eq!(rig.messages(), Vec::<String>::new());

    rig.handshake();
    assert_eq!(rig.messages(), ["0{}", "0/a,{}"]);
}

#[test]
fn confirmed_events_leave_in_submission_order_across_namespaces() {
    let mut rig = Rig::new();
    rig.connected("/");
    rig.connected("/b");

    rig.send("/", event("[\"a1\"]"));
    rig.send("/b", event("[\"b1\"]"));
    rig.send("/", event("[\"a2\"]"));
    assert!(rig.core.has_message());
    assert_eq!(rig.messages(), ["2[\"a1\"]", "2/b,[\"b1\"]", "2[\"a2\"]"]);
    assert!(!rig.core.has_message());
}

#[test]
fn a_full_receiver_blocks_one_packet_for_the_driver() {
    let mut rig = Rig::new();
    let mut handles = rig.connect("/", "{}", 1);
    rig.handshake();
    rig.messages();
    // The Connect packet fills the receiver.
    assert!(rig.confirm("/").is_none());
    let blocked = rig.server("2[\"e1\"]").expect("the packet is blocked");
    assert!(matches!(blocked.packet, ServerPacket::Event(_)));
    assert!(!blocked.closed.is_cancelled());

    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Ok(ServerPacket::Connect(_))
    ));
    blocked.server_packet_tx.try_send(blocked.packet).unwrap();
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Ok(ServerPacket::Event(_))
    ));
}

#[test]
fn a_blocked_packet_is_cancelled_when_its_handles_disconnect() {
    let mut rig = Rig::new();
    let _handles = rig.connect("/", "{}", 1);
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    let blocked = rig.server("2[\"e1\"]").expect("the packet is blocked");

    // A confirmed namespace sends DISCONNECT, which ends its handles.
    rig.disconnect("/");
    assert_eq!(rig.messages(), ["1"]);
    assert!(blocked.closed.is_cancelled());
}

#[test]
fn a_dropped_receiver_discards_packets_without_blocking() {
    let mut rig = Rig::new();
    let handles = rig.connect("/", "{}", 1);
    rig.handshake();
    drop(handles.server_packet_rx);

    assert!(rig.confirm("/").is_none());
    assert!(rig.server("2[\"e1\"]").is_none());
    assert!(rig.server("2[\"e2\"]").is_none());
}

#[test]
fn capacity_slots_return_when_the_engine_takes_the_packet() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");
    for _ in 0..8 {
        rig.send("/", event("[\"e\"]"));
    }
    // Every slot is queued, so the handles would wait for capacity.
    assert_eq!(handles.capacity.available_permits(), 0);

    assert!(rig.core.next_message().is_some());
    assert_eq!(handles.capacity.available_permits(), 1);
    rig.messages();
    assert_eq!(handles.capacity.available_permits(), 8);
}

#[test]
fn engine_loss_keeps_the_slots_of_unsent_events() {
    let mut rig = Rig::reconnecting();
    let handles = rig.connected("/");
    rig.send("/", event("[\"sent\"]"));
    rig.messages();
    rig.send("/", event("[\"unsent\"]"));
    rig.close();
    // The unsent event still holds its slot in the send buffer.
    assert_eq!(handles.capacity.available_permits(), 7);
    rig.reconnect();
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    assert_eq!(rig.messages(), ["2[\"unsent\"]"]);
    assert_eq!(handles.capacity.available_permits(), 8);
}
