//! What leaves for the engine and what reaches the handles, in what order,
//! and when each waits.

use futures_util::FutureExt;

use super::rig::{Effect, Rig, disconnect, event, listen};
use crate::manager::namespace::Read;
use crate::packet::ServerPacket;

#[test]
fn connect_goes_first_and_events_keep_order_per_namespace() {
    let mut rig = Rig::new();
    rig.connect("/", "{}", 32);
    rig.connect("/a", "{\"t\":1}", 32);
    assert_eq!(rig.effects(), [Effect::Open, listen("/"), listen("/a")]);
    assert_eq!(rig.core.read("/", true), Read::Wait);

    rig.handshake();
    assert_eq!(rig.messages(), ["0{}", "0/a,{\"t\":1}"]);
    // Nothing but CONNECT may reach a namespace the server has not confirmed.
    assert_eq!(rig.core.read("/", true), Read::Wait);
    assert_eq!(rig.core.read("/a", true), Read::Wait);

    rig.confirm("/");
    assert_eq!(rig.core.read("/a", true), Read::Wait);
    rig.send("/", event("[\"e1\"]"));
    assert_eq!(rig.messages(), ["2[\"e1\"]"]);
    rig.send("/", event("[\"e2\"]"));
    assert_eq!(rig.messages(), ["2[\"e2\"]"]);

    rig.confirm("/a");
    rig.send("/a", event("[\"e3\"]"));
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
fn a_packet_in_flight_holds_back_every_namespace() {
    let mut rig = Rig::new();
    rig.connected("/");
    rig.connected("/b");

    rig.send("/", event("[\"a1\"]"));
    assert_eq!(rig.core.read("/", true), Read::Wait);
    assert_eq!(rig.core.read("/b", true), Read::Wait);
    assert!(rig.core.has_message());

    assert_eq!(rig.messages(), ["2[\"a1\"]"]);
    assert_eq!(rig.core.read("/", true), Read::Send);
    assert_eq!(rig.core.read("/b", true), Read::Send);
    rig.send("/b", event("[\"b1\"]"));
    assert_eq!(rig.messages(), ["2/b,[\"b1\"]"]);
}

#[test]
fn events_wait_until_a_permit_reads_them() {
    let mut rig = Rig::new();
    rig.connected("/");
    assert_eq!(rig.core.read("/", false), Read::Wait);
    assert_eq!(rig.core.read("/", true), Read::Send);

    rig.send("/", event("[\"e1\"]"));
    assert_eq!(rig.core.read("/", true), Read::Wait);
    assert_eq!(rig.messages(), ["2[\"e1\"]"]);
    assert_eq!(rig.core.read("/", true), Read::Send);
}

#[test]
fn a_full_receiver_holds_one_packet_until_a_slot_frees() {
    let mut rig = Rig::new();
    let mut handles = rig.connect("/", "{}", 1);
    rig.handshake();
    rig.messages();
    // The Connect packet fills the receiver.
    rig.confirm("/");
    rig.server("2[\"e1\"]");

    let sender = rig.core.held_sender().expect("the packet is held");
    assert!(sender.clone().reserve_owned().now_or_never().is_none());

    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Ok(ServerPacket::Connect(_))
    ));
    let permit = sender.reserve_owned().now_or_never().unwrap();
    rig.core.deliver_held(permit);

    assert!(rig.core.held_sender().is_none());
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Ok(ServerPacket::Event(_))
    ));
}

#[test]
fn the_held_packet_is_discarded_when_its_namespace_ends() {
    let mut rig = Rig::new();
    let mut handles = rig.connect("/", "{}", 1);
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    rig.server("2[\"e1\"]");
    let sender = rig.core.held_sender().expect("the packet is held");

    // A confirmed namespace sends DISCONNECT, which ends it for the core.
    handles.closed.cancel();
    rig.step(disconnect("/"));
    assert_eq!(rig.messages(), ["1"]);
    assert!(rig.core.held_sender().is_none());

    // A reservation that finishes late finds nothing to send.
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Ok(ServerPacket::Connect(_))
    ));
    let permit = sender.reserve_owned().now_or_never().unwrap();
    rig.core.deliver_held(permit);
    handles.server_packet_rx.try_recv().unwrap_err();
}

#[test]
fn a_dropped_receiver_discards_packets_without_holding() {
    let mut rig = Rig::new();
    let handles = rig.connect("/", "{}", 1);
    rig.handshake();
    drop(handles.server_packet_rx);

    rig.confirm("/");
    rig.server("2[\"e1\"]");
    assert!(rig.core.held_sender().is_none());
    rig.server("2[\"e2\"]");
}
