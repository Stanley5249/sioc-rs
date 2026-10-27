//! How a namespace connects, ends from either side, and is connected again
//! under its name. Reuse keeps old deliveries and acks away from new handles.

use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig, ack, event, event_ack};
use crate::error::SocketError;
use crate::packet::ServerPacket;

#[test]
fn the_last_handle_leaves_an_unconfirmed_namespace() {
    let mut rig = Rig::new();
    let handles = rig.connect("/", "{}", 32);
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.send("/", event("[\"buffered\"]"));
    assert_eq!(handles.capacity.available_permits(), 7);

    // The queued CONNECT never leaves, and the buffered event is discarded.
    assert_eq!(rig.disconnect("/"), [Effect::Close]);
    assert_eq!(rig.messages(), Vec::<String>::new());
    assert!(handles.closed.is_cancelled());
    rig.close();
    rig.drop_client();
    assert!(rig.core.is_finished());
}

#[test]
fn server_end_fails_waiters_discards_queued_packets_and_bypasses_a_full_receiver() {
    type Case = (&'static str, fn(&ServerPacket) -> bool);
    let cases: [Case; 2] = [
        ("1", |packet| matches!(packet, ServerPacket::Disconnect)),
        ("4{\"message\":\"no\"}", |packet| {
            matches!(packet, ServerPacket::ConnectError(_))
        }),
    ];

    for (end, is_terminal) in cases {
        let mut rig = Rig::new();
        let mut handles = rig.connect("/", "{}", 1);
        rig.handshake();
        rig.messages();
        // The Connect packet fills the receiver.
        rig.confirm("/");
        rig.effects();

        let (sent_packet, mut sent) = event_ack("[\"a\"]");
        rig.send("/", sent_packet);
        rig.messages();
        let (unsent_packet, mut unsent) = event_ack("[\"b\"]");
        rig.send("/", unsent_packet);
        rig.send("/", ack(3));

        rig.server(end);

        assert_eq!(sent.try_recv().unwrap_err(), TryRecvError::Closed);
        // The server ended the namespace, so nothing it queued goes out.
        assert_eq!(unsent.try_recv().unwrap_err(), TryRecvError::Closed);
        assert_eq!(rig.messages(), Vec::<String>::new());
        assert!(is_terminal(&handles.terminal_packet_rx.try_recv().unwrap()));
        assert!(matches!(
            handles.server_packet_rx.try_recv(),
            Ok(ServerPacket::Connect(_))
        ));
        assert!(matches!(
            handles.server_packet_rx.try_recv(),
            Err(MpscTryRecvError::Disconnected)
        ));
        assert!(handles.closed.is_cancelled());
        assert!(handles.capacity.is_closed());
        // No socket needs the engine any more, like `Manager._destroy`.
        assert_eq!(rig.effects(), [Effect::Close]);
    }
}

#[test]
fn disconnect_after_confirmation_sends_accepted_packets_then_disconnect_then_closes() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");

    rig.send("/", event("[\"e1\"]"));
    rig.send("/", ack(4));
    rig.send("/", event("[\"e2\"]"));
    assert_eq!(rig.disconnect("/"), []);
    assert!(handles.closed.is_cancelled());
    assert!(matches!(
        rig.try_send("/", event("[\"late\"]")),
        Err(SocketError::Closed)
    ));

    assert_eq!(rig.messages(), ["2[\"e1\"]", "34[]", "2[\"e2\"]", "1"]);
    assert_eq!(rig.effects(), [Effect::Close]);
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn disconnect_before_confirmation_sends_nothing() {
    // CONNECT has not left yet, so the server never hears of the namespace and
    // the queued CONNECT goes back.
    let mut rig = Rig::new();
    rig.connect("/", "{}", 32);
    assert_eq!(rig.effects(), [Effect::Open]);
    assert_eq!(rig.disconnect("/"), [Effect::Close]);
    assert_eq!(rig.messages(), Vec::<String>::new());

    // CONNECT left but the server has not confirmed it: no DISCONNECT.
    let mut rig = Rig::new();
    rig.connect("/", "{}", 32);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}"]);
    assert_eq!(rig.effects(), [Effect::Open]);
    assert_eq!(rig.disconnect("/"), [Effect::Close]);
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn engine_loss_before_disconnect_left_ends_the_namespace_without_reconnecting() {
    let mut rig = Rig::reconnecting();
    rig.connected("/");
    let (unsent_packet, mut ack) = event_ack("[\"e1\"]");
    rig.send("/", unsent_packet);
    rig.disconnect("/");

    // The old engine can no longer flush, and no socket needs a new one.
    assert_eq!(rig.close(), []);
    assert_eq!(rig.reconnect(), []);
    assert_eq!(ack.try_recv().unwrap_err(), TryRecvError::Closed);
    rig.drop_client();
    assert!(rig.core.is_finished());
}

#[test]
fn reconnecting_a_name_after_the_server_ended_it_keeps_ack_ids() {
    let mut rig = Rig::new();
    let _first = rig.connected("/");
    let (old_packet, mut old_ack) = event_ack("[\"x\"]");
    rig.send("/", old_packet);
    rig.messages();

    rig.server("1");
    assert_eq!(old_ack.try_recv().unwrap_err(), TryRecvError::Closed);
    // No socket needs the engine, so it closes, like `Manager._destroy`.
    assert_eq!(rig.effects(), [Effect::Close]);

    let second = rig.connect("/", "{\"n\":2}", 32);
    second.result.unwrap();
    let third = rig.connect("/", "{}", 32);
    assert!(matches!(
        third.result,
        Err(SocketError::NamespaceConflict { .. })
    ));

    assert_eq!(rig.close(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{\"n\":2}"]);
    rig.confirm("/");
    let (new_packet, _new_ack) = event_ack("[\"y\"]");
    rig.send("/", new_packet);
    assert_eq!(rig.messages(), ["21[\"y\"]"]);
}

#[test]
fn live_handles_keep_the_name() {
    let mut rig = Rig::new();
    let _first = rig.connected("/");

    let second = rig.connect("/", "{}", 32);
    assert!(matches!(
        second.result,
        Err(SocketError::NamespaceConflict { .. })
    ));
}

#[test]
fn connecting_again_after_disconnect_follows_the_remaining_packets_and_disconnect() {
    let mut rig = Rig::new();
    rig.connected("/");
    rig.send("/", event("[\"e1\"]"));
    rig.disconnect("/");

    let busy = rig.connect("/", "{\"n\":2}", 32);
    let release = busy.result.unwrap().expect("the old flush owns the name");
    assert_eq!(rig.messages(), ["2[\"e1\"]", "1"]);
    assert!(release.is_cancelled());
    // The last socket left, so the engine closes and the next one opens,
    // like `Manager._destroy` followed by `Socket.connect`.
    assert_eq!(rig.effects(), [Effect::Close]);
    assert!(rig.connect("/", "{\"n\":2}", 32).result.unwrap().is_none());
    assert_eq!(rig.close(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{\"n\":2}"]);
}

#[test]
fn old_handles_cannot_touch_the_new_handles_of_their_name() {
    let mut rig = Rig::new();
    let _keep_engine = rig.connected("/keep");
    let old = rig.connected("/");
    rig.disconnect("/");
    rig.messages();
    let new = rig.connect("/", "{}", 32);
    new.result.unwrap();
    rig.messages();
    rig.confirm("/");

    // An old slot is refused, and the old handles' disconnect does nothing.
    assert!(old.capacity.is_closed());
    rig.core.disconnect(&"/".into(), &old.capacity);
    assert!(!new.closed.is_cancelled());
    assert_eq!(rig.core.connection("/", &old.capacity), None);
    rig.send("/", event("[\"new\"]"));
    assert_eq!(rig.messages(), ["2[\"new\"]"]);
}

#[test]
fn disconnecting_one_namespace_keeps_the_engine_for_the_other() {
    let mut rig = Rig::new();
    rig.connected("/");
    let _second = rig.connected("/b");

    assert_eq!(rig.disconnect("/"), []);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(rig.effects(), []);

    rig.send("/b", event("[\"still\"]"));
    assert_eq!(rig.messages(), ["2/b,[\"still\"]"]);
}

#[test]
fn server_packets_for_a_namespace_the_client_never_opened_are_discarded() {
    let mut rig = Rig::new();
    let mut handles = rig.connected("/");
    handles.server_packet_rx.try_recv().unwrap();

    rig.server("2/gone,[\"late\"]");
    rig.server("3/gone,0[\"late\"]");
    rig.server("1/gone,");
    assert_eq!(rig.effects(), []);
    assert_eq!(rig.messages(), Vec::<String>::new());
    handles.server_packet_rx.try_recv().unwrap_err();
    rig.send("/", event("[\"live\"]"));
    assert_eq!(rig.messages(), ["2[\"live\"]"]);
}

#[test]
fn a_repeated_server_connect_sends_nothing_more() {
    let mut rig = Rig::new();
    let _handles = rig.connected("/");
    rig.confirm("/");
    assert_eq!(rig.effects(), []);
    assert_eq!(rig.messages(), Vec::<String>::new());

    rig.send("/", event("[\"once\"]"));
    assert_eq!(rig.messages(), ["2[\"once\"]"]);
}

#[test]
fn a_blocked_packet_of_old_handles_never_reaches_new_ones() {
    let mut rig = Rig::new();
    let _old = rig.connect("/", "{}", 1);
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    let blocked = rig.server("2[\"old\"]").expect("the packet is blocked");

    rig.disconnect("/");
    rig.messages();
    let mut new = rig.connect("/", "{}", 1);
    assert!(new.result.unwrap().is_none());

    // The blocked packet keeps the old receiver, and its token tells the
    // driver to discard it.
    assert!(blocked.closed.is_cancelled());
    assert_eq!(
        new.server_packet_rx.try_recv().unwrap_err(),
        MpscTryRecvError::Empty
    );
}

#[test]
fn a_server_termination_after_reuse_ends_the_current_handles() {
    let mut rig = Rig::new();
    let _keep_engine = rig.connected("/keep");
    let _old = rig.connected("/");
    let (packet, mut waiter) = event_ack("[\"old\"]");
    rig.send("/", packet);
    rig.messages();
    rig.server("1");
    assert_eq!(waiter.try_recv().unwrap_err(), TryRecvError::Closed);
    let new = rig.connect("/", "{}", 1);
    new.result.unwrap();
    // The old handles are gone, so a later termination is for the current
    // ones, like `Socket.onpacket` handling DISCONNECT before `connected`.
    rig.server("1");
    rig.server("4{\"message\":\"again\"}");
    assert!(new.closed.is_cancelled());
}

#[test]
fn partial_binary_input_stays_with_the_old_handles_during_reuse() {
    for header_after_end in [false, true] {
        let mut rig = Rig::new();
        let _keep_engine = rig.connected("/keep");
        rig.connected("/");
        if !header_after_end {
            rig.server(r#"51-["old",{"_placeholder":true,"num":0}]"#);
        }
        rig.disconnect("/");
        rig.messages();
        if header_after_end {
            rig.server(r#"51-["old",{"_placeholder":true,"num":0}]"#);
        }
        let mut new = rig.connect("/", "{}", 1);
        rig.messages();

        rig.server_binary(b"old attachment");
        assert_eq!(
            new.server_packet_rx.try_recv().unwrap_err(),
            MpscTryRecvError::Empty
        );
        rig.confirm("/");
        new.server_packet_rx.try_recv().unwrap();
        rig.server(r#"51-["new",{"_placeholder":true,"num":0}]"#);
        rig.server_binary(b"new attachment");
        new.server_packet_rx.try_recv().unwrap();
    }
}
