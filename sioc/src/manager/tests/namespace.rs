//! How a namespace connects, ends from either side, and is reopened under
//! its name. Reuse keeps old deliveries and cleanup with the old handles.

use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig, disconnect, event, event_ack, listen, packet};
use crate::error::SocketError;
use crate::manager::namespace::Read;
use crate::manager::state::ManagerInput;
use crate::packet::ServerPacket;

#[test]
fn the_last_handle_leaves_an_unconfirmed_namespace() {
    let mut rig = Rig::new();
    let handles = rig.connect("/", "{}", 32);
    assert_eq!(rig.effects(), [Effect::Open, listen("/")]);

    // The token tells the actor to drain without a permit, so the end arrives
    // although the server never confirms. The queued CONNECT never leaves.
    handles.closed.cancel();
    assert_eq!(rig.core.read("/", true), Read::Discard);
    assert_eq!(rig.step(disconnect("/")), [Effect::Close]);
    assert_eq!(rig.messages(), Vec::<String>::new());
    rig.step(ManagerInput::Closed(Ok(())));
    assert!(rig.core.is_idle());
}

#[test]
fn server_end_fails_waiters_and_bypasses_a_full_receiver() {
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

        rig.server(end);

        assert_eq!(sent.try_recv().unwrap_err(), TryRecvError::Closed);
        // The old handles still drain, so the unsent event is still theirs.
        assert_eq!(unsent.try_recv().unwrap_err(), TryRecvError::Empty);
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
        assert_eq!(rig.effects(), []);

        // The old handles drain to the end of their channel, which fails the
        // unsent waiter and lets the engine go.
        assert_eq!(rig.core.read("/", true), Read::Discard);
        assert_eq!(rig.step(packet("/", unsent_packet)), []);
        assert_eq!(unsent.try_recv().unwrap_err(), TryRecvError::Closed);
        assert_eq!(rig.step(disconnect("/")), [Effect::Close]);
    }
}

#[test]
fn a_server_ended_namespace_keeps_the_engine_until_its_handles_end() {
    let mut rig = Rig::new();
    rig.connected("/");
    rig.server("1");
    assert_eq!(rig.effects(), []);
    assert_eq!(rig.core.read("/", true), Read::Discard);
    assert_eq!(rig.step(disconnect("/")), [Effect::Close]);
}

#[test]
fn disconnect_after_confirmation_sends_the_channel_then_disconnect_then_closes() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");

    // The handles leave, but the core still reads what they sent.
    handles.closed.cancel();
    assert_eq!(rig.core.read("/", true), Read::Send);
    rig.send("/", event("[\"e1\"]"));
    assert_eq!(rig.messages(), ["2[\"e1\"]"]);
    rig.send("/", event("[\"e2\"]"));
    assert_eq!(rig.messages(), ["2[\"e2\"]"]);
    assert_eq!(rig.effects(), []);

    assert_eq!(rig.step(disconnect("/")), []);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn disconnect_before_confirmation_sends_nothing() {
    // CONNECT has not left yet, so the server never hears of the namespace and
    // the queued CONNECT goes back.
    let mut rig = Rig::new();
    let handles = rig.connect("/", "{}", 32);
    assert_eq!(rig.effects(), [Effect::Open, listen("/")]);
    handles.closed.cancel();
    assert_eq!(rig.core.read("/", true), Read::Discard);
    assert_eq!(rig.step(disconnect("/")), [Effect::Close]);
    assert_eq!(rig.messages(), Vec::<String>::new());

    // CONNECT left but the server has not confirmed it: no DISCONNECT.
    let mut rig = Rig::new();
    let handles = rig.connect("/", "{}", 32);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}"]);
    assert_eq!(rig.effects(), [Effect::Open, listen("/")]);
    handles.closed.cancel();
    assert_eq!(rig.core.read("/", true), Read::Discard);
    assert_eq!(rig.step(disconnect("/")), [Effect::Close]);
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn engine_loss_before_a_disconnect_drains_ends_the_namespace_without_reconnecting() {
    let mut rig = Rig::reconnecting();
    let handles = rig.connected("/");
    let (unsent_packet, mut ack) = event_ack("[\"e1\"]");
    handles.closed.cancel();

    let effects = rig.step(ManagerInput::Closed(Ok(())));
    assert_eq!(effects, []);
    // A retry timer winning before inbox EOF must not start an unwanted
    // successor for participation that has already stopped accepting work.
    assert_eq!(rig.step(ManagerInput::Reconnect), []);
    // The unsent event keeps its waiter until the handles drain.
    assert_eq!(ack.try_recv().unwrap_err(), TryRecvError::Empty);

    assert_eq!(rig.core.read("/", true), Read::Discard);
    assert_eq!(rig.step(packet("/", unsent_packet)), []);
    assert_eq!(ack.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(rig.step(disconnect("/")), []);
    assert!(rig.core.is_idle());
}

#[test]
fn reopen_waits_for_the_old_handles_and_keeps_ack_ids() {
    let mut rig = Rig::new();
    let _first = rig.connected("/");
    let (old_packet, mut old_ack) = event_ack("[\"x\"]");
    rig.send("/", old_packet);
    rig.messages();

    rig.server("1");
    assert_eq!(old_ack.try_recv().unwrap_err(), TryRecvError::Closed);

    let mut second = rig.connect("/", "{\"n\":2}", 32);
    assert_eq!(second.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(rig.effects(), []);

    let mut third = rig.connect("/", "{}", 32);
    assert!(matches!(
        third.reply_rx.try_recv(),
        Ok(Err(SocketError::NamespaceConflict { .. }))
    ));

    // The old channel ends, so the waiting request takes the name on the same
    // engine.
    assert_eq!(rig.step(disconnect("/")), [listen("/")]);
    assert!(matches!(second.reply_rx.try_recv(), Ok(Ok(()))));

    assert_eq!(rig.messages(), ["0{\"n\":2}"]);
    rig.confirm("/");
    let (new_packet, _new_ack) = event_ack("[\"y\"]");
    rig.send("/", new_packet);
    assert_eq!(rig.messages(), ["21[\"y\"]"]);
}

#[test]
fn live_handles_that_have_not_left_keep_the_name() {
    let mut rig = Rig::new();
    let _first = rig.connected("/");

    let mut second = rig.connect("/", "{}", 32);
    assert!(matches!(
        second.reply_rx.try_recv(),
        Ok(Err(SocketError::NamespaceConflict { .. }))
    ));
}

#[test]
fn a_reopen_after_the_handles_left_waits_instead_of_conflicting() {
    let mut rig = Rig::new();
    let first = rig.connected("/");

    // The handles left, but the core has not heard the end of the channel yet.
    first.closed.cancel();
    let mut again = rig.connect("/", "{\"n\":2}", 32);
    assert_eq!(again.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);

    assert_eq!(rig.step(disconnect("/")), [listen("/")]);
    assert_eq!(rig.messages(), ["1", "0{\"n\":2}"]);
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
}

#[test]
fn a_reopen_waits_for_the_remaining_packets_and_disconnect() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");
    rig.send("/", event("[\"e1\"]"));
    handles.closed.cancel();

    let mut again = rig.connect("/", "{\"n\":2}", 32);
    assert_eq!(again.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);

    assert_eq!(rig.step(disconnect("/")), [listen("/")]);
    assert_eq!(rig.messages(), ["2[\"e1\"]", "1", "0{\"n\":2}"]);
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
}

#[test]
fn a_namespace_that_ends_before_its_channel_was_listened_to_frees_its_name() {
    let mut rig = Rig::new();
    // Drain no effect, so the actor has not taken the channel when the engine
    // ends.
    let mut handles = rig.connect("/", "{}", 32);
    rig.core.handle(ManagerInput::Closed(Ok(()))).unwrap();

    assert!(matches!(handles.reply_rx.try_recv(), Ok(Ok(()))));
    assert!(handles.closed.is_cancelled());
    assert!(rig.core.is_idle());
    assert_eq!(rig.effects(), [Effect::Open]);

    let mut again = rig.connect("/", "{}", 32);
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
}

#[test]
fn disconnecting_one_namespace_keeps_the_engine_for_the_other() {
    let mut rig = Rig::new();
    let first = rig.connected("/");
    let _second = rig.connected("/b");

    first.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), []);
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
    assert_eq!(rig.core.read("/", true), Read::Send);
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
fn reopen_discards_the_old_held_delivery_before_installing_new_handles() {
    let mut rig = Rig::new();
    let old = rig.connect("/", "{}", 1);
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    rig.server("2[\"old\"]");
    assert!(rig.core.held_sender().is_some());

    old.closed.cancel();
    let mut new = rig.connect("/", "{}", 1);
    assert_eq!(new.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);
    rig.step(disconnect("/"));

    assert!(rig.core.held_sender().is_none());
    new.reply_rx.try_recv().unwrap().unwrap();
    assert_eq!(
        new.server_packet_rx.try_recv().unwrap_err(),
        MpscTryRecvError::Empty
    );
}

#[test]
fn repeated_termination_keeps_the_old_participation_draining() {
    let mut rig = Rig::new();
    let _old = rig.connected("/");
    let (packet, mut waiter) = event_ack("[\"old\"]");
    rig.send("/", packet);
    rig.messages();
    rig.server("1");
    let mut new = rig.connect("/", "{}", 1);
    rig.server("1");
    rig.server("4{\"message\":\"again\"}");
    assert_eq!(new.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(waiter.try_recv().unwrap_err(), TryRecvError::Closed);
    rig.step(disconnect("/"));
    new.reply_rx.try_recv().unwrap().unwrap();
}

#[test]
fn partial_binary_input_stays_with_the_old_participation_during_reuse() {
    for header_after_end in [false, true] {
        let mut rig = Rig::new();
        let _keep_engine = rig.connected("/keep");
        let old = rig.connected("/");
        if !header_after_end {
            rig.server(r#"51-["old",{"_placeholder":true,"num":0}]"#);
        }
        old.closed.cancel();
        rig.step(disconnect("/"));
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
