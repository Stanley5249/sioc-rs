use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig, ack, event, event_ack, listen};
use crate::error::{ManagerError, SocketError};
use crate::manager::backoff::Backoff;
use crate::manager::namespace::Read;
use crate::manager::state::ManagerInput;
use crate::packet::{ClientPacket, ServerPacket};

fn packet(ns: &str, packet: ClientPacket) -> ManagerInput {
    ManagerInput::Packet {
        ns: ns.into(),
        packet,
    }
}

fn disconnect(ns: &str) -> ManagerInput {
    ManagerInput::Disconnect(ns.into())
}

// Ordering and confirmation

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

// Engine loss and acks

#[test]
fn engine_loss_fails_sent_waiters_and_unsent_events_go_out_after_confirmation() {
    let mut rig = Rig::reconnecting();
    rig.connected("/");

    let (first_packet, mut first) = event_ack("[\"e0\"]");
    let (second_packet, mut second) = event_ack("[\"e1\"]");
    let (third_packet, mut third) = event_ack("[\"e2\"]");
    rig.send("/", first_packet);
    assert_eq!(rig.messages(), ["20[\"e0\"]"]);

    let effects = rig.step(ManagerInput::Closed(Ok(())));
    assert_eq!(effects, [Effect::Schedule(Duration::ZERO)]);
    // Only the event the engine took lost its waiter.
    assert_eq!(first.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(second.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(third.try_recv().unwrap_err(), TryRecvError::Empty);
    assert!(!rig.core.has_message());
    // The unsent events stay with their handles.
    assert_eq!(rig.core.read("/", true), Read::Wait);

    assert_eq!(rig.step(ManagerInput::Reconnect), [Effect::Open]);
    rig.handshake();
    // The unconfirmed namespace sends CONNECT only, and never replays e0.
    assert_eq!(rig.messages(), ["0{}"]);
    assert_eq!(rig.core.read("/", true), Read::Wait);
    rig.confirm("/");
    rig.send("/", second_packet);
    assert_eq!(rig.messages(), ["21[\"e1\"]"]);
    rig.send("/", third_packet);
    assert_eq!(rig.messages(), ["22[\"e2\"]"]);

    rig.server("31[\"a\"]");
    second.try_recv().unwrap();
    assert_eq!(third.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[test]
fn duplicate_and_unknown_ack_ids_complete_no_other_waiter() {
    let mut rig = Rig::new();
    rig.connected("/");
    let (first_packet, mut first) = event_ack("[\"e0\"]");
    let (second_packet, mut second) = event_ack("[\"e1\"]");
    rig.send("/", first_packet);
    rig.messages();
    rig.send("/", second_packet);
    rig.messages();

    rig.server("30[\"a\"]");
    assert_eq!(&*first.try_recv().unwrap().payload, "[\"a\"]");

    rig.server("30[\"again\"]");
    rig.server("399[\"unknown\"]");
    rig.server("3/other,0[\"other\"]");
    assert_eq!(second.try_recv().unwrap_err(), TryRecvError::Empty);

    rig.server("31[\"b\"]");
    assert_eq!(&*second.try_recv().unwrap().payload, "[\"b\"]");
}

#[test]
fn dropping_the_ack_handle_keeps_the_event() {
    let mut rig = Rig::new();
    rig.connected("/");
    let (dropped_packet, dropped) = event_ack("[\"e0\"]");
    drop(dropped);
    let (live_packet, mut live) = event_ack("[\"e1\"]");

    rig.send("/", dropped_packet);
    assert_eq!(rig.messages(), ["20[\"e0\"]"]);
    rig.send("/", live_packet);
    assert_eq!(rig.messages(), ["21[\"e1\"]"]);

    // The late answer finds no waiter, and the next one still arrives.
    rig.server("30[\"late\"]");
    rig.server("31[\"b\"]");
    live.try_recv().unwrap();
}

#[test]
fn a_reply_requires_the_current_engine_confirmation() {
    let mut rig = Rig::reconnecting();
    rig.connect("/", "{}", 32);
    rig.handshake();

    assert_eq!(rig.core.read("/", true), Read::Wait);
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");
    rig.send("/", ack(5));
    assert_eq!(rig.messages(), ["35[]"]);

    rig.send("/", ack(6));
    assert_eq!(rig.messages(), ["36[]"]);

    // A newly submitted reply needs current confirmation. The actor owns
    // old session inputs, which are tested separately from this send rule.
    rig.step(ManagerInput::Closed(Ok(())));
    assert_eq!(rig.core.read("/", true), Read::Wait);
    rig.step(ManagerInput::Reconnect);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}"]);
    assert_eq!(rig.core.read("/", true), Read::Wait);
    rig.confirm("/");
    rig.send("/", ack(7));
    assert_eq!(rig.messages(), ["37[]"]);
}

// Server end

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

// Client disconnect

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

// Reopening a name

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

// Engine lifecycle

#[test]
fn the_first_engine_opens_at_once_and_stays_without_a_namespace() {
    let mut rig = Rig::new();
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.effects(), []);
    assert!(!rig.core.is_idle());
}

#[test]
fn release_with_no_namespace_closes_the_first_engine() {
    let mut rig = Rig::new();
    assert_eq!(
        rig.step(ManagerInput::Release),
        [Effect::Open, Effect::Close]
    );
    assert!(!rig.core.is_idle());

    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
    assert!(rig.core.is_idle());
}

#[test]
fn release_keeps_the_engine_until_the_last_namespace_disconnects() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");

    assert_eq!(rig.step(ManagerInput::Release), []);
    assert_eq!(rig.messages(), Vec::<String>::new());
    assert_eq!(rig.core.read("/", true), Read::Send);

    handles.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), []);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);
    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
    assert!(rig.core.is_idle());
}

#[test]
fn no_engine_opens_while_one_runs_or_closes() {
    let mut rig = Rig::reconnecting();
    rig.connect("/", "{}", 32);
    rig.connect("/b", "{}", 32);
    rig.handshake();
    assert_eq!(rig.effects(), [Effect::Open, listen("/"), listen("/b")]);

    rig.server("garbage");
    assert_eq!(rig.effects(), [Effect::Close]);
    rig.connect("/c", "{}", 32);
    assert_eq!(rig.effects(), [listen("/c")]);

    let effects = rig.step(ManagerInput::Closed(Ok(())));
    assert_eq!(effects, [Effect::Schedule(Duration::ZERO)]);
    rig.connect("/d", "{}", 32);
    assert_eq!(rig.effects(), [listen("/d")]);

    assert_eq!(rig.step(ManagerInput::Reconnect), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}", "0/b,{}", "0/c,{}", "0/d,{}"]);
}

#[test]
fn a_namespace_opened_while_the_client_closes_the_engine_opens_the_next_at_once() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");
    handles.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), []);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);

    rig.connect("/b", "{}", 32);
    assert_eq!(rig.effects(), [listen("/b")]);

    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0/b,{}"]);
}

#[test]
fn a_namespace_activated_while_flushing_uses_the_same_engine_once() {
    let mut rig = Rig::new();
    let old = rig.connected("/");
    old.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), []);
    // DISCONNECT has not been handed off, so this engine is still flushing.
    let mut new = rig.connect("/new", "{}", 32);
    new.reply_rx.try_recv().unwrap().unwrap();
    assert_eq!(rig.effects(), [listen("/new")]);
    assert_eq!(rig.messages(), ["1", "0/new,{}"]);
    assert_eq!(rig.step(ManagerInput::Release), []);
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn the_last_namespace_ending_during_a_wait_cancels_it() {
    let mut rig = Rig::reconnecting();
    let handles = rig.connected("/");
    let effects = rig.step(ManagerInput::Closed(Ok(())));
    assert_eq!(effects, [Effect::Schedule(Duration::ZERO)]);

    handles.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), [Effect::Cancel]);
    assert!(rig.core.is_idle());

    // A timer that fired before the cancel arrived changes nothing.
    assert_eq!(rig.step(ManagerInput::Reconnect), []);
}

#[test]
fn exhaustion_ends_namespaces_and_the_core_takes_new_ones() {
    let backoff = Backoff::new(Duration::ZERO, Duration::ZERO, 0.0, Some(1));
    let mut rig = Rig::with_backoff(Some(backoff), 1);
    let mut handles = rig.connected("/");

    let mut effects = rig.step(ManagerInput::Closed(Ok(())));
    effects.extend(rig.step(ManagerInput::Reconnect));
    assert_eq!(effects, [Effect::Schedule(Duration::ZERO), Effect::Open]);

    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
    assert_eq!(
        handles.terminal_packet_rx.try_recv().unwrap_err(),
        TryRecvError::Closed
    );
    // The Connect packet stays readable, then the receiver ends.
    handles.server_packet_rx.try_recv().unwrap();
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Err(MpscTryRecvError::Disconnected)
    ));
    assert!(handles.closed.is_cancelled());

    rig.step(disconnect("/"));
    assert!(rig.core.is_idle());

    let mut again = rig.connect("/", "{}", 32);
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), [Effect::Open, listen("/")]);
}

#[test]
fn without_reconnection_the_namespaces_end() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");
    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
    assert!(handles.closed.is_cancelled());
}

#[test]
fn delays_use_the_seeded_rng_and_restart_after_a_handshake() {
    let new_backoff = || Backoff::new(Duration::from_secs(1), Duration::from_secs(5), 0.5, None);

    let mut rng = fastrand::Rng::with_seed(42);
    let mut mirror = new_backoff();
    let first = mirror.next_delay(&mut rng).unwrap();
    let second = mirror.next_delay(&mut rng).unwrap();
    mirror.reset();
    let third = mirror.next_delay(&mut rng).unwrap();

    let mut rig = Rig::with_backoff(Some(new_backoff()), 42);
    rig.connected("/");

    let mut effects = rig.step(ManagerInput::Closed(Ok(())));
    effects.extend(rig.step(ManagerInput::Reconnect));
    // No handshake: the attempt count keeps growing.
    effects.extend(rig.step(ManagerInput::Closed(Ok(()))));
    effects.extend(rig.step(ManagerInput::Reconnect));
    rig.handshake();
    effects.extend(rig.step(ManagerInput::Closed(Ok(()))));

    assert_eq!(
        effects,
        [
            Effect::Schedule(first),
            Effect::Open,
            Effect::Schedule(second),
            Effect::Open,
            Effect::Schedule(third),
        ]
    );
}

#[test]
fn an_internal_error_is_returned_and_shutdown_leaves_the_core_idle() {
    let mut rig = Rig::reconnecting();
    let mut handles = rig.connected("/");
    let (sent_packet, mut sent) = event_ack("[\"e\"]");
    rig.send("/", sent_packet);
    rig.messages();

    let result = rig
        .core
        .handle(ManagerInput::Closed(Err(ManagerError::EngineEnded)));
    assert!(matches!(result, Err(ManagerError::EngineEnded)));

    rig.core.shutdown();
    assert_eq!(sent.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(rig.core.read("/", true), Read::Discard);
    // The Connect packet stays readable, then the receiver ends.
    handles.server_packet_rx.try_recv().unwrap();
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Err(MpscTryRecvError::Disconnected)
    ));
    assert!(handles.closed.is_cancelled());
    assert_eq!(rig.effects(), []);

    // Nothing waits on the application, so the core is idle at once.
    assert!(rig.core.is_idle());
}

// Server packets and the receiver

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
fn without_a_namespace_a_lost_engine_is_not_reopened() {
    let mut rig = Rig::reconnecting();
    assert_eq!(rig.effects(), [Effect::Open]);

    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
    assert!(rig.core.is_idle());

    let _handles = rig.connect("/", "{}", 32);
    assert_eq!(rig.effects(), [Effect::Open, listen("/")]);
}
