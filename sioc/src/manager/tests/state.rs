use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig};
use crate::error::{ManagerError, SocketError};
use crate::manager::backoff::Backoff;
use crate::manager::state::ManagerInput;
use crate::packet::ServerPacket;

// Ordering and confirmation

#[test]
fn connect_goes_first_and_events_keep_order_per_namespace() {
    let mut rig = Rig::new(8);
    rig.open("/", "{}");
    rig.open("/a", "{\"t\":1}");
    assert_eq!(rig.effects(), [Effect::Open]);

    rig.emit("/", "[\"e1\"]");
    rig.emit("/a", "[\"e3\"]");
    rig.emit("/", "[\"e2\"]");
    rig.handshake();

    // Nothing but CONNECT may reach a namespace the server has not confirmed.
    assert_eq!(rig.pull(), ["0{}", "0/a,{\"t\":1}"]);
    assert_eq!(rig.channel_len("/"), 2);
    assert_eq!(rig.channel_len("/a"), 1);

    rig.confirm("/");
    assert_eq!(rig.pull(), ["2[\"e1\"]", "2[\"e2\"]"]);
    rig.confirm("/a");
    assert_eq!(rig.pull(), ["2/a,[\"e3\"]"]);
}

#[test]
fn namespaces_take_turns_for_the_engine() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.connected("/b");
    rig.emit("/", "[\"a1\"]");
    rig.emit("/", "[\"a2\"]");
    rig.emit("/b", "[\"b1\"]");
    rig.emit("/b", "[\"b2\"]");

    assert_eq!(
        rig.pull(),
        ["2[\"a1\"]", "2/b,[\"b1\"]", "2[\"a2\"]", "2/b,[\"b2\"]"]
    );
}

#[test]
fn events_wait_in_the_channel_until_a_permit_reads_them() {
    let mut rig = Rig::new(2);
    rig.connected("/");
    rig.emit("/", "[\"e1\"]");
    rig.emit("/", "[\"e2\"]");
    assert_eq!(rig.channel_len("/"), 2);

    assert_eq!(rig.pull_one().unwrap(), "2[\"e1\"]");
    assert_eq!(rig.channel_len("/"), 1);
    assert_eq!(rig.pull(), ["2[\"e2\"]"]);
    assert_eq!(rig.channel_len("/"), 0);
}

#[test]
fn the_last_handle_leaves_a_full_unconfirmed_channel() {
    let mut rig = Rig::new(1);
    rig.open("/", "{}");
    rig.emit("/", "[\"e1\"]");
    assert_eq!(rig.channel_len("/"), 1);

    // The token tells the actor to drain without a permit, so the end arrives
    // although the server never confirms. The queued CONNECT never leaves.
    rig.disconnect("/");
    assert_eq!(rig.pull(), Vec::<String>::new());
    assert_eq!(rig.effects(), [Effect::Open, Effect::Close]);
    rig.close();
    assert!(rig.core.is_idle());
}

// Engine loss and acks

#[test]
fn engine_loss_fails_sent_waiters_and_unsent_events_go_out_after_confirmation() {
    let mut rig = Rig::reconnecting(8);
    rig.connected("/");

    let mut first = rig.emit_ack("/", "[\"e0\"]");
    let mut second = rig.emit_ack("/", "[\"e1\"]");
    let mut third = rig.emit_ack("/", "[\"e2\"]");
    assert_eq!(rig.pull_one().unwrap(), "20[\"e0\"]");

    rig.close();
    assert_eq!(rig.effects(), [Effect::Schedule(Duration::ZERO)]);
    // Only the event the engine took lost its waiter.
    assert_eq!(first.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(second.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(third.try_recv().unwrap_err(), TryRecvError::Empty);
    assert!(!rig.core.has_message());
    assert_eq!(rig.channel_len("/"), 2);

    rig.input(ManagerInput::Reconnect);
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    // The unconfirmed namespace sends CONNECT only, and never replays e0.
    assert_eq!(rig.pull(), ["0{}"]);
    rig.confirm("/");
    assert_eq!(rig.pull(), ["21[\"e1\"]", "22[\"e2\"]"]);

    rig.server("31[\"a\"]");
    second.try_recv().unwrap();
    assert_eq!(third.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[test]
fn duplicate_and_unknown_ack_ids_complete_no_other_waiter() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    let mut first = rig.emit_ack("/", "[\"e0\"]");
    let mut second = rig.emit_ack("/", "[\"e1\"]");
    rig.pull();

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
    let mut rig = Rig::new(8);
    rig.connected("/");
    drop(rig.emit_ack("/", "[\"e0\"]"));
    let mut live = rig.emit_ack("/", "[\"e1\"]");

    assert_eq!(rig.pull(), ["20[\"e0\"]", "21[\"e1\"]"]);

    // The late answer finds no waiter, and the next one still arrives.
    rig.server("30[\"late\"]");
    rig.server("31[\"b\"]");
    live.try_recv().unwrap();
}

#[test]
fn ack_replies_wait_in_the_channel_for_the_next_confirmation() {
    let mut rig = Rig::reconnecting(8);
    rig.open("/", "{}");
    rig.handshake();

    rig.reply_ack("/", 5);
    assert_eq!(rig.pull(), ["0{}"]);
    assert_eq!(rig.channel_len("/"), 1);
    rig.confirm("/");
    assert_eq!(rig.pull(), ["35[]"]);

    rig.reply_ack("/", 6);
    rig.reply_ack("/", 7);
    assert_eq!(rig.pull_one().unwrap(), "36[]");

    // The reply the engine never took survives the engine and goes out after
    // the next confirmation.
    rig.close();
    rig.input(ManagerInput::Reconnect);
    rig.handshake();
    assert_eq!(rig.pull(), ["0{}"]);
    rig.confirm("/");
    assert_eq!(rig.pull(), ["37[]"]);
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
        let mut rig = Rig::new(8);
        let mut handles = rig.open_with("/", "{}", 1);
        rig.handshake();
        rig.pull();
        // The Connect packet fills the receiver.
        rig.confirm("/");
        rig.effects();

        let mut sent = rig.emit_ack("/", "[\"a\"]");
        rig.pull();
        let mut unsent = rig.emit_ack("/", "[\"b\"]");

        rig.server(end);

        assert_eq!(sent.try_recv().unwrap_err(), TryRecvError::Closed);
        // The old handles still drain, so the unsent event waits in the channel.
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
        rig.poll("/");
        assert_eq!(unsent.try_recv().unwrap_err(), TryRecvError::Closed);
        assert_eq!(rig.effects(), [Effect::Close]);
    }
}

#[test]
fn a_server_ended_namespace_keeps_the_engine_until_its_handles_end() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.server("1");
    assert_eq!(rig.effects(), []);
    rig.poll("/");
    assert_eq!(rig.effects(), [Effect::Close]);
}

// Client disconnect

#[test]
fn disconnect_after_confirmation_sends_the_channel_then_disconnect_then_closes() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.emit("/", "[\"e1\"]");
    rig.emit("/", "[\"e2\"]");

    rig.disconnect("/");
    assert_eq!(rig.effects(), []);

    assert_eq!(rig.pull_one().unwrap(), "2[\"e1\"]");
    assert_eq!(rig.pull_one().unwrap(), "2[\"e2\"]");
    assert_eq!(rig.effects(), []);
    assert_eq!(rig.pull_one().unwrap(), "1");
    assert_eq!(rig.effects(), [Effect::Close]);
    assert!(rig.pull_one().is_none());
}

#[test]
fn disconnect_before_confirmation_sends_nothing() {
    // CONNECT has not left yet, so the server never hears of the namespace and
    // the queued CONNECT goes back.
    let mut rig = Rig::new(8);
    rig.open("/", "{}");
    rig.emit("/", "[\"e1\"]");
    rig.disconnect("/");
    assert_eq!(rig.pull(), Vec::<String>::new());
    assert_eq!(rig.effects(), [Effect::Open, Effect::Close]);

    // CONNECT left but the server has not confirmed it: no DISCONNECT.
    let mut rig = Rig::new(8);
    rig.open("/", "{}");
    rig.handshake();
    assert_eq!(rig.pull(), ["0{}"]);
    rig.emit("/", "[\"e1\"]");
    rig.disconnect("/");
    assert_eq!(rig.pull(), Vec::<String>::new());
    assert_eq!(rig.effects(), [Effect::Open, Effect::Close]);
}

#[test]
fn engine_loss_before_a_disconnect_drains_ends_the_namespace_without_reconnecting() {
    let mut rig = Rig::reconnecting(8);
    rig.connected("/");
    let mut ack = rig.emit_ack("/", "[\"e1\"]");
    rig.disconnect("/");

    rig.close();
    assert_eq!(rig.effects(), [Effect::Schedule(Duration::ZERO)]);
    // The unsent event keeps its waiter until the channel drains.
    assert_eq!(ack.try_recv().unwrap_err(), TryRecvError::Empty);

    rig.poll("/");
    assert_eq!(ack.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(rig.effects(), [Effect::Cancel]);
    assert!(rig.core.is_idle());
}

// Reopening a name

#[test]
fn reopen_waits_for_the_old_handles_and_keeps_ack_ids() {
    let mut rig = Rig::new(8);
    let _first = rig.connected("/");
    let mut old_ack = rig.emit_ack("/", "[\"x\"]");
    rig.pull();

    rig.server("1");
    assert_eq!(old_ack.try_recv().unwrap_err(), TryRecvError::Closed);

    let mut second = rig.open("/", "{\"n\":2}");
    assert_eq!(second.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(rig.effects(), []);

    let mut third = rig.open("/", "{}");
    assert!(matches!(
        third.reply_rx.try_recv(),
        Ok(Err(SocketError::NamespaceConflict { .. }))
    ));

    // The old channel ends, so the waiting request takes the name on the same
    // engine.
    rig.poll("/");
    assert!(matches!(second.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), []);

    assert_eq!(rig.pull(), ["0{\"n\":2}"]);
    rig.confirm("/");
    let _new_ack = rig.emit_ack("/", "[\"y\"]");
    assert_eq!(rig.pull(), ["21[\"y\"]"]);
}

#[test]
fn live_handles_that_have_not_left_keep_the_name() {
    let mut rig = Rig::new(8);
    let _first = rig.connected("/");

    let mut second = rig.open("/", "{}");
    assert!(matches!(
        second.reply_rx.try_recv(),
        Ok(Err(SocketError::NamespaceConflict { .. }))
    ));
}

#[test]
fn a_reopen_after_the_handles_left_waits_instead_of_conflicting() {
    let mut rig = Rig::new(8);
    let first = rig.connected("/");

    // The handles left, but the actor has not read the end of the channel yet.
    first.closed.cancel();
    let mut again = rig.open("/", "{\"n\":2}");
    assert_eq!(again.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);

    assert_eq!(rig.pull(), ["1", "0{\"n\":2}"]);
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), []);
}

#[test]
fn a_reopen_waits_for_the_remaining_packets_and_disconnect() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.emit("/", "[\"e1\"]");
    rig.disconnect("/");

    let mut again = rig.open("/", "{\"n\":2}");
    assert_eq!(again.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);

    assert_eq!(rig.pull(), ["2[\"e1\"]", "1", "0{\"n\":2}"]);
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), []);
}

#[test]
fn a_namespace_that_ends_before_its_channel_was_listened_to_frees_its_name() {
    let mut rig = Rig::new(8);
    // Hand the request to the core, but drain no output, so the actor has not
    // taken the channel when the engine ends.
    let (request, mut handles) = rig.request("/", "{}", 32);
    rig.core.handle(ManagerInput::Connect(request)).unwrap();
    rig.core.handle(ManagerInput::Closed(Ok(()))).unwrap();
    rig.pump();

    assert!(matches!(handles.reply_rx.try_recv(), Ok(Ok(()))));
    assert!(handles.closed.is_cancelled());
    assert!(rig.core.is_idle());

    let mut again = rig.open("/", "{}");
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
}

// Engine lifecycle

#[test]
fn the_first_engine_opens_at_once_and_stays_without_a_namespace() {
    let mut rig = Rig::new(8);
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.effects(), []);
    assert!(!rig.core.is_idle());
}

#[test]
fn release_with_no_namespace_closes_the_first_engine() {
    let mut rig = Rig::new(8);
    rig.input(ManagerInput::Release);
    assert_eq!(rig.effects(), [Effect::Open, Effect::Close]);
    assert!(!rig.core.is_idle());

    rig.close();
    assert_eq!(rig.effects(), []);
    assert!(rig.core.is_idle());
}

#[test]
fn no_engine_opens_while_one_runs_or_closes() {
    let mut rig = Rig::reconnecting(8);
    rig.open("/", "{}");
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.open("/b", "{}");
    rig.handshake();
    assert_eq!(rig.effects(), []);

    rig.server("garbage");
    assert_eq!(rig.effects(), [Effect::Close]);
    rig.open("/c", "{}");
    assert_eq!(rig.effects(), []);

    rig.close();
    assert_eq!(rig.effects(), [Effect::Schedule(Duration::ZERO)]);
    rig.open("/d", "{}");
    assert_eq!(rig.effects(), []);

    rig.input(ManagerInput::Reconnect);
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.pull(), ["0{}", "0/b,{}", "0/c,{}", "0/d,{}"]);
}

#[test]
fn a_namespace_opened_while_the_client_closes_the_engine_opens_the_next_at_once() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.disconnect("/");
    assert_eq!(rig.pull(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);

    rig.open("/b", "{}");
    assert_eq!(rig.effects(), []);

    rig.close();
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.pull(), ["0/b,{}"]);
}

#[test]
fn the_last_namespace_ending_during_a_wait_cancels_it() {
    let mut rig = Rig::reconnecting(8);
    rig.connected("/");
    rig.close();
    assert_eq!(rig.effects(), [Effect::Schedule(Duration::ZERO)]);

    rig.disconnect("/");
    assert_eq!(rig.effects(), [Effect::Cancel]);
    assert!(rig.core.is_idle());

    // A timer that fired before the cancel arrived changes nothing.
    rig.input(ManagerInput::Reconnect);
    assert_eq!(rig.effects(), []);
}

#[test]
fn exhaustion_ends_namespaces_and_the_core_takes_new_ones() {
    let backoff = Backoff::new(Duration::ZERO, Duration::ZERO, 0.0, Some(1));
    let mut rig = Rig::with_backoff(8, Some(backoff));
    let mut handles = rig.connected("/");

    rig.close();
    rig.input(ManagerInput::Reconnect);
    assert_eq!(
        rig.effects(),
        [Effect::Schedule(Duration::ZERO), Effect::Open]
    );

    rig.close();
    assert_eq!(rig.effects(), []);
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

    rig.disconnect("/");
    assert!(rig.core.is_idle());

    let mut again = rig.open("/", "{}");
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), [Effect::Open]);
}

#[test]
fn without_reconnection_the_namespaces_end() {
    let mut rig = Rig::new(8);
    let handles = rig.connected("/");
    rig.close();
    assert_eq!(rig.effects(), []);
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

    let mut rig = Rig::seeded(8, Some(new_backoff()), 42);
    rig.connected("/");

    rig.close();
    rig.input(ManagerInput::Reconnect);
    // No handshake: the attempt count keeps growing.
    rig.close();
    rig.input(ManagerInput::Reconnect);
    rig.handshake();
    rig.close();

    assert_eq!(
        rig.effects(),
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
    let mut rig = Rig::reconnecting(8);
    let mut handles = rig.connected("/");
    let mut unsent = rig.emit_ack("/", "[\"e\"]");

    let result = rig.try_input(ManagerInput::Closed(Err(ManagerError::EngineEnded)));
    assert!(matches!(result, Err(ManagerError::EngineEnded)));

    rig.shutdown();
    assert_eq!(unsent.try_recv().unwrap_err(), TryRecvError::Closed);
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
    let mut rig = Rig::new(8);
    let mut handles = rig.open_with("/", "{}", 1);
    rig.handshake();
    rig.pull();
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
    let mut rig = Rig::new(8);
    let mut handles = rig.open_with("/", "{}", 1);
    rig.handshake();
    rig.pull();
    rig.confirm("/");
    rig.server("2[\"e1\"]");
    let sender = rig.core.held_sender().expect("the packet is held");

    // A confirmed namespace sends DISCONNECT, which ends it for the core.
    rig.disconnect("/");
    assert_eq!(rig.pull(), ["1"]);
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
    let mut rig = Rig::new(8);
    let handles = rig.open_with("/", "{}", 1);
    rig.handshake();
    drop(handles.server_packet_rx);

    rig.confirm("/");
    rig.server("2[\"e1\"]");
    assert!(rig.core.held_sender().is_none());
    rig.server("2[\"e2\"]");
}
