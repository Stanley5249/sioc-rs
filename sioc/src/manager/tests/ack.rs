//! Ack waiters for sent events, and the replies the handles send.

use std::time::Duration;

use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig, ack, event_ack};
use crate::manager::namespace::Read;
use crate::manager::state::ManagerInput;

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
