use bytes::Bytes;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig};
use crate::manager::core::input_output::ManagerInput;
use crate::packet::ClientPacket;

#[test]
fn connect_goes_first_and_buffered_events_keep_order() {
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

    rig.confirm("/");
    assert_eq!(rig.pull(), ["2[\"e1\"]", "2[\"e2\"]"]);
    rig.confirm("/a");
    assert_eq!(rig.pull(), ["2/a,[\"e3\"]"]);
}

#[test]
fn engine_loss_fails_pulled_waiters_and_replays_nothing() {
    let mut rig = Rig::reconnecting(8);
    rig.connected("/");

    let mut first = rig.emit_ack("/", "[\"e0\"]");
    let mut second = rig.emit_ack("/", "[\"e1\"]");
    let mut third = rig.emit_ack("/", "[\"e2\"]");
    assert_eq!(rig.pull_one().unwrap(), "20[\"e0\"]");

    rig.close();
    assert_eq!(rig.effects(), [Effect::Schedule(std::time::Duration::ZERO)]);
    assert_eq!(first.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(second.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(third.try_recv().unwrap_err(), TryRecvError::Empty);
    assert!(!rig.core.has_message());

    rig.input(ManagerInput::Reconnect);
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
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
fn the_inbox_waits_while_the_buffer_is_full() {
    let mut rig = Rig::new(2);
    rig.connected("/");
    rig.emit("/", "[\"e1\"]");
    rig.emit("/", "[\"e2\"]");
    rig.emit("/", "[\"e3\"]");

    // Two slots, so the third packet stays in the inbox and its sender feels
    // the backpressure.
    assert_eq!(rig.inbox_len("/"), 1);
    rig.poll("/");
    assert_eq!(rig.inbox_len("/"), 1);

    assert_eq!(rig.pull_one().unwrap(), "2[\"e1\"]");
    rig.poll("/");
    assert_eq!(rig.inbox_len("/"), 0);
    assert_eq!(rig.pull(), ["2[\"e2\"]", "2[\"e3\"]"]);
}

#[test]
fn the_last_handle_leaves_a_full_unconfirmed_buffer() {
    let mut rig = Rig::new(1);
    rig.open("/", "{}");
    rig.emit("/", "[\"e1\"]");
    rig.emit("/", "[\"e2\"]");
    assert_eq!(rig.inbox_len("/"), 1);

    // The token tells the actor to drain without room, so the end arrives
    // although the buffer is full and the server never confirms.
    rig.disconnect("/");
    assert_eq!(rig.pull(), Vec::<String>::new());
    assert_eq!(rig.effects(), [Effect::Open, Effect::Close]);
    rig.close();
    assert!(rig.core.is_idle());
}

#[test]
fn binary_ack_replies_leave_with_their_attachments() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.send(
        "/",
        ClientPacket::Ack {
            payload: "[{\"_placeholder\":true,\"num\":0}]".into(),
            id: 4,
            attachments: Some(vec![Bytes::from_static(b"r")]),
        },
    );
    assert_eq!(
        rig.pull(),
        ["61-4[{\"_placeholder\":true,\"num\":0}]", "bin:r"]
    );
}

#[test]
fn ack_replies_wait_for_confirmation_and_die_with_the_engine() {
    let mut rig = Rig::reconnecting(8);
    rig.open("/", "{}");
    rig.handshake();

    rig.reply_ack("/", 5);
    assert_eq!(rig.pull(), ["0{}"]);
    rig.confirm("/");
    assert_eq!(rig.pull(), Vec::<String>::new());

    rig.reply_ack("/", 6);
    rig.reply_ack("/", 7);
    assert_eq!(rig.pull_one().unwrap(), "36[]");

    rig.close();
    rig.input(ManagerInput::Reconnect);
    rig.handshake();
    rig.confirm("/");
    assert_eq!(rig.pull(), ["0{}"]);
}
