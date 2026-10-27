//! Ack waiters for sent events, and the replies the handles send.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig, TestHandles, ack, event, event_ack};
use crate::error::SocketError;
use crate::packet::{ClientPacket, DynAckId, ServerPacket};

#[test]
fn engine_loss_fails_sent_waiters_and_unsent_events_go_out_after_confirmation() {
    let mut rig = Rig::reconnecting();
    rig.connected("/");

    let (first_packet, mut first) = event_ack("[\"e0\"]");
    let (second_packet, mut second) = event_ack("[\"e1\"]");
    let (third_packet, mut third) = event_ack("[\"e2\"]");
    rig.send("/", first_packet);
    assert_eq!(rig.messages(), ["20[\"e0\"]"]);
    rig.send("/", second_packet);
    rig.send("/", third_packet);

    assert_eq!(rig.close(), [Effect::Schedule(Duration::ZERO)]);
    // Only the event the engine took lost its waiter.
    assert_eq!(first.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(second.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(third.try_recv().unwrap_err(), TryRecvError::Empty);
    assert!(!rig.core.has_message());

    assert_eq!(rig.reconnect(), [Effect::Open]);
    rig.handshake();
    // The unconfirmed namespace sends CONNECT only, and never replays e0.
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");
    assert_eq!(rig.messages(), ["21[\"e1\"]", "22[\"e2\"]"]);

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
    rig.send("/", live_packet);
    assert_eq!(rig.messages(), ["20[\"e0\"]", "21[\"e1\"]"]);

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

    // A reply while unconfirmed is discarded, not buffered.
    rig.send_stale("/", ack(rig.ack_id("/", 4)));
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");
    rig.send("/", ack(rig.ack_id("/", 5)));
    rig.send("/", ack(rig.ack_id("/", 6)));
    assert_eq!(rig.messages(), ["35[]", "36[]"]);

    // A queued reply never reaches the next engine.
    rig.send("/", ack(rig.ack_id("/", 7)));
    rig.close();
    rig.send_stale("/", ack(rig.ack_id("/", 8)));
    rig.reconnect();
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");
    rig.send("/", ack(rig.ack_id("/", 9)));
    assert_eq!(rig.messages(), ["39[]"]);
}

#[test]
fn a_reply_captured_for_an_ended_connection_is_rejected() {
    let mut rig = Rig::reconnecting();
    let handles = rig.connected("/");
    let connection = rig.core.connection("/").expect("the socket is connected");
    let stale = rig.ack_id("/", 1);
    // A reply that waited for capacity across the reconnect stays behind.
    let slot = std::sync::Arc::clone(&handles.reply_capacity)
        .try_acquire_owned()
        .unwrap();

    rig.close();
    rig.reconnect();
    rig.handshake();
    rig.messages();
    rig.confirm("/");

    assert!(matches!(
        rig.core.acknowledge(&"/".into(), ack(stale), slot),
        Err(SocketError::Closed)
    ));
    assert!(connection.is_cancelled());
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn replies_and_events_keep_their_submission_order() {
    let mut rig = Rig::new();
    rig.connected("/");
    rig.send("/", event("[\"e1\"]"));
    rig.send("/", ack(rig.ack_id("/", 3)));
    rig.send("/", event("[\"e2\"]"));
    assert_eq!(rig.messages(), ["2[\"e1\"]", "33[]", "2[\"e2\"]"]);
}

/// The ack id of the next event that reached the handles, as the application
/// would take it.
fn delivered_id(handles: &mut TestHandles) -> DynAckId {
    loop {
        let packet = handles.server_packet_rx.try_recv();
        if let ServerPacket::Event(event) = packet.expect("an event arrived") {
            return event.id.expect("the event asks for a reply");
        }
    }
}

const BINARY_REPLY: &str = "[{\"_placeholder\":true,\"num\":0}]";

fn binary_ack(id: DynAckId) -> ClientPacket {
    ClientPacket::Ack {
        payload: BINARY_REPLY.into(),
        id,
        attachments: Some(vec![Bytes::from_static(b"r")]),
    }
}

#[test]
fn delivery_binds_the_id_so_its_reply_goes_out_on_that_connection() {
    let mut rig = Rig::new();
    let mut handles = rig.connected("/");

    rig.server("25[\"ask\"]");
    let id = delivered_id(&mut handles);
    assert_eq!(id.get(), 5);
    rig.send("/", ack(id));
    assert_eq!(rig.messages(), ["35[]"]);
}

#[test]
fn a_reply_with_a_reused_wire_id_goes_only_to_the_connection_that_asked() {
    let mut rig = Rig::reconnecting();
    let mut handles = rig.connected("/");
    rig.server("25[\"ask\"]");
    let old = delivered_id(&mut handles);

    rig.close();
    rig.reconnect();
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    // The server counts ids per connection, so the new request has id 5 too.
    rig.server("25[\"again\"]");
    let new = delivered_id(&mut handles);
    assert_eq!(old.get(), new.get());

    rig.send_stale("/", ack(old));
    assert_eq!(rig.messages(), Vec::<String>::new());
    rig.send("/", ack(new));
    assert_eq!(rig.messages(), ["35[]"]);
}

#[test]
fn a_reply_with_an_id_from_replaced_handles_is_rejected() {
    let mut rig = Rig::new();
    let _keep_engine = rig.connected("/keep");
    let mut a = rig.connected("/");
    rig.server("25[\"ask\"]");
    let id_a = delivered_id(&mut a);

    rig.disconnect("/");
    assert_eq!(rig.messages(), ["1"]);
    let mut b = rig.connect("/", "{}", 32);
    b.result.as_ref().unwrap();
    assert_eq!(rig.messages(), ["0{}"]);
    rig.confirm("/");

    rig.send_stale("/", ack(id_a));
    assert_eq!(rig.messages(), Vec::<String>::new());
    rig.server("27[\"ask\"]");
    let id_b = delivered_id(&mut b);
    rig.send("/", ack(id_b));
    assert_eq!(rig.messages(), ["37[]"]);
}

#[test]
fn a_reply_through_another_namespace_is_rejected() {
    let mut rig = Rig::new();
    let mut a = rig.connected("/a");
    rig.connected("/b");
    rig.server("2/a,5[\"ask\"]");
    let id = delivered_id(&mut a);

    rig.send_stale("/b", ack(id.clone()));
    assert_eq!(rig.messages(), Vec::<String>::new());
    rig.send("/a", ack(id));
    assert_eq!(rig.messages(), ["3/a,5[]"]);
}

#[test]
fn a_bound_binary_reply_sends_its_header_and_attachment_but_a_stale_one_sends_neither() {
    let mut rig = Rig::reconnecting();
    let mut handles = rig.connected("/");
    rig.server("25[\"ask\"]");
    let id = delivered_id(&mut handles);
    rig.server("26[\"ask\"]");
    let stale = delivered_id(&mut handles);

    rig.send("/", binary_ack(id));
    assert_eq!(
        rig.messages(),
        [format!("61-5{BINARY_REPLY}"), "bin:r".into()]
    );

    rig.close();
    rig.reconnect();
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    rig.send_stale("/", binary_ack(stale));
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn a_detached_id_is_rejected_on_a_confirmed_connection() {
    let mut rig = Rig::new();
    rig.connected("/");

    rig.send_stale("/", ack(DynAckId::new(5)));
    assert_eq!(rig.messages(), Vec::<String>::new());
}
