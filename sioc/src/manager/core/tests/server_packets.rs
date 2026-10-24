use bytes::Bytes;
use futures_util::FutureExt;
use tokio::sync::mpsc::error::SendError;

use super::rig::{Effect, Rig, event};
use crate::manager::core::input_output::ManagerInput;
use crate::packet::ServerPacket;

const PLACEHOLDER: &str = "{\"_placeholder\":true,\"num\":0}";

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
    rig.core.deliver(permit);

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
    rig.confirm("/");
    rig.server("2[\"e1\"]");
    let sender = rig.core.held_sender().expect("the packet is held");

    rig.disconnect("/");
    assert!(rig.core.held_sender().is_none());

    // A reservation that finishes late finds nothing to send.
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Ok(ServerPacket::Connect(_))
    ));
    let permit = sender.reserve_owned().now_or_never().unwrap();
    rig.core.deliver(permit);
    handles.server_packet_rx.try_recv().unwrap_err();
    rig.core.deliver(Err(SendError(())));
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

#[test]
fn binary_packets_leave_with_their_attachments_before_other_messages() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.connected("/b");

    let payload = "[\"e\",{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]";
    let attachments = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    rig.send("/", event(payload, None, Some(attachments)));
    rig.emit("/b", "[\"x\"]");
    rig.emit("/", "[\"y\"]");

    assert_eq!(
        rig.pull(),
        [
            format!("52-{payload}").as_str(),
            "bin:a",
            "bin:b",
            "2/b,[\"x\"]",
            "2[\"y\"]",
        ]
    );
}

#[test]
fn incoming_binary_packets_are_rebuilt() {
    let mut rig = Rig::new(8);
    let mut handles = rig.open("/", "{}");
    rig.handshake();
    rig.pull();
    rig.confirm("/");
    handles.server_packet_rx.try_recv().unwrap();
    let mut ack = rig.emit_ack("/", "[\"q\"]");
    rig.pull();

    rig.server(&format!("51-[\"e\",{PLACEHOLDER}]"));
    rig.server_binary(b"z");
    rig.server(&format!("61-0[{PLACEHOLDER}]"));
    rig.server_binary(b"w");

    let Ok(ServerPacket::Event(event)) = handles.server_packet_rx.try_recv() else {
        panic!("the binary event arrives");
    };
    assert_eq!(event.attachments.unwrap(), [Bytes::from_static(b"z")]);
    assert_eq!(
        ack.try_recv().unwrap().attachments.unwrap(),
        [Bytes::from_static(b"w")]
    );
}

#[test]
fn server_protocol_errors_close_the_engine_without_an_error() {
    let cases: [&dyn Fn(&mut Rig); 4] = [
        &|rig| rig.server("51-[\"e\",{\"_placeholder\":true,\"num\":3}]"),
        &|rig| rig.server_binary(b"z"),
        &|rig| {
            rig.server(&format!("51-[\"e\",{PLACEHOLDER}]"));
            rig.server("2[\"e\"]");
        },
        &|rig| rig.server("garbage"),
    ];

    for case in cases {
        let mut rig = Rig::reconnecting(8);
        rig.connected("/");
        case(&mut rig);
        assert_eq!(rig.effects(), [Effect::Close]);
        assert!(!rig.core.has_message());
    }
}

#[test]
fn reconstruction_restarts_with_the_next_engine() {
    let mut rig = Rig::reconnecting(8);
    rig.connected("/");
    rig.server(&format!("51-[\"e\",{PLACEHOLDER}]"));
    rig.close();
    rig.input(ManagerInput::Reconnect);
    rig.handshake();
    rig.effects();

    // The attachment belongs to a packet of the old engine.
    rig.server_binary(b"z");
    assert_eq!(rig.effects(), [Effect::Close]);
}
