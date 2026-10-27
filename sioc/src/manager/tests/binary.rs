//! Binary packets in both directions, driven through the core.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc::error::TryRecvError;

use super::rig::{Effect, Rig, event, event_ack};
use crate::packet::{ClientPacket, ServerPacket};

const PLACEHOLDER: &str = "{\"_placeholder\":true,\"num\":0}";

#[test]
fn binary_packets_leave_with_their_attachments_before_other_messages() {
    let mut rig = Rig::new();
    rig.connected("/");
    rig.connected("/b");

    let payload = "[\"e\",{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]";
    let attachments = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    rig.send(
        "/",
        ClientPacket::Event {
            payload: payload.into(),
            ack_tx: None,
            attachments: Some(attachments),
        },
    );
    rig.send("/b", event("[\"x\"]"));
    rig.send("/", event("[\"y\"]"));

    // Packets queued behind it wait until its attachments left.
    assert_eq!(
        rig.messages(),
        [
            format!("52-{payload}").as_str(),
            "bin:a",
            "bin:b",
            "2/b,[\"x\"]",
            "2[\"y\"]"
        ]
    );
}

#[test]
fn binary_ack_replies_leave_with_their_attachments() {
    let mut rig = Rig::new();
    rig.connected("/");
    rig.send(
        "/",
        ClientPacket::Ack {
            payload: "[{\"_placeholder\":true,\"num\":0}]".into(),
            id: rig.ack_id("/", 4),
            attachments: Some(vec![Bytes::from_static(b"r")]),
        },
    );
    assert_eq!(
        rig.messages(),
        ["61-4[{\"_placeholder\":true,\"num\":0}]", "bin:r"]
    );
}

#[test]
fn incoming_binary_packets_are_rebuilt() {
    let mut rig = Rig::new();
    let mut handles = rig.connect("/", "{}", 32);
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    handles.server_packet_rx.try_recv().unwrap();
    let (packet, mut ack) = event_ack("[\"q\"]");
    rig.send("/", packet);
    rig.messages();

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
fn server_protocol_errors_close_the_engine_without_an_internal_error() {
    let cases: [&dyn Fn(&mut Rig); 5] = [
        &|rig| {
            rig.server("51-[\"e\",{\"_placeholder\":true,\"num\":3}]");
        },
        // A placeholder inside an object field, as JS servers send it.
        &|rig| {
            rig.server("51-[\"e\",{\"file\":{\"_placeholder\":true,\"num\":3}}]");
        },
        &|rig| rig.server_binary(b"z"),
        &|rig| {
            rig.server(&format!("51-[\"e\",{PLACEHOLDER}]"));
            rig.server("2[\"e\"]");
        },
        &|rig| {
            rig.server("garbage");
        },
    ];

    for case in cases {
        let mut rig = Rig::reconnecting();
        let mut handles = rig.connected("/");
        assert!(matches!(
            handles.server_packet_rx.try_recv(),
            Ok(ServerPacket::Connect(_))
        ));
        case(&mut rig);
        assert_eq!(rig.effects(), [Effect::Close]);
        assert!(!rig.core.has_message());

        // A frame the broken engine still delivers reaches no handles.
        assert!(rig.server("2[\"late\"]").is_none());
        assert_eq!(rig.effects(), []);
        assert_eq!(
            handles.server_packet_rx.try_recv().unwrap_err(),
            TryRecvError::Empty
        );

        // The engine then finishes without an error, and the client reconnects.
        assert_eq!(rig.close(), [Effect::Schedule(Duration::ZERO)]);
    }
}

#[test]
fn reconstruction_restarts_with_the_next_engine() {
    let mut rig = Rig::reconnecting();
    rig.connected("/");
    rig.server(&format!("51-[\"e\",{PLACEHOLDER}]"));
    rig.close();
    rig.reconnect();
    rig.handshake();
    rig.effects();

    // The attachment belongs to a packet of the old engine.
    rig.server_binary(b"z");
    assert_eq!(rig.effects(), [Effect::Close]);
}

#[test]
fn an_incoming_binary_event_waits_for_every_attachment() {
    let mut rig = Rig::new();
    let mut handles = rig.connected("/");
    handles.server_packet_rx.try_recv().unwrap();

    let two = "{\"_placeholder\":true,\"num\":1}";
    rig.server(&format!("52-[\"e\",{PLACEHOLDER},{two}]"));
    rig.server_binary(b"a");
    handles.server_packet_rx.try_recv().unwrap_err();

    rig.server_binary(b"b");
    let Ok(ServerPacket::Event(event)) = handles.server_packet_rx.try_recv() else {
        panic!("the binary event arrives with its last attachment");
    };
    assert_eq!(
        event.attachments.unwrap(),
        [Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
}
