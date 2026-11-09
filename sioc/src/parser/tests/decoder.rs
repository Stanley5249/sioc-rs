//! Packet-kind, namespace and attachment validation through `TryFrom`.

use bytestring::ByteString;

use crate::error::PacketError;
use crate::parser::packet::{Ns, Packet};

fn bss(s: &'static str) -> ByteString {
    ByteString::from_static(s)
}

fn decode(s: &'static str) -> Result<Ns<Packet>, PacketError> {
    bss(s).try_into()
}

#[test]
fn binary_packets_reject_attachment_counts_outside_the_limit() {
    for payload in [r#"50-["x"]"#, "60-1[]", r#"511-["x"]"#, "611-1[]"] {
        assert!(
            matches!(
                Ns::<Packet>::try_from(bss(payload)),
                Err(PacketError::AttachmentCount { .. })
            ),
            "{payload}"
        );
    }
    for payload in [r#"510-["x"]"#, "610-1[]"] {
        Ns::<Packet>::try_from(bss(payload)).unwrap();
    }
}

#[test]
fn decode_empty_is_error() {
    assert!(matches!(decode(""), Err(PacketError::Empty)));
}

#[test]
fn decode_invalid_id_is_error() {
    assert!(matches!(
        decode("9"),
        Err(PacketError::InvalidId { id: '9' })
    ));
}

#[test]
fn decode_connect_default_ns() {
    let Ns(ns, pkt) = decode(r#"0{"sid":"s"}"#).unwrap();
    assert_eq!(&*ns, "/");
    assert!(matches!(pkt, Packet::Connect(_)));
}

#[test]
fn decode_connect_non_default_ns() {
    let Ns(ns, pkt) = decode(r#"0/chat,{"sid":"s"}"#).unwrap();
    assert_eq!(&*ns, "/chat");
    assert!(matches!(pkt, Packet::Connect(_)));
}

#[test]
fn decode_disconnect() {
    let Ns(ns, pkt) = decode("1").unwrap();
    assert_eq!(&*ns, "/");
    assert!(matches!(pkt, Packet::Disconnect));
}

#[test]
fn decode_event_no_id() {
    let Ns(_, pkt) = decode(r#"2["evt"]"#).unwrap();
    assert!(matches!(pkt, Packet::Event { id: None, .. }));
}

#[test]
fn decode_event_with_id() {
    let Ns(_, pkt) = decode(r#"27["evt"]"#).unwrap();
    assert!(matches!(pkt, Packet::Event { id: Some(7), .. }));
}

#[test]
fn decode_ack() {
    let Ns(_, pkt) = decode(r#"37["ok"]"#).unwrap();
    assert!(matches!(pkt, Packet::Ack { id: 7, .. }));
}

#[test]
fn decode_connect_error() {
    let Ns(_, pkt) = decode(r#"4{"message":"bad"}"#).unwrap();
    assert!(matches!(pkt, Packet::ConnectError(_)));
}

#[test]
fn decode_binary_event_no_id() {
    let Ns(_, pkt) = decode(r#"52-["img"]"#).unwrap();
    assert!(matches!(
        pkt,
        Packet::BinaryEvent {
            count: 2,
            id: None,
            ..
        }
    ));
}

#[test]
fn decode_binary_event_with_id() {
    let Ns(_, pkt) = decode(r#"52-3["img"]"#).unwrap();
    assert!(matches!(
        pkt,
        Packet::BinaryEvent {
            count: 2,
            id: Some(3),
            ..
        }
    ));
}

#[test]
fn decode_binary_ack() {
    let Ns(_, pkt) = decode(r#"62-7["ok"]"#).unwrap();
    assert!(matches!(
        pkt,
        Packet::BinaryAck {
            count: 2,
            id: 7,
            ..
        }
    ));
}

#[test]
fn decode_ack_missing_id_is_error() {
    assert!(matches!(
        decode(r#"3["noid"]"#),
        Err(PacketError::MissingAckId)
    ));
}

#[test]
fn decode_event_unexpected_attachments_is_error() {
    assert!(matches!(
        decode(r#"22-["x"]"#),
        Err(PacketError::UnexpectedAttachments { count: 2 })
    ));
}

#[test]
fn decode_binary_event_missing_count_is_error() {
    assert!(matches!(
        decode(r#"5["x"]"#),
        Err(PacketError::MissingAttachmentCount)
    ));
}

#[test]
fn decode_namespace_missing_delimiter_is_error() {
    assert!(matches!(
        decode(r#"0/chat["x"]"#),
        Err(PacketError::MissingNamespaceDelimiter)
    ));
}

#[test]
fn decode_binary_ack_missing_count_is_error() {
    assert!(matches!(
        decode(r#"6["ok"]"#),
        Err(PacketError::MissingAttachmentCount)
    ));
}

#[test]
fn decode_binary_ack_missing_id_is_error() {
    assert!(matches!(
        decode(r#"61-["ok"]"#),
        Err(PacketError::MissingAckId)
    ));
}
