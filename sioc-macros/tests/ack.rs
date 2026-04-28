use sioc::prelude::*;
use std::fmt::Debug;

#[derive(Debug, PartialEq, AckType, SerializePayload, DeserializePayload)]
struct Empty;

#[derive(Debug, PartialEq, AckType, SerializePayload, DeserializePayload)]
struct Status(bool, u32);

#[derive(Debug, PartialEq, AckType, SerializePayload, DeserializePayload)]
struct Save {
    ok: bool,
    id: u64,
}

/// Strict: rejects trailing elements.
#[derive(Debug, PartialEq, AckType, SerializePayload, DeserializePayload)]
#[sioc(strict)]
struct Strict(bool);

/// Flatten: collects trailing elements into `extras`.
#[derive(Debug, PartialEq, AckType, SerializePayload, DeserializePayload)]
struct Flex {
    ok: bool,
    #[sioc(flatten)]
    extras: Vec<serde_json::Value>,
}

fn roundtrip<A>(val: A)
where
    A: Debug + PartialEq + AckType + SerializePayload + DeserializePayload,
{
    let bytes = serialize_ack(&val).unwrap();
    assert_eq!(deserialize_ack::<A>(&bytes).unwrap(), val);
}

#[test]
fn wire_unit() {
    assert_eq!(serialize_ack(&Empty).unwrap(), "[]");
}

#[test]
fn wire_tuple() {
    assert_eq!(serialize_ack(&Status(true, 200)).unwrap(), "[true,200]");
}

#[test]
fn wire_named() {
    assert_eq!(
        serialize_ack(&Save { ok: true, id: 7 }).unwrap(),
        "[true,7]"
    );
}

#[test]
fn roundtrip_unit() {
    roundtrip(Empty);
}

#[test]
fn roundtrip_tuple() {
    roundtrip(Status(false, 404));
}

#[test]
fn roundtrip_named() {
    roundtrip(Save { ok: false, id: 0 });
}

#[test]
fn strict_rejects_trailing() {
    assert!(deserialize_ack::<Strict>("[true,\"extra\"]").is_err());
}

#[test]
fn flexible_discards_trailing() {
    assert_eq!(deserialize_ack::<Empty>("[null]").unwrap(), Empty);
}

#[test]
fn flatten_collects() {
    let ack = deserialize_ack::<Flex>("[true,1,\"x\"]").unwrap();
    assert!(ack.ok);
    assert_eq!(ack.extras.len(), 2);
}

#[test]
fn flatten_roundtrip() {
    roundtrip(Flex {
        ok: true,
        extras: vec![serde_json::json!(42)],
    });
}
