//! Wire-format tests for derived event types.

use sioc::prelude::*;

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Hello;

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Moved(i32, i32);

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Join {
    room: String,
    user: String,
}

/// Implicit name resolves to `auto_name`.
#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct AutoName;

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(name = "custom:event"))]
struct Renamed(u32);

#[derive(Debug, AckType)]
struct Reply;

#[derive(Debug, EventType)]
#[sioc(event(ack = "Reply"))]
struct Request;

#[derive(Debug, EventType)]
#[sioc(event(ack = "Reply", binary))]
struct BinaryRequest;

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Payload<T: serde::Serialize + serde::de::DeserializeOwned> {
    value: T,
}

/// Binary event: carries binary attachments.
#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(binary))]
struct Upload {
    name: String,
}

/// Strict: rejects trailing elements.
#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
#[sioc(strict)]
struct Chat(String);

/// Flatten: collects trailing elements into `tags`.
#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Stream {
    id: String,
    #[sioc(flatten)]
    tags: Vec<serde_json::Value>,
}

fn assert_binary_marker<E: EventType<Binary = HasBinary>>() {}
fn assert_default_policy<E: EventType<Ack = NoAck, Binary = NoBinary>>() {}
fn assert_ack_policy<E: EventType<Ack = HasAck<Reply>, Binary = NoBinary>>() {}
fn assert_combined_policy<E: EventType<Ack = HasAck<Reply>, Binary = HasBinary>>() {}

#[test]
fn event_policies() {
    assert_default_policy::<Hello>();
    assert_ack_policy::<Request>();
    assert_combined_policy::<BinaryRequest>();
}

#[test]
fn explicit_name_override() {
    assert_eq!(Renamed::NAME, "custom:event");
    assert_eq!(event_to_json(&Renamed(7)).unwrap(), r#"["custom:event",7]"#);
    roundtrip(&Renamed(7));
    event_from_json::<Renamed>(r#"["renamed",7]"#).unwrap_err();
}

fn roundtrip<E>(val: &E)
where
    E: std::fmt::Debug + PartialEq + EventType + SerializePayload + DeserializePayload,
{
    let bytes = event_to_json(val).unwrap();
    assert_eq!(event_from_json::<E>(&bytes).unwrap(), *val);
}

#[test]
fn binary_event_marker() {
    assert_binary_marker::<Upload>();
}

#[test]
fn default_names() {
    assert_eq!(Hello::NAME, "hello");
    assert_eq!(Moved::NAME, "moved");
    assert_eq!(Join::NAME, "join");
}

#[test]
fn name_implicit() {
    assert_eq!(AutoName::NAME, "auto_name");
}

#[test]
fn wire_unit() {
    assert_eq!(event_to_json(&Hello).unwrap(), "[\"hello\"]");
}

#[test]
fn wire_tuple() {
    assert_eq!(event_to_json(&Moved(3, -7)).unwrap(), "[\"moved\",3,-7]");
}

#[test]
fn wire_named() {
    assert_eq!(
        event_to_json(&Join {
            room: "lobby".into(),
            user: "alice".into()
        })
        .unwrap(),
        "[\"join\",\"lobby\",\"alice\"]",
    );
}

#[test]
fn roundtrip_unit() {
    roundtrip(&Hello);
}

#[test]
fn roundtrip_tuple() {
    roundtrip(&Moved(-1, 99));
}

#[test]
fn roundtrip_named() {
    roundtrip(&Join {
        room: "general".into(),
        user: "bob".into(),
    });
}

#[test]
fn roundtrip_implicit_name() {
    roundtrip(&AutoName);
}

#[test]
fn roundtrip_generic() {
    roundtrip(&Payload {
        value: "sioc".to_string(),
    });
}

#[test]
fn wrong_name_fails() {
    event_from_json::<Hello>("[\"bye\"]").unwrap_err();
}

#[test]
fn strict_rejects_trailing() {
    event_from_json::<Chat>("[\"chat\",\"hi\",null]").unwrap_err();
}

#[test]
fn named_payload_rejects_missing_field() {
    let error = event_from_json::<Join>(r#"["join","lobby"]"#)
        .unwrap_err()
        .to_string();
    assert!(error.contains("invalid length 1"));
}

#[test]
fn tuple_payload_reports_invalid_element_path() {
    let error = event_from_json::<Moved>(r#"["moved",1,"two"]"#)
        .unwrap_err()
        .to_string();
    assert!(error.contains("[2]"));
    assert!(error.contains("invalid type"));
}

#[test]
fn strict_accepts_exact_length() {
    assert_eq!(
        event_from_json::<Chat>(r#"["chat","hi"]"#).unwrap(),
        Chat("hi".into())
    );
}

#[test]
fn flexible_discards_trailing() {
    assert_eq!(
        event_from_json::<Hello>("[\"hello\",null,42]").unwrap(),
        Hello
    );
}

#[test]
fn flatten_collects() {
    use serde_json::json;
    let evt = event_from_json::<Stream>("[\"stream\",\"s1\",\"rock\",\"jazz\"]").unwrap();
    assert_eq!(
        evt,
        Stream {
            id: "s1".into(),
            tags: vec![json!("rock"), json!("jazz")]
        }
    );
}

#[test]
fn flatten_roundtrip() {
    roundtrip(&Stream {
        id: "s1".into(),
        tags: vec![serde_json::json!(42)],
    });
}
