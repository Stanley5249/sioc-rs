use sioc::prelude::*;

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Ping;

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Moved(i32, i32);

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Join {
    room: String,
    user: String,
}

/// Implicit name resolves to "auto_name".
#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct AutoName;

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Payload<T: serde::Serialize + serde::de::DeserializeOwned> {
    value: T,
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

fn roundtrip<E>(val: E)
where
    E: std::fmt::Debug + PartialEq + EventType + SerializePayload + DeserializePayload,
{
    let bytes = event_to_json(&val).unwrap();
    assert_eq!(event_from_json::<E>(&bytes).unwrap(), val);
}

#[test]
fn name_explicit() {
    assert_eq!(Ping::NAME, "ping");
    assert_eq!(Moved::NAME, "moved");
    assert_eq!(Join::NAME, "join");
}

#[test]
fn name_implicit() {
    assert_eq!(AutoName::NAME, "auto_name");
}

#[test]
fn wire_unit() {
    assert_eq!(event_to_json(&Ping).unwrap(), "[\"ping\"]");
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
    roundtrip(Ping);
}

#[test]
fn roundtrip_tuple() {
    roundtrip(Moved(-1, 99));
}

#[test]
fn roundtrip_named() {
    roundtrip(Join {
        room: "general".into(),
        user: "bob".into(),
    });
}

#[test]
fn roundtrip_implicit_name() {
    roundtrip(AutoName);
}

#[test]
fn roundtrip_generic() {
    roundtrip(Payload {
        value: "sioc".to_string(),
    });
}

#[test]
fn wrong_name_fails() {
    assert!(event_from_json::<Ping>("[\"pong\"]").is_err());
}

#[test]
fn strict_rejects_trailing() {
    assert!(event_from_json::<Chat>("[\"chat\",\"hi\",null]").is_err());
}

#[test]
fn flexible_discards_trailing() {
    assert_eq!(event_from_json::<Ping>("[\"ping\",null,42]").unwrap(), Ping);
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
    roundtrip(Stream {
        id: "s1".into(),
        tags: vec![serde_json::json!(42)],
    });
}
