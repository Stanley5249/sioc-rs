use serde::Serialize;
use sioc_core::event::Event;
use sioc_macros::Event;

#[derive(Event, Serialize)]
enum TestEvents {
    #[event(name = "ping")]
    Ping,

    #[event(name = "message")]
    Message(String),

    #[event(name = "user")]
    User { id: u32, name: String },

    #[event(name = "data")]
    Data(u32, String, bool),
}

#[test]
fn test_unit_variant() {
    let evt = TestEvents::Ping;
    assert_eq!(evt.name(), "ping");
    assert_eq!(evt.to_json().unwrap().as_ref(), b"[\"ping\"]");
}

#[test]
fn test_tuple_variant_single_field() {
    let evt = TestEvents::Message("hello".to_string());
    assert_eq!(evt.name(), "message");
    assert_eq!(evt.to_json().unwrap().as_ref(), b"[\"message\",\"hello\"]");
}

#[test]
fn test_tuple_variant_multiple_fields() {
    let evt = TestEvents::Data(42, "test".to_string(), true);
    assert_eq!(evt.name(), "data");
    assert_eq!(
        evt.to_json().unwrap().as_ref(),
        b"[\"data\",42,\"test\",true]"
    );
}

#[test]
fn test_struct_variant() {
    let evt = TestEvents::User {
        id: 1,
        name: "alice".into(),
    };
    assert_eq!(evt.name(), "user");
    assert_eq!(
        evt.to_json().unwrap().as_ref(),
        b"[\"user\",{\"id\":1,\"name\":\"alice\"}]"
    );
}
