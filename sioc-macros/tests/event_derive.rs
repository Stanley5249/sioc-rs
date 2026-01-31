use sioc_core::event::Event;
use sioc_macros::Event;

// No Serialize derive needed on these structs/enums!

#[derive(Event)]
enum TestEvents {
    #[sioc(event = "ping")]
    Ping,
    #[sioc(event = "message")]
    Message(String),
}

#[derive(Event)]
#[sioc(event = "struct_event")]
struct MyStruct {
    foo: String,
    bar: i32,
}

#[derive(Event)]
#[sioc(event = "new_type")]
struct MyWrapper(String, i32);

#[test]
fn test_ping_event() {
    let evt = TestEvents::Ping;
    assert_eq!(evt.name(), "ping");
    assert_eq!(evt.to_payload().unwrap().as_ref(), b"[\"ping\"]");
}

#[test]
fn test_message_event() {
    let msg = TestEvents::Message("hello world".to_string());
    assert_eq!(msg.name(), "message");
    assert_eq!(
        msg.to_payload().unwrap().as_ref(),
        b"[\"message\",\"hello world\"]"
    );
}

#[test]
fn test_struct_event() {
    let s = MyStruct {
        foo: "a".into(),
        bar: 1,
    };
    assert_eq!(s.name(), "struct_event");
    assert_eq!(
        s.to_payload().unwrap().as_ref(),
        b"[\"struct_event\",{\"foo\":\"a\",\"bar\":1}]"
    );
}

#[test]
fn test_wrapper_event() {
    let w = MyWrapper("inner".into(), 99);
    assert_eq!(w.name(), "new_type");
    assert_eq!(
        w.to_payload().unwrap().as_ref(),
        b"[\"new_type\",\"inner\",99]"
    );
}
