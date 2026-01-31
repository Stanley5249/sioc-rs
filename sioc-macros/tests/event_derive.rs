use sioc_core::event::Event;
use sioc_macros::Event;

// No Serialize/Deserialize derive needed on these structs/enums!

#[derive(Event)]
enum TestEvents {
    #[sioc(event = "ping")]
    Ping,
    #[sioc(event = "message")]
    Message(String),
    #[sioc(event = "complex")]
    Complex { a: i32, b: String },
}

#[derive(Event)]
#[sioc(event = "struct_unit")]
struct UnitStruct;

#[derive(Event)]
#[sioc(event = "new_type")]
struct MyWrapper(String, i32);

#[derive(Event)]
#[sioc(event = "struct_event")]
struct MyStruct {
    foo: String,
    bar: i32,
}

#[test]
fn test_ping_event() {
    let evt = TestEvents::Ping;
    assert_eq!(evt.name(), "ping");
    assert_eq!(evt.to_payload().unwrap().as_ref(), b"[\"ping\"]");
}

#[test]
fn test_message_event() {
    let msg = TestEvents::Message("hello".to_string());
    assert_eq!(msg.name(), "message");
    assert_eq!(
        msg.to_payload().unwrap().as_ref(),
        b"[\"message\",\"hello\"]"
    );
}

#[test]
fn test_complex_event() {
    let complex = TestEvents::Complex {
        a: 42,
        b: "test".into(),
    };
    assert_eq!(complex.name(), "complex");
    // Strictly Flattened: ["complex", 42, "test"]
    assert_eq!(
        complex.to_payload().unwrap().as_ref(),
        b"[\"complex\",42,\"test\"]"
    );
}

#[test]
fn test_unit_struct() {
    let evt = UnitStruct;
    assert_eq!(evt.name(), "struct_unit");
    assert_eq!(evt.to_payload().unwrap().as_ref(), b"[\"struct_unit\"]");
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

#[test]
fn test_struct_event() {
    let s = MyStruct {
        foo: "a".into(),
        bar: 1,
    };
    assert_eq!(s.name(), "struct_event");
    // Strictly Flattened: ["struct_event", "a", 1]
    assert_eq!(
        s.to_payload().unwrap().as_ref(),
        b"[\"struct_event\",\"a\",1]"
    );
}

#[test]
fn test_output_verification() {
    // This test verifies the exact serialization format for all event types

    // Enum Unit: ["event_name"]
    let ping = TestEvents::Ping;
    let ping_json = String::from_utf8(ping.to_payload().unwrap().to_vec()).unwrap();
    assert_eq!(ping_json, r#"["ping"]"#);

    // Enum Tuple: ["event_name", val1, val2, ...]
    let message = TestEvents::Message("hello".into());
    let message_json = String::from_utf8(message.to_payload().unwrap().to_vec()).unwrap();
    assert_eq!(message_json, r#"["message","hello"]"#);

    // Enum Named (FLATTENED): ["event_name", val1, val2, ...]
    let complex = TestEvents::Complex {
        a: 42,
        b: "test".into(),
    };
    let complex_json = String::from_utf8(complex.to_payload().unwrap().to_vec()).unwrap();
    assert_eq!(complex_json, r#"["complex",42,"test"]"#);

    // Struct Unit: ["event_name"]
    let unit = UnitStruct;
    let unit_json = String::from_utf8(unit.to_payload().unwrap().to_vec()).unwrap();
    assert_eq!(unit_json, r#"["struct_unit"]"#);

    // Struct Tuple: ["event_name", val1, val2, ...]
    let wrapper = MyWrapper("inner".into(), 99);
    let wrapper_json = String::from_utf8(wrapper.to_payload().unwrap().to_vec()).unwrap();
    assert_eq!(wrapper_json, r#"["new_type","inner",99]"#);

    // Struct Named (FLATTENED): ["event_name", val1, val2, ...]
    let my_struct = MyStruct {
        foo: "a".into(),
        bar: 1,
    };
    let struct_json = String::from_utf8(my_struct.to_payload().unwrap().to_vec()).unwrap();
    assert_eq!(struct_json, r#"["struct_event","a",1]"#);
}
