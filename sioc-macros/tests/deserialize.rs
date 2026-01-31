mod common;

use common::*;
use sioc_core::event::Event;

#[test]
fn test_ping_deserialize() {
    // Pass byte string slice directly
    let evt = TestEvents::from_payload(b"[\"ping\"]").unwrap();
    assert_eq!(evt, TestEvents::Ping);
}

#[test]
fn test_message_deserialize() {
    let evt = TestEvents::from_payload(b"[\"message\",\"hello\"]").unwrap();
    assert_eq!(evt, TestEvents::Message("hello".into()));
}

#[test]
fn test_complex_deserialize() {
    let evt = TestEvents::from_payload(b"[\"complex\",42,\"test\"]").unwrap();
    assert_eq!(
        evt,
        TestEvents::Complex {
            a: 42,
            b: "test".into()
        }
    );
}

#[test]
fn test_unit_struct_deserialize() {
    let evt = UnitStruct::from_payload(b"[\"struct_unit\"]").unwrap();
    assert_eq!(evt, UnitStruct);
}

#[test]
fn test_wrapper_deserialize() {
    let evt = MyWrapper::from_payload(b"[\"new_type\",\"inner\",99]").unwrap();
    assert_eq!(evt, MyWrapper("inner".into(), 99));
}

#[test]
fn test_struct_deserialize() {
    let evt = MyStruct::from_payload(b"[\"struct_event\",\"a\",1]").unwrap();
    assert_eq!(
        evt,
        MyStruct {
            foo: "a".into(),
            bar: 1
        }
    );
}
