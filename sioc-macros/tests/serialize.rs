mod common;

use common::*;
use sioc_core::event::Event;

#[test]
fn test_ping_serialize() {
    let evt = TestEvents::Ping;
    assert_eq!(evt.to_payload().unwrap(), b"[\"ping\"]");
}

#[test]
fn test_message_serialize() {
    let msg = TestEvents::Message("hello".to_string());
    assert_eq!(msg.to_payload().unwrap(), b"[\"message\",\"hello\"]");
}

#[test]
fn test_complex_serialize() {
    let evt = TestEvents::Complex {
        a: 42,
        b: "test".into(),
    };
    assert_eq!(evt.to_payload().unwrap(), b"[\"complex\",42,\"test\"]");
}

#[test]
fn test_unit_struct_serialize() {
    let evt = UnitStruct;
    assert_eq!(evt.to_payload().unwrap(), b"[\"struct_unit\"]");
}

#[test]
fn test_wrapper_serialize() {
    let w = MyWrapper("inner".into(), 99);
    assert_eq!(w.to_payload().unwrap(), b"[\"new_type\",\"inner\",99]");
}

#[test]
fn test_struct_serialize() {
    let s = MyStruct {
        foo: "a".into(),
        bar: 1,
    };
    assert_eq!(s.to_payload().unwrap(), b"[\"struct_event\",\"a\",1]");
}
