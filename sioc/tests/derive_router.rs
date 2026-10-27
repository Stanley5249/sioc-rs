//! Isolated dispatch and metadata tests for the `EventRouter` derive.

use bytes::Bytes;
use sioc::error::{AckIdError, AttachmentsError, EventError};
use sioc::prelude::*;

#[derive(Debug, PartialEq, EventType, DeserializePayload)]
struct Ping;

#[derive(Debug, PartialEq, EventType, DeserializePayload)]
#[sioc(event(name = "custom:message"))]
struct Message(u32);

#[derive(Debug, AckType)]
struct Reply;

#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(ack = "Reply", binary))]
struct Upload(Placeholder);

#[derive(Debug, EventRouter)]
enum Router {
    Ping(Event<Ping>),
    Message(Event<Message>),
    Upload(Event<Upload>),
}

#[derive(Debug, EventType, DeserializePayload)]
struct GenericPayload<S: serde::de::DeserializeOwned>(S);

#[derive(Debug, EventRouter)]
enum GenericRouter<S>
where
    S: serde::de::DeserializeOwned,
{
    Payload(Event<GenericPayload<S>>),
}

#[derive(Debug, EventRouter)]
enum EventTypeRouter<E>
where
    E: EventType + DeserializePayload,
{
    Event(Event<E>),
}

#[test]
fn dispatches_routers_parameterized_by_event_type() {
    let event =
        EventTypeRouter::<Message>::try_from(DynEvent::new(r#"["custom:message",7]"#, None))
            .unwrap();
    let EventTypeRouter::Event(Event {
        payload: Message(value),
        ..
    }) = event;
    assert_eq!(value, 7);
}

#[test]
fn dispatches_generic_routers_with_explicit_bounds() {
    let event =
        GenericRouter::<u32>::try_from(DynEvent::new(r#"["generic_payload",42]"#, None)).unwrap();
    let GenericRouter::Payload(Event {
        payload: GenericPayload(value),
        ..
    }) = event;
    assert_eq!(value, 42);
}

#[test]
fn dispatches_each_variant_and_reports_wire_name() {
    let ping = Router::try_from(DynEvent::new(r#"["ping"]"#, None)).unwrap();
    assert_eq!(ping.name(), "ping");
    assert!(matches!(ping, Router::Ping(Event { payload: Ping, .. })));

    let message = Router::try_from(DynEvent::new(r#"["custom:message",7]"#, None)).unwrap();
    assert_eq!(message.name(), "custom:message");
    assert!(matches!(
        message,
        Router::Message(Event {
            payload: Message(7),
            ..
        })
    ));

    let data = Bytes::from_static(b"upload");
    let upload = Router::try_from(
        DynEvent::new(r#"["upload",{"_placeholder":true,"num":0}]"#, Some(42))
            .with_attachments(vec![data.clone()]),
    )
    .unwrap();
    assert_eq!(upload.name(), "upload");
    let Router::Upload(event) = upload else {
        panic!("expected upload variant");
    };
    assert_eq!(event.payload.0.slot(), 0);
    assert_eq!(event.id.get(), 42);
    assert_eq!(event.attachments, vec![data]);
}

#[test]
fn rejects_invalid_event_payloads() {
    for payload in [
        r#"["unknown"]"#,
        "[]",
        "[7]",
        r#"["custom:message"]"#,
        r#"["custom:message","invalid"]"#,
        "true",
        "[",
        r#"["ping"] trailing"#,
    ] {
        assert!(
            matches!(
                Router::try_from(DynEvent::new(payload, None)),
                Err(EventError::Payload(_))
            ),
            "expected payload error for {payload}",
        );
    }
}

#[test]
fn dispatches_escaped_event_names() {
    let event = Router::try_from(DynEvent::new(r#"["p\u0069ng"]"#, None)).unwrap();
    assert!(matches!(event, Router::Ping(_)));
}

#[test]
fn enforces_ack_policy() {
    assert!(matches!(
        Router::try_from(DynEvent::new(r#"["ping"]"#, Some(1))),
        Err(EventError::AckId(AckIdError::Unexpected)),
    ));
    assert!(matches!(
        Router::try_from(DynEvent::new(
            r#"["upload",{"_placeholder":true,"num":0}]"#,
            None
        )),
        Err(EventError::AckId(AckIdError::Missing)),
    ));
}

#[test]
fn enforces_attachment_policy() {
    assert!(matches!(
        Router::try_from(DynEvent::new(r#"["ping"]"#, None).with_attachments(vec![])),
        Err(EventError::Attachments(AttachmentsError::Unexpected)),
    ));
    assert!(matches!(
        Router::try_from(DynEvent::new(
            r#"["upload",{"_placeholder":true,"num":0}]"#,
            Some(1)
        ),),
        Err(EventError::Attachments(AttachmentsError::Missing)),
    ));
}
