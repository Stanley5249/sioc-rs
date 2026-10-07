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
    assert_eq!(event.id.get(), ServerAckId::new(42));
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
