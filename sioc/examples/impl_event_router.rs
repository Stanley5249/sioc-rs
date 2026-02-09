use bytes::Bytes;
use serde::Deserialize;
use sioc::prelude::*;

// Event types for demonstration
#[derive(Debug, EventType, DeserializePayload)]
struct A;

#[derive(Debug, EventType, DeserializePayload)]
struct B;

// Enum to represent different events
#[derive(Debug)]
enum MyEvent {
    A(Event<A>),
    B(Event<B>),
}

// Helper enum for deserialization
enum MyEventHelper {
    A(<Event<A> as EventHandler>::Payload),
    B(<Event<B> as EventHandler>::Payload),
}

// Custom deserialization for MyEventHelper
impl<'de> Deserialize<'de> for MyEventHelper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(MyEventVisitor)
    }
}

// Visitor for deserializing the event sequence
struct MyEventVisitor;

impl<'de> serde::de::Visitor<'de> for MyEventVisitor {
    type Value = MyEventHelper;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Socket.IO event payload")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
    where
        V: serde::de::SeqAccess<'de>,
    {
        let name: &str = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &"event name"))?;

        match name {
            <Event<A> as EventHandler>::Payload::NAME => {
                let payload = <Event<A> as EventHandler>::Payload::deserialize_payload(&mut seq)?;
                Ok(MyEventHelper::A(payload))
            }
            <Event<B> as EventHandler>::Payload::NAME => {
                let payload = <Event<B> as EventHandler>::Payload::deserialize_payload(&mut seq)?;
                Ok(MyEventHelper::B(payload))
            }
            _ => Err(serde::de::Error::unknown_variant(
                name,
                &[
                    <Event<A> as EventHandler>::Payload::NAME,
                    <Event<B> as EventHandler>::Payload::NAME,
                ],
            )),
        }
    }
}

// Conversion from DynEvent to MyEvent
impl TryFrom<DynEvent> for MyEvent {
    type Error = sioc::error::Error;

    fn try_from(event: DynEvent) -> sioc::error::Result<Self> {
        let helper: MyEventHelper = serde_json::from_slice(&event.data)
            .map_err(|e| sioc::error::PayloadError::new::<MyEvent>(e).with_slice(&event.data))?;

        match helper {
            MyEventHelper::A(args) => {
                let event_a =
                    <Event<A> as EventHandler>::handle(args, event.id, event.attachments)?;
                Ok(MyEvent::A(event_a))
            }
            MyEventHelper::B(args) => {
                let event_b =
                    <Event<B> as EventHandler>::handle(args, event.id, event.attachments)?;
                Ok(MyEvent::B(event_b))
            }
        }
    }
}

fn main() -> sioc::error::Result<()> {
    // Example DynEvent for event "a"
    let event = DynEvent {
        data: Bytes::from_static(b"[\"a\"]"),
        attachments: None,
        id: None,
    };

    let my_event = MyEvent::try_from(event)?;

    println!("{:?}", my_event);

    Ok(())
}
