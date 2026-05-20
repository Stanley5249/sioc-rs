//! Payload serialization and deserialization traits and helpers.

use crate::ack::AckType;
use crate::error::PayloadError;
use crate::event::EventType;
use serde::ser::SerializeSeq;
use std::marker::PhantomData;

/// Serializes `payload` to JSON, returning the encoded string.
///
/// # Errors
///
/// Returns an error if serialization fails.
pub fn to_json<T>(payload: &T) -> Result<String, PayloadError>
where
    T: serde::Serialize,
{
    let mut buffer = String::new();

    // SAFETY: serde_json always produces valid UTF-8.
    let mut ser = serde_json::Serializer::new(unsafe { buffer.as_mut_vec() });

    match serde_path_to_error::serialize(payload, &mut ser) {
        Ok(()) => Ok(buffer),
        Err(e) => Err(PayloadError::new::<T>(e)),
    }
}

/// Deserializes a JSON string slice into `T`.
///
/// # Errors
///
/// Returns an error if deserialization fails.
pub fn from_json<'de, T>(payload: &'de str) -> Result<T, PayloadError>
where
    T: serde::Deserialize<'de>,
{
    let mut de = serde_json::Deserializer::from_str(payload);
    match serde_path_to_error::deserialize(&mut de) {
        Ok(payload) => Ok(payload),
        Err(e) => Err(PayloadError::new::<T>(e)),
    }
}

/// Serializes an [`EventType`] + [`SerializePayload`] value into its wire-format string representation.
///
/// # Errors
///
/// Returns an error if serialization fails.
pub fn event_to_json<E>(event: &E) -> Result<String, PayloadError>
where
    E: EventType + SerializePayload,
{
    to_json(&EventPayload(event))
}

/// Deserializes a wire-format string into a typed [`EventType`] + [`DeserializePayload`] value.
///
/// # Errors
///
/// Returns an error if deserialization fails.
pub fn event_from_json<E>(payload: &str) -> Result<E, PayloadError>
where
    E: EventType + DeserializePayload,
{
    let EventPayload(event) = from_json(payload)?;
    Ok(event)
}

/// Serializes an [`AckType`] + [`SerializePayload`] value into its wire-format string representation.
///
/// # Errors
///
/// Returns an error if serialization fails.
pub fn ack_to_json<A>(payload: &A) -> Result<String, PayloadError>
where
    A: AckType + SerializePayload,
{
    to_json(&AckPayload(payload))
}

/// Deserializes a wire-format string into a typed [`AckType`] + [`DeserializePayload`] value.
///
/// # Errors
///
/// Returns an error if deserialization fails.
pub fn ack_from_json<A>(payload: &str) -> Result<A, PayloadError>
where
    A: AckType + DeserializePayload,
{
    let AckPayload(ack) = from_json(payload)?;
    Ok(ack)
}

/// Serializes a struct's fields as sequential elements of a JSON array.
pub trait SerializePayload {
    /// Appends each field to `seq` in declaration order.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying serializer fails.
    fn serialize_payload<S>(&self, seq: &mut S) -> std::result::Result<(), S::Error>
    where
        S: serde::ser::SerializeSeq;
}

/// Deserializes a struct's fields from sequential elements of a JSON array.
pub trait DeserializePayload: Sized {
    /// Reads each field from `seq` in declaration order.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is malformed or a field fails to deserialize.
    fn deserialize_payload<'de, S>(seq: &mut S) -> std::result::Result<Self, S::Error>
    where
        S: serde::de::SeqAccess<'de>;
}

impl SerializePayload for () {
    fn serialize_payload<S>(&self, _: &mut S) -> std::result::Result<(), S::Error>
    where
        S: serde::ser::SerializeSeq,
    {
        Ok(())
    }
}

impl DeserializePayload for () {
    fn deserialize_payload<'de, S>(seq: &mut S) -> std::result::Result<Self, S::Error>
    where
        S: serde::de::SeqAccess<'de>,
    {
        while let Some(serde::de::IgnoredAny) = seq.next_element()? {}
        Ok(())
    }
}

struct EventPayload<T>(pub T);

impl<E> serde::Serialize for EventPayload<&E>
where
    E: EventType + SerializePayload,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(E::NAME)?;
        self.0.serialize_payload(&mut seq)?;
        seq.end()
    }
}

impl<'de, E> serde::Deserialize<'de> for EventPayload<E>
where
    E: EventType + DeserializePayload,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(EventVisitor(PhantomData))
    }
}

struct EventVisitor<E>(PhantomData<E>);

impl<'de, E> serde::de::Visitor<'de> for EventVisitor<EventPayload<E>>
where
    E: EventType + DeserializePayload,
{
    type Value = EventPayload<E>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Socket.IO event payload")
    }

    fn visit_seq<V>(self, mut seq: V) -> std::result::Result<Self::Value, V::Error>
    where
        V: serde::de::SeqAccess<'de>,
    {
        let name: &'de str = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &E::NAME))?;

        if name != E::NAME {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(name),
                &E::NAME,
            ));
        }

        E::deserialize_payload(&mut seq).map(EventPayload)
    }
}

struct AckPayload<T>(pub T);

impl<A> serde::Serialize for AckPayload<&A>
where
    A: AckType + SerializePayload,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        self.0.serialize_payload(&mut seq)?;
        seq.end()
    }
}

impl<'de, A> serde::Deserialize<'de> for AckPayload<A>
where
    A: AckType + DeserializePayload,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(AckVisitor(PhantomData))
    }
}

struct AckVisitor<T>(PhantomData<T>);

impl<'de, A> serde::de::Visitor<'de> for AckVisitor<A>
where
    A: AckType + DeserializePayload,
{
    type Value = AckPayload<A>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Socket.IO ack payload")
    }

    fn visit_seq<V>(self, mut seq: V) -> std::result::Result<Self::Value, V::Error>
    where
        V: serde::de::SeqAccess<'de>,
    {
        A::deserialize_payload(&mut seq).map(AckPayload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marker::NoBinary;

    struct TestEvent;

    impl EventType for TestEvent {
        const NAME: &'static str = "test";
        type Ack = crate::marker::NoAck;
        type Binary = NoBinary;
    }

    impl DeserializePayload for TestEvent {
        fn deserialize_payload<'de, S>(seq: &mut S) -> Result<Self, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            while let Some(serde::de::IgnoredAny) = seq.next_element()? {}
            Ok(TestEvent)
        }
    }

    impl SerializePayload for TestEvent {
        fn serialize_payload<S>(&self, _seq: &mut S) -> Result<(), S::Error>
        where
            S: serde::ser::SerializeSeq,
        {
            Ok(())
        }
    }

    struct TestAck;

    impl AckType for TestAck {
        type Binary = NoBinary;
    }

    impl DeserializePayload for TestAck {
        fn deserialize_payload<'de, S>(seq: &mut S) -> Result<Self, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            while let Some(serde::de::IgnoredAny) = seq.next_element()? {}
            Ok(TestAck)
        }
    }

    #[test]
    fn to_json_roundtrip() {
        assert_eq!(to_json(&42u32).unwrap(), "42");
    }

    #[test]
    fn from_json_roundtrip() {
        let value: u32 = from_json("42").unwrap();
        assert_eq!(value, 42u32);
    }

    #[test]
    fn from_json_invalid_fails() {
        assert!(from_json::<u32>("not_a_number").is_err());
    }

    #[test]
    fn event_to_json_roundtrip() {
        assert_eq!(event_to_json(&TestEvent).unwrap(), r#"["test"]"#);
    }

    #[test]
    fn event_from_json_non_array_fails() {
        assert!(event_from_json::<TestEvent>("42").is_err());
    }

    #[test]
    fn ack_to_json_roundtrip() {
        assert_eq!(ack_to_json(&()).unwrap(), "[]");
    }

    #[test]
    fn ack_from_json_non_array_fails() {
        assert!(ack_from_json::<TestAck>("42").is_err());
    }
}
