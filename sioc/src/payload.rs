//! Payload serialization and deserialization traits and helpers.

use std::marker::PhantomData;

use serde::ser::SerializeSeq;

use crate::ack::AckType;
use crate::error::PayloadError;
use crate::event::EventType;

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
    let mut track = serde_path_to_error::Track::new();
    let deserializer = serde_path_to_error::Deserializer::new(&mut de, &mut track);
    let result = T::deserialize(deserializer).and_then(|value| de.end().map(|()| value));

    result.map_err(|error| {
        PayloadError::new::<T>(serde_path_to_error::Error::new(track.path(), error))
    })
}

/// Serializes an [`EventType`] + [`SerializePayload`] value into its
/// wire-format string representation.
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

/// Deserializes a wire-format string into a typed [`EventType`] +
/// [`DeserializePayload`] value.
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

/// Serializes an [`AckType`] + [`SerializePayload`] value into its wire-format
/// string representation.
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

/// Deserializes a wire-format string into a typed [`AckType`] +
/// [`DeserializePayload`] value.
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
    /// Returns an error if the sequence is malformed or a field fails to
    /// deserialize.
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
        let name: String = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &E::NAME))?;

        if name != E::NAME {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(&name),
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
    use std::collections::BTreeMap;

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
    fn serializes_to_json() {
        assert_eq!(to_json(&42u32).unwrap(), "42");
    }

    #[test]
    fn serialization_error_preserves_type_and_cause() {
        let payload = BTreeMap::from([(vec![1u8], 42u32)]);
        let error = to_json(&payload).unwrap_err().to_string();
        assert!(error.contains(std::any::type_name::<BTreeMap<Vec<u8>, u32>>()));
        assert!(error.contains("key must be a string"));
    }

    #[test]
    fn deserialization_error_preserves_element_path() {
        let error = from_json::<Vec<u32>>(r#"[1,"two"]"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("[1]"));
        assert!(error.contains("invalid type"));
    }

    #[test]
    fn deserializes_from_json() {
        let value: u32 = from_json("42").unwrap();
        assert_eq!(value, 42u32);
    }

    #[test]
    fn from_json_invalid_fails() {
        from_json::<u32>("not_a_number").unwrap_err();
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

    #[test]
    fn event_from_json_roundtrip() {
        event_from_json::<TestEvent>(r#"["test"]"#).unwrap();
    }

    #[test]
    fn event_from_json_wrong_name_fails() {
        assert!(event_from_json::<TestEvent>(r#"["other"]"#).is_err());
    }

    #[test]
    fn event_from_json_empty_array_fails() {
        assert!(event_from_json::<TestEvent>("[]").is_err());
    }

    #[test]
    fn rejects_trailing_json_for_values_events_and_acks() {
        for payload in ["true false", "true trailing", "true]"] {
            assert!(from_json::<bool>(payload).is_err(), "{payload}");
        }
        assert!(event_from_json::<TestEvent>(r#"["test"] []"#).is_err());
        assert!(ack_from_json::<()>("[] null").is_err());
        assert!(from_json::<bool>("true \n\t ").unwrap());
    }

    #[test]
    fn accepts_escaped_event_names() {
        event_from_json::<TestEvent>(r#"["t\u0065st"]"#).unwrap();
    }

    #[test]
    fn deserialize_unit_ignores_extra_elements() {
        assert_eq!(ack_from_json::<()>("[1,2,3]").unwrap(), ());
    }
}
