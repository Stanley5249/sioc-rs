use crate::ack::AckType;
use crate::error::PayloadError;
use crate::event::EventType;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use serde_json::{self, Deserializer};
use std::marker::PhantomData;

/// Serializes a struct's fields as sequential elements of a JSON array.
pub trait SerializePayload {
    fn serialize_payload<S>(&self, seq: &mut S) -> std::result::Result<(), S::Error>
    where
        S: serde::ser::SerializeSeq;
}

/// Deserializes a struct's fields from sequential elements of a JSON array.
pub trait DeserializePayload: Sized {
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

pub struct EventPayload<T>(pub T);

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

/// Serializes an [`EventType`] + [`SerializePayload`] value into its wire-format byte representation.
pub fn serialize_event<E>(payload: &E) -> Result<Vec<u8>, PayloadError>
where
    E: EventType + SerializePayload,
{
    serialize(&EventPayload(payload))
}

/// Deserializes a wire-format byte slice into a typed [`EventType`] + [`DeserializePayload`] value.
pub fn deserialize_event<E>(data: &[u8]) -> Result<E, PayloadError>
where
    E: EventType + DeserializePayload,
{
    let EventPayload(event) = deserialize(data)?;
    Ok(event)
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

pub struct AckPayload<T>(pub T);

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

/// Serializes an [`AckType`] + [`SerializePayload`] value into its wire-format byte representation.
pub fn serialize_ack<A>(payload: &A) -> Result<Vec<u8>, PayloadError>
where
    A: AckType + SerializePayload,
{
    serialize(&AckPayload(payload))
}

/// Deserializes a wire-format byte slice into a typed [`AckType`] + [`DeserializePayload`] value.
pub fn deserialize_ack<A>(data: &[u8]) -> Result<A, PayloadError>
where
    A: AckType + DeserializePayload,
{
    let AckPayload(ack) = deserialize(data)?;
    Ok(ack)
}

pub fn serialize<T>(payload: &T) -> Result<Vec<u8>, PayloadError>
where
    T: Serialize,
{
    let mut bytes = Vec::new();
    let mut ser = serde_json::Serializer::new(&mut bytes);

    match serde_path_to_error::serialize(payload, &mut ser) {
        Ok(()) => Ok(bytes),
        Err(e) => Err(PayloadError::new::<T>(e)),
    }
}

pub fn deserialize<'de, T>(data: &'de [u8]) -> Result<T, PayloadError>
where
    T: Deserialize<'de>,
{
    let mut de = Deserializer::from_slice(data);

    match serde_path_to_error::deserialize(&mut de) {
        Ok(payload) => Ok(payload),
        Err(e) => Err(PayloadError::new::<T>(e)),
    }
}
