use crate::error::PayloadError;

pub fn serialize<T>(payload: &T) -> Result<Vec<u8>, PayloadError>
where
    T: serde::Serialize,
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
    T: serde::Deserialize<'de>,
{
    let mut de = serde_json::Deserializer::from_slice(data);

    match serde_path_to_error::deserialize(&mut de) {
        Ok(payload) => Ok(payload),
        Err(e) => Err(PayloadError::new::<T>(e)),
    }
}
