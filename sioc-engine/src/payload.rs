use crate::error::PayloadError;

/// Serializes `payload` to JSON, returning the encoded string.
pub fn serialize<T>(payload: &T) -> Result<String, PayloadError>
where
    T: serde::Serialize,
{
    let mut buffer = Vec::new();

    let mut ser = serde_json::Serializer::new(&mut buffer);

    match serde_path_to_error::serialize(payload, &mut ser) {
        Ok(()) => {
            // SAFETY: serde_json always produces valid UTF-8.
            Ok(unsafe { String::from_utf8_unchecked(buffer) })
        }
        Err(e) => Err(PayloadError::new::<T>(e)),
    }
}

/// Deserializes a JSON string slice into `T`.
pub fn deserialize<'de, T>(data: &'de str) -> Result<T, PayloadError>
where
    T: serde::Deserialize<'de>,
{
    let mut de = serde_json::Deserializer::from_str(data);

    match serde_path_to_error::deserialize(&mut de) {
        Ok(payload) => Ok(payload),
        Err(e) => Err(PayloadError::new::<T>(e)),
    }
}
