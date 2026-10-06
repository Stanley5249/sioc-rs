//! The polling payload: frames joined by a record separator.

use base64::prelude::{BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use bytestring::ByteString;

use crate::error::PollingError;
use crate::packet::{Frame, Packet};

pub const RECORD_SEPARATOR: char = '\x1e';

/// Decodes one frame: a packet, or `b` and base64 for binary.
pub fn decode_frame(bytes: &ByteString) -> Result<Frame, PollingError> {
    let mut chars = bytes.chars();

    if chars.next().is_some_and(|b| b == 'b') {
        let data = BASE64_STANDARD.decode(chars.as_str().as_bytes())?;
        Ok(Frame::Binary(Bytes::from(data)))
    } else {
        Ok(Frame::Packet(Packet::decode(bytes)?))
    }
}

/// Appends one encoded frame to `buffer`.
pub fn write_frame(frame: &Frame, buffer: &mut String) {
    match frame {
        Frame::Packet(packet) => packet.write(buffer),
        Frame::Binary(bytes) => {
            buffer.push('b');
            BASE64_STANDARD.encode_string(bytes, buffer);
        }
    }
}

pub fn decode_payload(bytes: &ByteString) -> Result<Vec<Frame>, PollingError> {
    bytes
        .split(RECORD_SEPARATOR)
        .map(|s| decode_frame(&bytes.slice_ref(s)))
        .collect()
}

pub fn encode_payload(frames: &[Frame]) -> String {
    let mut buffer = String::new();
    for (i, frame) in frames.iter().enumerate() {
        if i > 0 {
            buffer.push(RECORD_SEPARATOR);
        }
        write_frame(frame, &mut buffer);
    }
    buffer
}

/// Takes an ordered prefix whose wire encoding fits the handshake limit.
/// A single oversized frame travels alone, matching engine.io-client.
pub fn take_batch(
    first: Frame,
    frames: impl Iterator<Item = Frame>,
    max_payload: u64,
) -> (Vec<Frame>, Option<Frame>) {
    let mut size = encode_payload(std::slice::from_ref(&first)).len() as u64;
    let mut batch = vec![first];
    // python-engineio accepts at most sixteen packets per POST. Its handshake
    // advertises only a byte limit, so keep that interoperability ceiling too.
    for frame in frames.take(15) {
        let next_size = encode_payload(std::slice::from_ref(&frame)).len() as u64;
        if size.saturating_add(1).saturating_add(next_size) > max_payload {
            return (batch, Some(frame));
        }
        size += 1 + next_size;
        batch.push(frame);
    }
    (batch, None)
}
