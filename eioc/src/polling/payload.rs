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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_count_encoded_bytes_and_keep_order() {
        let text = || Packet::Message("a".into()).into();
        for (limit, count) in [(4, 1), (5, 2), (6, 2), (8, 3)] {
            let (batch, pending) = take_batch(text(), [text(), text()].into_iter(), limit);
            assert_eq!(batch.len(), count, "limit {limit}");
            assert_eq!(pending.is_some(), count < 3);
        }
        let binary = Frame::Binary(Bytes::from_static(b"abc")); // bYWJj = 5 bytes
        let unicode = Frame::Packet(Packet::Message("台".into())); // 4 UTF-8 bytes
        let (batch, pending) = take_batch(binary.clone(), [unicode.clone()].into_iter(), 9);
        assert_eq!(batch, [binary]);
        assert_eq!(pending, Some(unicode));

        let oversized: Frame = Packet::Message("oversized".into()).into();
        let (batch, pending) = take_batch(oversized.clone(), [text()].into_iter(), 1);
        assert_eq!(batch, [oversized]);
        assert_eq!(pending, Some(text()));

        let mut frames = (0..32).map(|_| text());
        let (batch, pending) = take_batch(text(), frames.by_ref(), u64::MAX);
        assert_eq!(batch.len(), 16);
        assert!(pending.is_none());
        assert_eq!(frames.count(), 17);
    }

    #[test]
    fn frame_decode_text_packet() {
        let frame = decode_frame(&ByteString::from_static("4hello")).unwrap();
        assert!(matches!(frame, Frame::Packet(Packet::Message(m)) if m == "hello"));
    }

    #[test]
    fn frame_decode_binary_base64() {
        use base64::prelude::{BASE64_STANDARD, Engine as _};
        let encoded = BASE64_STANDARD.encode(b"abc");
        let input = ByteString::from(format!("b{encoded}"));
        let frame = decode_frame(&input).unwrap();
        assert!(matches!(frame, Frame::Binary(b) if b.as_ref() == b"abc"));
    }

    #[test]
    fn frame_decode_invalid_base64_is_error() {
        decode_frame(&ByteString::from_static("b!!!")).unwrap_err();
    }

    #[test]
    fn frame_write_packet() {
        let frame = Frame::Packet(Packet::Message("hello".into()));
        let mut buf = String::new();
        write_frame(&frame, &mut buf);
        assert_eq!(buf, "4hello");
    }

    #[test]
    fn frame_write_binary() {
        use base64::prelude::{BASE64_STANDARD, Engine as _};
        let raw = Bytes::from_static(b"abc");
        let frame = Frame::Binary(raw);
        let mut buf = String::new();
        write_frame(&frame, &mut buf);
        assert_eq!(buf, format!("b{}", BASE64_STANDARD.encode(b"abc")));
    }

    #[test]
    fn decode_encode_payload_roundtrip() {
        let text = ByteString::from_static("4hello\x1e4world");
        let frames = decode_payload(&text).unwrap();
        assert_eq!(frames.len(), 2);
        let encoded = encode_payload(&frames);
        assert_eq!(encoded, "4hello\x1e4world");
    }

    #[test]
    fn encode_payload_single() {
        let frames = vec![Frame::Packet(Packet::Pong("probe".into()))];
        assert_eq!(encode_payload(&frames), "3probe");
    }

    #[test]
    fn encode_payload_empty() {
        assert_eq!(encode_payload(&[]), "");
    }

    #[test]
    fn decode_payload_error_propagates() {
        decode_payload(&ByteString::from_static("9invalid")).unwrap_err();
    }
}
