//! Conversions between WebSocket messages and Engine.IO frames.

use bytestring::ByteString;
use futures_util::{Stream, TryStreamExt};
use tokio_tungstenite::tungstenite::{
    Error as TungsteniteError, Message as WebSocketMessage, Utf8Bytes,
};

use crate::error::WebSocketError;
use crate::packet::{Frame, Packet};

/// Converts a WebSocket text frame from the server into a [`ByteString`]
/// without copying.
pub fn bytestring_from_utf8_bytes(utf8: Utf8Bytes) -> ByteString {
    // SAFETY: `tungstenite::Utf8Bytes` guarantees the inner `Bytes` is valid UTF-8.
    unsafe { ByteString::from_bytes_unchecked(utf8.into()) }
}

/// Reads the next frame, skipping WebSocket control messages.
///
/// Returns `None` at the server's `Close` packet or the peer's close frame,
/// so the transport stops before it sends into a closing socket.
pub async fn next_frame<S>(stream: &mut S) -> Result<Option<Frame>, WebSocketError>
where
    S: Stream<Item = Result<WebSocketMessage, TungsteniteError>> + Unpin,
{
    while let Some(message) = stream.try_next().await? {
        match message {
            WebSocketMessage::Text(text) => {
                tracing::trace!(bytes = text.len(), "received text frame");

                let bytes = bytestring_from_utf8_bytes(text);

                return match Packet::decode(&bytes)? {
                    Packet::Close => {
                        tracing::debug!("server closed");

                        Ok(None)
                    }
                    packet => Ok(Some(Frame::Packet(packet))),
                };
            }
            WebSocketMessage::Binary(binary) => {
                tracing::trace!(bytes = binary.len(), "received binary frame");

                return Ok(Some(Frame::Binary(binary)));
            }
            WebSocketMessage::Close(_) => {
                tracing::trace!("received close frame");

                return Ok(None);
            }
            _ => {}
        }
    }

    Ok(None)
}

/// Encodes a frame as a WebSocket message.
pub fn encode_frame(frame: Frame) -> WebSocketMessage {
    match frame {
        Frame::Packet(packet) => {
            let text = packet.encode();

            tracing::trace!(bytes = text.len(), "sent text frame");

            WebSocketMessage::text(text)
        }
        Frame::Binary(bytes) => {
            tracing::trace!(bytes = bytes.len(), "sent binary frame");

            WebSocketMessage::binary(bytes)
        }
    }
}
