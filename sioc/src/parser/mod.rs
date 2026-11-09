//! Socket.IO wire framing, like `socket.io-parser`'s `Packet`, `Encoder` and
//! `Decoder`. Packet definitions, text encoding and text decoding live in
//! separate modules; the manager owns binary reconstruction and routing.
//!
//! Reference: [socket.io-parser](https://github.com/socketio/socket.io/blob/socket.io-client@4.8.4/packages/socket.io-parser/lib/index.ts).

mod decoder;
mod encoder;
pub mod packet;

#[cfg(test)]
mod tests;
