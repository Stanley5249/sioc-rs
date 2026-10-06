//! WebSocket transport for Engine.IO v4.
//!
//! [`session`] runs one session over a [`stream`] opened by its connect step,
//! with the [`forward`] loops carrying each direction. [`message`] converts
//! between WebSocket messages and frames.

pub mod forward;
pub mod message;
pub mod session;
pub mod stream;

#[cfg(test)]
mod tests;
