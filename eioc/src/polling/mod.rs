//! HTTP long-polling transport for Engine.IO v4.
//!
//! [`session`] runs one session: the handshake, the [`forward`] loops built on
//! single [`request`]s, and an optional upgrade. [`payload`] encodes the frames
//! each request carries.

pub mod forward;
pub mod payload;
pub mod request;
pub mod session;

#[cfg(test)]
mod tests;
