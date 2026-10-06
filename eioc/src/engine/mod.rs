//! Engine.IO protocol task.
//!
//! [`session`] runs one session: the `protocol` loops beside the transport,
//! with a `heartbeat` deadline on server pings.

mod heartbeat;
mod protocol;
pub mod session;

#[cfg(test)]
mod tests;
