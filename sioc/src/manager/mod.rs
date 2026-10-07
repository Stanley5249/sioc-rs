//! Socket.IO namespace router.
//!
//! [`client_packet`] sends what the namespace handles ask for, across
//! Engine.IO sessions. [`supervisor`] opens a session whenever that loop asks
//! for one, and runs [`server_message`], which delivers what the server sends
//! to each namespace, beside the session's engine. Each direction has its own
//! loop, so neither waits on the other.

pub mod backoff;
pub mod client_packet;
pub mod connect_request;
pub mod routes;
pub mod server_message;
pub mod session;
pub mod supervisor;

#[cfg(test)]
mod tests;
