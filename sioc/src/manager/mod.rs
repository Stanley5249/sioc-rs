//! Socket.IO namespace router.
//!
//! Two loops share the work, so neither direction waits on the other:
//! [`server_message`] delivers what the server sends to each namespace, and
//! [`client_packet`] sends what the namespace handles ask for.

pub mod client_packet;
pub mod routes;
pub mod server_message;
pub mod session;

#[cfg(test)]
mod tests;
