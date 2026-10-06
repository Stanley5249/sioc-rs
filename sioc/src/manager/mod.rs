//! Socket.IO namespace router.
//!
//! Two loops share the work, so neither direction waits on the other:
//! [`server_message`] delivers what the server sends to each namespace, and
//! [`client_packet`] sends what the namespace handles ask for.

mod client_packet;
mod routes;
mod server_message;
mod session;
#[cfg(test)]
mod tests;

pub use client_packet::ConnectRequest;
pub use session::run;
