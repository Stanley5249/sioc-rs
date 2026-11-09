//! Socket.IO client and namespace handles.
//!
//! `connection` owns setup and manager lifecycle, like socket.io-client's
//! `Manager`. `socket` owns namespace I/O, like its `Socket`.
//! Private implementation modules re-export the application-facing handles,
//! and tests open namespaces through `connect` without a `Client`.

mod connection;
mod socket;

pub use connection::{Client, ClientBuilder};
#[cfg(test)]
pub(crate) use socket::connect;
pub use socket::{SocketBuilder, SocketReceiver, SocketSender};

#[cfg(test)]
mod tests;
