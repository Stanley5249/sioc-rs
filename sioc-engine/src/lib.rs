pub mod builder;
pub mod error;
pub mod packet;
pub mod socket;
pub mod transport;

pub const ENGINE_IO_VERSION: u64 = 4;

/// Convenience re-exports
pub mod prelude {
    pub use crate::builder::ClientBuilder;
    pub use crate::error::{Error, Result};
    pub use crate::packet::{Handshake, MessagePacket, Packet as EnginePacket};
    pub use crate::socket::EioClient;
    pub use crate::transport::{TransportReceiver, TransportSender};
}
