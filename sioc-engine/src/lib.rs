pub mod builder;
pub mod error;
pub mod packet;
pub mod socket;
pub mod transport;

pub const ENGINE_IO_VERSION: u64 = 4;

use crate::{error::Result, packet::Packet};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct EngineSender {
    tx: mpsc::Sender<Packet>,
}

impl EngineSender {
    pub async fn send(&self, packet: Packet) -> Result<()> {
        self.tx
            .send(packet)
            .await
            .map_err(|_| crate::error::Error::Closed)
    }
    pub async fn close(&self) -> Result<()> {
        self.send(Packet::Close).await
    }
}

pub type EngineReceiver = crate::transport::TransportReceiver;

/// Convenience re-exports
pub mod prelude {
    pub use crate::builder::ClientBuilder;
    pub use crate::error::{Error, Result};
    pub use crate::packet::{Handshake, Message, Packet};
    pub use crate::socket::EngineSocket;
    pub use crate::transport::{TransportReceiver, TransportSender};
    pub use crate::{EngineReceiver, EngineSender};
}
