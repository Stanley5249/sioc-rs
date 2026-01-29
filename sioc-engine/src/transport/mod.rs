use crate::error::Result;
use crate::packet::{Message, Packet};

pub mod polling;
pub mod websocket;

/// The Write Half - Enum Dispatch for sending packets
#[derive(Debug)]
pub enum TransportSender {
    Polling(polling::PollingSender),
    Websocket(websocket::WebsocketSender),
}

impl TransportSender {
    /// Sends a message through the transport (No BoxFuture!)
    pub async fn send(&mut self, msg: Message) -> Result<()> {
        match self {
            Self::Polling(s) => s.send(msg).await,
            Self::Websocket(s) => s.send(msg).await,
        }
    }
}

/// The Read Half - Enum Dispatch for receiving packets
#[derive(Debug)]
pub enum TransportReceiver {
    Polling(polling::PollingReceiver),
    Websocket(websocket::WebsocketReceiver),
}

impl TransportReceiver {
    pub async fn recv(&mut self) -> Result<Packet> {
        match self {
            Self::Polling(r) => r.recv().await,
            Self::Websocket(r) => r.recv().await,
        }
    }
}
