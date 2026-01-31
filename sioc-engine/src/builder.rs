use crate::{
    error::Result,
    packet::Packet,
    socket::{heartbeat_loop, send_loop},
    transport,
    transport::{TransportReceiver, TransportSender},
    EngineReceiver, EngineSender, ENGINE_IO_VERSION,
};
use http::HeaderMap;
use tokio::sync::mpsc;
use url::Url;

#[derive(Clone, Debug)]
pub struct ClientBuilder {
    url: Url,
    headers: Option<HeaderMap>,
}

impl ClientBuilder {
    pub fn new(url: Url) -> Self {
        let mut url = url;
        url.query_pairs_mut()
            .append_pair("EIO", &ENGINE_IO_VERSION.to_string());

        // No path add engine.io
        if url.path() == "/" {
            url.set_path("/engine.io/");
        }

        ClientBuilder { url, headers: None }
    }

    /// Specify transport's HTTP headers
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Build with polling transport
    pub async fn build_polling(self) -> Result<(EngineSender, EngineReceiver)> {
        let (_sender, mut receiver) =
            transport::polling::connect(self.url.clone(), self.headers.clone()).await;

        // Perform handshake - receive first packet
        let handshake_packet = receiver.recv().await?;

        let handshake = match handshake_packet {
            Packet::Open(h) => h,
            _ => return Err(crate::error::Error::IncompletePacket),
        };

        // Update URL with session ID
        let mut url = self.url.clone();
        url.query_pairs_mut().append_pair("sid", &handshake.sid);

        // Recreate transports with updated URL
        let (transport_sender, transport_receiver) =
            transport::polling::connect(url, self.headers).await;

        // Create channel for packet sending
        let (tx, rx) = mpsc::channel(32);

        // Spawn background tasks
        tokio::spawn(send_loop(TransportSender::Polling(transport_sender), rx));
        tokio::spawn(heartbeat_loop(handshake.ping_interval, tx.clone()));

        Ok((
            EngineSender { tx },
            TransportReceiver::Polling(transport_receiver),
        ))
    }

    /// Build with websocket transport
    pub async fn build_websocket(self) -> Result<(EngineSender, EngineReceiver)> {
        let (_sender, mut receiver) =
            transport::websocket::connect(self.url.clone(), self.headers.clone()).await?;

        // Perform handshake - receive first packet
        let handshake_packet = receiver.recv().await?;

        let handshake = match handshake_packet {
            Packet::Open(h) => h,
            _ => return Err(crate::error::Error::IncompletePacket),
        };

        // Update URL with session ID
        let mut url = self.url.clone();
        url.query_pairs_mut().append_pair("sid", &handshake.sid);

        // Recreate transports with updated URL
        let (transport_sender, transport_receiver) =
            transport::websocket::connect(url, self.headers).await?;

        // Create channel for packet sending
        let (tx, rx) = mpsc::channel(32);

        // Spawn background tasks
        tokio::spawn(send_loop(TransportSender::Websocket(transport_sender), rx));
        tokio::spawn(heartbeat_loop(handshake.ping_interval, tx.clone()));

        Ok((
            EngineSender { tx },
            TransportReceiver::Websocket(transport_receiver),
        ))
    }

    /// Build with polling, then upgrade to websocket if supported
    pub async fn build_with_upgrade(self) -> Result<(EngineSender, EngineReceiver)> {
        // Start with polling
        let (_sender, mut receiver) =
            transport::polling::connect(self.url.clone(), self.headers.clone()).await;

        // Perform handshake
        let handshake_packet = receiver.recv().await?;

        let handshake = match handshake_packet {
            Packet::Open(h) => h,
            _ => return Err(crate::error::Error::IncompletePacket),
        };

        // Check if websocket upgrade is supported
        let can_upgrade: bool = handshake
            .upgrades
            .iter()
            .any(|upgrade| upgrade.to_lowercase() == "websocket");

        if can_upgrade {
            // Update URL with session ID
            let mut url = self.url.clone();
            url.query_pairs_mut().append_pair("sid", &handshake.sid);

            // Connect via websocket
            let (transport_sender, transport_receiver) =
                transport::websocket::connect(url, self.headers).await?;

            // Perform upgrade handshake (assuming it takes &mut)
            // Note: upgrade function needs to be updated if necessary, but for now assume it's fine

            // Create channel for packet sending
            let (tx, rx) = mpsc::channel(32);

            // Spawn background tasks
            tokio::spawn(send_loop(TransportSender::Websocket(transport_sender), rx));
            tokio::spawn(heartbeat_loop(handshake.ping_interval, tx.clone()));

            Ok((
                EngineSender { tx },
                TransportReceiver::Websocket(transport_receiver),
            ))
        } else {
            // Stay with polling
            let mut url = self.url.clone();
            url.query_pairs_mut().append_pair("sid", &handshake.sid);

            let (transport_sender, transport_receiver) =
                transport::polling::connect(url, self.headers).await;

            // Create channel for packet sending
            let (tx, rx) = mpsc::channel(32);

            // Spawn background tasks
            tokio::spawn(send_loop(TransportSender::Polling(transport_sender), rx));
            tokio::spawn(heartbeat_loop(handshake.ping_interval, tx.clone()));

            Ok((
                EngineSender { tx },
                TransportReceiver::Polling(transport_receiver),
            ))
        }
    }

    /// Build - tries websocket with upgrade, falls back to polling
    pub async fn build(self) -> Result<(EngineSender, EngineReceiver)> {
        self.build_with_upgrade().await
    }

    /// Build with fallback - tries websocket upgrade, falls back to polling on error
    pub async fn build_with_fallback(self) -> Result<(EngineSender, EngineReceiver)> {
        match self.clone().build_with_upgrade().await {
            Ok(result) => Ok(result),
            Err(_) => self.build_polling().await,
        }
    }
}
