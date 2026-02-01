use crate::{
    error::{Error, Result},
    packet::Packet,
    socket::EioClient,
    transport::{self, TransportReceiver, TransportSender},
    ENGINE_IO_VERSION,
};
use http::HeaderMap;
use url::Url;

/// Transport type for the Engine.IO connection.
#[derive(Clone, Debug)]
pub enum TransportType {
    /// Use polling transport only.
    Polling,
    /// Use WebSocket transport only.
    Websocket,
    /// Start with polling and upgrade to WebSocket if supported (default).
    Auto,
}

#[derive(Clone, Debug)]
pub struct ClientBuilder {
    url: Url,
    headers: Option<HeaderMap>,
    transport_type: TransportType,
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

        ClientBuilder {
            url,
            headers: None,
            transport_type: TransportType::Auto,
        }
    }

    /// Set the transport type for the connection.
    pub fn transport_type(mut self, transport_type: TransportType) -> Self {
        self.transport_type = transport_type;
        self
    }

    /// Specify transport's HTTP headers
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Build with polling transport
    pub async fn build_polling(self) -> Result<EioClient> {
        let (_tx, mut rx) =
            transport::polling::connect(self.url.clone(), self.headers.clone()).await;

        // Perform handshake - receive first packet
        let handshake_packet = rx.recv().await?;

        let handshake = match handshake_packet {
            Packet::Open(h) => h,
            _ => return Err(Error::IncompletePacket),
        };

        // Update URL with session ID
        let mut url = self.url.clone();
        url.query_pairs_mut().append_pair("sid", &handshake.sid);

        // Recreate transports with updated URL
        let (transport_tx, transport_rx) = transport::polling::connect(url, self.headers).await;

        Ok(EioClient::new(
            TransportSender::Polling(transport_tx),
            TransportReceiver::Polling(transport_rx),
            handshake,
        ))
    }

    /// Build with websocket transport
    pub async fn build_websocket(self) -> Result<EioClient> {
        let (_tx, mut rx) =
            transport::websocket::connect(self.url.clone(), self.headers.clone()).await?;

        // Perform handshake - receive first packet
        let handshake_packet = rx.recv().await?;

        let handshake = match handshake_packet {
            Packet::Open(h) => h,
            _ => return Err(Error::IncompletePacket),
        };

        // Update URL with session ID
        let mut url = self.url.clone();
        url.query_pairs_mut().append_pair("sid", &handshake.sid);

        // Recreate transports with updated URL
        let (transport_tx, transport_rx) = transport::websocket::connect(url, self.headers).await?;

        Ok(EioClient::new(
            TransportSender::Websocket(transport_tx),
            TransportReceiver::Websocket(transport_rx),
            handshake,
        ))
    }

    /// Build with polling, then upgrade to websocket if supported
    pub async fn build_with_upgrade(self) -> Result<EioClient> {
        // Start with polling
        let (_tx, mut rx) =
            transport::polling::connect(self.url.clone(), self.headers.clone()).await;

        // Perform handshake
        let handshake_packet = rx.recv().await?;

        let handshake = match handshake_packet {
            Packet::Open(h) => h,
            _ => return Err(Error::IncompletePacket),
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
            let (transport_tx, transport_rx) =
                transport::websocket::connect(url, self.headers).await?;

            Ok(EioClient::new(
                TransportSender::Websocket(transport_tx),
                TransportReceiver::Websocket(transport_rx),
                handshake,
            ))
        } else {
            // Stay with polling
            let mut url = self.url.clone();
            url.query_pairs_mut().append_pair("sid", &handshake.sid);

            let (transport_tx, transport_rx) = transport::polling::connect(url, self.headers).await;

            Ok(EioClient::new(
                TransportSender::Polling(transport_tx),
                TransportReceiver::Polling(transport_rx),
                handshake,
            ))
        }
    }

    /// Build the Engine.IO client based on the configured transport type.
    ///
    /// Handles handshake and potential upgrades internally.
    pub async fn build(self) -> Result<EioClient> {
        match self.transport_type {
            TransportType::Polling => self.build_polling().await,
            TransportType::Websocket => self.build_websocket().await,
            TransportType::Auto => self.build_with_upgrade().await,
        }
    }
}
