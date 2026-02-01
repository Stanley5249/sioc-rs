use crate::error::{Error, Result};
use crate::packet::{MessagePacket, Packet};
use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::SinkExt;
use futures_util::StreamExt;
use http::HeaderMap;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// WebSocket sender - the write half
#[derive(Debug)]
pub struct WebsocketSender {
    transport_tx: SplitSink<WsStream, Message>,
}

impl WebsocketSender {
    /// Sends a message through the WebSocket
    pub async fn send(&mut self, msg: MessagePacket) -> Result<()> {
        let ws_msg = match msg {
            MessagePacket::Text(data) => {
                let utf8_payload: Utf8Bytes = data.try_into()?;
                Message::Text(utf8_payload)
            }
            MessagePacket::Binary(data) => Message::Binary(data),
        };

        self.transport_tx.send(ws_msg).await?;
        Ok(())
    }
}

/// WebSocket receiver - the read half
#[derive(Debug)]
pub struct WebsocketReceiver {
    transport_rx: SplitStream<WsStream>,
}

impl WebsocketReceiver {
    pub async fn recv(&mut self) -> Result<Packet> {
        loop {
            match self.transport_rx.next().await {
                Some(Ok(Message::Text(str))) => {
                    // Text messages: could be '4'+text or other packet types
                    return Packet::try_from(Bytes::from(str));
                }
                Some(Ok(Message::Binary(data))) => {
                    // Binary messages: raw binary data (Message type)
                    return Ok(Packet::Message(MessagePacket::Binary(data)));
                }
                Some(Ok(_)) => continue, // Ignore Ping/Pong/Close frames
                Some(Err(err)) => return Err(err.into()),
                None => return Err(Error::Closed),
            }
        }
    }
}

/// Creates a new WebSocket connection and splits it into sender/receiver
pub async fn connect(
    base_url: Url,
    _headers: Option<HeaderMap>,
) -> Result<(WebsocketSender, WebsocketReceiver)> {
    let mut url = base_url;
    url.query_pairs_mut().append_pair("transport", "websocket");

    // Determine scheme based on original URL
    let scheme = if url.scheme() == "https" || url.scheme() == "wss" {
        "wss"
    } else {
        "ws"
    };
    url.set_scheme(scheme).unwrap();

    let (ws_stream, _) = connect_async(url.as_str()).await?;
    let (transport_tx, transport_rx) = ws_stream.split();

    Ok((
        WebsocketSender { transport_tx },
        WebsocketReceiver { transport_rx },
    ))
}

/// Performs the WebSocket upgrade handshake
pub async fn upgrade(sender: &mut WebsocketSender, receiver: &mut WebsocketReceiver) -> Result<()> {
    // Send probe
    let probe_packet = Packet::Ping(Bytes::from("probe"));
    let probe_msg = probe_packet.into_message();
    sender.send(probe_msg).await?;

    // Await probe response
    let response = receiver.recv().await?;
    let expected = Packet::Pong(Bytes::from("probe"));
    if response != expected {
        return Err(Error::InvalidPacket);
    }

    // Send upgrade
    let upgrade_packet = Packet::Upgrade;
    let upgrade_msg = upgrade_packet.into_message();
    sender.send(upgrade_msg).await?;

    Ok(())
}
