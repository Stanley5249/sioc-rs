//! Wire-level transport implementations for Engine.IO v4.
//!
//! Engine.IO supports two transports — HTTP long-polling and WebSocket — plus
//! a hybrid "upgrade" strategy that starts with polling and switches to
//! WebSocket mid-session. [`Transport`] dispatches over the concrete
//! implementations so higher-level code stays transport-agnostic.

use crate::error::{Error, Result};
use crate::packet::{EioPacket, Frame, Handshake};
use crate::polling::PollingTransport;
use crate::websocket::{WebSocketConnector, WebSocketStream, WebsocketTransport};
use tokio::task::JoinHandle;
use url::Url;

/// Unified handle over the concrete transport implementations.
///
/// Higher-level code operates on this enum so it never needs to be generic
/// over the transport kind.
#[derive(Debug)]
pub enum Transport {
    /// HTTP long-polling transport.
    Polling(PollingTransport),

    /// WebSocket transport.
    Websocket(WebsocketTransport),
}

impl Transport {
    /// Connect via HTTP long-polling, perform the Engine.IO handshake, and
    /// schedule a WebSocket upgrade in the background if the server supports it.
    pub async fn connect_polling(
        base_url: Url,
        http_client: reqwest::Client,
        ws_connect: WebSocketConnector,
    ) -> Result<(
        Self,
        Handshake,
        Option<JoinHandle<Result<WebsocketTransport>>>,
    )> {
        let (polling, hs) = PollingTransport::connect(base_url.clone(), http_client).await?;

        let upgrade_handle = hs.can_upgrade_to_websocket().then(|| {
            let sid = hs.sid.clone();
            tokio::spawn(WebsocketTransport::connect_for_upgrade(
                base_url, sid, ws_connect,
            ))
        });

        Ok((Self::Polling(polling), hs, upgrade_handle))
    }

    /// Connect directly via WebSocket and perform the Engine.IO handshake.
    pub async fn connect_websocket<F, Fut>(base_url: Url, connect: F) -> Result<(Self, Handshake)>
    where
        F: FnOnce(Url) -> Fut,
        Fut: Future<Output = Result<WebSocketStream>>,
    {
        let mut websocket = WebsocketTransport::connect(base_url, connect).await?;

        let hs = loop {
            match websocket.recv().await? {
                Some(Frame::Packet(EioPacket::Open(handshake))) => break handshake,
                Some(Frame::Packet(EioPacket::Noop)) => continue,
                Some(frame) => return Err(frame.unexpected("expected Open packet as first frame")),
                None => return Err(Error::Close),
            }
        };

        Ok((Self::Websocket(websocket), hs))
    }

    /// Receives the next [`Frame`] from the server.
    ///
    /// Returns `Ok(None)` when the connection closes cleanly, `Err(_)` on a
    /// transport or protocol error. Cancel-safe.
    pub async fn recv(&mut self) -> Result<Option<Frame>> {
        match self {
            Self::Polling(t) => Ok(t.recv().await),
            Self::Websocket(t) => t.recv().await,
        }
    }

    /// Sends a frame, flushing immediately.
    pub async fn send(&mut self, frame: Frame) -> Result<()> {
        match self {
            Self::Polling(t) => t.send(frame).await,
            Self::Websocket(t) => t.send(frame).await,
        }
    }

    /// Buffers a frame without flushing.
    pub async fn feed(&mut self, frame: Frame) -> Result<()> {
        match self {
            Self::Polling(t) => t.send(frame).await,
            Self::Websocket(t) => t.feed(frame).await,
        }
    }

    /// Flushes buffered frames.
    pub async fn flush(&mut self) -> Result<()> {
        match self {
            Self::Polling(..) => Ok(()),
            Self::Websocket(t) => t.flush().await,
        }
    }

    /// Closes the transport, consuming it.
    ///
    /// For polling, aborts the background tasks.
    /// For WebSocket, sends the close frame.
    pub async fn close(self) -> Result<()> {
        match self {
            Self::Polling(t) => t.close().await,
            Self::Websocket(t) => t.close().await,
        }
    }
}
