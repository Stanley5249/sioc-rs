//! HTTP long-polling transport tasks for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::engine::EngineSender;
use crate::error::{PollingError, TransportError};
use crate::packet::{Frame, Handshake, Packet};
use crate::prelude::WebSocketStream;
use crate::websocket::WebSocketConnector;
use base64::prelude::*;
use bytes::Bytes;
use bytestring::ByteString;
use reqwest::Client;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use url::Url;

const SEPARATOR: char = '\x1e';

impl Frame {
    fn decode_binary(data: &[u8]) -> Result<Self, PollingError> {
        Ok(Self::Binary(Bytes::from(BASE64_STANDARD.decode(data)?)))
    }

    fn decode_packet(bytes: ByteString) -> Result<Self, PollingError> {
        Ok(Self::Packet(Packet::decode(bytes)?))
    }

    fn decode(bytes: ByteString) -> Result<Self, PollingError> {
        let mut chars = bytes.chars();

        if chars.next().is_some_and(|b| b == 'b') {
            Self::decode_binary(chars.as_str().as_bytes())
        } else {
            Self::decode_packet(bytes)
        }
    }

    fn write(&self, buffer: &mut String) {
        match self {
            Frame::Packet(packet) => packet.write(buffer),
            Frame::Binary(bytes) => {
                buffer.push('b');
                BASE64_STANDARD.encode_string(bytes, buffer);
            }
        }
    }
}

fn decode_frames(bytes: ByteString) -> Result<Vec<Frame>, PollingError> {
    bytes
        .split(SEPARATOR)
        .map(|s| Frame::decode(bytes.slice_ref(s)))
        .collect()
}

fn encode_frames(frames: &[Frame]) -> String {
    let mut buffer = String::new();
    for (i, frame) in frames.iter().enumerate() {
        if i > 0 {
            buffer.push(SEPARATOR);
        }
        frame.write(&mut buffer);
    }
    buffer
}

/// Builds the polling URL by appending the EIO version and transport parameters.
fn polling_url(mut base_url: Url) -> Url {
    base_url
        .query_pairs_mut()
        .append_pair("EIO", ENGINE_IO_VERSION)
        .append_pair("transport", "polling");
    base_url
}

/// Wraps a [`reqwest::Client`] with Engine.IO HTTP polling helpers.
#[derive(Clone)]
pub struct PollingClient(pub Client);

impl PollingClient {
    async fn get(&self, url: &Url) -> Result<Vec<Frame>, PollingError> {
        let response = self
            .0
            .get(url.as_str())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        tracing::trace!(bytes = response.len(), "<- GET");
        decode_frames(response.into())
    }

    async fn get_one(&self, url: &Url) -> Result<Frame, PollingError> {
        let response = self
            .0
            .get(url.as_str())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        tracing::trace!(bytes = response.len(), "<- GET");

        Frame::decode(response.into())
    }

    async fn post(&self, url: &Url, frames: &[Frame]) -> Result<(), PollingError> {
        let body = encode_frames(frames);
        tracing::trace!(bytes = body.len(), "-> POST");

        let response = self
            .0
            .post(url.as_str())
            .body(body)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        if !response.eq_ignore_ascii_case("ok") {
            return Err(PollingError::Response(response));
        }

        Ok(())
    }

    /// Loops batched POST requests until `token` fires or `rx` closes.
    ///
    /// Returns `rx` on exit so the WebSocket phase can reuse it.
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn post_until_cancelled(
        &self,
        url: &Url,
        mut rx: mpsc::Receiver<Frame>,
        token: CancellationToken,
    ) -> Result<mpsc::Receiver<Frame>, TransportError> {
        let mut buffer = Vec::with_capacity(8);

        loop {
            let count = tokio::select! {
                _ = token.cancelled() => {
                    tracing::debug!("cancelled polling POST");
                    break;
                },
                count = rx.recv_many(&mut buffer, 8) => count,
            };

            if count == 0 {
                break;
            }

            self.post(url, &buffer).await?;
            buffer.clear();
        }

        Ok(rx)
    }

    /// Drains all outbound frames from `rx` until the sender is dropped.
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn post_until_closed(
        &self,
        url: &Url,
        mut rx: mpsc::Receiver<Frame>,
    ) -> Result<(), TransportError> {
        let mut buffer = Vec::with_capacity(8);

        while rx.recv_many(&mut buffer, 8).await > 0 {
            self.post(url, &buffer).await?;
            buffer.clear();
        }

        Ok(())
    }

    /// Loops GET requests, decoding each response and forwarding frames to the engine.
    ///
    /// Exits when `token` fires, the engine channel closes, or an HTTP error occurs.
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn get_until_cancelled(
        &self,
        url: &Url,
        engine_tx: EngineSender,
        token: CancellationToken,
    ) -> Result<EngineSender, TransportError> {
        while !token.is_cancelled() {
            for frame in self.get(url).await? {
                engine_tx.send(frame).await?;
            }
        }

        tracing::debug!("cancelled polling GET");

        Ok(engine_tx)
    }

    /// Runs the full polling transport lifecycle: handshake, GET/POST loops, and optional WebSocket upgrade.
    #[tracing::instrument(skip_all, err)]
    pub async fn transport<C>(
        self,
        base_url: Url,
        connector: C,
        handshake_tx: oneshot::Sender<Handshake>,
        engine_tx: EngineSender,
        transport_rx: mpsc::Receiver<Frame>,
        token: CancellationToken,
    ) -> Result<(), TransportError>
    where
        C: WebSocketConnector,
    {
        let mut url = polling_url(base_url.clone());

        let span = tracing::debug_span!("connect", %url);

        let handshake = match self.get_one(&url).instrument(span).await? {
            Frame::Packet(Packet::Open(handshake)) => handshake,
            frame => return Err(TransportError::Open(frame)),
        };

        url.query_pairs_mut().append_pair("sid", &handshake.sid);
        let do_upgrade = handshake.can_upgrade_to_websocket();
        let sid = handshake.sid.clone();

        handshake_tx
            .send(handshake)
            .map_err(TransportError::SendHandshake)?;

        let _guard = token.clone().drop_guard();

        let child_token = token.child_token();

        let get_fut = self.get_until_cancelled(&url, engine_tx, child_token.clone());

        if do_upgrade {
            let post_fut = self.post_until_cancelled(&url, transport_rx, child_token.clone());

            let stream_fut = async {
                let _guard = child_token.drop_guard();

                let stream = WebSocketStream::connect(base_url, Some(&sid), connector).await?;

                tracing::debug!("paused polling transport");

                Ok::<_, TransportError>(stream)
            };

            let (get_result, post_result, stream_result) =
                tokio::join!(get_fut, post_fut, stream_fut);

            let engine_tx = get_result?;
            let transport_rx = post_result?;
            let stream = stream_result?;

            stream
                .transport(None, engine_tx, transport_rx, token)
                .await?;
        } else {
            let post_fut = self.post_until_closed(&url, transport_rx);

            let (get_result, post_result) = tokio::join!(get_fut, post_fut);

            get_result?;
            post_result?;
        }

        Ok(())
    }
}
