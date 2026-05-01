//! HTTP long-polling transport tasks for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::engine::EngineSender;
use crate::error::{PollingError, TransportError};
use crate::packet::{Frame, Handshake, Packet};
use crate::websocket::{WebSocketConnector, websocket_connect, websocket_loop};
use base64::prelude::*;
use bytes::Bytes;
use bytestring::ByteString;
use reqwest::Client;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
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

#[derive(Clone)]
struct PollingClient(Client);

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

        tracing::trace!(len = response.len(), "received GET");
        decode_frames(response.into())
    }

    async fn post(&self, url: &Url, frames: &[Frame]) -> Result<(), PollingError> {
        let body = encode_frames(frames);
        tracing::trace!(len = body.len(), "sending POST");

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
}

/// Builds the polling URL by appending the EIO version and transport parameters.
fn polling_url(mut base_url: Url) -> Url {
    base_url
        .query_pairs_mut()
        .append_pair("EIO", ENGINE_IO_VERSION)
        .append_pair("transport", "polling");
    base_url
}

/// Loops batched POST requests, draining outbound frames from `transport_rx`.
///
/// Returns `transport_rx` on exit so the WebSocket phase can reuse it.
/// Exits when `token` fires or `transport_rx` closes.
#[tracing::instrument(skip_all, err)]
async fn polling_post(
    url: Url,
    client: PollingClient,
    mut transport_rx: mpsc::Receiver<Frame>,
    token: CancellationToken,
) -> Result<mpsc::Receiver<Frame>, TransportError> {
    let mut buffer = Vec::with_capacity(8);

    loop {
        let count = tokio::select! {
            _ = token.cancelled() => {
                tracing::debug!("cancelled polling POST");
                break;
            },
            count = transport_rx.recv_many(&mut buffer, 8) => count,
        };

        if count == 0 {
            break;
        }

        client.post(&url, &buffer).await?;
        buffer.clear();
    }

    Ok(transport_rx)
}

/// Loops GET requests, decoding each response and forwarding frames to the engine.
///
/// Exits when `token` fires, the engine channel closes, or an HTTP error occurs.
#[tracing::instrument(skip_all, err)]
async fn polling_get(
    url: Url,
    client: PollingClient,
    engine_tx: EngineSender,
    token: CancellationToken,
) -> Result<EngineSender, TransportError> {
    while !token.is_cancelled() {
        for frame in client.get(&url).await? {
            engine_tx.send(frame).await?;
        }
    }

    tracing::debug!("cancelled polling GET");

    Ok(engine_tx)
}

#[tracing::instrument(skip_all, err)]
pub async fn polling_transport<C>(
    base_url: Url,
    http_client: reqwest::Client,
    connector: C,
    handshake_tx: oneshot::Sender<Handshake>,
    engine_tx: EngineSender,
    transport_rx: mpsc::Receiver<Frame>,
    token: CancellationToken,
) -> Result<(), TransportError>
where
    C: WebSocketConnector,
{
    let client = PollingClient(http_client);

    let mut url = polling_url(base_url.clone());

    tracing::debug!(%url, "connecting");

    let handshake = match client.get(&url).await?.remove(0) {
        Frame::Packet(Packet::Open(handshake)) => handshake,
        other => {
            return Err(TransportError::frame(
                other,
                "expected Open packet as first frame",
            ));
        }
    };

    tracing::debug!(sid = %handshake.sid, "received OPEN");

    url.query_pairs_mut().append_pair("sid", &handshake.sid);
    let do_upgrade = handshake.can_upgrade_to_websocket();
    let sid = handshake.sid.clone();

    handshake_tx
        .send(handshake)
        .map_err(TransportError::SendHandshake)?;

    let _guard = token.clone().drop_guard();

    let child_token = token.child_token();

    let get_fut = polling_get(url.clone(), client.clone(), engine_tx, child_token.clone());

    let post_fut = polling_post(url, client, transport_rx, child_token.clone());

    if do_upgrade {
        let stream = websocket_connect(base_url, Some(sid), connector).await?;

        tracing::debug!("paused polling transport");
        child_token.cancel();

        let (get_result, post_result) = tokio::join!(get_fut, post_fut);

        let engine_tx = get_result?;
        let transport_rx = post_result?;

        websocket_loop(stream, None, engine_tx, transport_rx, token).await?;
    } else {
        let (get_result, post_result) = tokio::join!(get_fut, post_fut);

        let _ = get_result?;
        let _ = post_result?;
    }

    Ok(())
}
