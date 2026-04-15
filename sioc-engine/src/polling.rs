//! HTTP long-polling transport tasks for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::engine::EngineSender;
use crate::error::{PollingError, TransportError};
use crate::packet::{Frame, Handshake, Packet};
use crate::websocket::{WebSocketConnector, websocket_connect, websocket_loop};
use base64::prelude::*;
use bytes::{BufMut, Bytes, BytesMut};
use reqwest::Client;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use url::Url;

const SEPARATOR: u8 = 0x1e;

fn encode_binary(buffer: &mut BytesMut, bytes: &[u8]) {
    let b64 = BASE64_STANDARD.encode(bytes);
    buffer.put_u8(b'b');
    buffer.put_slice(b64.as_bytes());
}

fn decode_binary(bytes: Bytes) -> Result<Bytes, PollingError> {
    Ok(Bytes::from(BASE64_STANDARD.decode(&bytes[1..])?))
}

fn encode_frames<'a, I>(frames: I) -> Bytes
where
    I: IntoIterator<Item = &'a Frame>,
{
    let mut buffer = BytesMut::new();
    for (i, frame) in frames.into_iter().enumerate() {
        if i > 0 {
            buffer.put_u8(SEPARATOR);
        }
        match frame {
            Frame::Packet(packet) => buffer.put_slice(&packet.encode()),
            Frame::Binary(bytes) => encode_binary(&mut buffer, bytes),
        }
    }
    buffer.freeze()
}

/// Decodes a single frame from a raw bytes value.
fn decode_frame(bytes: Bytes) -> Result<Frame, TransportError> {
    Ok(if bytes.first().is_some_and(|&b| b == b'b') {
        decode_binary(bytes)?.into()
    } else {
        Packet::decode(bytes)?.into()
    })
}

/// Decode all frames from a polling response body.
///
/// `Bytes::split` on a separator always yields at least one element, so the
/// returned `Vec` is guaranteed to be non-empty for any non-error response.
fn decode_frames(bytes: Bytes) -> Result<Vec<Frame>, TransportError> {
    bytes
        .split(|&b| b == SEPARATOR)
        .map(|s| decode_frame(bytes.slice_ref(s)))
        .collect()
}

/// Builds the polling URL by appending the EIO version and transport parameters.
fn polling_url(mut base_url: Url) -> Url {
    base_url
        .query_pairs_mut()
        .append_pair("EIO", &ENGINE_IO_VERSION.to_string())
        .append_pair("transport", "polling");
    base_url
}

/// Loops batched POST requests, draining outbound frames from `engine_rx`.
///
/// Returns `engine_rx` on exit so the WebSocket phase can reuse it.
/// Exits when `cancel` fires or `engine_rx` closes.
#[tracing::instrument(skip_all, err)]
async fn polling_post(
    url: Url,
    http_client: Client,
    mut transport_rx: mpsc::Receiver<Frame>,
    token: CancellationToken,
) -> Result<mpsc::Receiver<Frame>, TransportError> {
    let mut buffer = Vec::with_capacity(8);

    loop {
        let count = tokio::select! {
            _ = token.cancelled() => {
                tracing::debug!("cancel polling post");
                break;
            },
            count = transport_rx.recv_many(&mut buffer, 8) => count,
        };

        if count == 0 {
            break;
        }

        let request = encode_frames(&buffer);
        buffer.clear();

        tracing::trace!(?request, "sending POST");

        let response = http_client
            .post(url.as_str())
            .body(request)
            .send()
            .await
            .map_err(PollingError::Http)?
            .error_for_status()
            .map_err(PollingError::Http)?
            .text()
            .await
            .map_err(PollingError::Http)?;

        if !response.eq_ignore_ascii_case("ok") {
            return Err(PollingError::UnexpectedResponse { response }.into());
        }
    }

    Ok(transport_rx)
}

/// Loops GET requests, decoding each response and forwarding frames to the engine.
///
/// Exits when `cancel` fires, the engine channel closes, or an HTTP error occurs.
#[tracing::instrument(skip_all, err)]
async fn polling_get(
    url: Url,
    http_client: Client,
    engine_tx: EngineSender,
    token: CancellationToken,
) -> Result<EngineSender, TransportError> {
    while !token.is_cancelled() {
        let response = http_client
            .get(url.as_str())
            .send()
            .await
            .map_err(PollingError::Http)?
            .error_for_status()
            .map_err(PollingError::Http)?
            .bytes()
            .await
            .map_err(PollingError::Http)?;

        tracing::trace!(?response, "received GET");

        for frame in decode_frames(response)? {
            engine_tx.send(frame).await?;
        }
    }

    tracing::debug!("cancel polling get");

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
    let mut url = polling_url(base_url.clone());

    tracing::debug!(%url, "connecting");

    let bytes = http_client
        .get(url.clone())
        .send()
        .await
        .map_err(PollingError::Http)?
        .error_for_status()
        .map_err(PollingError::Http)?
        .bytes()
        .await
        .map_err(PollingError::Http)?;

    let handshake = match decode_frame(bytes)? {
        Frame::Packet(Packet::Open(handshake)) => handshake,
        other => {
            return Err(TransportError::frame(
                other,
                "expected Open packet as first frame",
            ));
        }
    };

    tracing::debug!(?handshake, "received OPEN");

    url.query_pairs_mut().append_pair("sid", &handshake.sid);
    let do_upgrade = handshake.can_upgrade_to_websocket();
    let sid = handshake.sid.clone();

    handshake_tx
        .send(handshake)
        .map_err(TransportError::SendHandshake)?;

    let _guard = token.clone().drop_guard();

    let child_token = token.child_token();

    let get_handle = tokio::spawn(polling_get(
        url.clone(),
        http_client.clone(),
        engine_tx,
        child_token.clone(),
    ));

    let post_handle = tokio::spawn(polling_post(
        url,
        http_client,
        transport_rx,
        child_token.clone(),
    ));

    if do_upgrade {
        let stream = websocket_connect(base_url, Some(sid), connector).await?;

        tracing::debug!("pause polling transport");
        child_token.cancel();

        let (get_result, post_result) = tokio::join!(get_handle, post_handle);
        let engine_tx = get_result??;
        let transport_rx = post_result??;

        websocket_loop(stream, None, engine_tx, transport_rx, token).await?;
    } else {
        let (get_result, post_result) = tokio::join!(get_handle, post_handle);
        let _ = get_result??;
        let _ = post_result??;
    }

    Ok(())
}
