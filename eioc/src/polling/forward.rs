//! The GET and POST loops that forward frames between the server and the
//! engine.

use std::pin::pin;

use reqwest::Client;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::connector::WebSocketStream;
use crate::error::{TransportError, WebSocketError};
use crate::packet::{Frame, Packet};

/// Why a polling loop stopped.
pub enum Stop {
    /// The upgrade paused polling.
    Paused,
    /// The engine or the server ended the session.
    Ended,
}

/// Batches client frames into POST requests until `pause` fires or the engine
/// closes `client_frame_rx`.
///
/// When the engine closes `client_frame_rx`, posts a `Close` packet to end the
/// session.
///
/// Batches contain at most sixteen frames and respect the handshake's wire-byte
/// limit. A single oversized frame travels alone, matching engine.io-client's
/// batching behavior.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn forward_client_frames(
    client: &Client,
    url: &Url,
    client_frame_rx: &mut mpsc::Receiver<Frame>,
    pause: &CancellationToken,
    max_payload: u64,
) -> Result<Stop, TransportError> {
    let mut pending = None;

    loop {
        // Pause only while idle, because a POST in flight may carry frames.
        let first = if let Some(frame) = pending.take() {
            Some(frame)
        } else {
            tokio::select! {
                () = pause.cancelled() => {
                    tracing::debug!("paused polling post");
                    return Ok(Stop::Paused);
                }
                frame = client_frame_rx.recv() => frame,
            }
        };

        let Some(first) = first else {
            tracing::trace!("sent close packet");

            crate::polling::request::post_frames(client, url, &[Packet::Close.into()]).await?;

            return Ok(Stop::Ended);
        };

        // Empty or disconnected means the ready prefix has ended. Pending
        // frames are posted before honoring an upgrade pause.
        let ready = std::iter::from_fn(|| {
            if pause.is_cancelled() {
                None
            } else {
                client_frame_rx.try_recv().ok()
            }
        });
        let (buffer, remainder) = crate::polling::payload::take_batch(first, ready, max_payload);
        pending = remainder;

        crate::polling::request::post_frames(client, url, &buffer).await?;
    }
}

/// Sends one response's frames to the engine, returning `true` at the server's
/// `Close`.
///
/// The `Close` packet itself stays here, because the transport ending is
/// what tells the engine the session is over.
pub async fn send_server_frames(
    frames: impl IntoIterator<Item = Frame>,
    server_frame_tx: &mpsc::Sender<Frame>,
) -> Result<bool, TransportError> {
    for frame in frames {
        if frame == Frame::Packet(Packet::Close) {
            tracing::debug!("server closed");

            return Ok(true);
        }

        server_frame_tx.send(frame).await?;
    }

    Ok(false)
}

/// Forwards server frames to the engine until `pause` fires or the server sends
/// `Close`.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn forward_server_frames(
    client: &Client,
    url: &Url,
    server_frame_tx: &mpsc::Sender<Frame>,
    pause: &CancellationToken,
) -> Result<Stop, TransportError> {
    // Pause only between requests, because the server answers the GET in
    // flight once the upgrade probe succeeds, and the answer may carry frames.
    while !pause.is_cancelled() {
        let frames = crate::polling::request::get_frames(client, url).await?;

        if send_server_frames(frames, server_frame_tx).await? {
            return Ok(Stop::Ended);
        }
    }

    tracing::debug!("paused polling get");

    Ok(Stop::Paused)
}

/// Runs the GET and POST loops until both pause or either one ends the session.
pub async fn forward_frames(
    client: &Client,
    url: &Url,
    server_frame_tx: &mpsc::Sender<Frame>,
    client_frame_rx: &mut mpsc::Receiver<Frame>,
    pause: &CancellationToken,
    max_payload: u64,
) -> Result<Stop, TransportError> {
    let mut forward_server = pin!(forward_server_frames(client, url, server_frame_tx, pause));
    let mut forward_client = pin!(forward_client_frames(
        client,
        url,
        client_frame_rx,
        pause,
        max_payload
    ));

    // A pause lets the other loop finish its request. An ended session
    // abandons it, because its result no longer matters.
    tokio::select! {
        stop = &mut forward_server => match stop? {
            Stop::Paused => forward_client.await,
            Stop::Ended => Ok(Stop::Ended),
        },
        stop = &mut forward_client => match stop? {
            Stop::Paused => forward_server.await,
            Stop::Ended => Ok(Stop::Ended),
        },
    }
}

/// Polls until the session ends, or until `upgrade` connects and polling
/// pauses.
///
/// Returns the upgraded stream, or `None` if the session ended first. A failed
/// upgrade falls back to long polling for the rest of the session.
pub async fn forward_frames_until_upgrade(
    client: &Client,
    url: &Url,
    server_frame_tx: &mpsc::Sender<Frame>,
    client_frame_rx: &mut mpsc::Receiver<Frame>,
    upgrade: impl Future<Output = Result<WebSocketStream, WebSocketError>>,
    max_payload: u64,
) -> Result<Option<WebSocketStream>, TransportError> {
    let pause = CancellationToken::new();
    let mut forward = pin!(forward_frames(
        client,
        url,
        server_frame_tx,
        client_frame_rx,
        &pause,
        max_payload
    ));

    tokio::select! {
        stop = &mut forward => {
            stop?;
            Ok(None)
        }
        result = upgrade => match result {
            Ok(stream) => {
                pause.cancel();

                match forward.await? {
                    Stop::Paused => Ok(Some(stream)),
                    Stop::Ended => Ok(None),
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to upgrade, continuing long polling");

                forward.await?;

                Ok(None)
            }
        },
    }
}
