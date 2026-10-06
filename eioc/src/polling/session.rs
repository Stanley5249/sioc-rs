//! One polling session, from the handshake to the end or an upgrade.

use reqwest::Client;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use url::Url;

use crate::connector::WebSocketConnector;
use crate::error::TransportError;
use crate::packet::{Frame, Handshake, Packet};

/// Runs the full polling transport lifecycle: handshake, GET/POST loops, and
/// optional WebSocket upgrade.
///
/// Finishes once the engine closes `client_frame_rx`. If the server ends the
/// session first, drops `server_frame_tx` and discards client frames until the
/// engine closes `client_frame_rx`.
///
/// # Errors
///
/// Returns an error if a network, protocol, or channel failure occurs.
///
/// # Panics
///
/// Never in practice: a decoded polling payload always holds at least one
/// frame.
#[tracing::instrument(skip_all)]
pub async fn run<C>(
    client: Client,
    base_url: Url,
    connector: C,
    handshake_tx: oneshot::Sender<Handshake>,
    server_frame_tx: mpsc::Sender<Frame>,
    mut client_frame_rx: mpsc::Receiver<Frame>,
) -> Result<(), TransportError>
where
    C: WebSocketConnector,
{
    let mut url = crate::polling::request::polling_url(base_url.clone());

    let span = tracing::debug_span!("connect", %url);

    // The server may batch packets after the Open packet, as engine.io-client
    // accepts, so the rest of the response goes to the engine.
    let mut frames = crate::polling::request::get_frames(&client, &url)
        .instrument(span)
        .await?
        .into_iter();
    let first = frames
        .next()
        .expect("a decoded payload holds at least one frame");
    let handshake = match first {
        Frame::Packet(Packet::Open(handshake)) => handshake,
        frame => return Err(TransportError::Open(frame)),
    };

    url.query_pairs_mut().append_pair("sid", &handshake.sid);
    let can_upgrade = handshake.can_upgrade_to_websocket();
    let sid = handshake.sid.clone();
    let max_payload = handshake.max_payload;

    handshake_tx
        .send(handshake)
        .map_err(TransportError::Handshake)?;

    let stream = if crate::polling::forward::send_server_frames(frames, &server_frame_tx).await? {
        None
    } else if can_upgrade {
        let upgrade = crate::websocket::stream::connect(base_url, Some(&sid), connector);

        crate::polling::forward::forward_frames_until_upgrade(
            &client,
            &url,
            &server_frame_tx,
            &mut client_frame_rx,
            upgrade,
            max_payload,
        )
        .await?
    } else {
        crate::polling::forward::forward_frames(
            &client,
            &url,
            &server_frame_tx,
            &mut client_frame_rx,
            &CancellationToken::new(),
            max_payload,
        )
        .await?;

        None
    };

    if let Some(stream) = stream {
        tracing::debug!("paused polling transport");

        return crate::websocket::session::run(stream, None, server_frame_tx, client_frame_rx)
            .await;
    }

    // The engine may queue frames before it learns the server ended the
    // session, and the closed session cannot accept them.
    drop(server_frame_tx);
    while client_frame_rx.recv().await.is_some() {}

    Ok(())
}
