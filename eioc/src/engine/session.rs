//! One Engine.IO session: the protocol loops beside the transport.

use std::time::Duration;

use futures_util::TryFutureExt;
use tokio::sync::{mpsc, oneshot};
use url::Url;

use crate::connector::WebSocketConnector;
use crate::error::Error;
use crate::packet::{Event, Message};
use crate::transport::TransportStrategy;

/// Drives the engine protocol and transport concurrently until the session
/// ends.
///
/// `event_tx` receives [`Event::Open`] once the server accepts the handshake,
/// then what the server sends, and `client_message_rx` carries what the client
/// sends. Dropping the sender of `client_message_rx` closes the session, and
/// the engine drops `event_tx` once the session has ended, whichever side
/// closed it.
///
/// Fails with [`EngineError::HandshakeTimeout`](crate::error::EngineError)
/// if the server has not accepted the handshake within `handshake_timeout`,
/// like engine.io-client's `timeout` option. `None` waits without a limit.
///
/// Returns once the session has ended and the sender of `client_message_rx` is
/// dropped, even after an error, so sends to `client_message_rx` never fail.
///
/// # Errors
///
/// Returns an error if either the engine or transport task fails.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a separate channel end or connection setting; an argument struct waits for the transport API review"
)]
pub async fn connect<C>(
    url: Url,
    http_client: reqwest::Client,
    websocket_connector: C,
    strategy: TransportStrategy,
    event_tx: mpsc::Sender<Event>,
    mut client_message_rx: mpsc::Receiver<Message>,
    handshake_timeout: Option<Duration>,
    server_frame_capacity: usize,
    client_frame_capacity: usize,
) -> Result<(), Error>
where
    C: WebSocketConnector,
{
    let (server_frame_tx, server_frame_rx) = mpsc::channel(server_frame_capacity);

    let (client_frame_tx, client_frame_rx) = mpsc::channel(client_frame_capacity);

    let (handshake_tx, handshake_rx) = oneshot::channel();

    let protocol_future = crate::engine::protocol::run_protocol(
        server_frame_rx,
        event_tx,
        &mut client_message_rx,
        client_frame_tx,
        handshake_rx,
        handshake_timeout,
    );

    let transport_future = crate::transport::open(
        strategy,
        url,
        http_client,
        websocket_connector,
        handshake_tx,
        server_frame_tx,
        client_frame_rx,
    );

    // The first error drops the other side, so it is the root cause, never a
    // closed channel that it caused.
    let result = tokio::try_join!(
        protocol_future.map_err(Error::Engine),
        transport_future.map_err(Error::Transport),
    );

    // The protocol dropped `event_tx`, which tells the upper layer to close the
    // session. Accept its messages until then, so it never sees a
    // failed send.
    crate::engine::protocol::drain(&mut client_message_rx).await;

    result?;

    Ok(())
}
