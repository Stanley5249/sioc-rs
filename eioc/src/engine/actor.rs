//! The engine task: one loop that carries data between the manager's channels
//! and the transport's, and asks [`EngineCore`] what each item means.

use std::future::Future;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;
use tracing::Instrument;
use url::Url;

use crate::connector::WebSocketConnector;
use crate::engine::state::EngineCore;
use crate::error::{EngineError, Error, TransportError};
use crate::packet::{ClientMessage, ServerMessage};
use crate::transport::{ClientFrame, ServerFrame, TransportStrategy};

/// What [`open`] needs, like engine.io-client's `SocketOptions`.
#[derive(Debug)]
pub struct EngineOptions<C> {
    /// The Engine.IO endpoint, like the `uri` and `path` options.
    pub url: Url,
    /// The client for polling requests. JS uses the platform's
    /// `XMLHttpRequest` or `fetch`.
    ///
    /// Closing a polling session waits for the server to answer the Close
    /// request, unlike the JS client. Set a timeout on this client to bound
    /// that wait.
    pub http_client: reqwest::Client,
    /// Opens WebSocket connections. JS uses the platform's
    /// `WebSocket`.
    pub websocket_connector: C,
    /// The first transport, like the `transports` option.
    pub transport: TransportStrategy,
    /// How long the server may take to accept the handshake, like the
    /// `timeout` option of socket.io-client's `Manager`. `None` waits without
    /// a limit.
    pub handshake_timeout: Option<Duration>,
    /// Slots of the channel to the upper layer. Rust bound.
    pub server_message_capacity: usize,
    /// Slots of the channel from the upper layer. Rust bound.
    pub client_message_capacity: usize,
    /// Slots of the channel from the transport. Rust bound.
    pub server_frame_capacity: usize,
    /// Slots of the channel to the transport, which also holds messages sent
    /// before the handshake, like `writeBuffer`. Rust bound.
    pub client_frame_capacity: usize,
}

/// A running engine, like engine.io-client's `Socket`. Rust channel: the JS
/// `Socket` is called directly, while here its one task is reached through
/// these channels.
///
/// The first message on `server_message_rx` is [`ServerMessage::Open`], and
/// its end is the end of the session, whichever side closed it. Dropping
/// `client_message_tx` closes the session, after the frames already accepted.
/// The task accepts messages until then, even after the session ended, so
/// sends never fail. Drop `client_message_tx` before awaiting `task`, which
/// waits for that hang-up.
///
/// `closed_rx` ends once the session is over, before the messages still
/// queued on `server_message_rx`. A reader that cannot take those messages,
/// because its own receiver is full, learns from it that the session is
/// gone, so it can drain them and hang up instead of waiting for capacity.
///
/// Dropping `client_message_tx` before the handshake drops the opening. Send
/// nothing before [`ServerMessage::Open`]: the transport reads its channel
/// only after the handshake, so once that channel is full the task cannot
/// see the hang-up, and a handshake may have no deadline. Dropping `task`
/// aborts the session at any point.
#[derive(Debug)]
pub struct Engine {
    /// What the client sends, like `Socket.write`.
    pub client_message_tx: mpsc::Sender<ClientMessage>,
    /// What the server sends, like the `"open"` and `"message"` events.
    pub server_message_rx: mpsc::Receiver<ServerMessage>,
    /// Ends when the session closes, like the `"close"` event, and never
    /// carries a value. Rust wait: a channel's end stays hidden behind its
    /// unread items, while JS emits `"close"` at once.
    pub closed_rx: watch::Receiver<()>,
    /// The session's result, like the `"error"` and `"close"` events. Rust
    /// cleanup: dropping it aborts the task, so an owner that is itself
    /// dropped leaves no task behind.
    pub task: AbortOnDropHandle<Result<(), Error>>,
}

/// Opens a session on its own task, like the `Socket` constructor.
#[must_use]
pub fn open<C>(options: EngineOptions<C>) -> Engine
where
    C: WebSocketConnector,
{
    let EngineOptions {
        url,
        http_client,
        websocket_connector,
        transport,
        handshake_timeout,
        server_message_capacity,
        client_message_capacity,
        server_frame_capacity,
        client_frame_capacity,
    } = options;

    let (server_message_tx, server_message_rx) = mpsc::channel(server_message_capacity);
    let (client_message_tx, client_message_rx) = mpsc::channel(client_message_capacity);
    let (server_frame_tx, server_frame_rx) = mpsc::channel(server_frame_capacity);
    let (client_frame_tx, client_frame_rx) = mpsc::channel(client_frame_capacity);
    let (closed_tx, closed_rx) = watch::channel(());

    let transport = crate::transport::open(
        transport,
        url,
        http_client,
        websocket_connector,
        server_frame_tx,
        client_frame_rx,
    );
    let core = EngineCore::new(handshake_timeout, Instant::now());
    let future = run(
        core,
        transport,
        server_frame_rx,
        client_frame_tx,
        server_message_tx,
        client_message_rx,
        closed_tx,
    );
    let task = tokio::spawn(future.instrument(tracing::info_span!("engine")));

    Engine {
        client_message_tx,
        server_message_rx,
        closed_rx,
        task: AbortOnDropHandle::new(task),
    }
}

/// Runs one session until the manager and the transport have both hung up.
///
/// Each direction takes its destination's permit before it reads an item, so
/// a full channel holds up only the data that needs it. The one exception is
/// a server frame, which is read before its permit so that pings ahead of the
/// next message are answered while the manager is full. That frame's message
/// waits in `pending`, and no frame is read until it is delivered.
///
/// The end of each channel drives cleanup, and `closed_tx` ends with the
/// session, ahead of the messages still queued for the manager. When the
/// manager hangs up, the session closes, the unread server messages are
/// discarded, and the transport sends Close once its channel ends, or is
/// dropped if the handshake is still pending. When the transport ends the
/// server direction, the session closes and the messages already received
/// are still delivered. A deadline failure drops the transport, because the
/// server no longer answers, and still delivers the message already received.
/// In every case the task keeps receiving manager messages until the manager
/// hangs up, so its sends never fail.
///
/// Every branch future is cancel safe: channels keep unread items, a
/// cancelled reservation returns its slot, the transport future lives here
/// and is polled by reference, so no other branch cancels a request in
/// flight, and the sleep is pinned.
///
/// # Errors
///
/// Returns the first failure, from the core or the transport. Later closed
/// channels that the failure caused never replace it.
///
/// # Panics
///
/// Never in practice: each `expect` restates the guard of its branch.
#[expect(
    clippy::too_many_lines,
    reason = "one select! loop whose branches borrow the same local channel ends"
)]
pub async fn run<T>(
    mut core: EngineCore,
    transport: T,
    server_frame_rx: mpsc::Receiver<ServerFrame>,
    client_frame_tx: mpsc::Sender<ClientFrame>,
    server_message_tx: mpsc::Sender<ServerMessage>,
    client_message_rx: mpsc::Receiver<ClientMessage>,
    closed_tx: watch::Sender<()>,
) -> Result<(), Error>
where
    T: Future<Output = Result<(), TransportError>>,
{
    let mut transport = Some(Box::pin(transport));
    let mut server_frame_rx = Some(server_frame_rx);
    let mut client_frame_tx = Some(client_frame_tx);
    let mut server_message_tx = Some(server_message_tx);
    let mut client_message_rx = Some(client_message_rx);
    let mut closed_tx = Some(closed_tx);
    // The message of the last server frame, waiting for a manager permit.
    let mut pending: Option<ServerMessage> = None;
    // The first failure, kept while cleanup runs.
    let mut result: Result<(), Error> = Ok(());

    let sleep = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(sleep);
    let mut armed = None;

    loop {
        // A manager that hung up no longer reads, so its messages are
        // discarded. Otherwise the manager channel ends after the last
        // message the transport delivered.
        if client_message_rx.is_none() {
            pending = None;
        }
        if pending.is_none() && (client_message_rx.is_none() || server_frame_rx.is_none()) {
            server_message_tx = None;
        }
        // Ending the transport channel makes the transport send Close, and
        // ending `closed_tx` tells a manager blocked on application capacity
        // to drain the messages left behind.
        if core.is_closed() {
            client_frame_tx = None;
            drop(closed_tx.take());
        }

        let deadline = core.deadline();
        if deadline != armed {
            if let Some(deadline) = deadline {
                sleep.as_mut().reset(deadline);
            }
            armed = deadline;
        }

        if client_message_rx.is_none()
            && server_message_tx.is_none()
            && transport.is_none()
            && server_frame_rx.is_none()
        {
            break;
        }

        let has_pong = core.has_pong();

        tokio::select! {
            frame = async {
                server_frame_rx.as_mut().expect("the guard checked the receiver").recv().await
            }, if pending.is_none() && server_frame_rx.is_some() => {
                if let Some(frame) = frame {
                    pending = core.on_frame(frame, Instant::now());
                } else {
                    server_frame_rx = None;
                    core.close();
                }
            },

            permit = async {
                server_message_tx
                    .as_ref()
                    .expect("a pending message keeps its sender")
                    .reserve()
                    .await
            }, if pending.is_some() => {
                // The manager drops its receiver only after this sender, so a
                // failure here is a library bug, reported unless a failure
                // came first.
                let Ok(permit) = permit else {
                    return result.and(Err(EngineError::ServerMessageClosed.into()));
                };
                permit.send(pending.take().expect("the guard checked the message"));
            },

            // A pong waits for the same sender, so it disables this branch,
            // whose reservation would otherwise hold the only slot. Without a
            // sender or a permit, the session can no longer send the message,
            // so it is discarded and the manager's sends succeed until it hangs
            // up.
            input = async {
                let permit = match &client_frame_tx {
                    Some(client_frame_tx) => client_frame_tx.reserve().await.ok(),
                    None => None,
                };
                let message = client_message_rx
                    .as_mut()
                    .expect("the guard checked the receiver")
                    .recv()
                    .await;
                (permit, message)
            }, if !has_pong && client_message_rx.is_some() => {
                let (permit, message) = input;
                if let Some(message) = message {
                    if let (Some(permit), Some(frame)) = (permit, core.on_message(message)) {
                        permit.send(frame);
                    }
                } else {
                    client_message_rx = None;
                    // Before the handshake the server holds no session to
                    // close, and the transport does not read its channel yet,
                    // so the opening is dropped instead of waited for.
                    if core.is_opening() {
                        transport = None;
                        server_frame_rx = None;
                    }
                    core.close();
                }
            },

            permit = async {
                client_frame_tx.as_ref().expect("a pong implies an open session").reserve().await
            }, if has_pong => {
                let pong = core.take_pong().expect("the guard checked the pong");
                // A failed reservation means the transport is gone, and its own
                // result says why.
                if let Ok(permit) = permit {
                    permit.send(pong);
                }
            },

            transport_result = async {
                transport.as_mut().expect("the guard checked the transport").as_mut().await
            }, if transport.is_some() => {
                transport = None;
                result = result.and(transport_result.map_err(Error::from));
            },

            () = &mut sleep, if armed.is_some() => {
                if let Err(error) = core.on_timeout(Instant::now()) {
                    // The server no longer answers, so its session and the
                    // frames still in flight are gone. A message already
                    // received still reaches the manager.
                    transport = None;
                    server_frame_rx = None;
                    result = result.and(Err(error.into()));
                }
            },

            else => unreachable!("the loop stops before every input is gone"),
        }
    }

    result
}
