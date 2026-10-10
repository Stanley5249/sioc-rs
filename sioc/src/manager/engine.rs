//! The manager's engines: the loop that runs one Engine.IO connection at a
//! time, and the state that decides when to open the next one.
//!
//! Like socket.io-client's `Manager`, which replaces `this.engine` on every
//! reconnection and unsubscribes from the old one, the manager runs each
//! engine with new channels and drops the old channels.

use std::future::Future;
use std::pin::Pin;

use eioc::prelude::{Message, ServerMessage};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Sleep;
use tracing::Instrument;

use crate::config::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::open_request::{EngineEvent, OpenHandles, OpenRequest};
use crate::manager::routes::Routes;

/// Runs each requested engine beside the server-message loop, one at a time,
/// until the client-packet loop drops its sender.
///
/// The server-message loop and its half-built binary packet end with their
/// engine.
pub async fn run_engines<F, Fut>(
    routes: &Routes,
    mut open_request_rx: mpsc::Receiver<OpenRequest>,
    mut connect_engine: F,
    channels: ChannelConfig,
) where
    F: FnMut(mpsc::Sender<ServerMessage>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    while let Some(request) = open_request_rx.recv().await {
        let OpenRequest {
            attempt,
            client_message_rx,
            engine_event_tx,
            engine_result_tx,
        } = request;

        let (server_message_tx, server_message_rx) = mpsc::channel(channels.manager);

        let engine = connect_engine(server_message_tx, client_message_rx);

        let engine_result = async {
            let (server_result, engine_result) = tokio::join!(
                crate::manager::server_message::server_messages_to_packets(
                    server_message_rx,
                    routes,
                    engine_event_tx,
                ),
                engine,
            );

            server_result?;
            engine_result?;

            Ok(())
        }
        .instrument(tracing::info_span!("engine", attempt))
        .await;

        // The client-packet loop drops the receiver early only when it ends
        // with its own error, which it reports instead.
        let _ = engine_result_tx.send(engine_result);
    }
}

/// The manager's engine state, like socket.io-client's `Manager._readyState`
/// and `_reconnecting` in one value.
#[derive(Debug)]
pub enum ReadyState {
    /// No engine; the next namespace opens one.
    Closed,
    /// An engine is connecting or connected.
    Open(OpenHandles),
    /// The engine is closing: the client-packet loop dropped its sender and
    /// waits for the engine's result.
    Closing {
        /// Read until it ends, so the server-message loop never sends into a
        /// closed channel.
        engine_event_rx: mpsc::UnboundedReceiver<EngineEvent>,
        engine_result_rx: oneshot::Receiver<Result<(), ManagerError>>,
        /// Whether the next engine waits for the backoff delay. The client
        /// closes the engine without it, like socket.io-client's
        /// `skipReconnect`.
        reconnect: bool,
    },
    /// Waiting out the backoff delay before the next engine.
    Reconnecting(Pin<Box<Sleep>>),
}

impl ReadyState {
    /// Closes the open engine, like socket.io-client's `engine.close()`. Any
    /// other state stays as it is.
    ///
    /// Drops `client_message_tx`, which closes the engine, and keeps the other
    /// ends until the engine finishes. `reconnect` tells whether the next
    /// engine waits for the backoff delay.
    pub fn close(&mut self, reconnect: bool) {
        *self = match std::mem::replace(self, Self::Closed) {
            Self::Open(OpenHandles {
                client_message_tx,
                engine_event_rx,
                engine_result_rx,
            }) => {
                drop(client_message_tx);

                tracing::debug!(reconnect, "engine closing");

                Self::Closing {
                    engine_event_rx,
                    engine_result_rx,
                    reconnect,
                }
            }
            other => other,
        };
    }

    /// Closes the engine and cancels a pending reconnection, like
    /// socket.io-client's `Manager._destroy` once no namespace is active.
    pub fn destroy(&mut self) {
        match self {
            Self::Open(_) => self.close(false),
            Self::Reconnecting(_) => {
                tracing::debug!("stopped reconnecting");

                *self = Self::Closed;
            }
            Self::Closed | Self::Closing { .. } => {}
        }
    }

    /// Returns the open engine, if any.
    #[must_use]
    pub fn engine(&self) -> Option<&OpenHandles> {
        match self {
            Self::Open(engine) => Some(engine),
            Self::Closed | Self::Closing { .. } | Self::Reconnecting(_) => None,
        }
    }

    /// Waits for the next event of the current state. `Closed` never reports
    /// one.
    ///
    /// Closes the open engine once it stops delivering server messages, then
    /// waits for its result.
    ///
    /// Cancel safe: it awaits only `recv`, a oneshot receiver, or a pinned
    /// `Sleep`, and closing has no suspend point. The caller leaves `Closing`
    /// once it reports `Close`, so the finished receiver is never polled
    /// again.
    pub async fn next_event(&mut self) -> EngineEvent {
        loop {
            match self {
                Self::Open(engine) => {
                    if let Some(event) = engine.engine_event_rx.recv().await {
                        return event;
                    }

                    self.close(true);
                }
                Self::Closing {
                    engine_event_rx,
                    engine_result_rx,
                    ..
                } => {
                    // No engine takes the buffered events any more.
                    while engine_event_rx.recv().await.is_some() {}

                    let engine_result = engine_result_rx
                        .await
                        .expect("run_engines reports every engine's result");

                    return EngineEvent::Close(engine_result);
                }
                Self::Reconnecting(delay) => {
                    delay.await;
                    return EngineEvent::Reconnect;
                }
                Self::Closed => std::future::pending().await,
            }
        }
    }
}
