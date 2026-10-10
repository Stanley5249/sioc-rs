//! The manager's engines: the loop that runs one Engine.IO connection at a
//! time, and the state that decides when to open the next one.
//!
//! Like socket.io-client's `Manager`, which replaces `this.engine` on every
//! reconnection and unsubscribes from the old one, the manager runs each
//! engine with new channels and drops the old channels.

use std::future::Future;
use std::pin::Pin;

use bytestring::ByteString;
use eioc::prelude::{Event, Message};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Sleep;
use tracing::Instrument;

use crate::client::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::open_request::{OpenHandles, OpenRequest};
use crate::manager::routes::Routes;
use crate::manager::server_message::ServerEvent;

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
    F: FnMut(mpsc::Sender<Event>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    while let Some(request) = open_request_rx.recv().await {
        let OpenRequest {
            attempt,
            client_message_rx,
            server_event_tx,
            engine_result_tx,
        } = request;

        let (event_tx, event_rx) = mpsc::channel(channels.manager);

        let engine = connect_engine(event_tx, client_message_rx);

        let engine_result = async {
            let (server_result, engine_result) = tokio::join!(
                crate::manager::server_message::server_messages_to_packets(
                    event_rx,
                    routes,
                    server_event_tx,
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
        server_event_rx: mpsc::UnboundedReceiver<ServerEvent>,
        engine_result_rx: oneshot::Receiver<Result<(), ManagerError>>,
    },
    /// Waiting out the backoff delay before the next engine.
    Reconnecting(Pin<Box<Sleep>>),
}

/// What the current [`ReadyState`] reports.
#[derive(Debug)]
pub enum EngineEvent {
    /// The server accepted the Engine.IO handshake, like socket.io-client's
    /// `Manager.onopen`.
    Opened,
    /// The server confirmed this namespace.
    Connected(ByteString),
    /// The engine stopped delivering server messages.
    ServerEnded,
    /// The engine finished, like socket.io-client's `Manager.onclose`.
    Closed(Result<(), ManagerError>),
    /// The backoff delay is over.
    ReconnectDue,
}

impl ReadyState {
    /// Closes the open engine, like socket.io-client's `engine.close()`. Any
    /// other state stays as it is.
    ///
    /// Drops `client_message_tx`, which closes the engine, and keeps the other
    /// ends until the engine finishes.
    pub fn close(&mut self) {
        *self = match std::mem::replace(self, Self::Closed) {
            Self::Open(OpenHandles {
                client_message_tx,
                server_event_rx,
                engine_result_rx,
            }) => {
                drop(client_message_tx);

                tracing::debug!("engine closing");

                Self::Closing {
                    server_event_rx,
                    engine_result_rx,
                }
            }
            other => other,
        };
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
    /// Cancel safe: it awaits only `recv`, a oneshot receiver, or a pinned
    /// `Sleep`. The caller leaves `Closing` once it reports `Closed`, so the
    /// finished receiver is never polled again.
    pub async fn next_event(&mut self) -> EngineEvent {
        match self {
            Self::Open(engine) => match engine.server_event_rx.recv().await {
                Some(ServerEvent::Opened) => EngineEvent::Opened,
                Some(ServerEvent::Connected(ns)) => EngineEvent::Connected(ns),
                None => EngineEvent::ServerEnded,
            },
            Self::Closing {
                server_event_rx,
                engine_result_rx,
            } => {
                // No engine takes the buffered events any more.
                while server_event_rx.recv().await.is_some() {}

                let engine_result = engine_result_rx
                    .await
                    .expect("run_engines reports every engine's result");

                EngineEvent::Closed(engine_result)
            }
            Self::Reconnecting(delay) => {
                delay.await;
                EngineEvent::ReconnectDue
            }
            Self::Closed => std::future::pending().await,
        }
    }
}
