//! Opens one Engine.IO session after another while the namespaces need one.

use std::future::Future;

use eioc::prelude::Message;
use tokio::sync::mpsc;

use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::routes::Routes;

/// Why the client-packet loop asks for a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionRequest {
    /// The client opened, or a namespace opened while no session was open.
    Open,
    /// The last session ended while namespaces were still open.
    Reconnect,
}

/// The client-packet loop's end of one Engine.IO session.
#[derive(Debug)]
pub struct Session {
    /// Counts sessions from 0, so an ack from an ended session is discarded.
    pub number: u64,
    /// Carries client messages to the session's engine. Dropping it closes
    /// the session.
    pub client_message_tx: mpsc::Sender<Message>,
    /// Reports which namespace generations the server confirmed, and ends
    /// with the session.
    pub connected_generation_rx: mpsc::UnboundedReceiver<u64>,
}

/// What the supervisor needs to open each session.
pub struct Supervisor<F> {
    /// Capacity of the channel from the engine to the server-message loop.
    pub server_message_capacity: usize,
    /// Capacity of the channel from the client-packet loop to the engine.
    pub client_message_capacity: usize,
    /// Delays each reconnection, or `None` to stop instead of reconnecting.
    pub backoff: Option<Backoff>,
    /// Runs one engine session; see [`eioc::engine::session::connect`].
    pub connect_engine: F,
}

impl<F, Fut> Supervisor<F>
where
    F: FnMut(mpsc::Sender<Message>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    /// Opens a session for each request of the client-packet loop until the
    /// loop hangs up, or until a reconnection is due and `backoff` gives up.
    ///
    /// Restarts the backoff after a session the server answered, like
    /// socket.io-client's `Manager.onreconnect`, and when a namespace opens
    /// while no session is open. Then drops `session_tx`, and waits until
    /// `session_request_rx` ends.
    ///
    /// # Errors
    ///
    /// Returns an internal error from a session, which ends the client, or the
    /// last session's error when the supervisor gives up reconnecting.
    pub async fn supervise(
        mut self,
        mut session_request_rx: mpsc::Receiver<SessionRequest>,
        session_tx: mpsc::Sender<Session>,
        routes: &Routes,
    ) -> Result<(), ManagerError> {
        let mut numbers = 0..;
        let mut last_result = Ok(());

        let result = loop {
            let Some(request) = session_request_rx.recv().await else {
                break Ok(());
            };

            if request == SessionRequest::Open {
                self.reset_backoff();
            }

            if request == SessionRequest::Reconnect {
                let Some(delay) = self.backoff.as_mut().and_then(Backoff::next_delay) else {
                    break last_result;
                };

                // The client-packet loop asks for one session at a time, so
                // only its hang-up can arrive while waiting.
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    request = session_request_rx.recv() => {
                        assert!(request.is_none(), "the client-packet loop asks for one session at a time");
                        break Ok(());
                    }
                }
            }

            let number = numbers.next().unwrap_or_default();

            let mut answered = false;

            last_result = self
                .run_session(number, &session_tx, routes, &mut answered)
                .await;

            if answered {
                self.reset_backoff();
            }

            match &last_result {
                Ok(()) => tracing::debug!(session = number, "session ended"),
                Err(error) if error.is_internal() => break last_result,
                Err(error) => tracing::warn!(session = number, %error, "session failed"),
            }
        };

        drop(session_tx);

        while session_request_rx.recv().await.is_some() {}

        result
    }

    fn reset_backoff(&mut self) {
        if let Some(backoff) = &mut self.backoff {
            backoff.reset();
        }
    }

    /// Hands a new session to the client-packet loop, then runs the
    /// server-message loop beside the engine until both end.
    ///
    /// Sets `answered` once the server sends a message.
    async fn run_session(
        &mut self,
        number: u64,
        session_tx: &mpsc::Sender<Session>,
        routes: &Routes,
        answered: &mut bool,
    ) -> Result<(), ManagerError> {
        let (server_message_tx, server_message_rx) = mpsc::channel(self.server_message_capacity);

        let (client_message_tx, client_message_rx) = mpsc::channel(self.client_message_capacity);

        // The server-message loop tells the client-packet loop which namespace
        // generations the server confirmed, so their buffered events can go
        // out. The channel is unbounded so that delivering server packets
        // never waits on the client's sending direction. Its length stays
        // below the number of namespace generations open in the session,
        // because each one travels at most once, so the server cannot grow it.
        let (connected_generation_tx, connected_generation_rx) = mpsc::unbounded_channel();

        let session = Session {
            number,
            client_message_tx,
            connected_generation_rx,
        };

        session_tx
            .send(session)
            .await
            .map_err(|_| ManagerError::Session)?;

        let (server_result, engine_result) = tokio::join!(
            crate::manager::server_message::server_messages_to_packets(
                server_message_rx,
                routes,
                connected_generation_tx,
                number,
                answered,
            ),
            (self.connect_engine)(server_message_tx, client_message_rx),
        );

        server_result?;
        engine_result?;

        Ok(())
    }
}
