//! The lifetime of one engine, like `Manager.engine` and its subscriptions.
//!
//! One owner closes admission, drains the server direction and observes the
//! connection result. Namespace backpressure applies while running; cleanup
//! forwards remaining messages without waiting for application capacity.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use eioc::prelude::{Message, ServerMessage};
use tokio::sync::mpsc;
use tracing::instrument::Instrumented;

use crate::error::ManagerError;

/// The future while opening, established, or completed. Rust cleanup: JS
/// opening and closing are synchronous, while Rust must reclaim the future.
enum Connection<Fut> {
    /// `Manager.open` before the engine's Open message.
    Opening(Pin<Box<Instrumented<Fut>>>),
    /// The established `Manager.engine` future.
    Open(Pin<Box<Instrumented<Fut>>>),
    /// Its result, retained until incoming EOF. Rust cleanup.
    Finished(Result<(), ManagerError>),
}

/// The incoming-data policy during cleanup. Rust cleanup: JS has no queued
/// engine-to-manager channel to reclaim.
#[derive(Clone, Copy)]
pub enum Drain {
    /// A peer hang-up still processes final packets and namespace termination.
    Deliver,
    /// Local close or internal failure discards remaining server messages.
    Discard,
}

/// An observation from the engine, like the listeners `Manager.open` installs.
pub enum EngineStep {
    /// One engine message, with cleanup delivery distinguished explicitly.
    Message {
        message: ServerMessage,
        draining: bool,
    },
    /// An engine permit or one early server message became ready. Rust wait.
    Progress,
    /// The connection future and its server direction have both finished.
    Closed(Result<(), ManagerError>),
}

/// A pending channel reservation. Rust wait: JS `engine.write` never awaits.
type Reservation = Pin<
    Box<dyn Future<Output = Result<mpsc::OwnedPermit<Message>, mpsc::error::SendError<()>>> + Send>,
>;

/// One engine session, like `Manager.engine`. Its fields never escape the
/// owner, so every termination uses the same polling and drain path.
pub struct Engine<Fut> {
    /// The owned opening, established or completed connection.
    connection: Connection<Fut>,
    /// `engine.write` until close stops admission.
    client_message_tx: Option<mpsc::Sender<Message>>,
    /// One immediately writable message slot. Rust bound.
    permit: Option<mpsc::OwnedPermit<Message>>,
    /// An in-progress slot reservation retained across polls. Rust wait.
    reservation: Option<Reservation>,
    /// The engine's data events until EOF.
    server_message_rx: mpsc::Receiver<ServerMessage>,
    /// One message read while application delivery is full. Rust bound.
    next_server_message: Option<ServerMessage>,
    /// How unread engine messages are reclaimed. Rust cleanup.
    drain: Drain,
}

impl<Fut> Engine<Fut>
where
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    /// Takes all ends of a new session, like `Manager.open` installing its
    /// engine and listeners.
    pub fn new(
        future: Instrumented<Fut>,
        client_message_tx: mpsc::Sender<Message>,
        server_message_rx: mpsc::Receiver<ServerMessage>,
    ) -> Self {
        Self {
            connection: Connection::Opening(Box::pin(future)),
            client_message_tx: Some(client_message_tx),
            permit: None,
            reservation: None,
            server_message_rx,
            next_server_message: None,
            drain: Drain::Deliver,
        }
    }

    /// Stops outgoing acceptance, like `engine.close`. Opening is cancellable
    /// by dropping its future, even when the handshake has no deadline.
    pub fn close(&mut self, drain: Drain) {
        if matches!(drain, Drain::Discard) {
            self.drain = Drain::Discard;
        }
        self.client_message_tx = None;
        self.permit = None;
        self.reservation = None;
        if matches!(self.connection, Connection::Opening(_)) {
            self.connection = Connection::Finished(Ok(()));
        }
    }

    /// Whether one message can be handed to this engine now, like an
    /// immediately writable `engine.write`.
    pub fn has_permit(&self) -> bool {
        self.permit.is_some() && !self.server_message_rx.is_closed()
    }

    /// Takes the already reserved output slot, like `engine.write` accepting
    /// one message. The caller transfers ownership directly on that permit.
    pub fn take_permit(&mut self) -> Option<mpsc::OwnedPermit<Message>> {
        if !self.has_permit() {
            return None;
        }
        self.permit.take()
    }

    /// Advances the session. Dropping this wait loses no message or permit,
    /// because the connection, reservation and early message remain owned.
    pub async fn next(&mut self, receive: bool) -> EngineStep {
        poll_fn(|cx| self.poll_next(receive, cx)).await
    }

    fn poll_next(&mut self, receive: bool, cx: &mut Context<'_>) -> Poll<EngineStep> {
        // Poll first so a connection that closes its server direction and
        // then awaits our hang-up is observed in this same turn.
        let result = match &mut self.connection {
            Connection::Opening(future) | Connection::Open(future) => future.as_mut().poll(cx),
            Connection::Finished(_) => Poll::Pending,
        };
        if let Poll::Ready(result) = result {
            let result = if self.client_message_tx.is_some() {
                Err(ManagerError::EngineEnded)
            } else {
                result.map_err(ManagerError::from)
            };
            self.connection = Connection::Finished(result);
            self.close(Drain::Deliver);
        }
        if self.server_message_rx.is_closed() {
            self.close(Drain::Deliver);
        }

        let draining = self.client_message_tx.is_none();
        if (receive || draining)
            && let Some(message) = self.next_server_message.take()
            && (!draining || matches!(self.drain, Drain::Deliver))
        {
            return Poll::Ready(EngineStep::Message { message, draining });
        }

        if self.next_server_message.is_none() || draining {
            match self.server_message_rx.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    if matches!(&message, ServerMessage::Open(_)) {
                        let connection =
                            std::mem::replace(&mut self.connection, Connection::Finished(Ok(())));
                        self.connection = match connection {
                            Connection::Opening(future) => Connection::Open(future),
                            connection => connection,
                        };
                    }
                    if draining && matches!(self.drain, Drain::Discard) {
                        return Poll::Ready(EngineStep::Progress);
                    }
                    if receive || draining {
                        return Poll::Ready(EngineStep::Message { message, draining });
                    }
                    self.next_server_message = Some(message);
                    return Poll::Ready(EngineStep::Progress);
                }
                Poll::Ready(None) => {
                    self.close(Drain::Deliver);
                    if matches!(self.connection, Connection::Finished(_)) {
                        let Connection::Finished(result) =
                            std::mem::replace(&mut self.connection, Connection::Finished(Ok(())))
                        else {
                            unreachable!("the connection finished before its result is taken");
                        };
                        return Poll::Ready(EngineStep::Closed(result));
                    }
                }
                Poll::Pending => {}
            }
        }

        if let Some(sender) = &self.client_message_tx
            && self.permit.is_none()
        {
            let reservation = self
                .reservation
                .get_or_insert_with(|| Box::pin(sender.clone().reserve_owned()));
            if let Poll::Ready(result) = reservation.as_mut().poll(cx) {
                self.reservation = None;
                if let Ok(permit) = result {
                    self.permit = Some(permit);
                } else {
                    self.connection = Connection::Finished(Err(ManagerError::EngineEnded));
                    self.close(Drain::Discard);
                }
                return Poll::Ready(EngineStep::Progress);
            }
        }
        Poll::Pending
    }
}
