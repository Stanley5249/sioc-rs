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
    /// A permit, early server message or close transition became ready.
    /// Rust wait: every ownership change wakes the caller before waiting again.
    Progress,
    /// The connection future and its server direction have both finished.
    Closed(Result<(), ManagerError>),
}

/// One engine session, like `Manager.engine`. Its fields never escape the
/// owner, so every termination uses the same polling and drain path.
pub struct Engine<Fut> {
    /// The owned opening, established or completed connection.
    connection: Connection<Fut>,
    /// `engine.write` until close stops admission.
    client_message_tx: Option<mpsc::Sender<Message>>,
    /// One immediately writable message slot. Rust bound.
    permit: Option<mpsc::OwnedPermit<Message>>,
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
            server_message_rx,
            next_server_message: None,
            drain: Drain::Deliver,
        }
    }

    /// Stops outgoing acceptance, like `engine.close`. Opening is cancellable
    /// by dropping its future, even when the handshake has no deadline.
    /// Returns whether admission stopped in this transition.
    pub fn close(&mut self, drain: Drain) -> bool {
        if matches!(drain, Drain::Discard) {
            self.drain = Drain::Discard;
        }
        let changed = self.client_message_tx.take().is_some();
        self.permit = None;
        if matches!(drain, Drain::Discard) && matches!(self.connection, Connection::Opening(_)) {
            self.connection = Connection::Finished(Ok(()));
        }
        changed
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

    /// Returns an unused slot after another input won the wait. Rust wait:
    /// packet selection owns its slot so engine cleanup cannot revoke it.
    pub fn return_permit(&mut self, permit: mpsc::OwnedPermit<Message>) {
        if self.client_message_tx.is_some() {
            assert!(
                self.permit.is_none(),
                "only one output slot is reserved for packet selection"
            );
            self.permit = Some(permit);
        }
    }

    /// Advances the session. An acquired permit stays owned by the engine.
    /// A pending reservation can restart when another input wins because this
    /// engine is the output channel's only writer and reservation waiter.
    pub async fn next(
        &mut self,
        can_receive_server_message: bool,
        can_reserve_client_message: bool,
    ) -> EngineStep {
        let reservation = self
            .client_message_tx
            .clone()
            .filter(|_| can_reserve_client_message && self.permit.is_none())
            .map(mpsc::Sender::reserve_owned);
        tokio::pin!(reservation);
        poll_fn(|cx| {
            self.poll_next(
                can_receive_server_message,
                reservation.as_mut().as_pin_mut(),
                cx,
            )
        })
        .await
    }

    fn poll_next(
        &mut self,
        can_receive_server_message: bool,
        reservation: Option<
            Pin<
                &mut impl Future<
                    Output = Result<mpsc::OwnedPermit<Message>, mpsc::error::SendError<()>>,
                >,
            >,
        >,
        cx: &mut Context<'_>,
    ) -> Poll<EngineStep> {
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
            if self.close(Drain::Deliver) {
                return Poll::Ready(EngineStep::Progress);
            }
        }
        if self.server_message_rx.is_closed() && self.close(Drain::Deliver) {
            return Poll::Ready(EngineStep::Progress);
        }

        let draining = self.client_message_tx.is_none();
        if (can_receive_server_message || draining)
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
                    if can_receive_server_message || draining {
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

        if self.client_message_tx.is_none() {
            return Poll::Pending;
        }
        let Some(reservation) = reservation else {
            return Poll::Pending;
        };
        let Poll::Ready(result) = reservation.poll(cx) else {
            return Poll::Pending;
        };
        if let Ok(permit) = result {
            self.permit = Some(permit);
        } else {
            self.connection = Connection::Finished(Err(ManagerError::EngineEnded));
            self.close(Drain::Discard);
        }
        Poll::Ready(EngineStep::Progress)
    }
}

#[cfg(test)]
mod tests {
    use tracing::Instrument;

    use super::*;

    #[tokio::test]
    async fn opening_failure_keeps_its_error_while_the_input_drains() {
        let (server_tx, server_rx) = mpsc::channel(1);
        let (client_tx, mut client_rx) = mpsc::channel(1);
        let future = async move {
            // An Engine.IO failure ends its server direction, then accepts
            // client input until the manager observes the half-close.
            drop(server_tx);
            while client_rx.recv().await.is_some() {}
            Err(eioc::error::EngineError::HandshakeTimeout.into())
        };
        let mut engine = Engine::new(
            future.instrument(tracing::Span::none()),
            client_tx,
            server_rx,
        );
        loop {
            if let EngineStep::Closed(result) = engine.next(false, false).await {
                assert!(matches!(
                    result,
                    Err(ManagerError::Engine(eioc::error::Error::Engine(
                        eioc::error::EngineError::HandshakeTimeout
                    )))
                ));
                break;
            }
        }
    }
}
