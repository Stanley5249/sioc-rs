//! The lifetime of one engine, like `Manager.engine` and its subscriptions.
//!
//! One owner closes admission, drains the server direction and observes the
//! task's result. Namespace backpressure applies while running; cleanup
//! forwards remaining messages without waiting for application capacity.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use eioc::prelude::{ClientMessage, ServerMessage};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

use crate::error::ManagerError;

/// The engine task, until it finishes. Rust cleanup: the result waits here
/// until the server direction has ended, independently of when the task
/// itself finishes.
enum Task {
    /// The running task. Closing admission ends it, even while opening,
    /// because the core sends nothing before the handshake.
    Running(AbortOnDropHandle<Result<(), eioc::error::Error>>),
    /// The task's result.
    Finished(Result<(), ManagerError>),
}

/// What happens to the server messages left after admission closes. Rust
/// cleanup: JS has no queued engine-to-manager channel to reclaim.
#[derive(Clone, Copy)]
pub enum Drain {
    /// The engine ended first, so its final packets and namespace termination
    /// are still processed.
    Deliver,
    /// The manager closed the engine or failed, so remaining server messages
    /// are discarded.
    Discard,
}

/// Whether the engine takes client messages, like `engine.write` before
/// `engine.close`. Draining starts exactly when admission ends, so the drain
/// policy lives in the variant that ends it.
enum Admission {
    /// Accepting messages.
    Open {
        /// `engine.write`.
        client_message_tx: mpsc::Sender<ClientMessage>,
        /// One immediately writable message slot. It holds a sender, so it
        /// lives and drops with `client_message_tx`. Rust bound.
        permit: Option<mpsc::OwnedPermit<ClientMessage>>,
    },
    /// Admission ended, with the policy for unread engine messages.
    Closed(Drain),
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
    /// The task and its server direction have both finished. Take the result
    /// with [`Engine::into_result`].
    Closed,
}

/// One engine session, like `Manager.engine`. Its fields never escape the
/// owner, so every termination uses the same polling and drain path.
pub struct Engine {
    task: Task,
    admission: Admission,
    /// The engine's data events until EOF.
    server_message_rx: mpsc::Receiver<ServerMessage>,
    /// One message read while application delivery is full, so the engine's
    /// hang-up behind it stays visible. Rust bound.
    next_server_message: Option<ServerMessage>,
}

impl Engine {
    /// Takes all ends of a new session, like `Manager.open` installing its
    /// engine and listeners.
    pub fn new(engine: eioc::engine::Engine) -> Self {
        let eioc::engine::Engine {
            client_message_tx,
            server_message_rx,
            task,
        } = engine;
        Self {
            task: Task::Running(task),
            admission: Admission::Open {
                client_message_tx,
                permit: None,
            },
            server_message_rx,
            next_server_message: None,
        }
    }

    /// Stops outgoing acceptance, like `engine.close`. A discard, once asked
    /// for, stays. Returns whether admission stopped in this transition.
    pub fn close(&mut self, drain: Drain) -> bool {
        let changed = matches!(self.admission, Admission::Open { .. });
        self.admission = match (&self.admission, drain) {
            (Admission::Open { .. } | Admission::Closed(_), Drain::Discard) => {
                Admission::Closed(Drain::Discard)
            }
            (Admission::Open { .. }, Drain::Deliver) => Admission::Closed(Drain::Deliver),
            (Admission::Closed(kept), Drain::Deliver) => Admission::Closed(*kept),
        };
        changed
    }

    /// Whether one message can be handed to this engine now, like an
    /// immediately writable `engine.write`.
    pub fn has_permit(&self) -> bool {
        matches!(
            self.admission,
            Admission::Open {
                permit: Some(_),
                ..
            }
        ) && !self.server_message_rx.is_closed()
    }

    /// Takes the already reserved output slot, like `engine.write` accepting
    /// one message. The caller transfers ownership directly on that permit.
    pub fn take_permit(&mut self) -> Option<mpsc::OwnedPermit<ClientMessage>> {
        if self.server_message_rx.is_closed() {
            return None;
        }
        match &mut self.admission {
            Admission::Open { permit, .. } => permit.take(),
            Admission::Closed(_) => None,
        }
    }

    /// Returns an unused slot after another input won the wait. Rust wait:
    /// packet selection owns its slot so engine cleanup cannot revoke it.
    pub fn return_permit(&mut self, returned: mpsc::OwnedPermit<ClientMessage>) {
        if let Admission::Open { permit, .. } = &mut self.admission {
            assert!(
                permit.is_none(),
                "only one output slot is reserved for packet selection"
            );
            *permit = Some(returned);
        }
    }

    /// Returns the task's result once [`EngineStep::Closed`] was reported.
    ///
    /// # Panics
    ///
    /// If the task has not finished, which `Closed` rules out.
    pub fn into_result(self) -> Result<(), ManagerError> {
        match self.task {
            Task::Finished(result) => result,
            Task::Running(_) => unreachable!("Closed is reported only after the task finished"),
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
        let reservation = match &self.admission {
            Admission::Open {
                client_message_tx,
                permit: None,
            } if can_reserve_client_message => Some(client_message_tx.clone().reserve_owned()),
            _ => None,
        };
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
                    Output = Result<mpsc::OwnedPermit<ClientMessage>, mpsc::error::SendError<()>>,
                >,
            >,
        >,
        cx: &mut Context<'_>,
    ) -> Poll<EngineStep> {
        // Poll first so a task that closes its server direction and then
        // awaits our hang-up is observed in this same turn.
        if let Task::Running(handle) = &mut self.task
            && let Poll::Ready(joined) = Pin::new(handle).poll(cx)
        {
            // The task's own failure always wins. An engine finishes only after
            // the manager closed admission, so success before that, or a
            // cancellation by a shutting-down runtime, is a library error.
            self.task = Task::Finished(match (joined, &self.admission) {
                (Err(error), _) if error.is_panic() => {
                    std::panic::resume_unwind(error.into_panic())
                }
                (Err(_), _) | (Ok(Ok(())), Admission::Open { .. }) => {
                    Err(ManagerError::EngineEnded)
                }
                (Ok(Err(error)), _) => Err(error.into()),
                (Ok(Ok(())), Admission::Closed(_)) => Ok(()),
            });
            if self.close(Drain::Deliver) {
                return Poll::Ready(EngineStep::Progress);
            }
        }
        if self.server_message_rx.is_closed() && self.close(Drain::Deliver) {
            return Poll::Ready(EngineStep::Progress);
        }

        let draining = !matches!(self.admission, Admission::Open { .. });
        let discards = matches!(self.admission, Admission::Closed(Drain::Discard));
        if (can_receive_server_message || draining)
            && let Some(message) = self.next_server_message.take()
            && !discards
        {
            return Poll::Ready(EngineStep::Message { message, draining });
        }

        if self.next_server_message.is_none() || draining {
            match self.server_message_rx.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    if discards {
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
                    if matches!(self.task, Task::Finished(_)) {
                        return Poll::Ready(EngineStep::Closed);
                    }
                }
                Poll::Pending => {}
            }
        }

        let Admission::Open { permit, .. } = &mut self.admission else {
            return Poll::Pending;
        };
        let Some(reservation) = reservation else {
            return Poll::Pending;
        };
        let Poll::Ready(result) = reservation.poll(cx) else {
            return Poll::Pending;
        };
        if let Ok(reserved) = result {
            *permit = Some(reserved);
        } else {
            // The engine drops its receiver early only when it returns an
            // error, and its result says why.
            self.close(Drain::Deliver);
        }
        Poll::Ready(EngineStep::Progress)
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::task::AbortOnDropHandle;

    use super::*;

    /// Wraps a fake engine task that ends its server direction at once and
    /// then accepts client input until the manager hangs up, or not.
    fn engine_with(
        task: impl FnOnce(
            mpsc::Sender<ServerMessage>,
            mpsc::Receiver<ClientMessage>,
        ) -> tokio::task::JoinHandle<Result<(), eioc::error::Error>>,
    ) -> Engine {
        let (server_message_tx, server_message_rx) = mpsc::channel(1);
        let (client_message_tx, client_message_rx) = mpsc::channel(1);
        Engine::new(eioc::engine::Engine {
            client_message_tx,
            server_message_rx,
            task: AbortOnDropHandle::new(task(server_message_tx, client_message_rx)),
        })
    }

    async fn result_of(mut engine: Engine) -> Result<(), ManagerError> {
        while !matches!(engine.next(false, true).await, EngineStep::Closed) {}
        engine.into_result()
    }

    #[tokio::test]
    async fn opening_failure_keeps_its_error_while_the_input_drains() {
        let engine = engine_with(|server_message_tx, mut client_message_rx| {
            tokio::spawn(async move {
                // An Engine.IO failure ends its server direction, then accepts
                // client input until the manager observes the half-close.
                drop(server_message_tx);
                while client_message_rx.recv().await.is_some() {}
                Err(eioc::error::EngineError::HandshakeTimeout.into())
            })
        });

        assert!(matches!(
            result_of(engine).await,
            Err(ManagerError::Engine(eioc::error::Error::Engine(
                eioc::error::EngineError::HandshakeTimeout
            )))
        ));
    }

    #[tokio::test]
    async fn a_discard_after_the_server_direction_ended_keeps_the_engine_error() {
        let mut engine = engine_with(|server_message_tx, mut client_message_rx| {
            // The server direction is over before the task runs.
            drop(server_message_tx);
            tokio::spawn(async move {
                while client_message_rx.recv().await.is_some() {}
                Err(eioc::error::EngineError::HandshakeTimeout.into())
            })
        });
        // The end of the server direction closes admission first.
        assert!(matches!(
            engine.next(false, false).await,
            EngineStep::Progress
        ));

        // A discard now must not abort the opening and replace its error.
        engine.close(Drain::Discard);

        assert!(matches!(
            result_of(engine).await,
            Err(ManagerError::Engine(eioc::error::Error::Engine(
                eioc::error::EngineError::HandshakeTimeout
            )))
        ));
    }

    #[tokio::test]
    async fn an_engine_that_stops_early_keeps_its_own_error() {
        let engine = engine_with(|server_message_tx, client_message_rx| {
            tokio::spawn(async move {
                drop(server_message_tx);
                drop(client_message_rx);
                Err(eioc::error::EngineError::ServerMessageClosed.into())
            })
        });

        assert!(matches!(
            result_of(engine).await,
            Err(ManagerError::Engine(eioc::error::Error::Engine(
                eioc::error::EngineError::ServerMessageClosed
            )))
        ));
    }

    #[tokio::test]
    async fn an_engine_that_succeeds_before_the_manager_closed_it_is_a_bug() {
        let engine = engine_with(|server_message_tx, client_message_rx| {
            tokio::spawn(async move {
                // A real engine waits for the manager to hang up first.
                drop(server_message_tx);
                drop(client_message_rx);
                Ok(())
            })
        });

        assert!(matches!(
            result_of(engine).await,
            Err(ManagerError::EngineEnded)
        ));
    }
}
