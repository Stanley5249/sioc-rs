//! The lifetime of one engine, like `Manager.engine` and its subscriptions.
//!
//! One owner closes admission, drains the server direction and observes the
//! task's result. Namespace backpressure applies while running; cleanup
//! forwards remaining messages without waiting for application capacity.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use eioc::prelude::{Message, ServerMessage};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

use crate::error::ManagerError;

type EngineTask = AbortOnDropHandle<Result<(), eioc::error::Error>>;

/// The engine task, like `Socket.readyState` while opening or open.
///
/// The manager queues CONNECT before the handshake, so an opening engine may
/// hold a full transport channel and cannot see the manager hang up. A local
/// close therefore aborts an opening, while an open engine finishes its
/// accepted messages and sends Close.
enum Task {
    /// Before the engine's Open message, like Engine.IO `"opening"`.
    Opening(EngineTask),
    /// After that message, like Engine.IO `"open"`.
    Open(EngineTask),
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

/// Whether the engine takes client messages, like `engine.write` before
/// `engine.close`.
enum Admission {
    /// Accepting messages.
    Open {
        /// `engine.write`.
        client_message_tx: mpsc::Sender<Message>,
        /// One immediately writable message slot. It holds a sender, so it
        /// lives and drops with `client_message_tx`. Rust bound.
        permit: Option<mpsc::OwnedPermit<Message>>,
    },
    /// Closed, with the policy for unread engine messages. Rust cleanup.
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
    /// The engine task and its server direction have both finished.
    Closed(Result<(), ManagerError>),
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
            task: Task::Opening(task),
            admission: Admission::Open {
                client_message_tx,
                permit: None,
            },
            server_message_rx,
            next_server_message: None,
        }
    }

    /// Stops outgoing acceptance, like `engine.close`, and aborts an opening
    /// on a discard. A discard, once asked for, stays. Returns whether
    /// admission stopped in this transition.
    pub fn close(&mut self, drain: Drain) -> bool {
        let (changed, drain) = match (&self.admission, drain) {
            (Admission::Open { .. }, drain) => (true, drain),
            (Admission::Closed(Drain::Discard), _) | (_, Drain::Discard) => (false, Drain::Discard),
            (Admission::Closed(Drain::Deliver), Drain::Deliver) => (false, Drain::Deliver),
        };
        self.admission = Admission::Closed(drain);
        if matches!(drain, Drain::Discard) && matches!(self.task, Task::Opening(_)) {
            self.task = Task::Finished(Ok(()));
        }
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
    pub fn take_permit(&mut self) -> Option<mpsc::OwnedPermit<Message>> {
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
    pub fn return_permit(&mut self, returned: mpsc::OwnedPermit<Message>) {
        if let Admission::Open { permit, .. } = &mut self.admission {
            assert!(
                permit.is_none(),
                "only one output slot is reserved for packet selection"
            );
            *permit = Some(returned);
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
                    Output = Result<mpsc::OwnedPermit<Message>, mpsc::error::SendError<()>>,
                >,
            >,
        >,
        cx: &mut Context<'_>,
    ) -> Poll<EngineStep> {
        // Poll first so a task that closes its server direction and then
        // awaits our hang-up is observed in this same turn.
        let result = match &mut self.task {
            Task::Opening(task) | Task::Open(task) => Pin::new(task).poll(cx).map(|joined| {
                joined.unwrap_or_else(|error| {
                    assert!(
                        error.is_panic(),
                        "the engine task is aborted only when its handle drops"
                    );
                    std::panic::resume_unwind(error.into_panic())
                })
            }),
            Task::Finished(_) => Poll::Pending,
        };
        if let Poll::Ready(result) = result {
            let result = if matches!(self.admission, Admission::Open { .. }) {
                Err(ManagerError::EngineEnded)
            } else {
                result.map_err(ManagerError::from)
            };
            self.task = Task::Finished(result);
            if self.close(Drain::Deliver) {
                return Poll::Ready(EngineStep::Progress);
            }
        }
        if self.server_message_rx.is_closed() && self.close(Drain::Deliver) {
            return Poll::Ready(EngineStep::Progress);
        }

        let drain = match self.admission {
            Admission::Open { .. } => None,
            Admission::Closed(drain) => Some(drain),
        };
        let draining = drain.is_some();
        let discards = matches!(drain, Some(Drain::Discard));
        if (can_receive_server_message || draining)
            && let Some(message) = self.next_server_message.take()
            && !discards
        {
            return Poll::Ready(EngineStep::Message { message, draining });
        }

        if self.next_server_message.is_none() || draining {
            match self.server_message_rx.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    if matches!(&message, ServerMessage::Open(_)) {
                        let task = std::mem::replace(&mut self.task, Task::Finished(Ok(())));
                        self.task = match task {
                            Task::Opening(task) => Task::Open(task),
                            task => task,
                        };
                    }
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
                        let Task::Finished(result) =
                            std::mem::replace(&mut self.task, Task::Finished(Ok(())))
                        else {
                            unreachable!("the task finished before its result is taken");
                        };
                        return Poll::Ready(EngineStep::Closed(result));
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
            self.task = Task::Finished(Err(ManagerError::EngineEnded));
            self.close(Drain::Discard);
        }
        Poll::Ready(EngineStep::Progress)
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::task::AbortOnDropHandle;

    use super::*;

    #[tokio::test]
    async fn opening_failure_keeps_its_error_while_the_input_drains() {
        let (server_message_tx, server_message_rx) = mpsc::channel(1);
        let (client_message_tx, mut client_message_rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            // An Engine.IO failure ends its server direction, then accepts
            // client input until the manager observes the half-close.
            drop(server_message_tx);
            while client_message_rx.recv().await.is_some() {}
            Err(eioc::error::EngineError::HandshakeTimeout.into())
        });
        let mut engine = Engine::new(eioc::engine::Engine {
            client_message_tx,
            server_message_rx,
            task: AbortOnDropHandle::new(task),
        });
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
