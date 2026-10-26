//! The lifetime of one engine, like `Manager.engine` and its subscriptions.
//!
//! One owner closes admission, drains the server direction and observes the
//! task's result. Namespace backpressure applies while running; cleanup
//! forwards remaining messages without waiting for application capacity.
//!
//! The session's end arrives on the engine's `closed_rx`, which ends ahead of
//! the messages still queued, so a full application receiver never hides it.
//! From then on every remaining message is read as a drain, and the end of the
//! server direction, not `closed_rx`, finishes the session.

use eioc::prelude::{ClientMessage, ServerMessage};
use tokio::sync::{mpsc, watch};
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

impl Admission {
    /// Ends admission. A discard, once asked for, stays. Returns whether
    /// admission stopped in this transition.
    fn close(&mut self, drain: Drain) -> bool {
        let changed = matches!(self, Self::Open { .. });
        *self = match (&*self, drain) {
            (Self::Open { .. } | Self::Closed(_), Drain::Discard) => Self::Closed(Drain::Discard),
            (Self::Open { .. }, Drain::Deliver) => Self::Closed(Drain::Deliver),
            (Self::Closed(kept), Drain::Deliver) => Self::Closed(*kept),
        };
        changed
    }
}

/// An observation from the engine, like the listeners `Manager.open` installs.
pub enum EngineStep {
    /// One engine message, with cleanup delivery distinguished explicitly.
    Message {
        message: ServerMessage,
        draining: bool,
    },
    /// A permit, the task's result, or the session's end became ready.
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
    /// Ends when the session closes, like the `"close"` event.
    closed_rx: watch::Receiver<()>,
}

impl Engine {
    /// Takes all ends of a new session, like `Manager.open` installing its
    /// engine and listeners.
    pub fn new(engine: eioc::engine::Engine) -> Self {
        let eioc::engine::Engine {
            client_message_tx,
            server_message_rx,
            closed_rx,
            task,
        } = engine;
        Self {
            task: Task::Running(task),
            admission: Admission::Open {
                client_message_tx,
                permit: None,
            },
            server_message_rx,
            closed_rx,
        }
    }

    /// Stops outgoing acceptance, like `engine.close`. A discard, once asked
    /// for, stays. Returns whether admission stopped in this transition.
    pub fn close(&mut self, drain: Drain) -> bool {
        self.admission.close(drain)
    }

    /// Whether the session is over, so nothing more is handed to it.
    fn has_ended(&self) -> bool {
        self.closed_rx.has_changed().is_err()
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
        ) && !self.has_ended()
    }

    /// Takes the already reserved output slot, like `engine.write` accepting
    /// one message. The caller transfers ownership directly on that permit.
    pub fn take_permit(&mut self) -> Option<mpsc::OwnedPermit<ClientMessage>> {
        if self.has_ended() {
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

    /// Advances the session by one observation.
    ///
    /// Every branch is cancel safe: the task handle and the receivers are
    /// polled by reference, and a pending reservation restarts on the next
    /// call, because this engine is the output channel's only writer and
    /// reservation waiter. An acquired permit stays owned by the engine.
    pub async fn next(
        &mut self,
        can_receive_server_message: bool,
        can_reserve_client_message: bool,
    ) -> EngineStep {
        let ended = self.has_ended();
        let Self {
            task,
            admission,
            server_message_rx,
            closed_rx,
        } = self;
        // The engine dropped its sender, so it holds nothing for us and our
        // hang-up loses nothing. The messages left are still read.
        if server_message_rx.is_closed() {
            admission.close(Drain::Deliver);
        }
        // An ended channel keeps returning `None`, so it is read only while it
        // still has messages.
        let server_direction_ended = server_message_rx.is_closed() && server_message_rx.is_empty();
        if server_direction_ended && matches!(task, Task::Finished(_)) {
            return EngineStep::Closed;
        }
        let draining = ended || matches!(admission, Admission::Closed(_));
        let discards = matches!(admission, Admission::Closed(Drain::Discard));
        let receives = (can_receive_server_message || draining) && !server_direction_ended;
        let reserving_tx = match admission {
            Admission::Open {
                client_message_tx,
                permit: None,
            } if can_reserve_client_message && !ended => Some(client_message_tx.clone()),
            _ => None,
        };
        let wants_permit = reserving_tx.is_some();
        let handle = match task {
            Task::Running(handle) => Some(handle),
            Task::Finished(_) => None,
        };
        let runs = handle.is_some();

        tokio::select! {
            // The task first, so a task that ended its server direction and
            // awaits our hang-up is observed in the same turn.
            biased;

            joined = async { handle.expect("the guard checked the task").await }, if runs => {
                // The task's own failure always wins. An engine finishes only
                // after the manager closed admission, so success before that,
                // or a cancellation by a shutting-down runtime, is a library
                // error.
                *task = Task::Finished(match (joined, &*admission) {
                    (Err(error), _) if error.is_panic() => {
                        std::panic::resume_unwind(error.into_panic())
                    }
                    (Err(_), _) | (Ok(Ok(())), Admission::Open { .. }) => {
                        Err(ManagerError::EngineEnded)
                    }
                    (Ok(Err(error)), _) => Err(error.into()),
                    (Ok(Ok(())), Admission::Closed(_)) => Ok(()),
                });
                admission.close(Drain::Deliver);
                EngineStep::Progress
            },

            // Only the wake matters: from now on `ended` turns every read into
            // a drain that needs no application capacity.
            _ = closed_rx.changed(), if !ended => EngineStep::Progress,

            message = server_message_rx.recv(), if receives => match message {
                Some(_) if discards => EngineStep::Progress,
                Some(message) => EngineStep::Message { message, draining },
                None => {
                    admission.close(Drain::Deliver);
                    EngineStep::Progress
                }
            },

            reserved = async {
                reserving_tx.expect("the guard checked the sender").reserve_owned().await
            }, if wants_permit => {
                if let Admission::Open { permit, .. } = admission
                    && let Ok(reserved) = reserved
                {
                    *permit = Some(reserved);
                } else {
                    // The engine drops its receiver early only when it
                    // returns an error, and its result says why.
                    admission.close(Drain::Deliver);
                }
                EngineStep::Progress
            },

            else => unreachable!("a finished task with an ended server direction returns above"),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;
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
        // The session is over from the start, like the server direction.
        let (_, closed_rx) = watch::channel(());
        Engine::new(eioc::engine::Engine {
            client_message_tx,
            server_message_rx,
            closed_rx,
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

    /// After the session's end was observed, its closed watch channel is no
    /// longer a branch, so a session whose server direction is still open
    /// waits instead of reporting progress again and again.
    #[tokio::test]
    async fn an_observed_session_end_is_not_reported_again() {
        let (server_message_tx, server_message_rx) = mpsc::channel(1);
        let (client_message_tx, _client_message_rx) = mpsc::channel(1);
        let (closed_tx, closed_rx) = watch::channel(());
        let mut engine = Engine::new(eioc::engine::Engine {
            client_message_tx,
            server_message_rx,
            closed_rx,
            task: AbortOnDropHandle::new(tokio::spawn(std::future::pending())),
        });
        {
            let mut waiting = std::pin::pin!(engine.next(false, false));
            assert!(waiting.as_mut().now_or_never().is_none());
            drop(closed_tx);
            assert!(matches!(waiting.now_or_never(), Some(EngineStep::Progress)));
        }
        assert!(engine.next(false, false).now_or_never().is_none());
        assert!(!engine.has_permit());
        drop(server_message_tx);
    }
}
