//! The task that owns the manager's I/O: the engine, the reconnection timer,
//! and the channels of every namespace's handles. It turns what they yield
//! into [`ManagerInput`]s and carries out the [`ManagerOutput`]s.

use std::future::poll_fn;
use std::pin::Pin;

use tokio::sync::mpsc;
use tokio::time::Sleep;

use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::engine::{Drain, Engine, EngineStep};
use crate::manager::namespace_inputs::NamespaceInputs;
use crate::manager::state::{ManagerCore, ManagerInput, ManagerOutput};

/// Runs the manager until the client handle and every namespace are gone, like
/// socket.io-client's `Manager` for as long as its sockets live.
///
/// # Errors
///
/// Returns an internal error, which is a library bug.
pub async fn run<F>(
    connect_request_rx: mpsc::Receiver<ConnectRequest>,
    connect_engine: F,
    backoff: Option<Backoff>,
) -> Result<(), ManagerError>
where
    F: FnMut() -> eioc::engine::Engine,
{
    let mut actor = ManagerActor {
        core: ManagerCore::new(backoff, fastrand::Rng::new()),
        connect_request_rx: Some(connect_request_rx),
        namespace_inputs: Vec::new(),
        next_namespace_input: 0,
        engine: None,
        sleep: None,
        connect_engine,
    };

    let result = actor.drive().await;
    actor.shutdown(result.is_err()).await;
    result
}

/// The one owner of the manager's I/O. Rust channel: the JS `Manager` is
/// called directly, while here every source is a channel this task reads.
struct ManagerActor<F> {
    /// The manager's state, like the fields of `Manager`.
    core: ManagerCore,
    /// Requests of [`Client::connect`](crate::client::Client::connect), like
    /// calls to `Manager.socket`, until the client handle drops.
    connect_request_rx: Option<mpsc::Receiver<ConnectRequest>>,
    /// Event and ACK inputs of every active or draining namespace. Rust
    /// channels: JS `Socket.emit` and `Socket.ack` call the manager directly.
    namespace_inputs: Vec<NamespaceInputs>,
    /// Where the next read of `namespace_inputs` starts, so namespaces waiting
    /// together take turns. Rust wait: JS writes each packet at once.
    next_namespace_input: usize,
    /// The running engine, like `Manager.engine`.
    engine: Option<Engine>,
    /// The reconnection delay, like the timer in `Manager.reconnect`.
    sleep: Option<Pin<Box<Sleep>>>,
    /// Starts one engine with new channels, like the `Engine` constructor in
    /// `Manager.open`.
    connect_engine: F,
}

impl<F> ManagerActor<F>
where
    F: FnMut() -> eioc::engine::Engine,
{
    /// Feeds the core until the client and every namespace are gone, or the
    /// core reports an internal error.
    async fn drive(&mut self) -> Result<(), ManagerError> {
        loop {
            self.apply_outputs();
            if self.is_finished() {
                return Ok(());
            }
            if self.send_message() {
                continue;
            }

            if let Some(input) = self.turn().await? {
                self.core.handle(input)?;
            }
        }
    }

    fn is_finished(&self) -> bool {
        self.connect_request_rx.is_none()
            && self.namespace_inputs.is_empty()
            && self.engine.is_none()
            && self.core.is_idle()
    }

    /// Carries out the effects of the last call into the core.
    fn apply_outputs(&mut self) {
        while let Some(output) = self.core.next_output() {
            match output {
                ManagerOutput::Open => self.open_engine(),
                ManagerOutput::Close => {
                    if let Some(engine) = &mut self.engine {
                        engine.close(Drain::Discard);
                    }
                }
                ManagerOutput::Schedule(delay) => {
                    self.sleep = Some(Box::pin(tokio::time::sleep(delay)));
                }
                ManagerOutput::Cancel => self.sleep = None,
                ManagerOutput::Listen {
                    ns,
                    client_packet_rx,
                    closed,
                } => self.namespace_inputs.push(NamespaceInputs {
                    ns,
                    client_packet_rx,
                    ack_packet_rx: None,
                    ack_first: false,
                    closed_wait: Box::pin(closed.clone().cancelled_owned()),
                    closed,
                }),
                ManagerOutput::ListenAcks { ns, ack_packet_rx } => {
                    let buffer = self
                        .namespace_inputs
                        .iter_mut()
                        .find(|buffer| buffer.ns == ns)
                        .expect("the event inbox is installed before its reply input");
                    buffer.ack_packet_rx = Some(ack_packet_rx);
                }
            }
        }
    }

    /// Starts an engine with new channels, like `Manager.open`, so nothing
    /// queued for an old engine reaches a new one.
    fn open_engine(&mut self) {
        assert!(
            self.engine.is_none(),
            "the core opens an engine only after the last one closed"
        );

        self.engine = Some(Engine::new((self.connect_engine)()));
    }

    /// Sends one waiting message on the held permit, like `engine.write`.
    /// Returns whether it sent one.
    fn send_message(&mut self) -> bool {
        let Some(engine) = &mut self.engine else {
            return false;
        };
        if !engine.has_permit() || !self.core.has_message() {
            return false;
        }
        let permit = engine.take_permit().expect("the check above saw a permit");
        let message = self.core.next_message().expect("has_message promised one");
        permit.send(message);
        true
    }

    /// Waits for one input and handles its I/O synchronously. Each branch
    /// future is cancel safe: channels retain unread packets, the engine owns
    /// its future and acquired slot, and the reconnection deadline is pinned.
    /// Packet selection owns its output permit until the header is handed off.
    async fn turn(&mut self) -> Result<Option<ManagerInput>, ManagerError> {
        let Self {
            core,
            connect_request_rx,
            namespace_inputs,
            next_namespace_input,
            engine,
            sleep,
            ..
        } = self;
        let held_packet_tx = core.held_sender();
        let can_receive_server_message = held_packet_tx.is_none();
        let mut client_message_permit = engine.as_mut().and_then(Engine::take_permit);
        let can_reserve_client_message = client_message_permit.is_none();
        let has_engine = engine.is_some();
        let has_client = connect_request_rx.is_some();
        let has_namespace_inputs = !namespace_inputs.is_empty();
        let waits = sleep.is_some();

        let input = tokio::select! {
            request = async {
                connect_request_rx.as_mut().expect("the guard checked the receiver").recv().await
            }, if has_client => {
                if let Some(request) = request {
                    Some(ManagerInput::Connect(request))
                } else {
                    *connect_request_rx = None;
                    Some(ManagerInput::Release)
                }
            },

            (index, packet) = poll_fn(|cx| {
                crate::manager::namespace_inputs::poll_namespace_inputs(
                    core, namespace_inputs, next_namespace_input, client_message_permit.is_some(), cx,
                )
            }), if has_namespace_inputs => match packet {
                Some(packet) => {
                    let ns = namespace_inputs[index].ns.clone();
                    core.handle(ManagerInput::Packet { ns, packet })?;
                    if client_message_permit.is_some()
                        && let Some(message) = core.next_message()
                    {
                        let permit = client_message_permit.take()
                            .expect("an output message is read only with an owned permit");
                        permit.send(message);
                    }
                    None
                }
                None => Some(ManagerInput::Disconnect(namespace_inputs.remove(index).ns)),
            },

            permit = async {
                held_packet_tx.expect("the guard checked the sender").reserve_owned().await
            }, if !can_receive_server_message => {
                core.deliver_held(permit);
                None
            },

            step = async {
                engine.as_mut()
                    .expect("the guard checked the engine")
                    .next(can_receive_server_message, can_reserve_client_message).await
            }, if has_engine => match step {
                EngineStep::Message { message, draining } => Some(if draining {
                    ManagerInput::Drain(message)
                } else {
                    ManagerInput::Message(message)
                }),
                EngineStep::Closed => {
                    // Complete session reclamation before the core can open
                    // its successor. Persistent event inboxes stay owned here.
                    client_message_permit = None;
                    let result = engine.take().expect("the guard checked the engine").into_result();
                    for buffer in namespace_inputs {
                        buffer.ack_packet_rx = None;
                    }
                    Some(ManagerInput::Closed(result))
                }
                EngineStep::Progress => None,
            },

            () = async {
                sleep.as_mut().expect("the guard checked the sleep").await;
            }, if waits => {
                *sleep = None;
                Some(ManagerInput::Reconnect)
            },

            else => unreachable!("drive stops before every input source is gone"),
        };
        if let Some(permit) = client_message_permit {
            engine
                .as_mut()
                .expect("a reserved slot belongs to the current engine")
                .return_permit(permit);
        }
        Ok(input)
    }

    /// Releases every resource. After an internal error, ends every namespace
    /// without waiting for the application: dropping the channels fails
    /// blocked sends. Then closes the engine and waits for its task, reading
    /// what the server still sends, so it never sees a failed send.
    async fn shutdown(mut self, failed: bool) {
        self.connect_request_rx = None;
        self.namespace_inputs.clear();
        if failed {
            self.core.shutdown();
        }
        tracing::debug!("client ended");
        if let Some(mut engine) = self.engine.take() {
            engine.close(Drain::Discard);
            while !matches!(engine.next(false, false).await, EngineStep::Closed) {}
            if let Err(error) = engine.into_result() {
                tracing::warn!(%error, "engine failed");
            }
        }
    }
}
