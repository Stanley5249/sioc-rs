//! The task that owns the manager's I/O: the engine, the reconnection timer,
//! and the channels of every namespace's handles. It turns what they yield
//! into [`ManagerInput`]s and carries out the [`ManagerOutput`]s.

use std::future::{Future, poll_fn};
use std::pin::Pin;

use eioc::prelude::{Message, ServerMessage};
use tokio::sync::mpsc;
use tokio::time::Sleep;
use tracing::Instrument;

use crate::config::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::engine::{Drain, Engine, EngineStep};
use crate::manager::send_buffer::SendBuffer;
use crate::manager::state::{ManagerCore, ManagerInput, ManagerOutput};
use crate::packet::{ClientPacket, ServerPacket};

/// Runs the manager until the client handle and every namespace are gone, like
/// socket.io-client's `Manager` for as long as its sockets live.
///
/// # Errors
///
/// Returns an internal error, which is a library bug.
pub async fn run<F, Fut>(
    connect_request_rx: mpsc::Receiver<ConnectRequest>,
    connect_engine: F,
    channels: ChannelConfig,
    backoff: Option<Backoff>,
) -> Result<(), ManagerError>
where
    F: FnMut(mpsc::Sender<ServerMessage>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    let mut actor = ManagerActor {
        core: ManagerCore::new(backoff, fastrand::Rng::new()),
        connect_request_rx: Some(connect_request_rx),
        send_buffers: Vec::new(),
        next_send_buffer: 0,
        engine: None,
        sleep: None,
        connect_engine,
        channels,
    };

    let result = actor.drive().await;
    actor.finish(result.is_err()).await;
    result
}

/// The one owner of the manager's I/O. Rust channel: the JS `Manager` is
/// called directly, while here every source is a channel this task reads.
struct ManagerActor<F, Fut> {
    /// The manager's state, like the fields of `Manager`.
    core: ManagerCore,
    /// Requests of [`Client::connect`](crate::client::Client::connect), like
    /// calls to `Manager.socket`, until the client handle drops.
    connect_request_rx: Option<mpsc::Receiver<ConnectRequest>>,
    /// The channel of every active or draining namespace's handles, like each
    /// `Socket.sendBuffer`.
    send_buffers: Vec<SendBuffer>,
    /// Where the next read of `send_buffers` starts, so namespaces waiting
    /// together take turns. Rust wait: JS writes each packet at once.
    next_send_buffer: usize,
    /// The running engine, like `Manager.engine`.
    engine: Option<Engine<Fut>>,
    /// The reconnection delay, like the timer in `Manager.reconnect`.
    sleep: Option<Pin<Box<Sleep>>>,
    /// Starts one engine, like the `Engine` constructor in `Manager.open`.
    connect_engine: F,
    /// The channel capacities, like the `opts` of `Manager`.
    channels: ChannelConfig,
}

/// What one turn of the `select!` saw. Rust wait: each JS source calls its
/// own handler.
enum Step {
    /// Something for the core to handle.
    Input(ManagerInput),
    /// The client handle dropped.
    ClientClosed,
    /// The engine has room for one message, or dropped its receiver.
    Engine(EngineStep),
    /// The next packet of a send buffer, by index, or `None` at its end.
    ClientPacket(usize, Option<ClientPacket>),
    /// The receiver of the held packet has room, or dropped.
    Deliver(Result<mpsc::OwnedPermit<ServerPacket>, mpsc::error::SendError<()>>),
    /// Every source is gone. `select!` can see that one turn before `drive`.
    Exhausted,
}

impl<F, Fut> ManagerActor<F, Fut>
where
    F: FnMut(mpsc::Sender<ServerMessage>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
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

            let step = self.next_step().await;
            if let Some(input) = self.apply(step) {
                self.core.handle(input)?;
            }
        }
    }

    fn is_finished(&self) -> bool {
        self.connect_request_rx.is_none()
            && self.send_buffers.is_empty()
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
                } => self.send_buffers.push(SendBuffer {
                    ns,
                    client_packet_rx,
                    closed_wait: Box::pin(closed.clone().cancelled_owned()),
                    closed,
                }),
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

        let (server_message_tx, server_message_rx) = mpsc::channel(self.channels.manager);
        let (client_message_tx, client_message_rx) = mpsc::channel(self.channels.engine);
        let future = (self.connect_engine)(server_message_tx, client_message_rx)
            .instrument(tracing::info_span!("engine"));

        self.engine = Some(Engine::new(future, client_message_tx, server_message_rx));
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

    /// Waits for the next thing any source yields.
    ///
    /// Every branch future is cancel safe: `recv`, `poll_recv`, and
    /// `reserve_owned` lose nothing when dropped, and the engine future, the
    /// sleep, and the cancel waits are polled by reference. The branches only
    /// report what happened, because the futures borrow the actor until
    /// `select!` returns.
    async fn next_step(&mut self) -> Step {
        let Self {
            core,
            connect_request_rx,
            send_buffers,
            next_send_buffer,
            engine,
            sleep,
            ..
        } = self;

        let held_packet_tx = core.held_sender();
        let holds_packet = held_packet_tx.is_some();
        let has_permit = engine.as_ref().is_some_and(Engine::has_permit);
        let runs_engine = engine.is_some();
        let has_client = connect_request_rx.is_some();
        let has_handles = !send_buffers.is_empty();
        let waits = sleep.is_some();
        let core = &*core;

        tokio::select! {
            request = async {
                connect_request_rx.as_mut().expect("the guard checked the receiver").recv().await
            }, if has_client => match request {
                Some(request) => Step::Input(ManagerInput::Connect(request)),
                None => Step::ClientClosed,
            },

            (index, packet) = poll_fn(|cx| {
                crate::manager::send_buffer::poll_send_buffers(core, send_buffers, next_send_buffer, has_permit, cx)
            }), if has_handles => Step::ClientPacket(index, packet),

            permit = async {
                held_packet_tx.expect("the guard checked the sender").reserve_owned().await
            }, if holds_packet => Step::Deliver(permit),

            step = async {
                engine.as_mut().expect("the guard checked the engine").next(!holds_packet).await
            }, if runs_engine => Step::Engine(step),

            () = async {
                sleep.as_mut().expect("the guard checked the sleep").await;
            }, if waits => {
                *sleep = None;
                Step::Input(ManagerInput::Reconnect)
            }

            else => Step::Exhausted,
        }
    }

    /// Applies a step that needs no transition, and returns the input of one
    /// that does.
    fn apply(&mut self, step: Step) -> Option<ManagerInput> {
        let input = match step {
            Step::Input(input) => input,
            Step::ClientClosed => {
                self.connect_request_rx = None;
                ManagerInput::Release
            }
            Step::ClientPacket(index, Some(packet)) => ManagerInput::Packet {
                ns: self.send_buffers[index].ns.clone(),
                packet,
            },
            Step::ClientPacket(index, None) => {
                ManagerInput::Disconnect(self.send_buffers.remove(index).ns)
            }
            Step::Engine(EngineStep::Message { message, draining }) => {
                if draining {
                    ManagerInput::Drain(message)
                } else {
                    ManagerInput::Message(message)
                }
            }
            Step::Engine(EngineStep::Closed(result)) => {
                // The owner reports completion only after its future and
                // incoming direction ended. Dropping it reclaims the session.
                self.engine = None;
                ManagerInput::Closed(result)
            }
            Step::Engine(EngineStep::Progress) => return None,
            Step::Deliver(permit) => {
                self.core.deliver_held(permit);
                return None;
            }
            Step::Exhausted => {
                assert!(
                    self.is_finished(),
                    "every source is gone only once the client and its namespaces are"
                );
                return None;
            }
        };
        Some(input)
    }

    /// Releases every resource. After an internal error, ends every namespace
    /// without waiting for the application: dropping the channels fails
    /// blocked sends. Then finishes the engine, reading what the server still
    /// sends, so it never sees a failed send.
    async fn finish(mut self, failed: bool) {
        self.connect_request_rx = None;
        self.send_buffers.clear();
        if failed {
            self.core.shutdown();
        }
        tracing::debug!("client ended");
        if let Some(engine) = &mut self.engine {
            engine.close(Drain::Discard);
            loop {
                if let EngineStep::Closed(result) = engine.next(false).await {
                    if let Err(error) = result {
                        tracing::warn!(%error, "engine failed");
                    }
                    break;
                }
            }
        }
    }
}
