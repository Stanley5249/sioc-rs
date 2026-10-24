//! The task that owns the manager's I/O: the engine, the reconnection timer,
//! and the channels of every namespace's handles. It turns what they yield
//! into [`ManagerInput`]s and carries out the [`ManagerOutput`]s.

use std::future::{Future, poll_fn};
use std::pin::Pin;

use eioc::prelude::{Message, ServerMessage};
use tokio::sync::mpsc;
use tokio::time::Sleep;
use tracing::Instrument;
use tracing::instrument::Instrumented;

use crate::config::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
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

/// One running engine, like `Manager.engine` with the subscriptions
/// `Manager.open` adds.
///
/// The engine's future and its server direction end at different times, and
/// the actor reports the engine closed only once both have, so no old message
/// reaches the next engine.
struct Engine<Fut> {
    /// The engine itself until it finishes, like the `Engine` object.
    future: Option<Pin<Box<Instrumented<Fut>>>>,
    /// What goes to the engine, like `engine.write`. Dropping it closes the
    /// engine, like `engine.close`.
    client_message_tx: Option<mpsc::Sender<Message>>,
    /// Room for one message, taken before a handle's packet is read so the
    /// packet goes out at once. Rust bound: `engine.write` never waits.
    permit: Option<mpsc::OwnedPermit<Message>>,
    /// What the server sends, like the engine's `"data"` events.
    server_message_rx: Option<mpsc::Receiver<ServerMessage>>,
    /// One server message read while a packet is held, so the engine's
    /// hang-up still shows. Rust bound: JS emits synchronously.
    next_server_message: Option<ServerMessage>,
    /// The engine's result, like the reason of its `"close"` event, kept until
    /// the server direction ends too.
    result: Option<Result<(), ManagerError>>,
}

impl<Fut> Engine<Fut> {
    /// Drops the sender and any permit, which closes the engine, like
    /// `engine.close`.
    fn close(&mut self) {
        self.client_message_tx = None;
        self.permit = None;
    }
}

/// What one turn of the `select!` saw. Rust wait: each JS source calls its
/// own handler.
enum Step {
    /// Something for the core to handle.
    Input(ManagerInput),
    /// The client handle dropped.
    ClientClosed,
    /// The engine has room for one message, or dropped its receiver.
    Permit(Result<mpsc::OwnedPermit<Message>, mpsc::error::SendError<()>>),
    /// The next packet of a send buffer, by index, or `None` at its end.
    ClientPacket(usize, Option<ClientPacket>),
    /// The receiver of the held packet has room, or dropped.
    Deliver(Result<mpsc::OwnedPermit<ServerPacket>, mpsc::error::SendError<()>>),
    /// A server message read while a packet is held.
    NextServerMessage(ServerMessage),
    /// The engine dropped its server message sender.
    ServerEnded,
    /// The engine future finished.
    EngineFinished(Result<(), ManagerError>),
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
            self.close_hung_up_engine();

            if let Some(result) = self.take_finished_engine() {
                self.core.handle(ManagerInput::Closed(result))?;
                continue;
            }
            if self.is_finished() {
                return Ok(());
            }
            if self.send_message() {
                continue;
            }
            if let Some(message) = self.take_next_server_message() {
                self.core.handle(ManagerInput::Message(message))?;
                continue;
            }

            let step = self.next_step().await;
            if let Some(input) = self.apply(step)? {
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
                        engine.close();
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

        self.engine = Some(Engine {
            future: Some(Box::pin(future)),
            client_message_tx: Some(client_message_tx),
            permit: None,
            server_message_rx: Some(server_message_rx),
            next_server_message: None,
            result: None,
        });
    }

    /// Closes an engine that hung up, even while a held packet leaves its last
    /// messages unread, so nothing more goes into a session that only drains.
    /// An empty hung-up server direction ends at once, so a held packet never
    /// delays the engine's cleanup.
    fn close_hung_up_engine(&mut self) {
        let Some(engine) = &mut self.engine else {
            return;
        };
        let Some(server_message_rx) = &engine.server_message_rx else {
            return;
        };
        if !server_message_rx.is_closed() {
            return;
        }
        let drained = server_message_rx.is_empty() && engine.next_server_message.is_none();
        engine.close();
        if drained {
            engine.server_message_rx = None;
        }
    }

    /// Takes the result of an engine whose future and server direction both
    /// ended, like `Manager.onclose`.
    fn take_finished_engine(&mut self) -> Option<Result<(), ManagerError>> {
        let finished = self
            .engine
            .as_ref()
            .is_some_and(|engine| engine.future.is_none() && engine.server_message_rx.is_none());
        if !finished {
            return None;
        }
        let result = self
            .engine
            .take()
            .and_then(|engine| engine.result)
            .expect("a finished engine keeps its result");
        Some(result)
    }

    /// Takes the server message read early, once no packet is held.
    fn take_next_server_message(&mut self) -> Option<ServerMessage> {
        if self.core.held_sender().is_some() {
            return None;
        }
        self.engine.as_mut()?.next_server_message.take()
    }

    /// Sends one waiting message on the held permit, like `engine.write`.
    /// Returns whether it sent one.
    fn send_message(&mut self) -> bool {
        let Some(engine) = &mut self.engine else {
            return false;
        };
        if engine.permit.is_none() || !self.core.has_message() {
            return false;
        }
        let permit = engine.permit.take().expect("the check above saw a permit");
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
        let has_permit = engine
            .as_ref()
            .is_some_and(|engine| engine.permit.is_some());
        let reserve_tx = engine
            .as_ref()
            .filter(|engine| engine.permit.is_none())
            .and_then(|engine| engine.client_message_tx.clone());
        let reserves = reserve_tx.is_some();
        let stashed = engine
            .as_ref()
            .is_some_and(|engine| engine.next_server_message.is_some());
        let (server_message_rx, future) = match engine {
            Some(engine) => (engine.server_message_rx.as_mut(), engine.future.as_mut()),
            None => (None, None),
        };
        // While a packet is held and one more message waits, the server
        // direction is not read, so its hang-up shows only through this check,
        // made at each poll.
        let (server_message_rx, held_server_rx) = if holds_packet && stashed {
            (None, server_message_rx.map(|rx| &*rx))
        } else {
            (server_message_rx, None)
        };
        let reads_server = server_message_rx.is_some();
        let runs_engine = future.is_some();
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

            permit = async {
                reserve_tx.expect("the guard checked the sender").reserve_owned().await
            }, if reserves => Step::Permit(permit),

            (index, packet) = poll_fn(|cx| {
                let hung_up = held_server_rx.is_some_and(mpsc::Receiver::is_closed);
                let has_permit = has_permit && !hung_up;
                crate::manager::send_buffer::poll_send_buffers(core, send_buffers, next_send_buffer, has_permit, cx)
            }), if has_handles => Step::ClientPacket(index, packet),

            permit = async {
                held_packet_tx.expect("the guard checked the sender").reserve_owned().await
            }, if holds_packet => Step::Deliver(permit),

            message = async {
                server_message_rx.expect("the guard checked the receiver").recv().await
            }, if reads_server => match message {
                Some(message) if holds_packet => Step::NextServerMessage(message),
                Some(message) => Step::Input(ManagerInput::Message(message)),
                None => Step::ServerEnded,
            },

            result = async {
                future.expect("the guard checked the future").await
            }, if runs_engine => Step::EngineFinished(result.map_err(ManagerError::from)),

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
    ///
    /// # Errors
    ///
    /// Returns an internal error if the engine dropped its message receiver
    /// or finished while the manager still held its sender, both of which the
    /// engine rules out.
    fn apply(&mut self, step: Step) -> Result<Option<ManagerInput>, ManagerError> {
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
            Step::Permit(permit) => {
                let engine = self.engine.as_mut().expect("a permit belongs to an engine");
                engine.permit = Some(permit.map_err(|_| ManagerError::EngineEnded)?);
                return Ok(None);
            }
            Step::Deliver(permit) => {
                self.core.deliver_held(permit);
                return Ok(None);
            }
            Step::NextServerMessage(message) => {
                let engine = self
                    .engine
                    .as_mut()
                    .expect("a message belongs to an engine");
                engine.next_server_message = Some(message);
                return Ok(None);
            }
            Step::ServerEnded => {
                // The engine hung up, so it finishes once the manager drops
                // its sender.
                let engine = self
                    .engine
                    .as_mut()
                    .expect("a receiver belongs to an engine");
                engine.server_message_rx = None;
                engine.close();
                return Ok(None);
            }
            Step::EngineFinished(result) => {
                let engine = self.engine.as_mut().expect("a future belongs to an engine");
                engine.future = None;
                if engine.client_message_tx.is_some() {
                    return Err(ManagerError::EngineEnded);
                }
                engine.result = Some(result);
                return Ok(None);
            }
            Step::Exhausted => {
                assert!(
                    self.is_finished(),
                    "every source is gone only once the client and its namespaces are"
                );
                return Ok(None);
            }
        };
        Ok(Some(input))
    }

    /// Releases every resource. After an internal error, ends every namespace
    /// without waiting for the application: dropping the channels fails
    /// blocked sends. Then finishes the engine, reading what the server still
    /// sends, so it never sees a failed send.
    async fn finish(self, failed: bool) {
        let Self {
            mut core,
            connect_request_rx,
            send_buffers,
            engine,
            ..
        } = self;

        // A queued `Client::connect` fails at once.
        drop(connect_request_rx);
        drop(send_buffers);
        if failed {
            core.shutdown();
        }

        tracing::debug!("client ended");

        let Some(Engine {
            future,
            client_message_tx,
            permit,
            server_message_rx,
            ..
        }) = engine
        else {
            return;
        };
        drop((client_message_tx, permit));

        let drain = async {
            if let Some(mut server_message_rx) = server_message_rx {
                while server_message_rx.recv().await.is_some() {}
            }
        };
        let finish = async {
            match future {
                Some(future) => future.await,
                None => Ok(()),
            }
        };
        let ((), result) = tokio::join!(drain, finish);
        if let Err(error) = result {
            tracing::warn!(%error, "engine failed");
        }
    }
}
