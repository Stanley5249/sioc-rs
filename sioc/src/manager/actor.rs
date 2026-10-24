//! The task that drives the manager core: it owns every channel end, the
//! engine, and the reconnection timer, and turns what they yield into inputs.
//!
//! It stays in one file past 400 lines, because `next_step` borrows every
//! field of the actor at once, which a split across modules would have to make
//! public.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytestring::ByteString;
use eioc::prelude::{Message, ServerMessage};
use futures_util::stream::{SelectAll, Stream, StreamExt};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::instrument::Instrumented;

use crate::config::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::core::input_output::{ManagerInput, ManagerOutput};
use crate::manager::core::manager_core::ManagerCore;
use crate::packet::{ClientPacket, ServerPacket};

/// Runs the manager until the client handle and every namespace are gone, like
/// socket.io-client's `Manager` for as long as its sockets live.
///
/// Opens the first engine at once, and gives each engine new channels.
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
        core: ManagerCore::new(channels, backoff, fastrand::Rng::new()),
        connect_request_rx: Some(connect_request_rx),
        inboxes: SelectAll::new(),
        engine: None,
        engines: 0,
        sleep: None,
        connect_engine,
        channels,
    };

    let result = actor.drive().await;
    actor.finish(result.is_err()).await;
    result
}

/// The one owner of the manager core and every resource around it. JS has no
/// counterpart, because the JS `Manager` runs its handlers on the event loop
/// and needs no task to own it.
struct ManagerActor<F, Fut> {
    /// The manager's state, like the fields of `Manager`.
    core: ManagerCore,
    /// The requests of [`Client::connect`](crate::client::Client::connect),
    /// like calls to `Manager.socket`, until the client handle drops.
    connect_request_rx: Option<mpsc::Receiver<ConnectRequest>>,
    /// Every namespace inbox the core handed out. JS has no counterpart,
    /// because its handles call the `Socket` directly.
    inboxes: SelectAll<InboxStream>,
    /// The running engine, like `Manager.engine`.
    engine: Option<Engine<Fut>>,
    /// How many engines opened, for the `engine` span. JS has no counterpart,
    /// because it logs with `debug` instead of spans.
    engines: u64,
    /// The reconnection delay, like the timer in `Manager.reconnect`.
    sleep: Option<Pin<Box<Sleep>>>,
    /// Starts one engine, like the `Engine` constructor in `Manager.open`.
    connect_engine: F,
    /// The channel capacities, like the `opts` of `Manager`.
    channels: ChannelConfig,
}

/// One running engine and the ends of its two channels, like
/// `Manager.engine` with the subscriptions `Manager.open` adds.
///
/// The engine's future and its server direction end at different times, and
/// the actor reports the engine closed only once both have, so no old message
/// reaches the next engine's state.
struct Engine<Fut> {
    /// The engine itself, until it finishes, like the `Engine` object.
    future: Option<Pin<Box<Instrumented<Fut>>>>,
    /// What the core sends, like `engine.write`. Dropping it closes the
    /// engine, like `engine.close`.
    client_message_tx: Option<mpsc::Sender<Message>>,
    /// What the server sends, like the engine's `"data"` events, until the
    /// engine hangs up.
    server_message_rx: Option<mpsc::Receiver<ServerMessage>>,
    /// The engine's result, like the reason of its `"close"` event, kept until
    /// the server direction ends too.
    result: Option<Result<(), ManagerError>>,
}

/// What one turn of the actor's `select!` saw. JS has no counterpart, because
/// each source calls its own handler.
enum Step {
    /// Something for the core to handle, like an event reaching a `Manager`
    /// or `Socket` handler.
    Input(ManagerInput),
    /// The client handle dropped, so no request comes any more. JS has no
    /// counterpart, because its `Manager` lives as long as it is referenced.
    ClientClosed,
    /// The engine has room for one message, or dropped its receiver. JS has no
    /// counterpart, because `engine.write` never waits.
    Send(Result<mpsc::OwnedPermit<Message>, mpsc::error::SendError<()>>),
    /// The receiver of the held packet has room, or dropped. JS has no
    /// counterpart, because it emits synchronously.
    Deliver(Result<mpsc::OwnedPermit<ServerPacket>, mpsc::error::SendError<()>>),
    /// The engine dropped its server message sender, like the end of its
    /// `"data"` events.
    ServerEnded,
    /// The engine future finished, like the engine's `"close"` event.
    EngineFinished(Result<(), ManagerError>),
    /// Every source is gone. `SelectAll` drops an ended inbox stream only when
    /// it polls it after its last item, so this turn can be the first to see
    /// the client and the inboxes all gone. JS has no counterpart, because it
    /// never waits on its sources.
    Exhausted,
}

/// The inbox of one namespace's handles, as the actor polls it. JS has no
/// counterpart, because its handles call the `Socket` directly.
struct Inbox {
    /// The namespace the inbox belongs to, like `Socket.nsp`.
    ns: ByteString,
    /// What the handles send.
    client_packet_rx: mpsc::Receiver<ClientPacket>,
    /// Cancelled once the namespace closes, by either side.
    closed: CancellationToken,
    /// The free slots of the namespace's send buffer.
    room: Arc<Semaphore>,
}

/// The packets of one inbox, then `None` at its end, after which the stream
/// ends. JS has no counterpart, because its handles call the `Socket` directly.
type InboxStream = Pin<Box<dyn Stream<Item = (ByteString, Option<ClientPacket>)> + Send>>;

impl<F, Fut> ManagerActor<F, Fut>
where
    F: FnMut(mpsc::Sender<ServerMessage>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    /// Feeds the core until the client and every namespace are gone, or the
    /// core reports an internal error. JS has no counterpart, because the
    /// event loop dispatches to each handler.
    async fn drive(&mut self) -> Result<(), ManagerError> {
        loop {
            self.apply_outputs();
            self.close_hung_up_engine();

            if let Some(result) = self.take_finished_engine() {
                self.core.handle(ManagerInput::Close(result))?;
                continue;
            }

            if self.is_finished() {
                return Ok(());
            }

            let step = self.next_step().await;
            if let Step::Exhausted = step {
                assert!(
                    self.is_finished(),
                    "every source is gone only once the client and its namespaces are"
                );
                return Ok(());
            }
            if let Some(input) = self.apply(step)? {
                self.core.handle(input)?;
            }
        }
    }

    /// Returns whether the client handle, every inbox, and the engine are gone
    /// and the core has nothing left, like a `Manager` no socket references.
    fn is_finished(&self) -> bool {
        self.connect_request_rx.is_none()
            && self.inboxes.is_empty()
            && self.core.is_idle()
            && self.engine.is_none()
    }

    /// Carries out the effects of the last call into the core, like the
    /// `Manager` acting on its own decisions.
    fn apply_outputs(&mut self) {
        while let Some(output) = self.core.next_output() {
            match output {
                ManagerOutput::Open => self.open_engine(),
                ManagerOutput::Close => {
                    if let Some(engine) = &mut self.engine {
                        engine.client_message_tx = None;
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
                    room,
                } => self.inboxes.push(inbox_stream(Inbox {
                    ns,
                    client_packet_rx,
                    closed,
                    room,
                })),
            }
        }
    }

    /// Starts an engine with new channels, like `Manager.open`.
    ///
    /// Each engine gets new channels, because the socket.io server closes the
    /// whole connection on a packet other than CONNECT for a namespace it has
    /// not joined, so nothing queued for an old engine may reach a new one.
    fn open_engine(&mut self) {
        assert!(
            self.engine.is_none(),
            "the core opens an engine only after the last one closed"
        );
        self.engines += 1;

        let (server_message_tx, server_message_rx) = mpsc::channel(self.channels.manager);
        let (client_message_tx, client_message_rx) = mpsc::channel(self.channels.engine);
        let future = (self.connect_engine)(server_message_tx, client_message_rx)
            .instrument(tracing::info_span!("engine", number = self.engines));

        self.engine = Some(Engine {
            future: Some(Box::pin(future)),
            client_message_tx: Some(client_message_tx),
            server_message_rx: Some(server_message_rx),
            result: None,
        });
    }

    /// Closes an engine that hung up, even while a held packet keeps its last
    /// server messages unread, so no buffered event goes into a session that
    /// only drains. Like `Manager.onclose` firing once the engine closes.
    ///
    /// A server direction that hung up with nothing left to read ends at once,
    /// so a held packet never delays the engine's cleanup. Messages still
    /// queued behind a held packet wait for its delivery, so none is lost.
    fn close_hung_up_engine(&mut self) {
        let Some(engine) = &mut self.engine else {
            return;
        };
        let Some(server_message_rx) = &engine.server_message_rx else {
            return;
        };
        if server_message_rx.is_closed() {
            engine.client_message_tx = None;
            if server_message_rx.is_empty() {
                engine.server_message_rx = None;
            }
        }
    }

    /// Takes the result of an engine whose future and server direction both
    /// ended, so the next engine may open, like `Manager.onclose`.
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

    /// Waits for the next thing any source yields. JS has no counterpart,
    /// because the event loop delivers each event to its handler.
    ///
    /// Every branch future is cancel safe: `recv` and `reserve_owned` lose
    /// nothing when dropped, the engine future and the sleep are polled by
    /// reference, and `SelectAll::next` keeps its streams. The branches only
    /// report what happened, because the futures borrow the engine until
    /// `select!` returns.
    async fn next_step(&mut self) -> Step {
        let Self {
            core,
            connect_request_rx,
            inboxes,
            engine,
            sleep,
            ..
        } = self;

        let held_packet_tx = core.held_sender();
        let holds_packet = held_packet_tx.is_some();
        let client_message_tx = engine
            .as_ref()
            .and_then(|engine| engine.client_message_tx.clone())
            .filter(|_| core.has_message());
        let can_send = client_message_tx.is_some();
        let (server_message_rx, future) = match engine {
            Some(engine) => (engine.server_message_rx.as_mut(), engine.future.as_mut()),
            None => (None, None),
        };
        let reads_server = server_message_rx.is_some() && !holds_packet;
        let runs_engine = future.is_some();
        let has_client = connect_request_rx.is_some();
        let waits = sleep.is_some();

        tokio::select! {
            request = async {
                connect_request_rx.as_mut().expect("the guard checked the receiver").recv().await
            }, if has_client => match request {
                Some(request) => Step::Input(ManagerInput::Connect(request)),
                None => Step::ClientClosed,
            },

            Some((ns, packet)) = inboxes.next() => Step::Input(match packet {
                Some(packet) => ManagerInput::Packet { ns, packet },
                None => ManagerInput::Disconnect(ns),
            }),

            permit = async {
                client_message_tx
                    .expect("the guard checked the sender")
                    .reserve_owned()
                    .await
            }, if can_send => Step::Send(permit),

            permit = reserve_held(held_packet_tx), if holds_packet => Step::Deliver(permit),

            message = async {
                server_message_rx.expect("the guard checked the receiver").recv().await
            }, if reads_server => match message {
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
    /// that does. JS has no counterpart, because each handler acts at once.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the engine dropped its message receiver
    /// or finished while the manager still held its sender, both of which the
    /// engine rules out.
    fn apply(&mut self, step: Step) -> Result<Option<ManagerInput>, ManagerError> {
        match step {
            Step::Input(input) => return Ok(Some(input)),
            Step::ClientClosed => {
                self.connect_request_rx = None;
                // Like the last namespace ending, if no inbox is left: the
                // engine closes once its queued packets leave.
                return Ok(Some(ManagerInput::Release));
            }
            Step::Send(permit) => {
                let message = self
                    .core
                    .next_message()
                    .expect("has_message promised a message");
                match permit {
                    Ok(permit) => {
                        permit.send(message);
                    }
                    Err(_) => return Err(mpsc::error::SendError(message).into()),
                }
            }
            Step::Deliver(permit) => self.core.deliver(permit),
            Step::ServerEnded => {
                // The engine hung up, so it finishes once the manager drops its
                // sender.
                let engine = self
                    .engine
                    .as_mut()
                    .expect("a receiver belongs to an engine");
                engine.server_message_rx = None;
                engine.client_message_tx = None;
            }
            Step::Exhausted => unreachable!("`drive` returns on the last step"),
            Step::EngineFinished(result) => {
                let engine = self.engine.as_mut().expect("a future belongs to an engine");
                engine.future = None;
                if engine.client_message_tx.is_some() {
                    return Err(ManagerError::EngineEnded);
                }
                engine.result = Some(result);
            }
        }
        Ok(None)
    }

    /// Releases every resource. JS has no counterpart, because its `Manager`
    /// never stops.
    ///
    /// After an internal error, ends every namespace without waiting for the
    /// application: dropping the inboxes fails blocked sends, and the core
    /// drops its senders and waiters. Then closes and finishes the engine
    /// directly, reading what the server still sends, so the engine never sees
    /// a failed send.
    async fn finish(self, failed: bool) {
        let Self {
            mut core,
            connect_request_rx,
            inboxes,
            engine,
            ..
        } = self;

        // A queued `Client::connect` fails at once instead of waiting for the
        // engine to close.
        drop(connect_request_rx);
        drop(inboxes);
        if failed {
            core.shutdown();
        }

        tracing::debug!("client ended");

        let Some(Engine {
            future,
            client_message_tx,
            server_message_rx,
            ..
        }) = engine
        else {
            return;
        };
        drop(client_message_tx);

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

/// Turns an inbox into a stream of its packets, then `None` at its end. JS has
/// no counterpart, because its handles call the `Socket` directly.
///
/// Takes one room permit before each packet and forgets it once a packet
/// arrives, so the core never buffers more than it has room for. Once
/// `closed` is cancelled, it closes the inbox and drains it without permits,
/// so the end never waits on the engine. A permit it holds when `closed` fires
/// goes back to the semaphore.
fn inbox_stream(inbox: Inbox) -> InboxStream {
    Box::pin(futures_util::stream::unfold(
        Some(inbox),
        |inbox| async move {
            let mut inbox = inbox?;
            let packet = recv_client_packet(&mut inbox).await;
            let ns = inbox.ns.clone();
            let next = packet.is_some().then_some(inbox);
            Some(((ns, packet), next))
        },
    ))
}

/// Receives the next packet of an inbox, or `None` at its end. JS has no
/// counterpart, because its handles call the `Socket` directly.
async fn recv_client_packet(inbox: &mut Inbox) -> Option<ClientPacket> {
    if !inbox.closed.is_cancelled() {
        let room = inbox.room.clone();
        let received = tokio::select! {
            biased;
            () = inbox.closed.cancelled() => None,
            permit = room.acquire_owned() => {
                let permit = permit.expect("the core never closes the room semaphore");
                tokio::select! {
                    biased;
                    packet = inbox.client_packet_rx.recv() => {
                        if packet.is_some() {
                            permit.forget();
                        }
                        Some(packet)
                    }
                    () = inbox.closed.cancelled() => None,
                }
            }
        };

        if let Some(packet) = received {
            return packet;
        }
    }

    inbox.client_packet_rx.close();
    inbox.client_packet_rx.recv().await
}

/// Waits for a slot on the receiver whose packet the core holds. JS has no
/// counterpart, because it emits synchronously.
async fn reserve_held(
    held_packet_tx: Option<mpsc::Sender<ServerPacket>>,
) -> Result<mpsc::OwnedPermit<ServerPacket>, mpsc::error::SendError<()>> {
    held_packet_tx
        .expect("the branch runs only while a packet is held")
        .reserve_owned()
        .await
}
