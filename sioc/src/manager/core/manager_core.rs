//! The manager's state and its transitions.
//!
//! It stays in one file past 400 lines, because every transition reads and
//! writes the same private fields, which a split across modules would have to
//! make public.

use std::collections::VecDeque;

use bytes::Bytes;
use eioc::prelude::{Message, ServerMessage};
use tokio::sync::mpsc;

use crate::config::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::core::decoder::Decoder;
use crate::manager::core::input_output::{ManagerInput, ManagerOutput};
use crate::manager::core::namespace::Namespace;
use crate::manager::core::namespaces::Namespaces;
use crate::packet::{Ns, ServerPacket};

/// The most outputs one call leaves in the queue. `Listen` is not queued,
/// because [`ManagerCore::next_output`] derives it from the namespaces.
const MAX_QUEUED_OUTPUTS: usize = 2;

/// Where the engine is, like socket.io-client's `Manager._readyState` and
/// `_reconnecting` in one value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadyState {
    /// No engine runs, like `"closed"`.
    Closed,
    /// An engine runs, like `"opening"` and `"open"`.
    Open,
    /// An engine runs that no namespace needs any more. It closes once the
    /// queued DISCONNECT leaves. JS has no counterpart, because it writes
    /// DISCONNECT at once.
    Flushing,
    /// The engine was asked to close and has not finished. JS has no
    /// counterpart, because `engine.close` is synchronous.
    Closing {
        /// Whether the next engine waits for the backoff delay. The client
        /// closes the engine without it, like `skipReconnect`.
        reconnect: bool,
    },
    /// Waiting out the backoff delay before the next engine, like
    /// `_reconnecting`.
    Reconnecting,
}

/// The synchronous core of the manager, like socket.io-client's `Manager`.
///
/// One caller applies one [`ManagerInput`] at a time with
/// [`handle`](Self::handle) and carries out the effects from
/// [`next_output`](Self::next_output). It drains `next_output` after every
/// `handle`, [`next_message`](Self::next_message), and
/// [`shutdown`](Self::shutdown), so the queue holds at most
/// [`MAX_QUEUED_OUTPUTS`] items.
#[derive(Debug)]
pub struct ManagerCore {
    /// Every namespace name, like `Manager.nsps`.
    namespaces: Namespaces,
    /// Where the engine is, like `Manager._readyState`.
    ready_state: ReadyState,
    /// The reconnection delays, like `Manager.backoff`. `None` when
    /// reconnection is off.
    backoff: Option<Backoff>,
    /// The jitter source of `backoff`, injected so tests can seed it. JS has
    /// no counterpart, because it calls `Math.random`.
    rng: fastrand::Rng,
    /// Rebuilds server packets, like `Manager.decoder`.
    decoder: Decoder,
    /// One server packet whose receiver is full. JS has no counterpart,
    /// because it emits synchronously.
    held_packet: Option<Ns<ServerPacket>>,
    /// The attachments of the binary packet whose header just left. They go
    /// out before any other message. JS has no counterpart, because
    /// `engine.write` takes a packet whole.
    attachments: VecDeque<Bytes>,
    /// Effects for the caller. JS has no counterpart, because it acts at once.
    outputs: VecDeque<ManagerOutput>,
}

impl ManagerCore {
    /// Creates a core with no namespace and asks for the first engine at once,
    /// like the `Manager` constructor calling `open` with `autoConnect`.
    ///
    /// Takes `rng` for the reconnection jitter, so tests can seed it.
    #[must_use]
    pub fn new(channels: ChannelConfig, backoff: Option<Backoff>, rng: fastrand::Rng) -> Self {
        let mut core = Self {
            namespaces: Namespaces::new(channels.manager),
            ready_state: ReadyState::Closed,
            backoff,
            rng,
            decoder: Decoder::default(),
            held_packet: None,
            attachments: VecDeque::new(),
            outputs: VecDeque::new(),
        };
        core.open_engine();
        core
    }

    /// Applies one transition. JS has no counterpart, because each input is
    /// its own event handler there.
    ///
    /// # Errors
    ///
    /// Returns the engine's error if it is internal, which is a library bug.
    /// The caller then calls [`shutdown`](Self::shutdown).
    ///
    /// # Panics
    ///
    /// Panics if a server message arrives while the core holds a packet, which
    /// the caller rules out by reading no engine message until
    /// [`deliver`](Self::deliver) ran.
    pub fn handle(&mut self, input: ManagerInput) -> Result<(), ManagerError> {
        let mut result = Ok(());

        match input {
            ManagerInput::Connect(request) => {
                let ns = request.ns.clone();
                tracing::debug!(%ns, "namespace opening");
                self.namespaces.entry(&ns).open(request);
            }
            ManagerInput::Packet { ns, packet } => self.namespace(&ns).accept(&ns, packet),
            ManagerInput::Disconnect(ns) => {
                tracing::debug!(%ns, "client closed");
                self.namespace(&ns).disconnect(&ns);
                self.release();
            }
            ManagerInput::Message(message) => self.message(message),
            ManagerInput::Close(engine_result) => result = self.close(engine_result),
            ManagerInput::Reconnect => self.reconnect(),
        }

        // After an internal error the caller shuts down, so no engine opens.
        if result.is_ok() {
            self.settle();
        }
        result
    }

    /// Takes the next effect, or `None` once the call has none left. JS has no
    /// counterpart, because it acts at once.
    pub fn next_output(&mut self) -> Option<ManagerOutput> {
        self.outputs
            .pop_front()
            .or_else(|| self.namespaces.take_listen())
    }

    /// Returns whether [`next_message`](Self::next_message) has a message for
    /// the engine, so the caller reserves an engine permit first. JS has no
    /// counterpart, because `engine.write` never waits.
    #[must_use]
    pub fn has_message(&self) -> bool {
        self.engine_runs() && (!self.attachments.is_empty() || self.namespaces.has_message())
    }

    /// Takes one message for the engine, like `Manager._packet` calling
    /// `engine.write`.
    ///
    /// CONNECT and DISCONNECT go first, then confirmed namespaces take turns
    /// with one message each. The attachments of a binary packet follow its
    /// header before any other message.
    pub fn next_message(&mut self) -> Option<Message> {
        if !self.engine_runs() {
            return None;
        }

        let needed = self.namespaces.needs_engine();
        let message = if let Some(attachment) = self.attachments.pop_front() {
            Message::Binary(attachment)
        } else {
            let (header, attachments) = self.namespaces.pop()?;
            self.attachments.extend(attachments);
            header
        };

        // A disconnecting namespace sent its last packet, which ends the last
        // need for the engine, so the engine closes after the DISCONNECT.
        if needed && !self.namespaces.needs_engine() {
            self.release();
        }
        self.finish_flushing();
        self.settle();

        Some(message)
    }

    /// Returns a sender to reserve a slot on when the core holds a server
    /// packet for a full receiver. The caller reads no engine message until it
    /// passes the reservation to [`deliver`](Self::deliver). JS has no
    /// counterpart, because it emits synchronously.
    #[must_use]
    pub fn held_sender(&self) -> Option<mpsc::Sender<ServerPacket>> {
        let Ns(ns, _) = self.held_packet.as_ref()?;
        self.namespaces.get(ns)?.server_packet_tx()
    }

    /// Sends the held packet through the reserved slot. JS has no counterpart,
    /// because it emits synchronously.
    ///
    /// Discards the packet if the reservation failed because the receiver
    /// dropped, or if the namespace ended first and no packet is held.
    pub fn deliver(
        &mut self,
        permit: Result<mpsc::OwnedPermit<ServerPacket>, mpsc::error::SendError<()>>,
    ) {
        let Some(Ns(ns, packet)) = self.held_packet.take() else {
            return;
        };

        match permit {
            Ok(permit) => {
                permit.send(packet);
            }
            Err(_) => {
                tracing::debug!(%ns, "discarded server packet for a dropped receiver");
            }
        }
    }

    /// Ends every namespace without waiting for the application, closes the
    /// engine, and cancels a pending wait, for when the caller hit an internal
    /// error. JS has no counterpart, because its `Manager` never stops.
    ///
    /// Dropping the senders ends the receivers, and dropping the buffers fails
    /// the waiters. The caller drops the inboxes it polls.
    pub fn shutdown(&mut self) {
        for (_, namespace) in self.namespaces.iter_mut() {
            namespace.shutdown();
        }
        self.held_packet = None;
        self.decoder.reset();
        self.attachments.clear();

        match self.ready_state {
            ReadyState::Open | ReadyState::Flushing => self.close_engine(false),
            ReadyState::Reconnecting => self.cancel_wait(),
            ReadyState::Closed | ReadyState::Closing { .. } => {}
        }
    }

    /// Returns whether every namespace is inactive and no engine runs, so the
    /// caller may finish. JS has no counterpart, because its `Manager` never
    /// stops.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.ready_state == ReadyState::Closed && self.namespaces.is_inactive()
    }

    fn namespace(&mut self, ns: &str) -> &mut Namespace {
        self.namespaces
            .get_mut(ns)
            .expect("an inbox only exists for a namespace with an entry")
    }

    fn engine_runs(&self) -> bool {
        matches!(self.ready_state, ReadyState::Open | ReadyState::Flushing)
    }

    /// Runs after every transition: drops a held packet whose namespace ended,
    /// and gives a namespace that became active an engine, like
    /// `Socket.connect` calling `Manager.open`. Its CONNECT is already queued.
    fn settle(&mut self) {
        if let Some(Ns(ns, _)) = &self.held_packet
            && !self.namespaces.get(ns).is_some_and(Namespace::is_active)
        {
            tracing::debug!(%ns, "discarded held server packet for a closed namespace");
            self.held_packet = None;
        }

        if self.namespaces.needs_engine() {
            match self.ready_state {
                ReadyState::Closed => {
                    self.reset_backoff();
                    self.open_engine();
                }
                ReadyState::Flushing => self.ready_state = ReadyState::Open,
                ReadyState::Open | ReadyState::Closing { .. } | ReadyState::Reconnecting => {}
            }
        }

        debug_assert!(
            self.outputs.len() <= MAX_QUEUED_OUTPUTS,
            "a call queues at most two outputs"
        );
    }

    /// Lets the engine go once the client ends its last namespace, like
    /// `Manager._destroy`. A pending wait is cancelled, and a running engine
    /// closes after its queued packets leave.
    fn release(&mut self) {
        if self.namespaces.needs_engine() {
            return;
        }

        match self.ready_state {
            ReadyState::Open => {
                self.ready_state = ReadyState::Flushing;
                self.finish_flushing();
            }
            ReadyState::Reconnecting => self.cancel_wait(),
            ReadyState::Closed | ReadyState::Flushing | ReadyState::Closing { .. } => {}
        }
    }

    /// Closes a flushing engine once nothing waits for it, like the
    /// `engine.close` that `Manager._destroy` calls after the last DISCONNECT.
    fn finish_flushing(&mut self) {
        if self.ready_state == ReadyState::Flushing
            && self.attachments.is_empty()
            && !self.namespaces.has_message()
        {
            self.close_engine(false);
        }
    }

    /// Asks the caller to close the engine, like `engine.close`.
    fn close_engine(&mut self, reconnect: bool) {
        tracing::debug!(reconnect, "engine closing");
        self.ready_state = ReadyState::Closing { reconnect };
        self.outputs.push_back(ManagerOutput::Close);
    }

    /// Asks the caller to stop waiting, like clearing the reconnection timer.
    fn cancel_wait(&mut self) {
        tracing::debug!("stopped reconnecting");
        self.ready_state = ReadyState::Closed;
        self.outputs.push_back(ManagerOutput::Cancel);
    }

    /// Asks the caller to open an engine, with CONNECT queued for every active
    /// namespace so they go out first, like `Manager.open` followed by
    /// `Socket.onopen`.
    fn open_engine(&mut self) {
        tracing::debug!("engine opening");

        self.ready_state = ReadyState::Open;
        self.outputs.push_back(ManagerOutput::Open);

        for (_, namespace) in self.namespaces.iter_mut() {
            namespace.queue_connect();
        }
    }

    /// Starts counting attempts from zero again, like `Manager.onreconnect`.
    fn reset_backoff(&mut self) {
        if let Some(backoff) = &mut self.backoff {
            backoff.reset();
        }
    }

    fn message(&mut self, message: ServerMessage) {
        assert!(
            self.held_packet.is_none(),
            "the caller reads no engine message while a server packet is held"
        );

        if !self.engine_runs() {
            tracing::debug!("discarded server message for a closing engine");
            return;
        }

        let decoded = match message {
            ServerMessage::Open(handshake) => {
                tracing::debug!(sid = %handshake.sid, "engine opened");

                // The handshake succeeded, so the next drop starts counting
                // attempts again, like `Manager.onreconnect`.
                self.reset_backoff();
                return;
            }
            ServerMessage::Text(text) => self.decoder.add_text(text),
            ServerMessage::Binary(bytes) => self.decoder.add_binary(bytes),
        };

        match decoded {
            Ok(Some(Ns(ns, decoded))) => {
                if let Some(packet) = self.namespaces.receive(&ns, decoded) {
                    tracing::debug!(%ns, "holding server packet for a full receiver");
                    self.held_packet = Some(Ns(ns, packet));
                }
            }
            Ok(None) => {}
            Err(error) => {
                // The server's fault, not a library bug, so the client
                // reconnects.
                tracing::warn!(%error, "server broke the protocol");

                self.decoder.reset();
                self.close_engine(true);
            }
        }
    }

    /// Handles the end of the engine and picks the next state, like
    /// `Manager.onclose` and `reconnect`.
    fn close(&mut self, result: Result<(), ManagerError>) -> Result<(), ManagerError> {
        let reconnect = match self.ready_state {
            ReadyState::Open | ReadyState::Flushing => true,
            ReadyState::Closing { reconnect } => reconnect,
            ReadyState::Closed | ReadyState::Reconnecting => {
                tracing::debug!("ignored close of an engine that is gone");
                return Ok(());
            }
        };

        tracing::debug!("engine closed");

        self.ready_state = ReadyState::Closed;
        self.decoder.reset();
        self.attachments.clear();

        // Like `Socket.onclose`, which calls `_clearAcks`.
        for (_, namespace) in self.namespaces.iter_mut() {
            namespace.engine_closed();
        }

        match result {
            Err(error) if error.is_internal() => return Err(error),
            Err(error) => tracing::warn!(%error, "engine failed"),
            Ok(()) => {}
        }

        // Without reconnect, `settle` opens the next engine at once for a
        // namespace that opened meanwhile, like `Socket.connect` calling
        // `Manager.open`. Without a namespace, wait for the next one, like
        // `Manager._destroy`.
        if !reconnect || !self.namespaces.needs_engine() {
            return Ok(());
        }

        if let Some(delay) = self.next_delay() {
            self.ready_state = ReadyState::Reconnecting;
            self.outputs.push_back(ManagerOutput::Schedule(delay));
        } else {
            // Closes every namespace but keeps the client, like
            // socket.io-client, whose `Manager` and `Socket`s outlive
            // `reconnect_failed`.
            for (ns, namespace) in self.namespaces.iter_mut() {
                namespace.end(ns, None);
            }
        }

        Ok(())
    }

    /// Counts an attempt and returns its delay, or `None` if reconnection is
    /// off or gave up, like `Manager.reconnect` checking `backoff.attempts`.
    fn next_delay(&mut self) -> Option<std::time::Duration> {
        let Some(backoff) = &mut self.backoff else {
            tracing::warn!("reconnection is off");
            return None;
        };

        let delay = backoff.next_delay_with(&mut self.rng);
        if let Some(delay) = delay {
            tracing::info!(attempt = backoff.attempts(), ?delay, "reconnecting");
        } else {
            tracing::warn!(attempts = backoff.attempts(), "gave up reconnecting");
        }
        delay
    }

    fn reconnect(&mut self) {
        if self.ready_state != ReadyState::Reconnecting {
            tracing::debug!("ignored reconnect with no wait pending");
            return;
        }

        if self.namespaces.needs_engine() {
            self.open_engine();
        } else {
            self.ready_state = ReadyState::Closed;
        }
    }
}
