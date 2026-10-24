//! The manager's state and its transitions, like socket.io-client's `Manager`
//! without the engine it owns.
//!
//! [`ManagerCore`] never awaits and spawns nothing. It touches the
//! application's channels only through `try_send`, `oneshot::Sender::send`,
//! and permits its caller hands in, so plain `#[test]`s drive it.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use bytestring::ByteString;
use eioc::prelude::{Message, ServerMessage};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::decoder::{Decoded, Decoder};
use crate::manager::namespace::{Delivery, Namespace, Read};
use crate::packet::{ClientPacket, Ns, Packet, ServerPacket};

/// One transition for the core, like an event reaching a `Manager` or
/// `Socket` handler.
#[derive(Debug)]
pub enum ManagerInput {
    /// The application opens a namespace, like `Manager.socket` followed by
    /// `Socket.connect`.
    Connect(ConnectRequest),
    /// A handle's packet that [`ManagerCore::read`] asked for, like
    /// `Socket.packet`.
    Packet {
        /// The namespace the packet belongs to, like `Socket.nsp`.
        ns: ByteString,
        /// What the handle sent.
        packet: ClientPacket,
    },
    /// The handles' channel of a namespace ended, like `Socket.disconnect`.
    Disconnect(ByteString),
    /// The engine delivered a message, like `Manager.onopen` and `ondata`.
    /// The caller passes none while [`ManagerCore::held_sender`] is `Some`.
    Message(ServerMessage),
    /// Remaining engine messages during reclamation. Rust cleanup: processing
    /// final control packets must not await an application's full receiver.
    Drain(ServerMessage),
    /// The engine finished, like `Manager.onclose`.
    Closed(Result<(), ManagerError>),
    /// The reconnection delay is over, like the timer in `Manager.reconnect`.
    Reconnect,
    /// The client handle dropped, like `Manager._destroy` once no socket is
    /// left. The engine closes once no namespace needs it.
    Release,
}

/// One effect for the caller, like the `Manager` acting on the engine or a
/// timer.
#[derive(Debug)]
pub enum ManagerOutput {
    /// Start an engine with new channels, like `Manager.open`.
    Open,
    /// Close the running engine, like `engine.close`.
    Close,
    /// Wait, then pass [`ManagerInput::Reconnect`], like the `setTimeout` in
    /// `Manager.reconnect`.
    Schedule(Duration),
    /// Stop waiting, like clearing that timer in `Manager._destroy`.
    Cancel,
    /// Read the handles' channel of a namespace that became active, as
    /// [`ManagerCore::read`] says, and pass its end as
    /// [`ManagerInput::Disconnect`]. Rust channel: JS handles call the
    /// `Socket` directly.
    Listen {
        /// The namespace the channel belongs to.
        ns: ByteString,
        /// What the handles send, like `Socket.sendBuffer`.
        client_packet_rx: mpsc::Receiver<ClientPacket>,
        /// Cancelled once the handles leave or the namespace ends, which turns
        /// [`Read::Wait`] into [`Read::Discard`].
        closed: CancellationToken,
    },
}

/// Where the engine is, like `Manager._readyState` and `_reconnecting`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadyState {
    /// No engine runs, like `"closed"`.
    Closed,
    /// An engine runs, like `"opening"` and `"open"`.
    Open,
    /// An engine runs that no namespace needs. It closes once its write
    /// buffer empties, like `Manager._destroy` after the last DISCONNECT.
    Flushing,
    /// The engine was asked to close and has not finished. Rust wait: JS
    /// `engine.close` is synchronous.
    Closing {
        /// Whether the next engine waits for the backoff delay, unlike
        /// `skipReconnect`.
        reconnect: bool,
    },
    /// Waiting out the backoff delay, like `_reconnecting`.
    Reconnecting,
}

/// One message waiting for an engine permit, like an entry of the engine's
/// `writeBuffer`.
#[derive(Debug)]
enum Write {
    /// CONNECT with the auth payload, like `Socket.onopen` sending it. Kept
    /// unencoded, so a namespace that leaves first can take it back.
    Connect { ns: ByteString, auth: ByteString },
    /// Any other message, already encoded.
    Encoded(Message),
}

/// The synchronous core of the manager, like socket.io-client's `Manager`.
///
/// One caller applies one [`ManagerInput`] at a time with
/// [`handle`](Self::handle) and drains [`next_output`](Self::next_output)
/// after every call. Outgoing messages leave one at a time through
/// [`next_message`](Self::next_message), each with an engine permit.
#[derive(Debug)]
pub struct ManagerCore {
    /// Every namespace name, like `Manager.nsps`.
    namespaces: BTreeMap<ByteString, Namespace>,
    /// Where the engine is, like `Manager._readyState`.
    ready_state: ReadyState,
    /// The reconnection delays, like `Manager.backoff`. `None` when
    /// reconnection is off.
    backoff: Option<Backoff>,
    /// The jitter source, so tests can seed it, where JS calls `Math.random`.
    rng: fastrand::Rng,
    /// Rebuilds server packets, like `Manager.decoder`.
    decoder: Decoder,
    /// One server packet whose receiver is full. Rust bound: JS emits
    /// synchronously.
    held_packet: Option<Delivery>,
    /// Messages waiting for the engine, like the engine's `writeBuffer`: the
    /// CONNECT and DISCONNECT packets and the attachments of the one packet
    /// in flight. Packets of the handles wait in their channels instead.
    write_buffer: VecDeque<Write>,
    /// Effects for the caller, drained after every call. Rust wait: the JS
    /// `Manager` acts at once.
    outputs: VecDeque<ManagerOutput>,
}

impl ManagerCore {
    /// Creates a core with no namespace and asks for the first engine at once,
    /// like the `Manager` constructor calling `open` with `autoConnect`.
    #[must_use]
    pub fn new(backoff: Option<Backoff>, rng: fastrand::Rng) -> Self {
        let mut core = Self {
            namespaces: BTreeMap::new(),
            ready_state: ReadyState::Closed,
            backoff,
            rng,
            decoder: Decoder::default(),
            held_packet: None,
            write_buffer: VecDeque::new(),
            outputs: VecDeque::new(),
        };
        core.open_engine();
        core
    }

    /// Applies one transition, like a `Manager` or `Socket` handler running.
    ///
    /// # Errors
    ///
    /// Returns the engine's error if it is internal, which is a library bug.
    /// The caller then calls [`shutdown`](Self::shutdown).
    ///
    /// # Panics
    ///
    /// Panics if a server message arrives while the core holds a packet, which
    /// the caller rules out.
    pub fn handle(&mut self, input: ManagerInput) -> Result<(), ManagerError> {
        let release = matches!(input, ManagerInput::Disconnect(_) | ManagerInput::Release);
        match input {
            ManagerInput::Connect(request) => {
                let ns = request.ns.clone();
                tracing::debug!(%ns, "namespace opening");
                let namespace = self.namespaces.entry(ns.clone()).or_default();
                if namespace.is_inactive() {
                    self.decoder.disconnect(&ns);
                }
                namespace.open(request);
            }
            ManagerInput::Packet { ns, packet } => self.send(&ns, packet),
            ManagerInput::Disconnect(ns) => self.disconnect(&ns),
            ManagerInput::Message(message) => {
                assert!(
                    self.held_packet.is_none(),
                    "the caller waits for the held delivery before reading more data"
                );
                self.message(message);
            }
            ManagerInput::Drain(message) => self.message(message),
            ManagerInput::Closed(result) => {
                // After an internal error the caller shuts down, so no engine
                // opens.
                self.finish_engine(result)?;
            }
            ManagerInput::Reconnect => self.reconnect(),
            ManagerInput::Release => {}
        }

        self.settle();
        if release {
            self.release();
        }
        Ok(())
    }

    /// Takes the next effect, or `None` once the call has none left. Rust
    /// wait: the JS `Manager` acts at once.
    pub fn next_output(&mut self) -> Option<ManagerOutput> {
        self.outputs.pop_front().or_else(|| {
            self.namespaces.iter_mut().find_map(|(ns, namespace)| {
                let (client_packet_rx, closed) = namespace.take_listen()?;
                Some(ManagerOutput::Listen {
                    ns: ns.clone(),
                    client_packet_rx,
                    closed,
                })
            })
        })
    }

    /// Returns what the caller does with the next packet of `ns`, given
    /// whether it holds an engine permit, like `Socket.emitBuffered` running
    /// once connected. A packet goes out only when nothing waits before it,
    /// so the write buffer holds the messages of one packet at most.
    #[must_use]
    pub fn read(&self, ns: &str, has_permit: bool) -> Read {
        let engine_ready = has_permit && self.engine_runs() && self.write_buffer.is_empty();
        self.namespaces
            .get(ns)
            .map_or(Read::Discard, |namespace| namespace.read(engine_ready))
    }

    /// Returns whether a message waits for an engine permit, like a nonempty
    /// `writeBuffer`.
    #[must_use]
    pub fn has_message(&self) -> bool {
        self.engine_runs() && !self.write_buffer.is_empty()
    }

    /// Takes one message for the engine, like `engine.write`.
    pub fn next_message(&mut self) -> Option<Message> {
        if !self.engine_runs() {
            return None;
        }
        let message = match self.write_buffer.pop_front()? {
            Write::Connect { ns, auth } => encode(&ns, &Packet::Connect(auth)),
            Write::Encoded(message) => message,
        };
        self.finish_flushing();
        Some(message)
    }

    /// Returns a sender to reserve a slot on while the core holds a server
    /// packet for a full receiver. Rust bound: JS emits synchronously.
    #[must_use]
    pub fn held_sender(&self) -> Option<mpsc::Sender<ServerPacket>> {
        self.held_packet
            .as_ref()
            .map(|delivery| delivery.sender.clone())
    }

    /// Sends the held packet through the reserved slot, or discards it if the
    /// receiver dropped or the namespace ended first.
    pub fn deliver_held(
        &mut self,
        permit: Result<mpsc::OwnedPermit<ServerPacket>, mpsc::error::SendError<()>>,
    ) {
        let Some(Delivery {
            ns, packet, closed, ..
        }) = self.held_packet.take()
        else {
            return;
        };
        if let Ok(permit) = permit
            && !closed.is_cancelled()
        {
            permit.send(packet);
        } else {
            tracing::debug!(%ns, "discarded server packet for a dropped receiver");
        }
    }

    /// Ends every namespace without waiting for the application, after an
    /// internal error. The caller then closes the engine itself. Rust cleanup:
    /// the JS `Manager` never stops.
    pub fn shutdown(&mut self) {
        for namespace in self.namespaces.values_mut() {
            namespace.shutdown();
        }
        self.held_packet = None;
        self.write_buffer.clear();
        self.decoder.reset();
        self.ready_state = ReadyState::Closed;
    }

    /// Returns whether every namespace is inactive and no engine runs. Rust
    /// cleanup: the JS `Manager` never stops.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.ready_state == ReadyState::Closed
            && self.namespaces.values().all(Namespace::is_inactive)
    }

    fn engine_runs(&self) -> bool {
        matches!(self.ready_state, ReadyState::Open | ReadyState::Flushing)
    }

    fn needs_engine(&self) -> bool {
        self.namespaces.values().any(Namespace::is_active)
    }

    /// Encodes a packet the caller read, like `Socket.packet` calling
    /// `Manager._packet`. The header leaves on the permit the caller holds;
    /// attachments follow before any other message.
    fn send(&mut self, ns: &ByteString, packet: ClientPacket) {
        let engine_runs = self.engine_runs();
        let Some(namespace) = self
            .namespaces
            .get_mut(ns)
            .filter(|namespace| namespace.is_connected() && engine_runs)
        else {
            // The namespace or its handles are gone, so nothing sends it.
            tracing::debug!(%ns, "discarded client packet for a closed namespace");
            return;
        };

        let (packet, attachments) = match packet {
            ClientPacket::Event {
                payload,
                ack_tx,
                attachments,
            } => {
                let id = ack_tx.map(|ack_tx| namespace.register_ack(ack_tx));
                let packet = match &attachments {
                    None => Packet::Event { payload, id },
                    Some(attachments) => Packet::BinaryEvent {
                        payload,
                        id,
                        count: attachments.len(),
                    },
                };
                (packet, attachments)
            }
            ClientPacket::Ack {
                payload,
                id,
                attachments,
            } => {
                let packet = match &attachments {
                    None => Packet::Ack { payload, id },
                    Some(attachments) => Packet::BinaryAck {
                        payload,
                        id,
                        count: attachments.len(),
                    },
                };
                (packet, attachments)
            }
        };

        tracing::trace!(%ns, %packet, "sent packet");
        self.write_buffer
            .push_back(Write::Encoded(encode(ns, &packet)));
        let attachments = attachments.into_iter().flatten();
        self.write_buffer
            .extend(attachments.map(|attachment| Write::Encoded(Message::Binary(attachment))));
    }

    fn disconnect(&mut self, ns: &ByteString) {
        tracing::debug!(%ns, "client closed");
        self.decoder.disconnect(ns);
        if self
            .held_packet
            .as_ref()
            .is_some_and(|delivery| delivery.ns == ns)
        {
            self.held_packet = None;
        }
        let namespace = self
            .namespaces
            .get_mut(ns)
            .expect("a channel only exists for a namespace with an entry");

        if namespace.disconnect() {
            self.write_buffer
                .push_back(Write::Encoded(encode(ns, &Packet::Disconnect)));
        } else {
            // The server never heard of the namespace, so its CONNECT stays
            // home.
            self.write_buffer.retain(
                |write| !matches!(write, Write::Connect { ns: queued, .. } if queued == ns),
            );
        }
    }

    /// Runs after every transition: drops a held packet whose namespace ended,
    /// gives an engine to namespaces that need one, and queues CONNECT for
    /// namespaces that became active, like `Socket.connect`.
    fn settle(&mut self) {
        if let Some(delivery) = &self.held_packet
            && delivery.closed.is_cancelled()
        {
            tracing::debug!(ns = %delivery.ns, "discarded held server packet for a closed namespace");
            self.held_packet = None;
        }

        // Reopening is a core transition after old delivery cleanup, never a
        // side effect of a namespace cleanup helper.
        for (ns, namespace) in &mut self.namespaces {
            if let Some(request) = namespace.take_reopen() {
                self.decoder.disconnect(ns);
                namespace.open(request);
            }
        }

        if self.needs_engine() {
            match self.ready_state {
                ReadyState::Closed => {
                    self.reset_backoff();
                    self.open_engine();
                }
                ReadyState::Flushing => self.ready_state = ReadyState::Open,
                ReadyState::Open | ReadyState::Closing { .. } | ReadyState::Reconnecting => {}
            }
        }

        let engine_runs = self.engine_runs();
        for (ns, namespace) in &mut self.namespaces {
            if let Some(auth) = namespace.take_connect()
                && engine_runs
            {
                self.write_buffer.push_back(Write::Connect {
                    ns: ns.clone(),
                    auth,
                });
            }
        }
    }

    /// Lets the engine go once no namespace needs it, like `Manager._destroy`:
    /// a pending wait stops, and a running engine closes after its write
    /// buffer empties.
    fn release(&mut self) {
        if self.needs_engine() {
            return;
        }
        match self.ready_state {
            ReadyState::Open => {
                self.ready_state = ReadyState::Flushing;
                self.finish_flushing();
            }
            ReadyState::Reconnecting => {
                tracing::debug!("stopped reconnecting");
                self.ready_state = ReadyState::Closed;
                self.outputs.push_back(ManagerOutput::Cancel);
            }
            ReadyState::Closed | ReadyState::Flushing | ReadyState::Closing { .. } => {}
        }
    }

    fn finish_flushing(&mut self) {
        if self.ready_state == ReadyState::Flushing && self.write_buffer.is_empty() {
            self.close_engine(false);
        }
    }

    fn close_engine(&mut self, reconnect: bool) {
        tracing::debug!(reconnect, "engine closing");
        self.ready_state = ReadyState::Closing { reconnect };
        self.outputs.push_back(ManagerOutput::Close);
    }

    /// Asks for an engine, with CONNECT queued for every active namespace so
    /// they go out first, like `Manager.open` followed by `Socket.onopen`.
    fn open_engine(&mut self) {
        tracing::debug!("engine opening");
        self.ready_state = ReadyState::Open;
        self.outputs.push_back(ManagerOutput::Open);

        for (ns, namespace) in &mut self.namespaces {
            namespace.take_connect();
            if let Some(auth) = namespace.auth() {
                self.write_buffer.push_back(Write::Connect {
                    ns: ns.clone(),
                    auth: auth.clone(),
                });
            }
        }
    }

    fn reset_backoff(&mut self) {
        if let Some(backoff) = &mut self.backoff {
            backoff.reset();
        }
    }

    fn message(&mut self, message: ServerMessage) {
        if !self.engine_runs() {
            tracing::debug!("discarded server message for a closing engine");
            return;
        }

        let decoded = match message {
            ServerMessage::Open(handshake) => {
                tracing::debug!(sid = %handshake.sid, "engine opened");
                // The handshake succeeded, so the next drop counts attempts
                // from zero, like `Manager.onreconnect`.
                self.reset_backoff();
                return;
            }
            ServerMessage::Text(text) => self.decoder.add_text(text),
            ServerMessage::Binary(bytes) => self.decoder.add_binary(bytes),
        };

        match decoded {
            Ok(Some(Ns(ns, decoded))) => self.receive(&ns, decoded),
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

    /// Routes one decoded server packet, like `Socket.onpacket`.
    fn receive(&mut self, ns: &ByteString, decoded: Decoded) {
        let Some(namespace) = self.namespaces.get_mut(ns) else {
            tracing::debug!(%ns, "discarded server packet for an unknown namespace");
            return;
        };

        let packet = match decoded {
            Decoded::Ack { id, ack } => return namespace.resolve_ack(ns, id, ack),
            Decoded::Packet(packet) => packet,
        };

        match &packet {
            ServerPacket::Connect(connect) => {
                tracing::debug!(%ns, sid = %connect.sid, "connected");
                namespace.confirm();
            }
            ServerPacket::Disconnect | ServerPacket::ConnectError(_) => {
                tracing::debug!(%ns, %packet, "namespace ended by the server");
                // The server refused the namespace or left it, so both end it.
                namespace.end(ns, Some(packet));
                return;
            }
            ServerPacket::Event(_) => {}
        }

        if let Some(delivery) = namespace.deliver(ns, packet) {
            if self.held_packet.is_none() {
                tracing::debug!(%ns, "holding server packet for a full receiver");
                self.held_packet = Some(delivery);
            } else {
                // Only Drain inputs can reach a full receiver while another
                // packet is held. Keep that original delivery, and reclaim
                // later overflow without waiting for the application.
                tracing::debug!(%ns, "discarded overflow during engine cleanup");
            }
        }
    }

    /// Handles the end of the engine and picks the next state, like
    /// `Manager.onclose` and `reconnect`.
    fn finish_engine(&mut self, result: Result<(), ManagerError>) -> Result<(), ManagerError> {
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
        self.write_buffer.clear();
        for namespace in self.namespaces.values_mut() {
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
        if !reconnect || !self.needs_engine() {
            return Ok(());
        }

        if let Some(delay) = self.next_delay() {
            self.ready_state = ReadyState::Reconnecting;
            self.outputs.push_back(ManagerOutput::Schedule(delay));
        } else {
            // The namespaces end but the client stays, like the JS `Manager`
            // and `Socket`s outliving `reconnect_failed`.
            for (ns, namespace) in &mut self.namespaces {
                namespace.end(ns, None);
            }
        }
        Ok(())
    }

    /// Counts an attempt and returns its delay, or `None` if reconnection is
    /// off or gave up, like `Manager.reconnect` checking `backoff.attempts`.
    fn next_delay(&mut self) -> Option<Duration> {
        let Some(backoff) = &mut self.backoff else {
            tracing::warn!("reconnection is off");
            return None;
        };
        let delay = backoff.next_delay(&mut self.rng);
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
        // `release` cancels the wait once no namespace needs an engine.
        debug_assert!(self.needs_engine(), "a pending wait has a namespace");
        self.open_engine();
    }
}

/// Encodes a packet for namespace `ns` as its text message.
fn encode(ns: &str, packet: &Packet) -> Message {
    Message::Text(packet.encode(ns).into())
}
