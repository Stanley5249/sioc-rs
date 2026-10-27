//! The manager's state and its transitions, like socket.io-client's `Manager`
//! without the engine it owns.
//!
//! [`ManagerCore`] never awaits and spawns nothing. The socket handles call
//! its transitions directly under the manager's lock, and the driver calls
//! the rest. It touches the application's channels only through `try_send`,
//! `oneshot::Sender::send`, semaphores, and permits its caller hands in, so
//! plain `#[test]`s drive it.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytestring::ByteString;
use eioc::prelude::{ClientMessage, ServerMessage};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::error::{ManagerError, SocketError};
use crate::manager::backoff::Backoff;
use crate::manager::decoder::{Decoded, Decoder};
use crate::manager::socket::{Auth, Availability, Blocked, Handles, Queued, Socket, SocketState};
use crate::packet::{ClientPacket, ServerPacket};
use crate::parser::packet::{Ns, Packet};

/// One effect for the driver, like the `Manager` acting on the engine or a
/// timer.
#[derive(Debug)]
pub enum ManagerOutput {
    /// Start an engine with new channels, like `Manager.open`.
    Open,
    /// Close the running engine, like `engine.close`.
    Close,
    /// Wait, then call [`ManagerCore::reconnect`], like the `setTimeout` in
    /// `Manager.reconnect`.
    Schedule(Duration),
    /// Stop waiting, like clearing that timer in `Manager._destroy`.
    Cancel,
}

/// Where the engine is, like `Manager._readyState` and `_reconnecting`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadyState {
    /// No engine runs, like `"closed"`.
    Closed,
    /// An engine runs before its handshake, like `"opening"`. Messages wait
    /// in the write buffer, like `Socket.connect` waiting for the manager's
    /// `"open"` event, so nothing reaches the engine before Open.
    Opening,
    /// An engine runs after its handshake, like `"open"`.
    Open,
    /// An engine runs that no socket needs. It closes once its write buffer
    /// empties, like `Manager._destroy` after the last DISCONNECT.
    Flushing,
    /// The engine was asked to close and its resources are still owned. Rust
    /// wait: JS `engine.close()` returns immediately; the driver waits for the
    /// engine task and channels to finish before opening a successor.
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
    /// unencoded, so a socket that disconnects first can take it back.
    Connect { ns: ByteString, auth: Auth },
    /// An event or ACK reply of a socket, like `Socket.packet`. Encoded when
    /// the engine takes it, so its ack id is assigned in send order.
    Packet {
        ns: ByteString,
        packet: ClientPacket,
        slot: OwnedSemaphorePermit,
    },
    /// The current participation's final packet, still cancellable.
    Disconnect { ns: ByteString },
    /// An attachment whose header was already submitted. Namespace termination
    /// cannot cancel it without breaking Engine stream framing.
    Encoded(ClientMessage),
}

/// The synchronous core of the manager, like socket.io-client's `Manager`.
#[derive(Debug)]
pub struct ManagerCore {
    /// Every socket, like `Manager.nsps`.
    sockets: BTreeMap<ByteString, Socket>,
    /// Where the engine is, like `Manager._readyState`.
    ready_state: ReadyState,
    /// The reconnection delays, like `Manager.backoff`. `None` when
    /// reconnection is off.
    backoff: Option<Backoff>,
    /// The jitter source, so tests can seed it, where JS calls `Math.random`.
    rng: fastrand::Rng,
    /// Rebuilds server packets, like `Manager.decoder`.
    decoder: Decoder,
    /// Messages waiting for the engine, like the engine's `writeBuffer`. Each
    /// packet holds a capacity slot of its socket, so the buffer is bounded
    /// by the sockets' capacities and their CONNECT and DISCONNECT packets.
    write_buffer: VecDeque<Write>,
    /// Effects for the driver to carry out. The JS `Manager` acts at once
    /// instead.
    outputs: VecDeque<ManagerOutput>,
    /// Whether the `Client` handle lives, which gives the driver a completion
    /// point for `Client::join`.
    has_client: bool,
    /// Set once the driver shut down, so no handle connects again.
    shut_down: bool,
}

impl ManagerCore {
    /// Creates a core with no socket and asks for the first engine at once,
    /// like the `Manager` constructor calling `open` with `autoConnect`.
    #[must_use]
    pub fn new(backoff: Option<Backoff>, rng: fastrand::Rng) -> Self {
        let mut core = Self {
            sockets: BTreeMap::new(),
            ready_state: ReadyState::Closed,
            backoff,
            rng,
            decoder: Decoder::default(),
            write_buffer: VecDeque::new(),
            outputs: VecDeque::new(),
            has_client: true,
            shut_down: false,
        };
        core.open_engine();
        core
    }

    /// Takes new handles for `ns`, like `Manager.socket` followed by
    /// `Socket.connect`. Returns the previous participation's release token
    /// instead while it still flushes, so the caller waits and retries
    /// without leaving anything in the core.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError::NamespaceConflict`] while live handles hold the
    /// name, or [`SocketError::Closed`] after the driver shut down.
    pub fn connect(
        &mut self,
        ns: ByteString,
        handles: Handles,
    ) -> Result<Option<CancellationToken>, SocketError> {
        if self.shut_down {
            return Err(SocketError::Closed);
        }
        let socket = self.sockets.entry(ns.clone()).or_default();
        match socket.availability() {
            Availability::Free => {}
            Availability::Busy(release) => return Ok(Some(release)),
            Availability::Taken => return Err(SocketError::NamespaceConflict { ns }),
        }
        tracing::debug!(%ns, "namespace opening");
        // A partial binary packet of the old handles never reaches new ones.
        self.decoder.disconnect(&ns);
        let auth = handles.auth.clone();
        socket.connect(handles);
        if self.engine_runs() {
            self.write_buffer.push_back(Write::Connect { ns, auth });
        }
        self.settle();
        Ok(None)
    }

    /// Accepts an event from the handles that own `slot`, like `Socket.emit`:
    /// it waits in the send buffer until the server confirms the socket.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError::Closed`] if those handles have ended.
    pub fn emit(
        &mut self,
        ns: &ByteString,
        packet: ClientPacket,
        slot: OwnedSemaphorePermit,
    ) -> Result<(), SocketError> {
        let Some(socket) = self.sockets.get_mut(ns) else {
            return Err(SocketError::Closed);
        };
        match &mut socket.state {
            SocketState::Connecting {
                handles,
                send_buffer,
            } if handles.owns(&slot) => send_buffer.push_back((packet, slot)),
            SocketState::Connected { handles, .. } if handles.owns(&slot) => {
                self.write_buffer.push_back(Write::Packet {
                    ns: ns.clone(),
                    packet,
                    slot,
                });
            }
            _ => return Err(SocketError::Closed),
        }
        Ok(())
    }

    /// Returns the token of the confirmed connection of `ns`, which delivered
    /// events bind their ack ids to.
    #[cfg(test)]
    #[must_use]
    pub fn connection(&self, ns: &str) -> Option<CancellationToken> {
        match &self.sockets.get(ns)?.state {
            SocketState::Connected { connection, .. } => Some(connection.clone()),
            _ => None,
        }
    }

    /// Accepts an ACK reply, like the function `Socket.ack` returns, if the
    /// connection that delivered its event is still current and belongs to
    /// the handles that own `slot`.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError::Closed`] if that connection has ended.
    pub fn acknowledge(
        &mut self,
        ns: &ByteString,
        packet: ClientPacket,
        slot: OwnedSemaphorePermit,
    ) -> Result<(), SocketError> {
        let ClientPacket::Ack { id, .. } = &packet else {
            unreachable!("only replies are acknowledged");
        };
        // A namespace has at most one uncancelled connection token, so a live
        // token of `ns` is its current connection.
        let delivered = id
            .connection(ns)
            .is_some_and(|connection| !connection.is_cancelled());
        let current = self.sockets.get(ns).is_some_and(|socket| {
            matches!(&socket.state, SocketState::Connected { handles, .. } if handles.owns(&slot))
        });
        if !delivered || !current {
            tracing::debug!(%ns, id = id.get(), "rejected ack for an ended connection");
            return Err(SocketError::Closed);
        }
        self.write_buffer.push_back(Write::Packet {
            ns: ns.clone(),
            packet,
            slot,
        });
        Ok(())
    }

    /// Disconnects the handles that own `capacity`, like `Socket.disconnect`.
    /// A confirmed socket sends its accepted packets, then DISCONNECT. An
    /// unconfirmed one discards them and takes back its CONNECT. Repeated
    /// calls and calls from ended handles do nothing.
    pub fn disconnect(&mut self, ns: &ByteString, capacity: &Arc<Semaphore>) {
        let Some(socket) = self.sockets.get_mut(ns) else {
            return;
        };
        if !socket.handles().is_some_and(|handles| {
            Arc::ptr_eq(&handles.capacity, capacity) && !handles.closed.is_cancelled()
        }) {
            return;
        }
        tracing::debug!(%ns, "client closed");
        self.decoder.disconnect(ns);
        if socket.disconnect(ns) {
            self.write_buffer
                .push_back(Write::Disconnect { ns: ns.clone() });
        } else {
            // The server never heard of the socket, so its CONNECT stays home.
            self.write_buffer.retain(
                |write| !matches!(write, Write::Connect { ns: queued, .. } if queued == ns),
            );
        }
        self.release();
    }

    /// The client handle dropped, like `Manager._destroy` once no socket is
    /// left. The engine closes once no socket needs it.
    pub fn drop_client(&mut self) {
        self.has_client = false;
        self.release();
    }

    /// Handles an engine message, like `Manager.onopen` and `ondata`. Returns
    /// a server packet whose receiver is full, for the driver to deliver.
    pub fn receive(&mut self, message: ServerMessage) -> Option<Blocked> {
        if !self.engine_runs() {
            tracing::debug!("discarded server message for a closing engine");
            return None;
        }

        let decoded = match message {
            ServerMessage::Open(handshake) => {
                tracing::debug!(sid = %handshake.sid, "engine opened");
                if self.ready_state == ReadyState::Opening {
                    self.ready_state = ReadyState::Open;
                }
                // The handshake succeeded, so the next drop counts attempts
                // from zero, like `Manager.onreconnect`.
                self.reset_backoff();
                return None;
            }
            ServerMessage::Text(text) => self.decoder.add_text(text),
            ServerMessage::Binary(bytes) => self.decoder.add_binary(bytes),
        };

        match decoded {
            Ok(Some(Ns(ns, decoded))) => self.route(&ns, decoded),
            Ok(None) => None,
            Err(error) => {
                // The server's fault, not a library bug, so the client
                // reconnects.
                tracing::warn!(%error, "server broke the protocol");
                self.decoder.reset();
                self.close_engine(true);
                None
            }
        }
    }

    /// Handles the end of the engine and picks the next state, like
    /// `Manager.onclose` and `reconnect`.
    ///
    /// # Errors
    ///
    /// Returns the engine's error if it is internal, which is a library bug.
    /// The driver then stops, and its shutdown guard calls
    /// [`shutdown`](Self::shutdown).
    pub fn close(&mut self, result: Result<(), ManagerError>) -> Result<(), ManagerError> {
        let reconnect = match self.ready_state {
            ReadyState::Opening | ReadyState::Open | ReadyState::Flushing => true,
            ReadyState::Closing { reconnect } => reconnect,
            ReadyState::Closed | ReadyState::Reconnecting => {
                tracing::debug!("ignored close of an engine that is gone");
                return Ok(());
            }
        };

        tracing::debug!("engine closed");
        self.ready_state = ReadyState::Closed;
        self.decoder.reset();
        self.return_unsent();

        match result {
            Err(error) if error.is_internal() => return Err(error),
            Err(error) => tracing::warn!(%error, "engine failed"),
            Ok(()) => {}
        }

        if reconnect && self.needs_engine() {
            if let Some(delay) = self.next_delay() {
                self.ready_state = ReadyState::Reconnecting;
                self.outputs.push_back(ManagerOutput::Schedule(delay));
            } else {
                // The sockets end but the client stays, like the JS `Manager`
                // and `Socket`s outliving `reconnect_failed`.
                for (ns, socket) in &mut self.sockets {
                    socket.end(ns, None);
                }
            }
        }
        // Without reconnect, a socket that connected meanwhile opens the next
        // engine at once, like `Socket.connect` calling `Manager.open`.
        self.settle();
        Ok(())
    }

    /// The reconnection delay is over, like the timer in `Manager.reconnect`.
    pub fn reconnect(&mut self) {
        if self.ready_state != ReadyState::Reconnecting {
            tracing::debug!("ignored reconnect with no wait pending");
            return;
        }
        // `release` cancels the wait once no socket needs an engine.
        debug_assert!(self.needs_engine(), "a pending wait has a socket");
        self.open_engine();
    }

    /// Takes the next effect, or `None` once there is none left.
    pub fn next_output(&mut self) -> Option<ManagerOutput> {
        self.outputs.pop_front()
    }

    /// Returns whether a message waits for an engine permit, like a nonempty
    /// `writeBuffer`.
    #[must_use]
    pub fn has_message(&self) -> bool {
        self.engine_writes() && !self.write_buffer.is_empty()
    }

    /// Takes one message for the engine, like `engine.write`. The caller holds
    /// the permit it goes out on. An event registers its ack waiter now, so an
    /// engine's end fails exactly the waiters of events it took, and a
    /// binary packet's attachments go next, before anything else.
    pub fn next_message(&mut self) -> Option<ClientMessage> {
        if !self.engine_writes() {
            return None;
        }
        let message = match self.write_buffer.pop_front()? {
            Write::Connect { ns, auth } => encode(&ns, &Packet::Connect(auth.0)),
            Write::Encoded(message) => message,
            Write::Disconnect { ns } => {
                let socket = self.sockets.get_mut(&ns).expect("DISCONNECT has an owner");
                socket.end(&ns, None);
                encode(&ns, &Packet::Disconnect)
            }
            Write::Packet { ns, packet, slot } => {
                let socket = self
                    .sockets
                    .get_mut(&ns)
                    .expect("a queued packet belongs to a socket");
                assert!(
                    socket.handles().is_some_and(|handles| handles.owns(&slot)),
                    "queued packets keep their accepting participation until submission"
                );
                let (packet, attachments) = wire_packet(socket, packet);
                tracing::trace!(%ns, %packet, "sent packet");
                for attachment in attachments.into_iter().flatten().rev() {
                    self.write_buffer
                        .push_front(Write::Encoded(ClientMessage::Binary(attachment)));
                }
                // The engine owns the packet now, so its handles may queue
                // another one.
                drop(slot);
                encode(&ns, &packet)
            }
        };
        self.release();
        self.finish_flushing();
        Some(message)
    }

    /// Ends every socket without waiting for the application, once the
    /// driver stops for any reason, including an internal error.
    pub fn shutdown(&mut self) {
        for (ns, socket) in &mut self.sockets {
            socket.end(ns, None);
        }
        self.write_buffer.clear();
        self.decoder.reset();
        self.ready_state = ReadyState::Closed;
        self.shut_down = true;
    }

    /// Returns whether the client handle and every socket are gone and no
    /// engine runs, which is the driver's completion point for `Client::join`.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        !self.has_client && self.ready_state == ReadyState::Closed && !self.needs_engine()
    }

    fn engine_runs(&self) -> bool {
        matches!(
            self.ready_state,
            ReadyState::Opening | ReadyState::Open | ReadyState::Flushing
        )
    }

    /// Whether the engine has opened, so the write buffer may leave.
    fn engine_writes(&self) -> bool {
        matches!(self.ready_state, ReadyState::Open | ReadyState::Flushing)
    }

    fn needs_engine(&self) -> bool {
        self.sockets.values().any(Socket::needs_engine)
    }

    /// Routes one decoded server packet, like `Socket.onpacket`.
    fn route(&mut self, ns: &ByteString, decoded: Decoded) -> Option<Blocked> {
        let Some(socket) = self.sockets.get_mut(ns) else {
            tracing::debug!(%ns, "discarded server packet for an unknown namespace");
            return None;
        };

        let packet = match decoded {
            Decoded::Ack { id, ack } => {
                socket.receive_ack(ns, id, ack);
                return None;
            }
            Decoded::Packet(packet) => packet,
        };

        match &packet {
            ServerPacket::Connect(connect) => {
                tracing::debug!(%ns, sid = %connect.sid, "connected");
                let send_buffer = socket.receive_connect();
                self.write_buffer
                    .extend(send_buffer.into_iter().map(|(packet, slot)| Write::Packet {
                        ns: ns.clone(),
                        packet,
                        slot,
                    }));
            }
            ServerPacket::Disconnect | ServerPacket::ConnectError(_) => {
                tracing::debug!(%ns, %packet, "namespace ended by the server");
                // The server refused the socket or left it, so both end it
                // without sending what its handles queued.
                self.write_buffer.retain(|write| match write {
                    Write::Connect { ns: owner, .. }
                    | Write::Packet { ns: owner, .. }
                    | Write::Disconnect { ns: owner } => owner != ns,
                    Write::Encoded(_) => true,
                });
                self.decoder.disconnect(ns);
                socket.end(ns, Some(packet));
                self.release();
                return None;
            }
            ServerPacket::Event(_) => {}
        }

        socket.deliver(ns, packet)
    }

    /// Puts the unsent events of each confirmed socket back into its send
    /// buffer and discards the rest of the write buffer, like `Socket.onclose`
    /// keeping `sendBuffer`. Replies never outlive their engine.
    fn return_unsent(&mut self) {
        let mut unsent: BTreeMap<ByteString, VecDeque<Queued>> = BTreeMap::new();
        for write in std::mem::take(&mut self.write_buffer) {
            let Write::Packet { ns, packet, slot } = write else {
                continue;
            };
            let current = self.sockets.get(&ns).is_some_and(|socket| {
                matches!(&socket.state, SocketState::Connected { handles, .. } if handles.owns(&slot))
            });
            if current && matches!(packet, ClientPacket::Event { .. }) {
                unsent.entry(ns).or_default().push_back((packet, slot));
            }
        }
        for (ns, socket) in &mut self.sockets {
            socket.close_connection(ns, unsent.remove(ns).unwrap_or_default());
        }
    }

    /// Runs after every transition that can need an engine: gives an engine
    /// to sockets that need one.
    fn settle(&mut self) {
        if !self.needs_engine() {
            return;
        }
        match self.ready_state {
            ReadyState::Closed => {
                self.reset_backoff();
                self.open_engine();
            }
            ReadyState::Flushing => self.ready_state = ReadyState::Open,
            ReadyState::Opening
            | ReadyState::Open
            | ReadyState::Closing { .. }
            | ReadyState::Reconnecting => {}
        }
    }

    /// Lets the engine go once no socket needs it, like `Manager._destroy`:
    /// a pending wait stops, and a running engine closes after its write
    /// buffer empties.
    fn release(&mut self) {
        if self.needs_engine() {
            return;
        }
        match self.ready_state {
            // Nothing has reached the server yet, so there is nothing to flush.
            ReadyState::Opening => self.close_engine(false),
            ReadyState::Open => {
                self.ready_state = ReadyState::Flushing;
                self.finish_flushing();
            }
            ReadyState::Reconnecting => {
                tracing::debug!("stopped reconnecting");
                self.ready_state = ReadyState::Closed;
                self.outputs.push_back(ManagerOutput::Cancel);
            }
            ReadyState::Flushing => self.finish_flushing(),
            ReadyState::Closed | ReadyState::Closing { .. } => {}
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

    /// Asks for an engine, with CONNECT queued for every connecting socket so
    /// they go out first once it opens, like `Manager.open` followed by
    /// `Socket.onopen`.
    fn open_engine(&mut self) {
        tracing::debug!("engine opening");
        self.ready_state = ReadyState::Opening;
        self.outputs.push_back(ManagerOutput::Open);

        for (ns, socket) in &self.sockets {
            if let Some(handles) = socket.handles() {
                self.write_buffer.push_back(Write::Connect {
                    ns: ns.clone(),
                    auth: handles.auth.clone(),
                });
            }
        }
    }

    fn reset_backoff(&mut self) {
        if let Some(backoff) = &mut self.backoff {
            backoff.reset();
        }
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
}

/// Builds the wire packet for a socket's packet, like `Socket.packet`, and
/// assigns an event's ack id. Returns the attachments that follow it.
fn wire_packet(socket: &mut Socket, packet: ClientPacket) -> (Packet, Option<Vec<bytes::Bytes>>) {
    match packet {
        ClientPacket::Event {
            payload,
            ack_tx,
            attachments,
        } => {
            let id = ack_tx.map(|ack_tx| socket.add_ack(ack_tx));
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
                None => Packet::Ack {
                    payload,
                    id: id.get(),
                },
                Some(attachments) => Packet::BinaryAck {
                    payload,
                    id: id.get(),
                    count: attachments.len(),
                },
            };
            (packet, attachments)
        }
    }
}

/// Encodes a packet for namespace `ns` as its text message.
fn encode(ns: &str, packet: &Packet) -> ClientMessage {
    ClientMessage::Text(packet.encode(ns).into())
}
