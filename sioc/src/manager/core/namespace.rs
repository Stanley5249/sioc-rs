//! One namespace name and everything the manager keeps for it.
//!
//! It stays in one file past 400 lines, because every state change reads and
//! writes the same private fields, which a split across modules would have to
//! make public.

use std::sync::Arc;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::SocketError;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::core::acks::Acks;
use crate::manager::core::send_buffer::{ControlQueue, Outgoing, SendBuffer};
use crate::packet::{ClientPacket, DynAck, ServerPacket};

/// One namespace name, like socket.io-client's `Socket`, which
/// `Manager.nsps` keeps for every name and never prunes.
///
/// Methods that log or encode take the name from the caller, because the map
/// in [`Namespaces`](crate::manager::core::namespaces::Namespaces) owns it.
#[derive(Debug)]
pub struct Namespace {
    /// Ack ids and waiters, like `Socket.ids` and `Socket.acks`.
    acks: Acks,
    /// Packets waiting for the engine, like `Socket.sendBuffer`.
    send_buffer: SendBuffer,
    /// CONNECT and DISCONNECT waiting for the engine, which `Socket.onopen`
    /// and `Socket.disconnect` write at once in JS.
    control: ControlQueue,
    /// The capacity of the send buffer, given as room to each new set of
    /// handles. JS has no counterpart, because its buffer is unbounded.
    capacity: usize,
    /// The inbox of new handles until the caller takes it with
    /// [`take_listen`](Self::take_listen). JS has no counterpart, because its
    /// handles call the `Socket` directly.
    client_packet_rx: Option<mpsc::Receiver<ClientPacket>>,
    /// Where the namespace is in its life, like `Socket.active` and
    /// `Socket.connected`.
    state: NamespaceState,
}

/// Where a namespace is in its life, like `Socket.active` and the
/// `Socket.connected` flag, plus the drain states JS needs no handles for.
#[derive(Debug)]
enum NamespaceState {
    /// No handles, like `!Socket.active`.
    Inactive,
    /// Live handles, like `Socket.active`.
    Active(Active),
    /// A confirmed namespace whose inbox ended. It sends the rest of its buffer
    /// before DISCONNECT. JS has no counterpart, because it writes
    /// synchronously. Holds at most one request to open the name again.
    Disconnecting(Option<ConnectRequest>),
    /// The server or the manager ended the namespace, but its old handles still
    /// drain what they sent. JS has no counterpart, because it has no handles.
    /// Holds at most one request to open the name again.
    Draining(Option<ConnectRequest>),
}

/// The channel ends of a namespace with live handles, like the state
/// socket.io-client's `Socket` has while `active`.
#[derive(Debug)]
struct Active {
    /// The JSON auth payload, sent with the CONNECT of every engine, like
    /// `Socket.auth`.
    auth: ByteString,
    /// Delivers the server's packets to the receiver, like the listeners
    /// `Socket.emitEvent` calls.
    server_packet_tx: mpsc::Sender<ServerPacket>,
    /// Delivers the one terminal packet even when the receiver is full. JS has
    /// no counterpart, because its listeners never fill.
    terminal_packet_tx: Option<oneshot::Sender<ServerPacket>>,
    /// Shared with the handles and cancelled when this value drops. JS has no
    /// counterpart, because its handles are the `Socket`.
    closed: CancellationToken,
    /// The free slots of the send buffer, which the caller takes one at a
    /// time before it reads the inbox. JS has no counterpart, because its
    /// buffer is unbounded.
    room: Arc<Semaphore>,
    /// Whether the server confirmed the namespace to the current engine, like
    /// `Socket.connected`.
    connected: bool,
}

impl Drop for Active {
    fn drop(&mut self) {
        // The handles share this token, so leaving `Active` closes them.
        self.closed.cancel();
    }
}

impl Namespace {
    /// Creates an inactive namespace whose send buffer holds `capacity`
    /// packets, like `Manager.socket` creating a `Socket`.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            acks: Acks::default(),
            send_buffer: SendBuffer::default(),
            control: ControlQueue::default(),
            capacity,
            client_packet_rx: None,
            state: NamespaceState::Inactive,
        }
    }

    /// Opens the namespace, queues its CONNECT, and reports the result to the
    /// caller, like `Socket.connect`.
    ///
    /// Holds the request while the old handles of the name drain, and refuses
    /// it while the name is active or already has a request waiting.
    pub fn open(&mut self, request: ConnectRequest) {
        match &mut self.state {
            NamespaceState::Inactive => {}
            NamespaceState::Disconnecting(reopen @ None)
            | NamespaceState::Draining(reopen @ None) => {
                *reopen = Some(request);
                return;
            }
            NamespaceState::Active(_)
            | NamespaceState::Disconnecting(Some(_))
            | NamespaceState::Draining(Some(_)) => {
                let ns = request.ns;

                // A cancelled connect drops its reply receiver.
                let _ = request
                    .reply_tx
                    .send(Err(SocketError::NamespaceConflict { ns }));
                return;
            }
        }

        let ConnectRequest {
            payload,
            client_packet_rx,
            closed,
            server_packet_tx,
            terminal_packet_tx,
            reply_tx,
            ..
        } = request;

        // The caller may have cancelled connect while the request was queued.
        let _ = reply_tx.send(Ok(()));

        self.control.push_connect(payload.clone());
        self.client_packet_rx = Some(client_packet_rx);
        self.state = NamespaceState::Active(Active {
            auth: payload,
            server_packet_tx,
            terminal_packet_tx: Some(terminal_packet_tx),
            closed,
            room: Arc::new(Semaphore::new(self.capacity)),
            connected: false,
        });
    }

    /// Takes the inbox, token, and room of handles that just became active.
    /// JS has no counterpart, because its handles call the `Socket` directly.
    pub fn take_listen(
        &mut self,
    ) -> Option<(
        mpsc::Receiver<ClientPacket>,
        CancellationToken,
        Arc<Semaphore>,
    )> {
        let NamespaceState::Active(active) = &self.state else {
            return None;
        };
        let client_packet_rx = self.client_packet_rx.take()?;
        Some((client_packet_rx, active.closed.clone(), active.room.clone()))
    }

    /// Returns whether the namespace has live handles, like `Socket.active`.
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self.state, NamespaceState::Active(_))
    }

    /// Returns whether the namespace still needs an engine, either to talk to
    /// the server or to send what it buffered before DISCONNECT, like
    /// `Manager._destroy` checking `socket.active`.
    #[must_use]
    pub fn needs_engine(&self) -> bool {
        matches!(
            self.state,
            NamespaceState::Active(_) | NamespaceState::Disconnecting(_)
        )
    }

    /// Returns whether the namespace has no handles and nothing to drain,
    /// like `!Socket.active`.
    #[must_use]
    pub fn is_inactive(&self) -> bool {
        matches!(self.state, NamespaceState::Inactive)
    }

    /// Returns a sender to the namespace's receiver, if the namespace is
    /// active. JS has no counterpart, because its listeners never fill.
    #[must_use]
    pub fn server_packet_tx(&self) -> Option<mpsc::Sender<ServerPacket>> {
        match &self.state {
            NamespaceState::Active(active) => Some(active.server_packet_tx.clone()),
            _ => None,
        }
    }

    /// Queues CONNECT with the auth payload for a new engine, like
    /// `Socket.onopen`. Does nothing unless the namespace is active.
    pub fn queue_connect(&mut self) {
        if let NamespaceState::Active(active) = &self.state {
            self.control.push_connect(active.auth.clone());
        }
    }

    /// Takes a packet from the handles of `ns`, like `Socket.packet`.
    ///
    /// Discards the packet unless the namespace is active, and discards an ack
    /// reply until the server confirms the namespace, because its id came from
    /// an earlier engine.
    pub fn accept(&mut self, ns: &str, packet: ClientPacket) {
        let NamespaceState::Active(active) = &self.state else {
            tracing::debug!(%ns, "discarded client packet for a closed namespace");
            return;
        };

        match packet {
            ClientPacket::Event {
                payload,
                ack_tx,
                attachments,
            } => {
                let ack_tx = ack_tx.map(|ack_tx| (self.acks.next_id(), ack_tx));
                self.send_buffer
                    .push_event(ns, payload, ack_tx, attachments);
            }
            ClientPacket::Ack { id, .. } if !active.connected => {
                tracing::debug!(%ns, id, "discarded ack for an unconfirmed namespace");
                active.room.add_permits(1);
            }
            ClientPacket::Ack {
                payload,
                id,
                attachments,
            } => {
                self.send_buffer
                    .push_ack_reply(ns, payload, id, attachments);
            }
        }
    }

    /// Records the server's CONNECT, like `Socket.onconnect`. Events wait until
    /// then.
    pub fn confirm(&mut self) {
        if let NamespaceState::Active(active) = &mut self.state {
            active.connected = true;
        }
    }

    /// Sends a server packet to the receiver of `ns`, like `Socket.emitEvent`.
    ///
    /// Returns the packet if the receiver is full, so the caller can hold it.
    /// The server may still send packets for a namespace the client left, and
    /// the application may drop a receiver it no longer reads, so both are
    /// discarded.
    pub fn deliver(&mut self, ns: &str, packet: ServerPacket) -> Option<ServerPacket> {
        let NamespaceState::Active(active) = &self.state else {
            tracing::debug!(%ns, "discarded server packet for a closed namespace");
            return None;
        };

        match active.server_packet_tx.try_send(packet) {
            Ok(()) => None,
            Err(TrySendError::Full(packet)) => Some(packet),
            Err(TrySendError::Closed(_)) => {
                tracing::debug!(%ns, "discarded server packet for a dropped receiver");
                None
            }
        }
    }

    /// Completes the waiter of an ack, like `Socket.onack`.
    pub fn resolve_ack(&mut self, ns: &str, id: u64, ack: DynAck) {
        self.acks.resolve(ns, id, ack);
    }

    /// Ends the namespace for the server or the manager, like
    /// `Socket.ondisconnect` and `Socket.destroy`. The old handles drain next.
    ///
    /// Delivers `terminal` on its own channel, so it arrives even when the
    /// receiver's queue is full. Fails every waiter and drops every buffered
    /// packet, because nothing can send them any more.
    pub fn end(&mut self, ns: &str, terminal: Option<ServerPacket>) {
        match std::mem::replace(&mut self.state, NamespaceState::Inactive) {
            NamespaceState::Active(mut active) => {
                if let (Some(packet), Some(terminal_packet_tx)) =
                    (terminal, active.terminal_packet_tx.take())
                    && terminal_packet_tx.send(packet).is_err()
                {
                    tracing::debug!(%ns, "discarded terminal packet for a dropped receiver");
                }

                self.clear();
                self.client_packet_rx = None;
                self.state = NamespaceState::Draining(None);
            }
            // The inbox already ended, so nothing is left to drain.
            NamespaceState::Disconnecting(reopen) => self.become_inactive(reopen),
            other => self.state = other,
        }
    }

    /// Ends the namespace for good, because the manager stops. Dropping the
    /// state fails the senders, the waiters, and the reply of a waiting
    /// request. JS has no counterpart, because its `Manager` never stops.
    pub fn shutdown(&mut self) {
        self.clear();
        self.client_packet_rx = None;
        self.state = NamespaceState::Inactive;
    }

    /// Ends the namespace once its inbox ends, like `Socket.disconnect`.
    ///
    /// A confirmed namespace sends its buffer, then DISCONNECT. An
    /// unconfirmed one sends nothing and drops its buffer, because the server
    /// closes the whole connection on any packet for a namespace it has not
    /// joined.
    pub fn disconnect(&mut self, ns: &str) {
        match std::mem::replace(&mut self.state, NamespaceState::Inactive) {
            NamespaceState::Active(active) if !active.connected => {
                self.control.cancel_connect();
                self.become_inactive(None);
            }
            NamespaceState::Active(_) if self.send_buffer.is_empty() => {
                self.control.push_disconnect();
                self.become_inactive(None);
            }
            NamespaceState::Active(_) => self.state = NamespaceState::Disconnecting(None),
            NamespaceState::Draining(reopen) => self.become_inactive(reopen),
            other => {
                tracing::debug!(%ns, "ignored the end of an inbox that was gone");
                self.state = other;
            }
        }
    }

    /// Handles the end of an engine, like `Socket.onclose`.
    ///
    /// Fails the waiters of events the engine took, which is `_clearAcks`.
    /// Waiters of buffered events keep waiting. Drops ack replies and queued
    /// CONNECT and DISCONNECT, because they belong to the engine that is gone.
    /// A namespace in the middle of its disconnect ends, because the next
    /// engine has nothing to disconnect from.
    pub fn engine_closed(&mut self) {
        self.acks.clear();
        self.control.clear();
        let dropped = self.send_buffer.drop_ack_replies();

        match &mut self.state {
            NamespaceState::Active(active) => {
                active.connected = false;
                active.room.add_permits(dropped);
            }
            NamespaceState::Disconnecting(reopen) => {
                let reopen = reopen.take();
                self.become_inactive(reopen);
            }
            NamespaceState::Inactive | NamespaceState::Draining(_) => {}
        }
    }

    /// Returns whether CONNECT or DISCONNECT waits for the engine. JS has no
    /// counterpart, because it writes them at once.
    #[must_use]
    pub fn has_control(&self) -> bool {
        !self.control.is_empty()
    }

    /// Returns whether the engine can take a buffered packet, like
    /// `Socket.emitBuffered` running once `connected`.
    #[must_use]
    pub fn has_data(&self) -> bool {
        let sends = match &self.state {
            NamespaceState::Active(active) => active.connected,
            NamespaceState::Disconnecting(_) => true,
            NamespaceState::Inactive | NamespaceState::Draining(_) => false,
        };
        sends && !self.send_buffer.is_empty()
    }

    /// Takes the oldest CONNECT or DISCONNECT of `ns`, like the `packet`
    /// call in `Socket.onopen` or `Socket.disconnect`.
    pub fn pop_control(&mut self, ns: &str) -> Option<Message> {
        self.control.pop(ns)
    }

    /// Takes the oldest buffered packet of `ns` as its header and attachments,
    /// frees its slot, and moves its ack waiter to `acks`, like `Socket.emit`
    /// once the engine took the packet.
    ///
    /// Queues DISCONNECT after the last packet of a disconnecting namespace.
    pub fn pop(&mut self, ns: &str) -> Option<(Message, Vec<Bytes>)> {
        if !self.has_data() {
            return None;
        }

        let Outgoing {
            header,
            attachments,
            ack_tx,
            ..
        } = self.send_buffer.pop()?;

        tracing::trace!(%ns, %header, "sent packet");

        if let Some((id, ack_tx)) = ack_tx {
            self.acks.insert(id, ack_tx);
        }

        match &mut self.state {
            NamespaceState::Active(active) => active.room.add_permits(1),
            NamespaceState::Disconnecting(reopen) if self.send_buffer.is_empty() => {
                let reopen = reopen.take();
                self.control.push_disconnect();
                self.become_inactive(reopen);
            }
            _ => {}
        }

        Some((header, attachments))
    }

    /// Drops every waiter, buffered packet, and queued control packet, like
    /// `Socket.destroy` with `_clearAcks`.
    fn clear(&mut self) {
        self.acks.clear();
        self.send_buffer.clear();
        self.control.clear();
    }

    /// Leaves for `Inactive`, then opens the waiting request, if any, like
    /// `Socket.onclose` followed by `Socket.connect`.
    ///
    /// Keeps a queued DISCONNECT, which still has to leave.
    fn become_inactive(&mut self, reopen: Option<ConnectRequest>) {
        self.acks.clear();
        self.send_buffer.clear();
        self.state = NamespaceState::Inactive;

        if let Some(request) = reopen {
            self.open(request);
        }
    }
}
