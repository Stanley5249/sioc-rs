//! One namespace name and everything the manager keeps for it.

use std::collections::HashMap;

use bytestring::ByteString;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::SocketError;
use crate::manager::connect_request::ConnectRequest;
use crate::packet::{ClientPacket, DynAck, ServerPacket};

/// One namespace name, like socket.io-client's `Socket`, which
/// `Manager.nsps` keeps for every name and never prunes.
#[derive(Debug, Default)]
pub struct Namespace {
    /// The next ack id, like `Socket.ids`. It never restarts, so a late ack
    /// for a closed namespace never answers an event of the reopened one.
    next_ack_id: u64,
    /// Waiters of events the engine took, like `Socket.acks`.
    acks: HashMap<u64, oneshot::Sender<DynAck>>,
    /// What the handles send, like `Socket.sendBuffer`, until the actor takes
    /// it to read. Packets wait here until they can go to the engine.
    client_packet_rx: Option<mpsc::Receiver<ClientPacket>>,
    /// A request to open the name again once its old handles finish. Rust
    /// cleanup: JS reuses the one `Socket` at once.
    reopen: Option<ConnectRequest>,
    /// Whether the namespace became active and its CONNECT is not queued yet,
    /// like `Socket.connect` waiting for `onopen`.
    connect_due: bool,
    /// Where the namespace is in its life.
    state: NamespaceState,
}

/// Where a namespace is in its life, like `Socket.active`.
#[derive(Debug, Default)]
enum NamespaceState {
    /// No handles, like `!Socket.active`.
    #[default]
    Inactive,
    /// Live handles, like `Socket.active`.
    Active(Active),
    /// The server or the manager ended the namespace while its old handles
    /// still drain what they sent. Rust cleanup: JS has no handles to drain.
    Draining,
}

/// The ends a namespace holds while its handles live, like the state of an
/// active `Socket`.
#[derive(Debug)]
struct Active {
    /// The JSON auth payload, sent with the CONNECT of every engine, like
    /// `Socket.auth`.
    auth: ByteString,
    /// Delivers the server's packets, like the listeners `Socket.emitEvent`
    /// calls.
    server_packet_tx: mpsc::Sender<ServerPacket>,
    /// Delivers the one terminal packet even when the receiver is full. Rust
    /// bound: JS listeners never fill.
    terminal_packet_tx: Option<oneshot::Sender<ServerPacket>>,
    /// Shared with the handles, which cancel it when the last one leaves, and
    /// cancelled when this value drops. Rust cleanup: JS handles are the
    /// `Socket`.
    closed: CancellationToken,
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

/// What the actor does with the next packet of a namespace's handles.
#[derive(Debug, PartialEq, Eq)]
pub enum Read {
    /// Read it once an engine permit is held, because it goes out at once.
    Send,
    /// Read it to drop it, because the namespace or its handles are gone.
    Discard,
    /// Leave it in the channel until the server confirms the namespace.
    Wait,
}

impl Namespace {
    /// Opens the namespace and reports the result to the caller, like
    /// `Socket.connect`. Returns the auth payload if it became active.
    ///
    /// Holds the request while the old handles of the name finish, and
    /// refuses it while another request already waits.
    pub fn open(&mut self, request: ConnectRequest) -> Option<ByteString> {
        let conflict = match &self.state {
            NamespaceState::Inactive => false,
            // Live handles that have not left keep the name.
            NamespaceState::Active(active) if !active.closed.is_cancelled() => true,
            NamespaceState::Active(_) | NamespaceState::Draining => self.reopen.is_some(),
        };

        if conflict {
            let ns = request.ns;
            // A cancelled connect drops its reply receiver.
            let _ = request
                .reply_tx
                .send(Err(SocketError::NamespaceConflict { ns }));
            return None;
        }
        if !self.is_inactive() {
            self.reopen = Some(request);
            return None;
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

        self.client_packet_rx = Some(client_packet_rx);
        self.connect_due = true;
        self.state = NamespaceState::Active(Active {
            auth: payload.clone(),
            server_packet_tx,
            terminal_packet_tx: Some(terminal_packet_tx),
            closed,
            connected: false,
        });
        Some(payload)
    }

    /// Takes the channel and token of handles that just became active.
    pub fn take_listen(&mut self) -> Option<(mpsc::Receiver<ClientPacket>, CancellationToken)> {
        let NamespaceState::Active(active) = &self.state else {
            return None;
        };
        Some((self.client_packet_rx.take()?, active.closed.clone()))
    }

    /// Takes the auth payload of a namespace that became active since the
    /// last call, so its CONNECT is queued once, like `Socket.connect`.
    pub fn take_connect(&mut self) -> Option<ByteString> {
        if !std::mem::take(&mut self.connect_due) {
            return None;
        }
        self.auth().cloned()
    }

    /// Returns whether the namespace has live handles that still need an
    /// engine, like `Socket.active`.
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self.state, NamespaceState::Active(_))
    }

    /// Returns whether the namespace has no handles and nothing to drain.
    #[must_use]
    pub fn is_inactive(&self) -> bool {
        matches!(self.state, NamespaceState::Inactive)
    }

    /// Returns the auth payload for the CONNECT of a new engine, like
    /// `Socket.onopen`, if the namespace is active.
    #[must_use]
    pub fn auth(&self) -> Option<&ByteString> {
        match &self.state {
            NamespaceState::Active(active) => Some(&active.auth),
            NamespaceState::Inactive | NamespaceState::Draining => None,
        }
    }

    /// Returns what the actor does with the next packet of the handles, given
    /// whether the engine can take a packet now.
    #[must_use]
    pub fn read(&self, engine_ready: bool) -> Read {
        match &self.state {
            NamespaceState::Active(active) if active.connected && engine_ready => Read::Send,
            NamespaceState::Active(active) if active.connected || !active.closed.is_cancelled() => {
                Read::Wait
            }
            NamespaceState::Active(_) | NamespaceState::Inactive | NamespaceState::Draining => {
                Read::Discard
            }
        }
    }

    /// Returns whether the server confirmed the namespace to the current
    /// engine, like `Socket.connected`.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        matches!(&self.state, NamespaceState::Active(active) if active.connected)
    }

    /// Registers the waiter of an event that goes to the engine now, like
    /// `Socket._registerAckCallback`, and returns its id.
    ///
    /// Reclaims waiters whose handles were dropped or timed out first, so
    /// unanswered requests do not accumulate.
    pub fn register_ack(&mut self, ack_tx: oneshot::Sender<DynAck>) -> u64 {
        self.acks.retain(|_, ack_tx| !ack_tx.is_closed());

        let id = self.next_ack_id;
        self.next_ack_id += 1;
        self.acks.insert(id, ack_tx);
        id
    }

    /// Completes the waiter of an ack, like `Socket.onack`. Removes it first,
    /// so each ack arrives at most once and an unknown id completes nothing.
    pub fn resolve_ack(&mut self, ns: &str, id: u64, ack: DynAck) {
        let Some(ack_tx) = self.acks.remove(&id) else {
            tracing::debug!(%ns, id, "discarded ack with no pending handle");
            return;
        };
        if ack_tx.send(ack).is_err() {
            tracing::debug!(%ns, id, "discarded ack for a dropped handle");
        }
    }

    /// Records the server's CONNECT, like `Socket.onconnect`.
    pub fn confirm(&mut self) {
        if let NamespaceState::Active(active) = &mut self.state {
            active.connected = true;
        }
    }

    /// Sends a server packet to the receiver, like `Socket.emitEvent`, and
    /// returns it if the receiver is full.
    ///
    /// Packets for a namespace the client left, or for a dropped receiver,
    /// are discarded, because the server may still send them.
    pub fn deliver(&self, ns: &str, packet: ServerPacket) -> Option<ServerPacket> {
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

    /// Returns a sender to the receiver, to reserve a slot for a held packet.
    #[must_use]
    pub fn server_packet_tx(&self) -> Option<mpsc::Sender<ServerPacket>> {
        match &self.state {
            NamespaceState::Active(active) => Some(active.server_packet_tx.clone()),
            NamespaceState::Inactive | NamespaceState::Draining => None,
        }
    }

    /// Ends the namespace for the server or the manager, like
    /// `Socket.ondisconnect` and `Socket.destroy`. The old handles drain next.
    ///
    /// Delivers `terminal` on its own channel, so it arrives even when the
    /// receiver is full, and fails every waiter.
    pub fn end(&mut self, ns: &str, terminal: Option<ServerPacket>) {
        let NamespaceState::Active(mut active) = std::mem::take(&mut self.state) else {
            return;
        };

        if let (Some(packet), Some(terminal_packet_tx)) =
            (terminal, active.terminal_packet_tx.take())
            && terminal_packet_tx.send(packet).is_err()
        {
            tracing::debug!(%ns, "discarded terminal packet for a dropped receiver");
        }
        drop(active);
        self.acks.clear();

        // A channel the actor never took has no old handles to drain, so the
        // name is free at once.
        if self.client_packet_rx.take().is_some() {
            self.finish();
        } else {
            self.state = NamespaceState::Draining;
        }
    }

    /// Handles the end of the handles' channel, like `Socket.disconnect`.
    /// Returns whether DISCONNECT goes out, which only a confirmed namespace
    /// sends, because the server closes the whole connection on any packet
    /// for a namespace it has not joined.
    pub fn disconnect(&mut self) -> bool {
        let connected = self.is_connected();
        self.state = NamespaceState::Inactive;
        self.acks.clear();
        self.finish();
        connected
    }

    /// Handles the end of an engine, like `Socket.onclose`: fails the waiters
    /// of events the engine took, which is `_clearAcks`. Packets the engine
    /// never took stay in the channel.
    pub fn engine_closed(&mut self) {
        self.acks.clear();
        if let NamespaceState::Active(active) = &mut self.state {
            active.connected = false;
        }
    }

    /// Ends the namespace for good, because the manager stops. Rust cleanup:
    /// the JS `Manager` never stops.
    pub fn shutdown(&mut self) {
        *self = Self {
            next_ack_id: self.next_ack_id,
            ..Self::default()
        };
    }

    /// Leaves for `Inactive`, then opens the waiting request, if any.
    fn finish(&mut self) {
        self.state = NamespaceState::Inactive;
        if let Some(request) = self.reopen.take() {
            self.open(request);
        }
    }
}
