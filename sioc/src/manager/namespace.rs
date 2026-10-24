//! One namespace name and everything the manager keeps for it.
//!
//! The state and its transitions stay together so participation ownership,
//! input handoff and termination can be reviewed against the same resources.

use std::collections::HashMap;

use bytestring::ByteString;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
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
    /// A request to open the name again once its old handles finish. Rust
    /// cleanup: JS reuses the one `Socket` at once.
    reopen: Option<ConnectRequest>,
    /// Where the namespace is in its life, like `Socket.active`.
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
    /// Waiters of events this participation sent, like `Socket.acks`.
    acks: HashMap<u64, oneshot::Sender<DynAck>>,
    /// The inbox until ownership transfers to the actor's `SendBuffer`, like
    /// `Socket.sendBuffer`. Only its owner drains it.
    client_packet_rx: Option<mpsc::Receiver<ClientPacket>>,
    /// The confirmed engine's ACK route, like `Socket.ack`. Rust channel.
    ack_route_tx: watch::Sender<Option<mpsc::Sender<ClientPacket>>>,
    /// The reply input until the actor takes ownership. Rust channel.
    ack_packet_rx: Option<mpsc::Receiver<ClientPacket>>,
    /// Per-engine reply capacity, matching the event inbox. Rust bound.
    ack_capacity: usize,
    /// The JSON auth payload, sent with the CONNECT of every engine, like
    /// `Socket.auth`.
    auth: ByteString,
    /// Delivers the server's packets, like the listeners `Socket.emitEvent`
    /// calls.
    server_packet_tx: mpsc::Sender<ServerPacket>,
    /// Delivers the one terminal packet even when the receiver is full. Rust
    /// bound: JS listeners never fill.
    terminal_packet_tx: Option<oneshot::Sender<ServerPacket>>,
    /// Shared with the handles, which cancel it on `disconnect` or when the
    /// last one drops, and cancelled when this value drops. Rust cleanup: JS
    /// handles are the `Socket`.
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

/// What the actor does with the next packet of a namespace's handles. Rust
/// bound: JS writes every packet at once.
#[derive(Debug, PartialEq, Eq)]
pub enum Read {
    /// Read it once an engine permit is held, because it goes out at once.
    Send,
    /// Read it to drop it, because the namespace or its handles are gone.
    Discard,
    /// Leave it in the channel until the server confirms the namespace.
    Wait,
}

/// A delivery waiting for its original receiver, like `Socket.emitEvent`.
/// Rust bound: JS listeners never fill. Owning the route prevents namespace
/// reuse from redirecting the packet to replacement handles.
#[derive(Debug)]
pub struct Delivery {
    /// The original `Socket.nsp`, used only for cleanup and diagnostics.
    pub ns: ByteString,
    /// The packet waiting for `Socket.emitEvent`.
    pub packet: ServerPacket,
    /// The original receiver's channel, the Rust equivalent of its listeners.
    pub sender: mpsc::Sender<ServerPacket>,
    /// Cancels this delivery when its participation ends. Rust cleanup.
    pub closed: CancellationToken,
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
            ack_route_tx,
            closed,
            server_packet_tx,
            terminal_packet_tx,
            reply_tx,
            ..
        } = request;

        // The caller may have cancelled connect while the request was queued.
        let _ = reply_tx.send(Ok(()));

        self.state = NamespaceState::Active(Active {
            acks: HashMap::new(),
            ack_capacity: client_packet_rx.max_capacity(),
            client_packet_rx: Some(client_packet_rx),
            ack_route_tx,
            ack_packet_rx: None,
            auth: payload.clone(),
            server_packet_tx,
            terminal_packet_tx: Some(terminal_packet_tx),
            closed,
            connected: false,
        });
        Some(payload)
    }

    /// Takes the channel and token of handles that just became active. Rust
    /// channel: JS handles call the `Socket` directly.
    pub fn take_listen(&mut self) -> Option<(mpsc::Receiver<ClientPacket>, CancellationToken)> {
        let NamespaceState::Active(active) = &mut self.state else {
            return None;
        };
        Some((active.client_packet_rx.take()?, active.closed.clone()))
    }

    /// Hands the actor the confirmed engine's reply input. Rust channel:
    /// `Socket.ack` writes directly in JS, while queued Rust replies need a
    /// session-local owner that can discard them on engine end.
    pub fn take_acks(&mut self) -> Option<mpsc::Receiver<ClientPacket>> {
        let NamespaceState::Active(active) = &mut self.state else {
            return None;
        };
        active.ack_packet_rx.take()
    }

    /// Returns whether the namespace has live handles that still need an
    /// engine, like `Socket.active`.
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self.state, NamespaceState::Active(_))
    }

    /// Returns whether the namespace has no handles and nothing to drain,
    /// like `!Socket.active`.
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
    /// whether the engine can take a packet now, like `Socket.emitBuffered`
    /// running once connected.
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
        let NamespaceState::Active(active) = &mut self.state else {
            unreachable!("only an active namespace registers an ack");
        };
        active.acks.retain(|_, ack_tx| !ack_tx.is_closed());

        let id = self.next_ack_id;
        self.next_ack_id += 1;
        active.acks.insert(id, ack_tx);
        id
    }

    /// Completes the waiter of an ack, like `Socket.onack`. Removes it first,
    /// so each ack arrives at most once and an unknown id completes nothing.
    pub fn resolve_ack(&mut self, ns: &str, id: u64, ack: DynAck) {
        let NamespaceState::Active(active) = &mut self.state else {
            return;
        };
        let Some(ack_tx) = active.acks.remove(&id) else {
            tracing::debug!(%ns, id, "discarded ack with no pending handle");
            return;
        };
        if ack_tx.send(ack).is_err() {
            tracing::debug!(%ns, id, "discarded ack for a dropped handle");
        }
    }

    /// Records the server's CONNECT, like `Socket.onconnect`.
    pub fn confirm(&mut self) {
        if let NamespaceState::Active(active) = &mut self.state
            && !active.connected
        {
            let (tx, rx) = mpsc::channel(active.ack_capacity);
            active.ack_route_tx.send_replace(Some(tx));
            active.ack_packet_rx = Some(rx);
            active.connected = true;
        }
    }

    /// Sends a server packet to the receiver, like `Socket.emitEvent`, and
    /// returns it if the receiver is full.
    ///
    /// Packets for a namespace the client left, or for a dropped receiver,
    /// are discarded, because the server may still send them.
    pub fn deliver(&self, ns: &ByteString, packet: ServerPacket) -> Option<Delivery> {
        let NamespaceState::Active(active) = &self.state else {
            tracing::debug!(%ns, "discarded server packet for a closed namespace");
            return None;
        };

        match active.server_packet_tx.try_send(packet) {
            Ok(()) => None,
            Err(TrySendError::Full(packet)) => Some(Delivery {
                ns: ns.clone(),
                packet,
                sender: active.server_packet_tx.clone(),
                closed: active.closed.clone(),
            }),
            Err(TrySendError::Closed(_)) => {
                tracing::debug!(%ns, "discarded server packet for a dropped receiver");
                None
            }
        }
    }

    /// Ends the namespace for the server or the manager, like
    /// `Socket.ondisconnect` and `Socket.destroy`. The old handles drain next.
    ///
    /// Delivers `terminal` on its own channel, so it arrives even when the
    /// receiver is full, and fails every waiter.
    pub fn end(&mut self, ns: &str, terminal: Option<ServerPacket>) {
        // Repeated termination preserves the draining participation and its
        // waiting reopen. Only the actor's inbox EOF finishes that ownership.
        if !self.is_active() {
            return;
        }
        let NamespaceState::Active(mut active) = std::mem::take(&mut self.state) else {
            unreachable!("the active state was checked before taking it");
        };

        if let (Some(packet), Some(terminal_packet_tx)) =
            (terminal, active.terminal_packet_tx.take())
            && terminal_packet_tx.send(packet).is_err()
        {
            tracing::debug!(%ns, "discarded terminal packet for a dropped receiver");
        }
        // A channel the actor never took has no old handles to drain, so the
        // name is free at once.
        if active.client_packet_rx.is_none() {
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
        connected
    }

    /// Handles the end of an engine, like `Socket.onclose`: fails the waiters
    /// of events the engine took, which is `_clearAcks`. Packets the engine
    /// never took stay in the channel.
    pub fn engine_closed(&mut self, ns: &str) {
        if matches!(&self.state, NamespaceState::Active(active) if active.closed.is_cancelled()) {
            // The old engine can no longer flush this participation. Finish
            // admission before retry decisions; only inbox reclamation remains.
            self.end(ns, None);
            return;
        }
        if let NamespaceState::Active(active) = &mut self.state {
            active.acks.clear();
            active.ack_route_tx.send_replace(None);
            active.ack_packet_rx = None;
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

    /// Takes a waiting request only after old participation has finished.
    /// Rust cleanup: the core opens it after reclaiming the old deliveries.
    pub fn take_reopen(&mut self) -> Option<ConnectRequest> {
        self.is_inactive().then(|| self.reopen.take()).flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registering_an_ack_reclaims_dropped_waiters() {
        let mut namespace = Namespace::default();
        let (request, _handles) = ConnectRequest::new("/".into(), "{}".into(), 1, 1);
        namespace.open(request);
        let (live_tx, _live_rx) = oneshot::channel();
        namespace.register_ack(live_tx);
        for _ in 0..100 {
            let (ack_tx, ack_rx) = oneshot::channel();
            namespace.register_ack(ack_tx);
            drop(ack_rx);
        }
        let NamespaceState::Active(active) = &namespace.state else {
            unreachable!()
        };
        assert_eq!(active.acks.len(), 2);

        let (ack_tx, _ack_rx) = oneshot::channel();
        assert_eq!(namespace.register_ack(ack_tx), 101);
        let NamespaceState::Active(active) = &namespace.state else {
            unreachable!()
        };
        assert_eq!(active.acks.len(), 2);
    }
}
