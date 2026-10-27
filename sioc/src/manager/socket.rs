//! Persistent per-namespace state, like socket.io-client's `Socket`.
//! A locally disconnecting participation owns its work until submission or
//! cancellation. A replacement waits outside the core for its release.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytestring::ByteString;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::packet::{ClientPacket, DynAck, DynEvent, ServerPacket};

/// A packet and its bounded queue slot, held until Engine submission.
pub type Queued = (ClientPacket, OwnedSemaphorePermit);

/// One persistent namespace, like `Manager.nsps`'s `Socket`.
#[derive(Debug, Default)]
pub struct Socket {
    /// Like `Socket.ids`, never reset by handle replacement or Engine loss.
    next_ack_id: u64,
    pub state: SocketState,
}

/// Whether a name can take new handles.
#[derive(Debug)]
pub enum Availability {
    Free,
    /// The previous participation still flushes. The token is its release.
    Busy(CancellationToken),
    Taken,
}

/// Socket.IO participation lifecycle. Engine lifecycle belongs to the manager.
#[derive(Debug, Default)]
pub enum SocketState {
    #[default]
    Disconnected,
    Connecting {
        handles: Handles,
        send_buffer: VecDeque<Queued>,
    },
    Connected {
        handles: Handles,
        acks: HashMap<u64, oneshot::Sender<DynAck>>,
        connection: CancellationToken,
    },
    /// Admission ended, but accepted packets and then DISCONNECT still belong
    /// to these handles. The name cannot be activated again until they finish.
    Disconnecting {
        handles: Handles,
        acks: HashMap<u64, oneshot::Sender<DynAck>>,
    },
}

/// A CONNECT payload, empty for none. Its `Debug` hides the content, because
/// auth often holds a secret and `Debug` of every handle reaches the core.
#[derive(Clone)]
pub struct Auth(pub ByteString);

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Auth(..)")
    }
}

/// Core-owned ends of one sender/receiver pair.
#[derive(Debug)]
pub struct Handles {
    pub auth: Auth,
    /// Event admission capacity and participation identity.
    pub capacity: Arc<Semaphore>,
    /// Independent reply capacity.
    pub reply_capacity: Arc<Semaphore>,
    pub closed: CancellationToken,
    pub server_packet_tx: mpsc::Sender<ServerPacket>,
    /// Terminal packets bypass a full data queue.
    pub terminal_packet_tx: oneshot::Sender<ServerPacket>,
    pub release: Release,
}

/// Cancels its token when the core drops the handles that own it, on every
/// end and during unwinding, so a waiting connect never outlives the
/// participation it waits for.
#[derive(Debug, Default)]
pub struct Release(pub CancellationToken);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl Handles {
    pub fn owns(&self, slot: &OwnedSemaphorePermit) -> bool {
        Arc::ptr_eq(&self.capacity, slot.semaphore())
            || Arc::ptr_eq(&self.reply_capacity, slot.semaphore())
    }

    fn close_admission(&self) {
        self.capacity.close();
        self.reply_capacity.close();
        self.closed.cancel();
    }

    fn end(self, ns: &str, terminal: Option<ServerPacket>) {
        self.close_admission();
        if let Some(packet) = terminal
            && self.terminal_packet_tx.send(packet).is_err()
        {
            tracing::debug!(%ns, "discarded terminal packet for a dropped receiver");
        }
    }
}

/// One server packet held for its original application receiver.
#[derive(Debug)]
pub struct Blocked {
    pub ns: ByteString,
    pub packet: ServerPacket,
    pub server_packet_tx: mpsc::Sender<ServerPacket>,
    pub closed: CancellationToken,
}

impl Socket {
    /// The current owner, including a participation still flushing.
    pub fn handles(&self) -> Option<&Handles> {
        match &self.state {
            SocketState::Disconnected => None,
            SocketState::Connecting { handles, .. }
            | SocketState::Connected { handles, .. }
            | SocketState::Disconnecting { handles, .. } => Some(handles),
        }
    }

    pub fn availability(&self) -> Availability {
        match &self.state {
            SocketState::Disconnected => Availability::Free,
            SocketState::Disconnecting { handles, .. } => {
                Availability::Busy(handles.release.0.clone())
            }
            SocketState::Connecting { .. } | SocketState::Connected { .. } => Availability::Taken,
        }
    }

    /// Whether live handles need an Engine, rather than just the old flush.
    pub fn needs_engine(&self) -> bool {
        matches!(
            self.state,
            SocketState::Connecting { .. } | SocketState::Connected { .. }
        )
    }

    /// Accepts handles for a name that is free.
    pub fn connect(&mut self, handles: Handles) {
        debug_assert!(matches!(self.state, SocketState::Disconnected));
        self.state = SocketState::Connecting {
            handles,
            send_buffer: VecDeque::new(),
        };
    }

    pub fn receive_connect(&mut self) -> VecDeque<Queued> {
        if !matches!(self.state, SocketState::Connecting { .. }) {
            return VecDeque::new();
        }
        let SocketState::Connecting {
            handles,
            send_buffer,
        } = std::mem::take(&mut self.state)
        else {
            unreachable!("the connecting state was checked before taking it");
        };
        self.state = SocketState::Connected {
            handles,
            acks: HashMap::new(),
            connection: CancellationToken::new(),
        };
        send_buffer
    }

    /// Registers only in the participation that still owns the queued packet.
    pub fn add_ack(&mut self, ack_tx: oneshot::Sender<DynAck>) -> u64 {
        let (SocketState::Connected { acks, .. } | SocketState::Disconnecting { acks, .. }) =
            &mut self.state
        else {
            unreachable!("only a confirmed participation submits events");
        };
        let id = self.next_ack_id;
        self.next_ack_id += 1;
        acks.retain(|_, ack_tx| !ack_tx.is_closed());
        acks.insert(id, ack_tx);
        id
    }

    pub fn receive_ack(&mut self, ns: &str, id: u64, ack: DynAck) {
        let (SocketState::Connected { acks, .. } | SocketState::Disconnecting { acks, .. }) =
            &mut self.state
        else {
            return;
        };
        let Some(ack_tx) = acks.remove(&id) else {
            tracing::debug!(%ns, id, "discarded ack with no pending handle");
            return;
        };
        if ack_tx.send(ack).is_err() {
            tracing::debug!(%ns, id, "discarded ack for a dropped handle");
        }
    }

    /// Delivers to live handles. An event's ack id remembers the connection
    /// it arrived on, so its reply can never reach another one.
    pub fn deliver(&self, ns: &ByteString, mut packet: ServerPacket) -> Option<Blocked> {
        let handles = self
            .handles()
            .filter(|handles| !handles.closed.is_cancelled())?;
        if let (
            ServerPacket::Event(DynEvent { id: Some(id), .. }),
            SocketState::Connected { connection, .. },
        ) = (&mut packet, &self.state)
        {
            id.bind(ns, connection);
        }
        match handles.server_packet_tx.try_send(packet) {
            Ok(()) => None,
            Err(TrySendError::Full(packet)) => Some(Blocked {
                ns: ns.clone(),
                packet,
                server_packet_tx: handles.server_packet_tx.clone(),
                closed: handles.closed.clone(),
            }),
            Err(TrySendError::Closed(_)) => {
                tracing::debug!(%ns, "discarded server packet for a dropped receiver");
                None
            }
        }
    }

    /// Ends only the current owner, which releases its name.
    pub fn end(&mut self, ns: &str, terminal: Option<ServerPacket>) {
        let handles = match std::mem::take(&mut self.state) {
            SocketState::Disconnected => return,
            SocketState::Connecting { handles, .. }
            | SocketState::Disconnecting { handles, .. } => handles,
            SocketState::Connected {
                handles,
                connection,
                ..
            } => {
                connection.cancel();
                handles
            }
        };
        handles.end(ns, terminal);
    }

    pub fn close_connection(&mut self, ns: &str, unsent: VecDeque<Queued>) {
        let state = std::mem::take(&mut self.state);
        self.state = match state {
            SocketState::Connected {
                handles,
                connection,
                ..
            } => {
                connection.cancel();
                SocketState::Connecting {
                    handles,
                    send_buffer: unsent,
                }
            }
            SocketState::Connecting {
                handles,
                mut send_buffer,
            } => {
                for queued in unsent.into_iter().rev() {
                    send_buffer.push_front(queued);
                }
                SocketState::Connecting {
                    handles,
                    send_buffer,
                }
            }
            SocketState::Disconnecting { handles, .. } => {
                handles.end(ns, None);
                SocketState::Disconnected
            }
            SocketState::Disconnected => SocketState::Disconnected,
        };
    }

    /// Closes admission without giving up ownership of accepted work.
    pub fn disconnect(&mut self, ns: &str) -> bool {
        match std::mem::take(&mut self.state) {
            SocketState::Connected {
                handles,
                acks,
                connection,
            } => {
                handles.close_admission();
                connection.cancel();
                self.state = SocketState::Disconnecting { handles, acks };
                true
            }
            SocketState::Connecting { handles, .. } => {
                handles.end(ns, None);
                false
            }
            state => {
                self.state = state;
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adding_an_ack_reclaims_dropped_waiters() {
        let (server_packet_tx, _) = mpsc::channel(1);
        let (terminal_packet_tx, _) = oneshot::channel();
        let mut socket = Socket::default();
        socket.connect(Handles {
            auth: Auth("{}".into()),
            capacity: Arc::new(Semaphore::new(1)),
            reply_capacity: Arc::new(Semaphore::new(1)),
            closed: CancellationToken::new(),
            server_packet_tx,
            terminal_packet_tx,
            release: Release::default(),
        });
        socket.receive_connect();
        let (live_tx, _live_rx) = oneshot::channel();
        socket.add_ack(live_tx);
        for _ in 0..100 {
            let (ack_tx, ack_rx) = oneshot::channel();
            socket.add_ack(ack_tx);
            drop(ack_rx);
        }
        let SocketState::Connected { acks, .. } = &socket.state else {
            unreachable!()
        };
        assert_eq!(acks.len(), 2);
        let (ack_tx, _ack_rx) = oneshot::channel();
        assert_eq!(socket.add_ack(ack_tx), 101);
        let SocketState::Connected { acks, .. } = &socket.state else {
            unreachable!()
        };
        assert_eq!(acks.len(), 2);
    }
}
