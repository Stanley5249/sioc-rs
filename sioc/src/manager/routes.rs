//! Where the server's packets for each open namespace go.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use bytestring::ByteString;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::packet::{DynAck, ServerPacket};

/// Where the server's packets for each open namespace go.
///
/// A namespace is open exactly while its route is here, so removing the route
/// closes it, whichever side closes. Both loops share the map. Every method
/// locks only for its own body, so the lock is never held across an `.await`.
#[derive(Default)]
pub struct Routes(Mutex<HashMap<ByteString, Route>>);

/// An open namespace.
///
/// Dropping it closes the namespace: the receiver ends, the pending acks fail,
/// and the senders close.
struct Route {
    /// Tells a reopened namespace apart from the one before it.
    generation: u64,
    server_packet_tx: mpsc::Sender<ServerPacket>,
    /// Acks of events sent or waiting in the send buffer, which the server may
    /// answer.
    ack_txs: HashMap<u64, oneshot::Sender<DynAck>>,
    /// Whether the server confirmed the namespace in the current session.
    connected: bool,
    _closed: DropGuard,
}

impl Routes {
    fn lock(&self) -> MutexGuard<'_, HashMap<ByteString, Route>> {
        self.0
            .lock()
            .expect("no Routes method panics while it holds the lock")
    }

    /// Opens a namespace, returning `false` if it is already open.
    pub fn insert(
        &self,
        ns: ByteString,
        generation: u64,
        server_packet_tx: mpsc::Sender<ServerPacket>,
        closed: CancellationToken,
    ) -> bool {
        let mut routes = self.lock();

        if routes.contains_key(&ns) {
            return false;
        }

        let route = Route {
            generation,
            server_packet_tx,
            ack_txs: HashMap::new(),
            connected: false,
            _closed: closed.drop_guard(),
        };
        routes.insert(ns, route);

        true
    }

    /// Closes a namespace for the server, returning its receiver's sender for
    /// one last packet.
    pub fn close(&self, ns: &str) -> Option<mpsc::Sender<ServerPacket>> {
        let route = self.lock().remove(ns)?;

        Some(route.server_packet_tx)
    }

    /// Closes one generation of a namespace for the client, returning `false`
    /// if the server closed it first.
    pub fn close_generation(&self, ns: &str, generation: u64) -> bool {
        let mut routes = self.lock();

        if routes
            .get(ns)
            .is_none_or(|route| route.generation != generation)
        {
            return false;
        }

        routes.remove(ns);

        true
    }

    /// Returns whether this generation of the namespace is still open.
    pub fn is_open(&self, ns: &str, generation: u64) -> bool {
        self.lock()
            .get(ns)
            .is_some_and(|route| route.generation == generation)
    }

    /// Marks a route connected, returning its generation only the first time.
    pub fn mark_connected(&self, ns: &str) -> Option<u64> {
        let mut routes = self.lock();
        let route = routes.get_mut(ns)?;

        (!std::mem::replace(&mut route.connected, true)).then_some(route.generation)
    }

    /// Returns the sender to the namespace's receiver, if the namespace is
    /// open.
    pub fn server_packet_tx(&self, ns: &str) -> Option<mpsc::Sender<ServerPacket>> {
        self.lock()
            .get(ns)
            .map(|route| route.server_packet_tx.clone())
    }

    /// Registers an ack receiver. Without an open route it is dropped, which
    /// fails the [`AckHandle`](crate::ack::AckHandle).
    pub fn register_ack(
        &self,
        ns: &str,
        generation: u64,
        id: u64,
        ack_tx: oneshot::Sender<DynAck>,
    ) {
        if let Some(route) = self
            .lock()
            .get_mut(ns)
            .filter(|route| route.generation == generation)
        {
            route.ack_txs.insert(id, ack_tx);
        }
    }

    /// Removes the ack receiver for `id`, so each ack is delivered at most
    /// once.
    pub fn take_ack(&self, ns: &str, id: u64) -> Option<oneshot::Sender<DynAck>> {
        self.lock().get_mut(ns)?.ack_txs.remove(&id)
    }

    /// Returns whether no namespace is open.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Marks every namespace unconfirmed and fails the acks of sent events,
    /// for when an Engine.IO session ends but the namespaces stay open, like
    /// socket.io-client's `Socket._clearAcks`.
    ///
    /// A connected route flushed its send buffer, so all its acks belong to
    /// sent events. An unconnected route sent nothing since the last session
    /// ended, so all its acks belong to buffered events and keep waiting.
    pub fn end_session(&self) {
        for route in self.lock().values_mut() {
            if route.connected {
                route.ack_txs.clear();
            }

            route.connected = false;
        }
    }

    /// Closes every namespace, for when the client ends.
    pub fn clear(&self) {
        self.lock().clear();
    }
}
