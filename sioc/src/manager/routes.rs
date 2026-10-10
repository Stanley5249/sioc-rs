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
    server_packet_tx: mpsc::Sender<ServerPacket>,
    terminal_packet_tx: oneshot::Sender<ServerPacket>,
    closed: CancellationToken,
    /// Acks of events sent or waiting in the send buffer, which the server may
    /// answer.
    ack_txs: HashMap<u64, oneshot::Sender<DynAck>>,
    /// Whether the server confirmed the namespace to the current engine.
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
        server_packet_tx: mpsc::Sender<ServerPacket>,
        terminal_packet_tx: oneshot::Sender<ServerPacket>,
        closed: CancellationToken,
    ) -> bool {
        let mut routes = self.lock();

        if routes.contains_key(&ns) {
            return false;
        }

        let route = Route {
            server_packet_tx,
            terminal_packet_tx,
            closed: closed.clone(),
            ack_txs: HashMap::new(),
            connected: false,
            _closed: closed.drop_guard(),
        };
        routes.insert(ns, route);

        true
    }

    /// Closes a namespace for the server, returning its terminal packet sender.
    pub fn close(&self, ns: &str) -> Option<oneshot::Sender<ServerPacket>> {
        let route = self.lock().remove(ns)?;
        Some(route.terminal_packet_tx)
    }

    /// Closes a namespace for the client, returning whether the server
    /// confirmed it to the current engine, or `None` if the server closed it
    /// first.
    pub fn close_client(&self, ns: &str) -> Option<bool> {
        self.lock().remove(ns).map(|route| route.connected)
    }

    /// Returns whether the namespace is open.
    pub fn is_open(&self, ns: &str) -> bool {
        self.lock().contains_key(ns)
    }

    /// Returns whether the server confirmed the namespace to the current
    /// engine.
    pub fn is_connected(&self, ns: &str) -> bool {
        self.lock().get(ns).is_some_and(|route| route.connected)
    }

    /// Marks a route connected, returning `true` only the first time.
    pub fn mark_connected(&self, ns: &str) -> bool {
        self.lock()
            .get_mut(ns)
            .is_some_and(|route| !std::mem::replace(&mut route.connected, true))
    }

    /// Returns the sender to the namespace's receiver, if the namespace is
    /// open.
    pub fn server_packet_tx(
        &self,
        ns: &str,
    ) -> Option<(mpsc::Sender<ServerPacket>, CancellationToken)> {
        self.lock()
            .get(ns)
            .map(|route| (route.server_packet_tx.clone(), route.closed.clone()))
    }

    /// Registers a pending ack sender. Without an open route it is dropped,
    /// which fails the [`AckHandle`](crate::ack::AckHandle).
    pub fn register_ack(&self, ns: &str, id: u64, ack_tx: oneshot::Sender<DynAck>) {
        if let Some(route) = self.lock().get_mut(ns) {
            // Dropping or timing out a handle closes its receiver. Reclaim
            // those registrations before adding work, so repeated unanswered
            // requests retain only live waiters and the newest batch.
            route.ack_txs.retain(|_, sender| !sender.is_closed());
            if !ack_tx.is_closed() {
                route.ack_txs.insert(id, ack_tx);
            }
        }
    }

    /// Removes the pending ack sender for `id`, so each ack is delivered at
    /// most once.
    pub fn take_ack(&self, ns: &str, id: u64) -> Option<oneshot::Sender<DynAck>> {
        self.lock().get_mut(ns)?.ack_txs.remove(&id)
    }

    /// Returns whether no namespace is open.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Marks every namespace unconfirmed and fails the acks of sent events,
    /// for when an engine closes but the namespaces stay open, like
    /// socket.io-client's `Socket._clearAcks`.
    ///
    /// A connected route flushed its send buffer, so all its acks belong to
    /// sent events. An unconnected route sent nothing since the last engine
    /// closed, so all its acks belong to buffered events and keep waiting.
    pub fn clear_acks(&self) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registering_acks_reclaims_cancelled_waiters() {
        let routes = Routes::default();
        let (server_packet_tx, _server_packet_rx) = mpsc::channel(1);
        let (terminal_packet_tx, _terminal_packet_rx) = oneshot::channel();
        assert!(routes.insert(
            "/".into(),
            server_packet_tx,
            terminal_packet_tx,
            CancellationToken::new()
        ));

        let (live_tx, live_rx) = oneshot::channel();
        routes.register_ack("/", 0, live_tx);
        for id in 1..=100 {
            let (ack_tx, ack_rx) = oneshot::channel();
            routes.register_ack("/", id, ack_tx);
            drop(ack_rx);
        }
        assert_eq!(routes.lock()["/"].ack_txs.len(), 2);
        routes
            .take_ack("/", 0)
            .unwrap()
            .send(DynAck::new("[]"))
            .unwrap();
        assert_eq!(&*live_rx.blocking_recv().unwrap().payload, "[]");
    }
}
