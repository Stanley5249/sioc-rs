//! Socket.IO namespace router.
//!
//! Two loops share the work, so neither direction waits on the other:
//! [`server_message`] delivers what the server sends to each namespace, and
//! [`client_packet`] sends what the namespace handles ask for.

mod client_packet;
mod server_message;
#[cfg(test)]
mod tests;

use crate::error::{ManagerError, SocketError};
use crate::packet::{ClientPacket, DynAck, ServerPacket};
use bytestring::ByteString;
use eioc::prelude::Message;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};

/// A namespace opened by [`Client::connect`](crate::client::Client::connect).
#[derive(Debug)]
pub(crate) struct ConnectRequest {
    pub ns: ByteString,
    pub payload: ByteString,
    pub client_packet_rx: mpsc::Receiver<ClientPacket>,
    /// Cancelled once the namespace closes, by either side.
    pub closed: CancellationToken,
    pub server_packet_tx: mpsc::Sender<ServerPacket>,
    pub reply_tx: oneshot::Sender<Result<(), SocketError>>,
}

/// The client packet sender of a namespace, as every [`SocketSender`](crate::client::SocketSender) clone holds it.
///
/// Every clone shares the `closed` token. Once it is cancelled, sends fail and
/// the manager closes the channel, then drains the packets sent before.
#[derive(Clone, Debug)]
pub(crate) struct ClientPacketTx {
    client_packet_tx: mpsc::Sender<ClientPacket>,
    closed: CancellationToken,
}

impl ClientPacketTx {
    pub fn new(client_packet_tx: mpsc::Sender<ClientPacket>, closed: CancellationToken) -> Self {
        Self {
            client_packet_tx,
            closed,
        }
    }

    /// Sends a client packet, failing once the namespace has closed.
    pub async fn send(&self, client_packet: ClientPacket) -> Result<(), SocketError> {
        if self.closed.is_cancelled() {
            return Err(SocketError::Closed);
        }

        self.client_packet_tx
            .send(client_packet)
            .await
            .map_err(|_| SocketError::Closed)
    }

    /// Closes the namespace for every clone. Closing again does nothing.
    pub fn close(&self) {
        self.closed.cancel();
    }

    /// Waits until the namespace closes, by either side.
    pub async fn closed(&self) {
        self.closed.cancelled().await;
    }
}

/// Routes packets between the namespace handles and the engine until the session ends.
///
/// Closes the session by dropping `client_message_tx` once the client handle
/// and every namespace are gone, then returns when the engine closes
/// `server_message_rx`.
///
/// # Errors
///
/// Returns an error if the engine channel closes early or the server breaks the protocol.
pub(crate) async fn run(
    connect_request_rx: mpsc::Receiver<ConnectRequest>,
    server_message_rx: mpsc::Receiver<Message>,
    client_message_tx: mpsc::Sender<Message>,
) -> Result<(), ManagerError> {
    let routes = Routes::default();

    // The server-message loop tells the client-packet loop which namespace
    // generations the server confirmed, so their buffered events can go out.
    // The channel is unbounded so that delivering server packets never waits on
    // the client's sending direction. It stays short because each generation
    // travels at most once.
    let (connected_generation_tx, connected_generation_rx) = mpsc::unbounded_channel();

    tokio::try_join!(
        server_message::server_messages_to_packets(
            server_message_rx,
            &routes,
            connected_generation_tx
        ),
        client_packet::client_packets_to_messages(
            connect_request_rx,
            connected_generation_rx,
            &routes,
            client_message_tx
        ),
    )?;

    Ok(())
}

/// Where the server's packets for each open namespace go.
///
/// A namespace is open exactly while its route is here, so removing the route
/// closes it, whichever side closes. Both loops share the map. Every method
/// locks only for its own body, so the lock is never held across an `.await`.
#[derive(Default)]
struct Routes(Mutex<HashMap<ByteString, Route>>);

/// An open namespace.
///
/// Dropping it closes the namespace: the receiver ends, the pending acks fail,
/// and the senders close.
struct Route {
    /// Tells a reopened namespace apart from the one before it.
    generation: u64,
    server_packet_tx: mpsc::Sender<ServerPacket>,
    ack_txs: HashMap<u64, oneshot::Sender<DynAck>>,
    connected: bool,
    _closed: DropGuard,
}

impl Routes {
    fn lock(&self) -> MutexGuard<'_, HashMap<ByteString, Route>> {
        // No critical section can panic halfway, so a poisoned map is still consistent.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Opens a namespace, returning `false` if it is already open.
    fn insert(
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
    fn close(&self, ns: &str) -> Option<mpsc::Sender<ServerPacket>> {
        let route = self.lock().remove(ns)?;

        Some(route.server_packet_tx)
    }

    /// Closes one generation of a namespace for the client, returning `false`
    /// if the server closed it first.
    fn close_generation(&self, ns: &str, generation: u64) -> bool {
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

    fn is_open(&self, ns: &str, generation: u64) -> bool {
        self.lock()
            .get(ns)
            .is_some_and(|route| route.generation == generation)
    }

    /// Marks a route connected, returning its generation only the first time.
    fn mark_connected(&self, ns: &str) -> Option<u64> {
        let mut routes = self.lock();
        let route = routes.get_mut(ns)?;

        (!std::mem::replace(&mut route.connected, true)).then_some(route.generation)
    }

    fn server_packet_tx(&self, ns: &str) -> Option<mpsc::Sender<ServerPacket>> {
        self.lock()
            .get(ns)
            .map(|route| route.server_packet_tx.clone())
    }

    /// Registers an ack receiver. Without an open route it is dropped, which fails the [`AckHandle`](crate::ack::AckHandle).
    fn register_ack(&self, ns: &str, generation: u64, id: u64, ack_tx: oneshot::Sender<DynAck>) {
        if let Some(route) = self
            .lock()
            .get_mut(ns)
            .filter(|route| route.generation == generation)
        {
            route.ack_txs.insert(id, ack_tx);
        }
    }

    fn take_ack(&self, ns: &str, id: u64) -> Option<oneshot::Sender<DynAck>> {
        self.lock().get_mut(ns)?.ack_txs.remove(&id)
    }

    fn clear(&self) {
        self.lock().clear();
    }
}
