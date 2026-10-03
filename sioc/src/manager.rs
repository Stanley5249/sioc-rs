//! Socket.IO namespace router.
//!
//! Two loops share the work, so neither direction waits on the other:
//! [`server_message`] delivers what the server sends to each namespace, and
//! [`directive`] sends what the namespace handles ask for.

mod directive;
mod server_message;
#[cfg(test)]
mod tests;

use crate::error::ManagerError;
use crate::packet::{Directive, DynAck, Signal};
use bytestring::ByteString;
use eioc::prelude::Message;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use tokio::sync::{mpsc, oneshot};

/// A namespace opened by [`Client::connect`](crate::client::Client::connect).
#[derive(Debug)]
pub(crate) struct NewSocket {
    pub ns: ByteString,
    pub payload: ByteString,
    pub directive_rx: mpsc::Receiver<Directive>,
    pub signal_tx: mpsc::Sender<Signal>,
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
    new_socket_rx: mpsc::Receiver<NewSocket>,
    server_message_rx: mpsc::Receiver<Message>,
    client_message_tx: mpsc::Sender<Message>,
) -> Result<(), ManagerError> {
    let routes = Routes::default();
    let (control_tx, control_rx) = mpsc::unbounded_channel();

    tokio::try_join!(
        server_message::route_server_messages(server_message_rx, &routes, control_tx),
        directive::route_directives(new_socket_rx, control_rx, &routes, client_message_tx),
    )?;

    Ok(())
}

/// What the server-message loop tells the directive loop about a namespace.
///
/// The channel is unbounded so that delivering server packets never waits on
/// the client's sending direction. It stays short because only real state
/// changes of namespaces the client opened travel on it: at most one of each
/// per open.
#[derive(Debug)]
enum Control {
    /// The server confirmed the namespace, so buffered events can go out.
    Connected(ByteString),
    /// The server closed the namespace.
    Disconnected(ByteString),
}

/// Where the server's packets for each open namespace go.
///
/// Both loops share it. Every method locks only for its own body, so the lock
/// is never held across an `.await`.
#[derive(Default)]
struct Routes(Mutex<HashMap<ByteString, Route>>);

struct Route {
    signal_tx: mpsc::Sender<Signal>,
    ack_txs: HashMap<u64, oneshot::Sender<DynAck>>,
    connected: bool,
}

impl Routes {
    fn lock(&self) -> MutexGuard<'_, HashMap<ByteString, Route>> {
        // No critical section can panic halfway, so a poisoned map is still consistent.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a route, returning `false` if the namespace is already open.
    fn insert(&self, ns: ByteString, signal_tx: mpsc::Sender<Signal>) -> bool {
        let mut routes = self.lock();

        if routes.contains_key(&ns) {
            return false;
        }

        let route = Route {
            signal_tx,
            ack_txs: HashMap::new(),
            connected: false,
        };
        routes.insert(ns, route);

        true
    }

    /// Removes a route. Dropping it ends the namespace receiver and fails its pending acks.
    fn remove(&self, ns: &str) -> Option<mpsc::Sender<Signal>> {
        self.lock().remove(ns).map(|route| route.signal_tx)
    }

    /// Marks a route connected, returning `true` only the first time.
    fn connect(&self, ns: &str) -> bool {
        self.lock()
            .get_mut(ns)
            .is_some_and(|route| !std::mem::replace(&mut route.connected, true))
    }

    fn signal_tx(&self, ns: &str) -> Option<mpsc::Sender<Signal>> {
        self.lock().get(ns).map(|route| route.signal_tx.clone())
    }

    /// Registers an ack receiver. Without a route it is dropped, which fails the [`AckHandle`](crate::ack::AckHandle).
    fn register_ack(&self, ns: &str, id: u64, ack_tx: oneshot::Sender<DynAck>) {
        if let Some(route) = self.lock().get_mut(ns) {
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
