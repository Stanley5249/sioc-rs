//! Every namespace's state, shared by both loops.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use bytestring::ByteString;
use eioc::prelude::Message;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::SocketError;
use crate::manager::connect_request::ConnectRequest;
use crate::packet::{ClientPacket, DynAck, ServerPacket};

/// Every namespace name the client has opened, like socket.io-client's
/// `Manager.nsps`, which keeps one `Socket` per name and never prunes it.
///
/// Both loops share the map. Every method locks only for its own body, so the
/// lock is never held across an `.await`.
#[derive(Default)]
pub struct Namespaces(Mutex<HashMap<ByteString, Namespace>>);

/// One namespace name.
struct Namespace {
    /// The next ack id, kept across reopens like socket.io-client's
    /// `Socket.ids`, so a late ack for a closed namespace never answers an
    /// event of the reopened one.
    next_ack_id: u64,
    state: NamespaceState,
}

enum NamespaceState {
    /// No handles are left.
    Closed,
    /// The handles are live.
    Open(OpenNamespace),
    /// The namespace closed, but its old handles still drain the packets they
    /// sent. Holds at most one request to open the name again, which waits
    /// until then.
    Draining(Option<ConnectRequest>),
}

struct OpenNamespace {
    /// The JSON auth payload, sent with the CONNECT packet to every engine.
    auth: ByteString,
    confirmation: Confirmation,
    server_packet_tx: mpsc::Sender<ServerPacket>,
    /// Taken by the server's one terminal packet.
    terminal_packet_tx: Option<oneshot::Sender<ServerPacket>>,
    closed: CancellationToken,
    /// Acks of events sent or waiting in the send buffer, which the server may
    /// answer.
    ack_txs: HashMap<u64, oneshot::Sender<DynAck>>,
    /// Events held until the server confirms the namespace.
    buffered_messages_rx: mpsc::Receiver<Vec<Message>>,
}

impl Drop for OpenNamespace {
    fn drop(&mut self) {
        // The handles share this token, so leaving `Open` closes them.
        self.closed.cancel();
    }
}

/// How far the open engine has confirmed a namespace.
///
/// socket.io-client's `Socket.connected` covers both steps, because its
/// `onconnect` flushes the send buffer at once. Here the server-message loop
/// sees the CONNECT before the client-packet loop flushes, so events keep
/// waiting behind the buffered ones until then.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Confirmation {
    /// The server has not confirmed the namespace to the open engine.
    Unconfirmed,
    /// The server confirmed the namespace, but the send buffer still holds
    /// events.
    Confirmed,
    /// The send buffer is empty, so events go straight to the engine.
    Flushed,
}

/// The client-packet loop's end of a namespace's channels.
#[derive(Debug)]
pub struct ClientEnd {
    pub ns: ByteString,
    /// What the namespace's [`SocketSender`](crate::client::SocketSender)
    /// clones send.
    pub client_packet_rx: mpsc::Receiver<ClientPacket>,
    /// Cancelled once the namespace closes, by either side.
    pub closed: CancellationToken,
    /// Holds events until the server confirms the namespace.
    pub buffered_messages_tx: mpsc::Sender<Vec<Message>>,
}

impl Namespaces {
    fn lock(&self) -> MutexGuard<'_, HashMap<ByteString, Namespace>> {
        self.0
            .lock()
            .expect("no Namespaces method panics while it holds the lock")
    }

    /// Opens a namespace and reports the result to the caller, like
    /// socket.io-client's `Manager.socket` and `Socket.connect`.
    ///
    /// Returns the client end if the name was closed. Holds the request while
    /// the old handles of the name drain, and refuses it while the name is open
    /// or already has a request waiting.
    pub fn open(&self, request: ConnectRequest, buffer_capacity: usize) -> Option<ClientEnd> {
        let mut namespaces = self.lock();
        let namespace = namespaces.entry(request.ns.clone()).or_insert(Namespace {
            next_ack_id: 0,
            state: NamespaceState::Closed,
        });

        match &mut namespace.state {
            NamespaceState::Closed => {}
            NamespaceState::Draining(reopen @ None) => {
                *reopen = Some(request);
                return None;
            }
            NamespaceState::Open(_) | NamespaceState::Draining(Some(_)) => {
                let ns = request.ns;

                // A cancelled connect drops its reply receiver.
                let _ = request
                    .reply_tx
                    .send(Err(SocketError::NamespaceConflict { ns }));

                return None;
            }
        }

        let ConnectRequest {
            ns,
            payload,
            client_packet_rx,
            closed,
            server_packet_tx,
            terminal_packet_tx,
            reply_tx,
        } = request;

        let (buffered_messages_tx, buffered_messages_rx) = mpsc::channel(buffer_capacity);

        namespace.state = NamespaceState::Open(OpenNamespace {
            auth: payload,
            confirmation: Confirmation::Unconfirmed,
            server_packet_tx,
            terminal_packet_tx: Some(terminal_packet_tx),
            closed: closed.clone(),
            ack_txs: HashMap::new(),
            buffered_messages_rx,
        });

        // The caller may have cancelled connect while the request was queued.
        let _ = reply_tx.send(Ok(()));

        Some(ClientEnd {
            ns,
            client_packet_rx,
            closed,
            buffered_messages_tx,
        })
    }

    /// Closes a namespace for the server and delivers its terminal packet, a
    /// `DISCONNECT` or a `CONNECT_ERROR`. The old handles drain next.
    pub fn close(&self, ns: &str, terminal_packet: ServerPacket) {
        let mut namespaces = self.lock();
        let Some(namespace) = namespaces
            .get_mut(ns)
            .filter(|namespace| matches!(namespace.state, NamespaceState::Open(_)))
        else {
            tracing::debug!(%ns, "discarded terminal packet for a closed namespace");
            return;
        };

        let NamespaceState::Open(mut open) =
            std::mem::replace(&mut namespace.state, NamespaceState::Draining(None))
        else {
            unreachable!("the filter above keeps only open namespaces");
        };

        let terminal_packet_tx = open
            .terminal_packet_tx
            .take()
            .expect("only closing a namespace takes its terminal packet sender");

        if terminal_packet_tx.send(terminal_packet).is_err() {
            tracing::debug!(%ns, "discarded terminal packet for a dropped receiver");
        }
    }

    /// Closes a namespace once its handles end, like socket.io-client's
    /// `Socket.disconnect`.
    ///
    /// Returns whether the server confirmed the namespace, so that the caller
    /// sends DISCONNECT, and the request waiting to open the name again.
    ///
    /// # Panics
    ///
    /// Panics if the namespace is closed, which live handles rule out.
    pub fn close_client(&self, ns: &str) -> (bool, Option<ConnectRequest>) {
        let mut namespaces = self.lock();
        let namespace = namespaces
            .get_mut(ns)
            .expect("a namespace with live handles has an entry");

        match std::mem::replace(&mut namespace.state, NamespaceState::Closed) {
            NamespaceState::Open(open) => (open.confirmation != Confirmation::Unconfirmed, None),
            NamespaceState::Draining(reopen) => (false, reopen),
            NamespaceState::Closed => unreachable!("a namespace with live handles is not closed"),
        }
    }

    /// Closes every open namespace without a terminal packet, for when
    /// reconnection gives up or the client ends.
    pub fn close_all(&self) {
        for namespace in self.lock().values_mut() {
            if let NamespaceState::Open(_) = namespace.state {
                namespace.state = NamespaceState::Draining(None);
            }
        }
    }

    /// Marks a namespace confirmed, returning `true` only the first time per
    /// engine.
    pub fn mark_connected(&self, ns: &str) -> bool {
        self.with_open(ns, |open| {
            let first = open.confirmation == Confirmation::Unconfirmed;
            if first {
                open.confirmation = Confirmation::Confirmed;
            }
            first
        })
        .unwrap_or(false)
    }

    /// Takes the events buffered for a confirmed namespace and marks it
    /// flushed. Returns `None` for any other namespace, such as one reopened
    /// after a late report.
    pub fn flush(&self, ns: &str) -> Option<Vec<Message>> {
        self.with_open(ns, |open| {
            if open.confirmation != Confirmation::Confirmed {
                return None;
            }

            open.confirmation = Confirmation::Flushed;

            let mut messages = Vec::new();
            while let Ok(buffered) = open.buffered_messages_rx.try_recv() {
                messages.extend(buffered);
            }
            Some(messages)
        })
        .flatten()
    }

    /// Returns whether the namespace is open.
    pub fn is_open(&self, ns: &str) -> bool {
        self.with_open(ns, |_| ()).is_some()
    }

    /// Returns whether the server confirmed the namespace to the open engine.
    pub fn is_confirmed(&self, ns: &str) -> bool {
        self.with_open(ns, |open| open.confirmation != Confirmation::Unconfirmed)
            .unwrap_or(false)
    }

    /// Returns whether events for the namespace go straight to the engine.
    pub fn is_flushed(&self, ns: &str) -> bool {
        self.with_open(ns, |open| open.confirmation == Confirmation::Flushed)
            .unwrap_or(false)
    }

    /// Returns whether any namespace is open.
    pub fn any_open(&self) -> bool {
        self.lock()
            .values()
            .any(|namespace| matches!(namespace.state, NamespaceState::Open(_)))
    }

    /// Returns the name and auth payload of every open namespace, for the
    /// CONNECT packets of a new engine.
    pub fn auths(&self) -> Vec<(ByteString, ByteString)> {
        self.lock()
            .iter()
            .filter_map(|(ns, namespace)| match &namespace.state {
                NamespaceState::Open(open) => Some((ns.clone(), open.auth.clone())),
                NamespaceState::Closed | NamespaceState::Draining(_) => None,
            })
            .collect()
    }

    /// Returns the sender to the namespace's receiver, if the namespace is
    /// open.
    pub fn server_packet_tx(
        &self,
        ns: &str,
    ) -> Option<(mpsc::Sender<ServerPacket>, CancellationToken)> {
        self.with_open(ns, |open| {
            (open.server_packet_tx.clone(), open.closed.clone())
        })
    }

    /// Assigns the next ack id of the namespace and registers the ack sender
    /// under it. Without an open namespace the sender is dropped, which fails
    /// the [`AckHandle`](crate::ack::AckHandle).
    ///
    /// # Panics
    ///
    /// Panics if the namespace has no entry, which live handles rule out.
    pub fn register_ack(&self, ns: &str, ack_tx: oneshot::Sender<DynAck>) -> u64 {
        let mut namespaces = self.lock();
        let namespace = namespaces
            .get_mut(ns)
            .expect("a namespace with live handles has an entry");

        let id = namespace.next_ack_id;
        namespace.next_ack_id += 1;

        if let NamespaceState::Open(open) = &mut namespace.state {
            // Dropping or timing out a handle closes its receiver. Reclaim
            // those registrations before adding work, so repeated unanswered
            // requests retain only live waiters and the newest batch.
            open.ack_txs.retain(|_, ack_tx| !ack_tx.is_closed());
            if !ack_tx.is_closed() {
                open.ack_txs.insert(id, ack_tx);
            }
        }

        id
    }

    /// Removes the pending ack sender for `id`, so each ack is delivered at
    /// most once.
    pub fn take_ack(&self, ns: &str, id: u64) -> Option<oneshot::Sender<DynAck>> {
        self.with_open(ns, |open| open.ack_txs.remove(&id))
            .flatten()
    }

    /// Marks every namespace unconfirmed and fails the acks of sent events,
    /// for when an engine closes but the namespaces stay open, like
    /// socket.io-client's `Socket._clearAcks`.
    ///
    /// A confirmed namespace flushed its send buffer, so all its acks belong to
    /// sent events. An unconfirmed one sent nothing since the last engine
    /// closed, so all its acks belong to buffered events and keep waiting.
    pub fn clear_acks(&self) {
        for namespace in self.lock().values_mut() {
            if let NamespaceState::Open(open) = &mut namespace.state {
                if open.confirmation != Confirmation::Unconfirmed {
                    open.ack_txs.clear();
                }

                open.confirmation = Confirmation::Unconfirmed;
            }
        }
    }

    /// Runs `f` on the namespace if it is open.
    fn with_open<T>(&self, ns: &str, f: impl FnOnce(&mut OpenNamespace) -> T) -> Option<T> {
        match &mut self.lock().get_mut(ns)?.state {
            NamespaceState::Open(open) => Some(f(open)),
            NamespaceState::Closed | NamespaceState::Draining(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot::error::TryRecvError;

    use super::*;

    fn connect_request(
        ns: &str,
    ) -> (
        ConnectRequest,
        crate::manager::connect_request::ConnectHandles,
    ) {
        ConnectRequest::new(ns.into(), ByteString::new(), 1, 1)
    }

    #[test]
    fn open_takes_a_free_name() {
        let namespaces = Namespaces::default();
        let (request, mut handles) = connect_request("/");
        assert!(namespaces.open(request, 1).is_some());
        assert!(namespaces.is_open("/"));
        assert!(matches!(handles.reply_rx.try_recv(), Ok(Ok(()))));
    }

    #[test]
    fn open_refuses_an_open_name() {
        let namespaces = Namespaces::default();
        let (request, _handles) = connect_request("/");
        let _client_end = namespaces.open(request, 1).unwrap();

        let (request, mut handles) = connect_request("/");
        assert!(namespaces.open(request, 1).is_none());
        assert!(matches!(
            handles.reply_rx.try_recv(),
            Ok(Err(SocketError::NamespaceConflict { .. }))
        ));
    }

    #[test]
    fn open_holds_one_request_while_the_old_handles_drain() {
        let namespaces = Namespaces::default();
        let (request, _handles) = connect_request("/");
        let _client_end = namespaces.open(request, 1).unwrap();
        namespaces.close("/", ServerPacket::Disconnect);

        let (request, mut waiting) = connect_request("/");
        assert!(namespaces.open(request, 1).is_none());
        assert!(matches!(
            waiting.reply_rx.try_recv(),
            Err(TryRecvError::Empty)
        ));

        let (request, mut refused) = connect_request("/");
        assert!(namespaces.open(request, 1).is_none());
        assert!(matches!(
            refused.reply_rx.try_recv(),
            Ok(Err(SocketError::NamespaceConflict { .. }))
        ));

        let (disconnect, reopen) = namespaces.close_client("/");
        assert!(!disconnect);
        assert!(namespaces.open(reopen.unwrap(), 1).is_some());
        assert!(matches!(waiting.reply_rx.try_recv(), Ok(Ok(()))));
    }

    #[test]
    fn ack_ids_continue_after_a_reopen() {
        let namespaces = Namespaces::default();
        let (request, _handles) = connect_request("/");
        let _client_end = namespaces.open(request, 1).unwrap();
        assert_eq!(namespaces.register_ack("/", oneshot::channel().0), 0);
        namespaces.close_client("/");

        let (request, _handles) = connect_request("/");
        let _client_end = namespaces.open(request, 1).unwrap();
        assert_eq!(namespaces.register_ack("/", oneshot::channel().0), 1);
    }

    #[test]
    fn flush_waits_for_confirmation_and_runs_once() {
        let namespaces = Namespaces::default();
        let (request, _handles) = connect_request("/");
        let client_end = namespaces.open(request, 1).unwrap();
        client_end
            .buffered_messages_tx
            .try_send(vec![Message::Text("buffered".into())])
            .unwrap();

        assert!(namespaces.flush("/").is_none());
        assert!(namespaces.mark_connected("/"));
        assert!(!namespaces.mark_connected("/"));
        assert!(!namespaces.is_flushed("/"));
        assert_eq!(
            namespaces.flush("/").unwrap(),
            [Message::Text("buffered".into())]
        );
        assert!(namespaces.is_flushed("/"));
        assert!(namespaces.flush("/").is_none());
    }

    #[test]
    fn registering_acks_reclaims_cancelled_waiters() {
        let namespaces = Namespaces::default();
        let (request, _handles) = connect_request("/");
        let _client_end = namespaces.open(request, 1).unwrap();

        let (live_tx, live_rx) = oneshot::channel();
        assert_eq!(namespaces.register_ack("/", live_tx), 0);
        for _ in 1..=100 {
            let (ack_tx, ack_rx) = oneshot::channel();
            namespaces.register_ack("/", ack_tx);
            drop(ack_rx);
        }
        assert_eq!(
            namespaces.with_open("/", |open| open.ack_txs.len()),
            Some(2)
        );
        namespaces
            .take_ack("/", 0)
            .unwrap()
            .send(DynAck::new("[]"))
            .unwrap();
        assert_eq!(&*live_rx.blocking_recv().unwrap().payload, "[]");
    }
}
