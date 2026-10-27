//! Namespace I/O, like the send and receive directions of socket.io-client's
//! `Socket`. Both handles stay together so their fields and construction
//! remain private.

use std::sync::Arc;

use bytestring::ByteString;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::ack::Acknowledge;
use crate::config::ChannelConfig;
use crate::error::SocketError;
use crate::event::Emit;
use crate::manager::driver::Manager;
use crate::manager::socket::{Auth, Handles, Release};
use crate::marker::{AckId, AckMarker, AckType, BinaryMarker};
use crate::packet::{ClientPacket, DynEvent, ServerPacket};

/// Sender for a Socket.IO namespace, like `Socket.emit` and `Socket.ack`.
/// Each call runs one transition of the shared manager core.
///
/// Clone it to emit from several tasks: clones share the namespace.
/// Disconnecting any clone closes all of them, and dropping the last clone
/// also disconnects the namespace.
#[derive(Clone, Debug)]
pub struct SocketSender {
    shared: Arc<SenderShared>,
}

/// What every clone of one [`SocketSender`] shares. Dropping the last clone
/// disconnects the namespace.
#[derive(Debug)]
struct SenderShared {
    manager: Arc<Manager>,
    ns: ByteString,
    /// The namespace's event capacity, which the manager closes when the
    /// namespace ends, and which identifies these handles.
    capacity: Arc<Semaphore>,
    /// The namespace's ACK reply capacity.
    reply_capacity: Arc<Semaphore>,
    closed: CancellationToken,
}

impl Drop for SenderShared {
    fn drop(&mut self) {
        self.manager
            .with_core(|core| core.disconnect(&self.ns, &self.capacity));
    }
}

/// Opens `ns` on `manager` and returns its handles, like `Manager.socket`
/// followed by `Socket.connect`. `channels.sender` bounds the queued events
/// and, separately, the queued replies, and `channels.receiver` the
/// receiver.
///
/// While the previous handles of `ns` still flush, waits for their release
/// and tries again. Cancel safe: a waiting call holds only a token clone.
///
/// # Errors
///
/// Returns [`SocketError::NamespaceConflict`] while live handles hold the
/// name, or [`SocketError::Closed`] once the manager has ended.
pub(crate) async fn connect(
    manager: &Arc<Manager>,
    ns: ByteString,
    auth: ByteString,
    channels: ChannelConfig,
) -> Result<(SocketSender, SocketReceiver), SocketError> {
    loop {
        match try_connect(manager, ns.clone(), auth.clone(), channels)? {
            Ok(handles) => return Ok(handles),
            Err(release) => release.cancelled().await,
        }
    }
}

/// One attempt of [`connect`]. Returns the previous participation's release
/// token while it still flushes.
fn try_connect(
    manager: &Arc<Manager>,
    ns: ByteString,
    auth: ByteString,
    channels: ChannelConfig,
) -> Result<Result<(SocketSender, SocketReceiver), CancellationToken>, SocketError> {
    let capacity = Arc::new(Semaphore::new(channels.sender));
    let reply_capacity = Arc::new(Semaphore::new(channels.sender));
    let closed = CancellationToken::new();
    let (server_packet_tx, server_packet_rx) = mpsc::channel(channels.receiver);
    let (terminal_packet_tx, terminal_packet_rx) = oneshot::channel();
    let handles = Handles {
        auth: Auth(auth),
        capacity: Arc::clone(&capacity),
        reply_capacity: Arc::clone(&reply_capacity),
        closed: closed.clone(),
        server_packet_tx,
        terminal_packet_tx,
        release: Release::default(),
    };
    if let Some(busy) = manager
        .with_core(|core| core.connect(ns.clone(), handles))
        .ok_or(SocketError::Closed)??
    {
        return Ok(Err(busy));
    }

    let sender = SocketSender {
        shared: Arc::new(SenderShared {
            manager: Arc::clone(manager),
            ns,
            capacity,
            reply_capacity,
            closed,
        }),
    };
    let receiver = SocketReceiver {
        server_packet_rx,
        terminal_packet_rx: Some(terminal_packet_rx),
    };
    Ok(Ok((sender, receiver)))
}

impl SocketSender {
    /// Sends one raw client packet; [`emit`](Self::emit) and
    /// [`acknowledge`](Self::acknowledge) build theirs and call this.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError::AttachmentCount`] if a binary packet has zero or
    /// more than [`MAX_ATTACHMENTS`](crate::binary::MAX_ATTACHMENTS)
    /// attachments, or [`SocketError::Closed`] once the namespace is closed
    /// or, for an ack, once the connection that delivered its event ended.
    pub async fn send(&self, packet: ClientPacket) -> Result<(), SocketError> {
        let SenderShared {
            manager,
            ns,
            capacity,
            reply_capacity,
            closed,
        } = &*self.shared;
        if closed.is_cancelled() {
            return Err(SocketError::Closed);
        }

        let attachments = match &packet {
            ClientPacket::Event { attachments, .. } | ClientPacket::Ack { attachments, .. } => {
                attachments
            }
        };
        if let Some(attachments) = attachments {
            let count = attachments.len();
            if !(1..=crate::binary::MAX_ATTACHMENTS).contains(&count) {
                return Err(SocketError::AttachmentCount { count });
            }
        }

        if let ClientPacket::Ack { id, .. } = &packet {
            // A reply belongs to the connection that delivered its event, and
            // the core checks that again once it holds a slot.
            if id
                .connection(ns)
                .is_none_or(CancellationToken::is_cancelled)
            {
                tracing::debug!(%ns, id = id.get(), "rejected ack for an ended connection");
                return Err(SocketError::Closed);
            }
            // Ending the connection never strands this wait: engine loss
            // discards the queued replies, which returns their slots, and
            // ending the handles closes the capacity.
            let slot = Arc::clone(reply_capacity)
                .acquire_owned()
                .await
                .map_err(|_| SocketError::Closed)?;
            manager
                .with_core(|core| core.acknowledge(ns, packet, slot))
                .ok_or(SocketError::Closed)?
        } else {
            let slot = Arc::clone(capacity)
                .acquire_owned()
                .await
                .map_err(|_| SocketError::Closed)?;
            manager
                .with_core(|core| core.emit(ns, packet, slot))
                .ok_or(SocketError::Closed)?
        }
    }
}

impl SocketSender {
    /// Emits an event; returns `()` or an [`AckHandle`](crate::ack::AckHandle)
    /// depending on the ack policy.
    ///
    /// The event waits in the core until the server confirms the namespace,
    /// also after a reconnection. A semaphore bounds accepted, unsubmitted
    /// events to [`ChannelConfig::sender`](crate::config::ChannelConfig::sender)
    /// packets, so emitting awaits space when full. Successful emission means
    /// acceptance, not Engine submission or delivery to the server.
    /// The [`AckHandle`](crate::ack::AckHandle) fails if the Engine.IO session
    /// drops after submission, like socket.io-client's `Socket._clearAcks`.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails, or any error from
    /// [`send`](Self::send).
    pub async fn emit<E, A, B>(&self, event: E) -> Result<E::Output, SocketError>
    where
        E: Emit<A, B>,
        A: AckMarker,
        B: BinaryMarker,
    {
        let (client_packet, output) = event.prepare()?;
        self.send(client_packet).await?;
        Ok(output)
    }

    /// Acknowledges a received event.
    ///
    /// The reply goes only to the namespace connection that delivered the
    /// event. If that connection has ended, because the Engine.IO session
    /// dropped, the namespace disconnected, or new handles replaced these,
    /// the call fails with [`SocketError::Closed`] while the namespace may
    /// still be open, so a receive loop that propagates this error with `?`
    /// ends after a reconnection. socket.io-client's ack callback instead
    /// writes to whatever engine is current, but a server may count ack ids
    /// per connection, so that reply could answer a different request.
    ///
    /// A successful call means the reply was accepted, not delivered. Replies
    /// use separate bounded capacity, and accepted replies are discarded when
    /// their engine ends before they leave.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails, or any error from
    /// [`send`](Self::send).
    pub async fn acknowledge<T, A, B>(&self, id: AckId<A>, payload: T) -> Result<(), SocketError>
    where
        T: Acknowledge<A, B>,
        A: AckType,
        B: BinaryMarker,
    {
        let client_packet = payload.into_client_packet(id)?;
        self.send(client_packet).await
    }

    /// Disconnects the namespace for every clone.
    ///
    /// Once the server has confirmed the namespace, events and acks sent
    /// before the call are submitted to Engine in order, then DISCONNECT.
    /// Server termination or Engine loss cancels the remaining local flush.
    /// Before confirmation, buffered events are discarded and no DISCONNECT
    /// goes out, because socket.io-client's `Socket.disconnect` sends one
    /// only to a connected namespace. Later sends fail with
    /// [`SocketError::Closed`]. Calling it after the namespace has closed,
    /// by either side, does nothing.
    pub fn disconnect(&self) {
        let SenderShared {
            manager,
            ns,
            capacity,
            closed,
            ..
        } = &*self.shared;
        manager.with_core(|core| core.disconnect(ns, capacity));
        // The manager cancels it too, unless a panic already ended it.
        closed.cancel();
    }

    /// Waits until these handles stop accepting packets for good, like
    /// socket.io-client's `Socket.active` turning false.
    ///
    /// Resolves after [`disconnect`](Self::disconnect) or the drop of the last
    /// clone, a server `DISCONNECT` or `CONNECT_ERROR`, reconnection giving up
    /// or being off, or the end of the client. An Engine.IO session that
    /// drops and reconnects does not resolve it. It resolves as soon as
    /// admission closes, so accepted packets and DISCONNECT may still be on
    /// their way to Engine.
    pub async fn closed(&self) {
        self.shared.closed.cancelled().await;
    }
}

/// Receiver for a Socket.IO namespace, like `Socket.emitEvent` listeners.
/// It receives packets from the manager task.
///
/// Read it continuously: bounded queues apply backpressure to the server.
///
/// # Backpressure
///
/// socket.io-client runs handlers as packets arrive and has no
/// limit. Here, once this receiver holds
/// [`ChannelConfig::receiver`](crate::config::ChannelConfig::receiver)
/// packets, the client holds the next one and reads nothing else from the
/// connection until this receiver takes it. Nothing is lost while the
/// Engine.IO session lives, but meanwhile:
///
/// - Every namespace of the same [`Client`](crate::client::Client) waits,
///   because they share one connection, as with one socket.io-client `Manager`.
///   That includes their events, the server's acks for their emits, CONNECT
///   confirmations, and server `DISCONNECT` or `CONNECT_ERROR`. While this
///   namespace's own `DISCONNECT` waits, [`SocketSender::closed`] stays pending
///   and emits still go out, which the socket.io server answers by closing the
///   whole connection.
/// - Heartbeat answers stop once the queues behind the held packet fill. A
///   receiver left unread for a whole ping window, 45 seconds with the server's
///   defaults, ends the session for every namespace, and the cycle repeats
///   after each reconnection while it stays unread.
/// - When a session has ended, the client drains it without waiting for the
///   application. Events that then find this receiver full are discarded,
///   including the held one, even if the server's `DISCONNECT` for this
///   namespace follows them. socket.io-client would have delivered them.
///
/// Open a separate [`Client`](crate::client::Client) for a namespace that
/// must stay independent, like socket.io-client's `multiplex: false`.
///
/// # Items and reconnection
///
/// The receiver yields only what the server sends. When the Engine.IO session
/// drops, the client reconnects and sends CONNECT again, so a reconnection
/// shows up as another [`ServerPacket::Connect`] with a new `sid`. The server
/// forgets the socket's rooms in between.
///
/// | Server sends      | Item                           | Then                            |
/// |-------------------|--------------------------------|---------------------------------|
/// | `CONNECT`         | [`ServerPacket::Connect`]      | events, until the session drops |
/// | `EVENT`           | [`ServerPacket::Event`]        | more items                      |
/// | `CONNECT_ERROR`   | [`ServerPacket::ConnectError`] | `None`                          |
/// | `DISCONNECT`      | [`ServerPacket::Disconnect`]   | `None`, with no reconnection    |
///
/// `None` ends the receiver after a server `DISCONNECT`, a `CONNECT_ERROR`, a
/// client disconnect, an internal error, or when reconnection gives up or is
/// off, once the packets queued before the end are consumed. The client logs
/// engine errors with `tracing`, and
/// [`Client::join`](crate::client::Client::join) reports internal errors.
///
/// Dropping the receiver does not disconnect the namespace. Later server
/// events are discarded, while the [`SocketSender`] keeps working until it
/// disconnects or its last clone drops.
#[derive(Debug)]
pub struct SocketReceiver {
    server_packet_rx: mpsc::Receiver<ServerPacket>,
    terminal_packet_rx: Option<oneshot::Receiver<ServerPacket>>,
}

impl SocketReceiver {
    /// Receives the next server packet, then the server's terminal packet
    /// after queued packets drain. Repeated calls after the end return `None`.
    ///
    /// Cancel safe: both receivers retain their item until their await
    /// completes, and the terminal receiver is removed only after completion.
    pub async fn recv(&mut self) -> Option<ServerPacket> {
        if let Some(packet) = self.server_packet_rx.recv().await {
            return Some(packet);
        }
        let packet = self.terminal_packet_rx.as_mut()?.await.ok();
        self.terminal_packet_rx = None;
        packet
    }

    /// Receives the next packet without waiting, with the same ordering as
    /// [`recv`](Self::recv).
    ///
    /// # Errors
    ///
    /// Returns `Empty` while waiting for a packet, or `Disconnected` after
    /// the namespace has ended and its queued packets were consumed.
    pub fn try_recv(&mut self) -> Result<ServerPacket, mpsc::error::TryRecvError> {
        match self.server_packet_rx.try_recv() {
            Ok(packet) => return Ok(packet),
            Err(mpsc::error::TryRecvError::Empty) => return Err(mpsc::error::TryRecvError::Empty),
            Err(mpsc::error::TryRecvError::Disconnected) => {}
        }
        let Some(rx) = &mut self.terminal_packet_rx else {
            return Err(mpsc::error::TryRecvError::Disconnected);
        };
        match rx.try_recv() {
            Ok(packet) => {
                self.terminal_packet_rx = None;
                Ok(packet)
            }
            Err(oneshot::error::TryRecvError::Empty) => Err(mpsc::error::TryRecvError::Empty),
            Err(oneshot::error::TryRecvError::Closed) => {
                self.terminal_packet_rx = None;
                Err(mpsc::error::TryRecvError::Disconnected)
            }
        }
    }

    /// Returns the next application event. [`ServerPacket::Connect`],
    /// [`ServerPacket::Disconnect`], and [`ServerPacket::ConnectError`] are
    /// skipped. Returns `None` once the namespace closes, by either side,
    /// or the client ends.
    ///
    /// Cancel safe: [`recv`](Self::recv) retains pending packets, and skipped
    /// protocol packets have no suspend point after consumption.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be converted into `E`.
    pub async fn listen<E>(&mut self) -> Result<Option<E>, E::Error>
    where
        E: TryFrom<DynEvent>,
    {
        loop {
            match self.recv().await {
                None => return Ok(None),
                Some(ServerPacket::Event(e)) => return E::try_from(e).map(Some),
                Some(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytestring::ByteString;
    use serde_json::Map;

    use super::*;
    use crate::packet::{Connect, ConnectError};

    struct Pass(DynEvent);

    impl From<DynEvent> for Pass {
        fn from(e: DynEvent) -> Self {
            Self(e)
        }
    }

    #[tokio::test]
    async fn listen_skips_protocol_packets() {
        let (tx, rx) = mpsc::channel(8);
        let mut receiver = SocketReceiver {
            server_packet_rx: rx,
            terminal_packet_rx: None,
        };
        tx.send(ServerPacket::Connect(Connect {
            sid: ByteString::default(),
            extra: Map::default(),
        }))
        .await
        .unwrap();
        tx.send(ServerPacket::Disconnect).await.unwrap();
        tx.send(ServerPacket::ConnectError(ConnectError {
            message: "err".into(),
            extra: Map::default(),
        }))
        .await
        .unwrap();
        tx.send(ServerPacket::Event(DynEvent::new(r#"["hi"]"#, None)))
            .await
            .unwrap();
        let Pass(event) = receiver.listen::<Pass>().await.unwrap().unwrap();
        assert_eq!(event.payload, r#"["hi"]"#);
    }

    #[tokio::test]
    async fn listen_returns_none_on_closed_channel() {
        let (tx, rx) = mpsc::channel::<ServerPacket>(4);
        let mut receiver = SocketReceiver {
            server_packet_rx: rx,
            terminal_packet_rx: None,
        };
        drop(tx);
        assert!(receiver.listen::<Pass>().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn cancelling_terminal_receive_preserves_the_packet() {
        use futures_util::FutureExt;

        let (tx, rx) = mpsc::channel(1);
        let (terminal_tx, terminal_rx) = oneshot::channel();
        let mut receiver = SocketReceiver {
            server_packet_rx: rx,
            terminal_packet_rx: Some(terminal_rx),
        };
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(tx);
        assert!(receiver.recv().now_or_never().is_none());
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        terminal_tx.send(ServerPacket::Disconnect).unwrap();
        assert!(matches!(receiver.try_recv(), Ok(ServerPacket::Disconnect)));
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn terminal_packet_follows_buffered_packets_and_receiver_stays_ended() {
        let (tx, rx) = mpsc::channel(1);
        let (terminal_tx, terminal_rx) = oneshot::channel();
        let mut receiver = SocketReceiver {
            server_packet_rx: rx,
            terminal_packet_rx: Some(terminal_rx),
        };
        tx.send(ServerPacket::Event(DynEvent::new(r#"["last"]"#, None)))
            .await
            .unwrap();
        terminal_tx.send(ServerPacket::Disconnect).unwrap();
        drop(tx);
        assert!(matches!(receiver.try_recv(), Ok(ServerPacket::Event(_))));
        assert!(matches!(
            receiver.recv().await,
            Some(ServerPacket::Disconnect)
        ));
        assert!(receiver.recv().await.is_none());
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}
