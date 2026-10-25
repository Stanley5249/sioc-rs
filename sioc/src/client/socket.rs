//! Namespace I/O, like the send and receive directions of socket.io-client's
//! `Socket`. Both handles and their channel handoff stay together so endpoint
//! fields and construction remain private.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::ack::Acknowledge;
use crate::error::SocketError;
use crate::event::Emit;
use crate::manager::connect_request::ConnectHandles;
use crate::marker::{AckId, AckMarker, AckType, BinaryMarker};
use crate::packet::{ClientPacket, DynEvent, ServerPacket};

impl ConnectHandles {
    /// Converts the manager's channel ends into the public namespace handles.
    pub fn into_socket(
        self,
    ) -> (
        SocketSender,
        SocketReceiver,
        oneshot::Receiver<Result<(), SocketError>>,
    ) {
        (
            SocketSender::new(self.client_packet_tx, self.closed),
            SocketReceiver {
                server_packet_rx: self.server_packet_rx,
                terminal_packet_rx: Some(self.terminal_packet_rx),
            },
            self.reply_rx,
        )
    }
}

/// Sender for a Socket.IO namespace, like `Socket.emit` and `Socket.ack`.
/// Rust channel: sends packets to the manager task.
///
/// Clone it to emit from several tasks: clones share the namespace.
/// Disconnecting any clone closes all of them, and dropping the last clone
/// also disconnects the namespace.
#[derive(Clone, Debug)]
pub struct SocketSender {
    /// Carries packets to the manager task, which encodes and sends them.
    client_packet_tx: mpsc::Sender<ClientPacket>,
    closed: CancellationToken,
    // Last-handle drop also wakes a namespace whose send buffer is full.
    _closed: Arc<DropGuard>,
}

impl SocketSender {
    /// Wraps the channel to a namespace and the token that marks it closed.
    #[must_use]
    fn new(client_packet_tx: mpsc::Sender<ClientPacket>, closed: CancellationToken) -> Self {
        Self {
            client_packet_tx,
            _closed: Arc::new(closed.clone().drop_guard()),
            closed,
        }
    }

    /// Sends one raw client packet; [`emit`](Self::emit) and
    /// [`acknowledge`](Self::acknowledge) build theirs and call this.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError::AttachmentCount`] if a binary packet has zero or
    /// more than [`MAX_ATTACHMENTS`](crate::binary::MAX_ATTACHMENTS)
    /// attachments, or [`SocketError::Closed`] once the namespace is closed.
    pub async fn send(&self, packet: ClientPacket) -> Result<(), SocketError> {
        if self.closed.is_cancelled() {
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

        self.client_packet_tx
            .send(packet)
            .await
            .map_err(|_| SocketError::Closed)
    }
}

impl SocketSender {
    /// Emits an event; returns `()` or an [`AckHandle`](crate::ack::AckHandle)
    /// depending on the ack policy.
    ///
    /// The event waits in the namespace's channel until the server confirms
    /// the namespace, also after a reconnection. The channel holds
    /// [`ChannelConfig::manager`](crate::config::ChannelConfig::manager)
    /// packets, so emitting awaits space when it is full.
    /// The [`AckHandle`](crate::ack::AckHandle) fails if the
    /// Engine.IO session drops after the event went out, like
    /// socket.io-client's `Socket._clearAcks`.
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
        let client_packet = payload.into_client_packet(id.get())?;
        self.send(client_packet).await
    }

    /// Disconnects the namespace for every clone.
    ///
    /// Once the server has confirmed the namespace, events and acks sent
    /// before the call still go out, followed by a DISCONNECT packet. Before
    /// that, buffered events are discarded and no DISCONNECT goes out, because
    /// socket.io-client's `Socket.disconnect` sends one only to a connected
    /// namespace. Later sends fail with [`SocketError::Closed`].
    /// Calling it after the namespace has closed, by either side, does
    /// nothing.
    pub fn disconnect(&self) {
        self.closed.cancel();
    }

    /// Waits until the namespace closes, by either side, or the client ends.
    ///
    /// Resolves as soon as the namespace stops accepting packets, which can be
    /// before its DISCONNECT packet reaches the server.
    pub async fn closed(&self) {
        self.closed.cancelled().await;
    }
}

/// Receiver for a Socket.IO namespace, like `Socket.emitEvent` listeners.
/// Rust channel: receives packets from the manager task.
///
/// Read it continuously: bounded queues apply backpressure to the server.
/// A stalled consumer can delay heartbeat responses until the server times out.
///
/// Every namespace of a [`Client`](crate::client::Client) shares one
/// connection, as with one socket.io-client `Manager`, so a stalled receiver
/// also holds up the other namespaces of that client. Open a separate
/// [`Client`](crate::client::Client) for a namespace that
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
/// off. The client logs engine errors with `tracing`, and
/// [`Client::join`](crate::client::Client::join) reports internal errors.
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
