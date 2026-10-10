//! A request to open a namespace, and the handles its caller keeps.

use bytestring::ByteString;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::SocketError;
use crate::packet::{ClientPacket, ServerPacket};

/// A namespace opened by [`Client::connect`](crate::client::Client::connect).
#[derive(Debug)]
pub struct ConnectRequest {
    /// The namespace to open, such as `/` or `/chat`.
    pub ns: ByteString,
    /// The JSON auth payload sent with the CONNECT packet.
    pub payload: ByteString,
    /// What the namespace's [`SocketSender`](crate::client::SocketSender)
    /// clones send.
    pub client_packet_rx: mpsc::Receiver<ClientPacket>,
    /// Cancelled once the namespace closes, by either side.
    pub closed: CancellationToken,
    /// Delivers the server's packets to the namespace's
    /// [`SocketReceiver`](crate::client::SocketReceiver).
    pub server_packet_tx: mpsc::Sender<ServerPacket>,
    /// Delivers the server's one terminal packet independently of queue
    /// capacity.
    pub terminal_packet_tx: oneshot::Sender<ServerPacket>,
    /// Reports whether the namespace opened, before its CONNECT goes out.
    pub reply_tx: oneshot::Sender<Result<(), SocketError>>,
}

/// The ends of a [`ConnectRequest`]'s channels that its caller keeps.
#[derive(Debug)]
pub struct ConnectHandles {
    /// Feeds the namespace's [`SocketSender`](crate::client::SocketSender).
    pub client_packet_tx: mpsc::Sender<ClientPacket>,
    /// The token the namespace's senders share.
    pub closed: CancellationToken,
    /// Feeds the namespace's [`SocketReceiver`](crate::client::SocketReceiver).
    pub server_packet_rx: mpsc::Receiver<ServerPacket>,
    /// Receives the server's one terminal packet after queued packets drain.
    pub terminal_packet_rx: oneshot::Receiver<ServerPacket>,
    /// Reports whether the namespace opened.
    pub reply_rx: oneshot::Receiver<Result<(), SocketError>>,
}

impl ConnectRequest {
    /// Builds the request to open `ns`, with the channel ends its caller keeps.
    pub fn new(
        ns: ByteString,
        payload: ByteString,
        client_packet_capacity: usize,
        server_packet_capacity: usize,
    ) -> (Self, ConnectHandles) {
        let (client_packet_tx, client_packet_rx) = mpsc::channel(client_packet_capacity);
        let (server_packet_tx, server_packet_rx) = mpsc::channel(server_packet_capacity);
        let (terminal_packet_tx, terminal_packet_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        let closed = CancellationToken::new();

        let request = Self {
            ns,
            payload,
            client_packet_rx,
            closed: closed.clone(),
            server_packet_tx,
            terminal_packet_tx,
            reply_tx,
        };
        let handles = ConnectHandles {
            client_packet_tx,
            closed,
            server_packet_rx,
            terminal_packet_rx,
            reply_rx,
        };

        (request, handles)
    }
}
