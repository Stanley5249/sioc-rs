//! What goes into the core and what comes out of it.

use std::sync::Arc;
use std::time::Duration;

use bytestring::ByteString;
use eioc::prelude::ServerMessage;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::error::ManagerError;
use crate::manager::connect_request::ConnectRequest;
use crate::packet::ClientPacket;

/// One transition for the core to apply. JS has no counterpart, because each
/// input is its own event handler on the `Manager` or the `Socket`.
#[derive(Debug)]
pub enum ManagerInput {
    /// The application opens a namespace, like socket.io-client's
    /// `Manager.socket` followed by `Socket.connect`.
    Connect(ConnectRequest),
    /// A namespace handle sent a packet, like `Socket.packet`.
    Packet {
        /// The namespace the packet belongs to, like `Socket.nsp`.
        ns: ByteString,
        /// What the handle sent, like the packet `Socket.packet` takes.
        packet: ClientPacket,
    },
    /// The inbox of a namespace ended, like `Socket.disconnect`.
    Disconnect(ByteString),
    /// The engine delivered a message, like `Manager.onopen` and `ondata`.
    ///
    /// The caller passes none while the core holds a server packet for a full
    /// receiver (see
    /// [`held_sender`](crate::manager::core::manager_core::ManagerCore::held_sender)),
    /// because the core has room for one.
    Message(ServerMessage),
    /// The engine finished, like `Manager.onclose`.
    Close(Result<(), ManagerError>),
    /// The reconnection delay is over, like the timer in `Manager.reconnect`.
    Reconnect,
    /// The client handle dropped and no inbox remains, so nothing can need a
    /// new engine, like `Manager._destroy` once no socket is active. The engine
    /// closes once its queued packets leave.
    Release,
}

/// One effect for the caller to carry out. JS has no counterpart, because the
/// `Manager` acts at once.
#[derive(Debug)]
pub enum ManagerOutput {
    /// Start an engine, like `Manager.open`.
    Open,
    /// Close the running engine, like `engine.close`.
    Close,
    /// Wait this long, then pass [`ManagerInput::Reconnect`], like the
    /// `setTimeout` in `Manager.reconnect`.
    Schedule(Duration),
    /// Stop waiting, like clearing that timer in `Manager._destroy`.
    Cancel,
    /// Poll a namespace's inbox until it ends. JS has no counterpart, because
    /// its handles call the `Socket` directly.
    ///
    /// Before each receive, the caller takes one permit from `room` and forgets
    /// it once a packet arrives, so the core never buffers more than the room
    /// it gave. Once `closed`
    /// is cancelled, because the namespace ended or its last handle left, the
    /// caller closes the inbox and drains it to the end without permits, so
    /// the end never waits on the engine. It passes every item as
    /// [`ManagerInput::Packet`] and the end as [`ManagerInput::Disconnect`].
    Listen {
        /// The namespace the inbox belongs to, like `Socket.nsp`.
        ns: ByteString,
        /// What the namespace's handles send. JS has no counterpart, because
        /// its handles call the `Socket` directly.
        client_packet_rx: mpsc::Receiver<ClientPacket>,
        /// Cancelled once the namespace closes, by either side, like
        /// `Socket.active` turning false.
        closed: CancellationToken,
        /// The free slots of the namespace's send buffer. JS has no
        /// counterpart, because its `sendBuffer` is unbounded.
        room: Arc<Semaphore>,
    },
}
