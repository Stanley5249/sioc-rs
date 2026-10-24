//! The channel of each namespace's handles, as the actor reads it, like
//! socket.io-client's `Socket.sendBuffer`.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytestring::ByteString;
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use crate::manager::namespace::Read;
use crate::manager::state::ManagerCore;
use crate::packet::ClientPacket;

/// The channel of one namespace's handles, like `Socket.sendBuffer`. Rust
/// channel: JS handles call the `Socket` directly.
pub struct SendBuffer {
    /// The namespace the channel belongs to, like `Socket.nsp`.
    pub ns: ByteString,
    /// The packets the handles sent and the actor has not read, like the
    /// entries of `Socket.sendBuffer`.
    pub client_packet_rx: mpsc::Receiver<ClientPacket>,
    /// Cancelled once the handles leave or the namespace ends, like
    /// `Socket.active` turning false. Rust cleanup.
    pub closed: CancellationToken,
    /// Wakes the actor once `closed` is cancelled, while the core leaves the
    /// channel unread. Rust wait.
    pub closed_wait: Pin<Box<WaitForCancellationFutureOwned>>,
}

/// Reads the next packet the core asks for, from the namespaces in turn.
/// Returns the index of the channel and its packet, or `None` at its end.
///
/// A channel is read only when its packet can go out at once or is to be
/// dropped, so unsent packets wait in the channel, which bounds them. Once a
/// channel's token is cancelled, it is closed so blocked handles fail and the
/// rest drains to its end.
pub fn poll_send_buffers(
    core: &ManagerCore,
    send_buffers: &mut [SendBuffer],
    next: &mut usize,
    has_permit: bool,
    cx: &mut Context<'_>,
) -> Poll<(usize, Option<ClientPacket>)> {
    let len = send_buffers.len();
    for offset in 0..len {
        let index = (*next + offset) % len;
        let channel = &mut send_buffers[index];

        // Polling the wait only before it fires keeps it from being polled
        // after it finished.
        if channel.closed.is_cancelled() || channel.closed_wait.as_mut().poll(cx).is_ready() {
            channel.client_packet_rx.close();
        }

        if core.read(&channel.ns, has_permit) == Read::Wait {
            continue;
        }
        if let Poll::Ready(packet) = channel.client_packet_rx.poll_recv(cx) {
            *next = index + 1;
            return Poll::Ready((index, packet));
        }
    }
    Poll::Pending
}
