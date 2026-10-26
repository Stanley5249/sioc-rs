//! The bounded inputs of each namespace's handles. The event inbox holds
//! `Socket.sendBuffer`; the ACK inbox serves `Socket.ack` for one engine.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytestring::ByteString;
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use crate::manager::namespace::Read;
use crate::manager::state::ManagerCore;
use crate::packet::ClientPacket;

/// The actor's inputs for one namespace. Rust channels: JS handles call
/// `Socket.emit` and `Socket.ack` directly. Event input survives reconnection;
/// ACK input belongs to the current engine.
pub struct NamespaceInputs {
    /// The namespace these inputs belong to, like `Socket.nsp`.
    pub ns: ByteString,
    /// The events the handles sent and the actor has not read, like the
    /// entries of `Socket.sendBuffer`.
    pub client_packet_rx: mpsc::Receiver<ClientPacket>,
    /// The current engine's ACK input, like `Socket.ack` writing directly.
    /// Rust channel: this input is replaced, while unsent events stay put.
    pub ack_packet_rx: Option<mpsc::Receiver<ClientPacket>>,
    /// Whether ACK input is polled first. Rust wait: alternating the first
    /// opportunity prevents either input from starving the other.
    pub ack_first: bool,
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
///
/// A poll function rather than `select!`, because the set of namespaces
/// changes at run time and each one is read only when the core allows it. A
/// namespace the core holds back registers no wakeup here: only the actor's
/// other inputs change the core's answer, and each of them ends the actor's
/// wait, so the next turn polls again.
pub fn poll_namespace_inputs(
    core: &ManagerCore,
    namespace_inputs: &mut [NamespaceInputs],
    next: &mut usize,
    has_client_message_permit: bool,
    cx: &mut Context<'_>,
) -> Poll<(usize, Option<ClientPacket>)> {
    let len = namespace_inputs.len();
    for offset in 0..len {
        let index = (*next + offset) % len;
        let inputs = &mut namespace_inputs[index];

        // Polling the wait only before it fires keeps it from being polled
        // after it finished.
        if inputs.closed.is_cancelled() || inputs.closed_wait.as_mut().poll(cx).is_ready() {
            inputs.client_packet_rx.close();
            if let Some(rx) = &mut inputs.ack_packet_rx {
                rx.close();
            }
        }

        let read = core.read(&inputs.ns, has_client_message_permit);
        if read == Read::Discard {
            inputs.ack_packet_rx = None;
        }
        if read == Read::Wait {
            continue;
        }
        let mut events_ended = false;
        for ack in [inputs.ack_first, !inputs.ack_first] {
            let receiver = if ack {
                inputs.ack_packet_rx.as_mut()
            } else {
                Some(&mut inputs.client_packet_rx)
            };
            let Some(receiver) = receiver else {
                continue;
            };
            match receiver.poll_recv(cx) {
                Poll::Ready(Some(packet)) => {
                    inputs.ack_first = !ack;
                    *next = index + 1;
                    return Poll::Ready((index, Some(packet)));
                }
                Poll::Ready(None) => {
                    if ack {
                        inputs.ack_packet_rx = None;
                    } else {
                        events_ended = true;
                    }
                    if events_ended && inputs.ack_packet_rx.is_none() {
                        *next = index + 1;
                        return Poll::Ready((index, None));
                    }
                }
                // Event EOF cannot release the name while its reply input
                // still owns queued packets or outstanding sender permits.
                Poll::Pending => {}
            }
        }
    }
    Poll::Pending
}
