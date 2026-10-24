//! The ack ids of a namespace and the waiters of the events that carry them.

use std::collections::HashMap;

use tokio::sync::oneshot;

use crate::packet::DynAck;

/// Ack ids and waiters, like socket.io-client's `Socket.ids` and
/// `Socket.acks`.
#[derive(Debug, Default)]
pub struct Acks {
    /// The next ack id, like `Socket.ids`. It lives as long as the name and
    /// never restarts, so a late ack for a closed namespace never answers
    /// an event of the reopened one.
    next_ack_id: u64,
    /// Waiters of events the engine took, like `Socket.acks`.
    waiters: HashMap<u64, oneshot::Sender<DynAck>>,
}

impl Acks {
    /// Takes the next ack id for an event that asks for an ack, like
    /// `this.ids++` in `Socket.emit`.
    ///
    /// Dropping or timing out a handle closes its receiver. Reclaiming those
    /// waiters before adding work means repeated unanswered requests retain
    /// only live waiters.
    pub fn next_id(&mut self) -> u64 {
        self.waiters.retain(|_, ack_tx| !ack_tx.is_closed());

        let id = self.next_ack_id;
        self.next_ack_id += 1;
        id
    }

    /// Starts waiting for the answer to event `id`, unless nobody listens,
    /// like `Socket._registerAckCallback`.
    pub fn insert(&mut self, id: u64, ack_tx: oneshot::Sender<DynAck>) {
        if !ack_tx.is_closed() {
            self.waiters.insert(id, ack_tx);
        }
    }

    /// Completes the waiter of `id`, like `Socket.onack`.
    ///
    /// Removes it before sending, so each ack arrives at most once and an
    /// unknown id completes nothing.
    pub fn resolve(&mut self, ns: &str, id: u64, ack: DynAck) {
        let Some(ack_tx) = self.waiters.remove(&id) else {
            tracing::debug!(%ns, id, "discarded ack with no pending handle");
            return;
        };

        if ack_tx.send(ack).is_err() {
            tracing::debug!(%ns, id, "discarded ack for a dropped handle");
        }
    }

    /// Fails every waiter, like `Socket._clearAcks`.
    pub fn clear(&mut self) {
        self.waiters.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taking_an_id_reclaims_dropped_waiters() {
        let mut acks = Acks::default();
        let (live_tx, _live_rx) = oneshot::channel();
        let id = acks.next_id();
        acks.insert(id, live_tx);
        for _ in 0..100 {
            let (ack_tx, ack_rx) = oneshot::channel();
            let id = acks.next_id();
            acks.insert(id, ack_tx);
            drop(ack_rx);
        }
        assert_eq!(acks.waiters.len(), 2);

        acks.next_id();
        assert_eq!(acks.waiters.len(), 1);
    }
}
