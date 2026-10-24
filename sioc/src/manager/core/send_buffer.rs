//! The packets a namespace holds until the engine takes them: its send
//! buffer, and the CONNECT and DISCONNECT that go before it.

use std::collections::VecDeque;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;
use tokio::sync::oneshot;

use crate::packet::{DynAck, Packet};

/// An encoded event or ack reply with the waiter of its answer, like one
/// entry of socket.io-client's `Socket.sendBuffer`.
#[derive(Debug)]
pub struct Outgoing {
    /// The text message that carries the packet, like the first item
    /// `Encoder.encode` returns.
    pub header: Message,
    /// The binary messages that follow the header, like the rest of what
    /// `Encoder.encode` returns.
    pub attachments: Vec<Bytes>,
    /// The id and waiter of the event's ack, like the callback in
    /// `Socket.acks`. It stays here until the header leaves, because the
    /// server cannot answer an event it has not seen.
    pub ack_tx: Option<(u64, oneshot::Sender<DynAck>)>,
    /// Whether this answers a server event, whose id belongs to one engine.
    /// JS has no counterpart, because it never buffers replies.
    pub is_ack_reply: bool,
}

/// Events and ack replies in the order the handles sent them, like
/// socket.io-client's `Socket.sendBuffer`.
///
/// JS keeps it only while the socket is disconnected. Here it also holds
/// packets while the engine has no capacity, because the core never awaits.
/// The room semaphore the caller polls with bounds it.
#[derive(Debug, Default)]
pub struct SendBuffer(VecDeque<Outgoing>);

impl SendBuffer {
    /// Returns whether no entry waits, like an empty `sendBuffer`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Encodes an event for namespace `ns` and adds it behind the others, like
    /// `Socket.emit` pushing to `sendBuffer`.
    pub fn push_event(
        &mut self,
        ns: &str,
        payload: ByteString,
        ack_tx: Option<(u64, oneshot::Sender<DynAck>)>,
        attachments: Option<Vec<Bytes>>,
    ) {
        let id = ack_tx.as_ref().map(|(id, _)| *id);
        let packet = match &attachments {
            None => Packet::Event { payload, id },
            Some(attachments) => Packet::BinaryEvent {
                payload,
                id,
                count: attachments.len(),
            },
        };

        self.push(ns, &packet, attachments, ack_tx, false);
    }

    /// Encodes the reply to server event `id` for namespace `ns` and adds it
    /// behind the others, like the `ack` callback `Socket.ack` returns.
    pub fn push_ack_reply(
        &mut self,
        ns: &str,
        payload: ByteString,
        id: u64,
        attachments: Option<Vec<Bytes>>,
    ) {
        let packet = match &attachments {
            None => Packet::Ack { payload, id },
            Some(attachments) => Packet::BinaryAck {
                payload,
                id,
                count: attachments.len(),
            },
        };

        self.push(ns, &packet, attachments, None, true);
    }

    fn push(
        &mut self,
        ns: &str,
        packet: &Packet,
        attachments: Option<Vec<Bytes>>,
        ack_tx: Option<(u64, oneshot::Sender<DynAck>)>,
        is_ack_reply: bool,
    ) {
        tracing::trace!(%ns, %packet, "buffered packet");

        self.0.push_back(Outgoing {
            header: Message::Text(packet.encode(ns).into()),
            attachments: attachments.unwrap_or_default(),
            ack_tx,
            is_ack_reply,
        });
    }

    /// Takes the oldest entry, like `Socket.emitBuffered` taking one.
    pub fn pop(&mut self) -> Option<Outgoing> {
        self.0.pop_front()
    }

    /// Drops every ack reply, because its id means nothing to the next engine,
    /// and returns how many it dropped. Events keep their place and their
    /// waiters. JS has no counterpart, because it never buffers replies.
    pub fn drop_ack_replies(&mut self) -> usize {
        let len = self.0.len();
        self.0.retain(|outgoing| !outgoing.is_ack_reply);
        len - self.0.len()
    }

    /// Drops every entry, which fails the waiters, like `sendBuffer = []`.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

/// A packet that leaves before any buffered event, like the packets
/// `Socket.onopen` and `Socket.disconnect` write.
#[derive(Debug)]
enum Control {
    /// CONNECT with the auth payload, like `Socket.onopen` sending it.
    Connect(ByteString),
    /// DISCONNECT, like `Socket.disconnect`.
    Disconnect,
}

/// The CONNECT and DISCONNECT of one namespace, in the order they must leave.
/// JS has no counterpart, because it writes them to the engine at once.
///
/// They alternate, so at most two wait.
#[derive(Debug, Default)]
pub struct ControlQueue(VecDeque<Control>);

impl ControlQueue {
    /// Queues CONNECT, unless CONNECT already waits last, like
    /// `Socket.onopen`.
    pub fn push_connect(&mut self, auth: ByteString) {
        if !matches!(self.0.back(), Some(Control::Connect(_))) {
            self.0.push_back(Control::Connect(auth));
        }
    }

    /// Queues DISCONNECT, like `Socket.disconnect`.
    pub fn push_disconnect(&mut self) {
        self.0.push_back(Control::Disconnect);
    }

    /// Drops a CONNECT that has not left, so the server never hears of the
    /// namespace. JS has no counterpart, because its CONNECT leaves at once.
    pub fn cancel_connect(&mut self) {
        self.0
            .retain(|control| !matches!(control, Control::Connect(_)));
    }

    /// Returns whether nothing waits. JS has no counterpart, because it
    /// writes control packets at once.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Drops everything, because it belongs to an engine that is gone, like
    /// the engine dropping its `writeBuffer` on close.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Takes the oldest packet, encoded for namespace `ns`, like
    /// `Manager._packet` encoding it.
    pub fn pop(&mut self, ns: &str) -> Option<Message> {
        let packet = match self.0.pop_front()? {
            Control::Connect(auth) => Packet::Connect(auth),
            Control::Disconnect => Packet::Disconnect,
        };

        tracing::trace!(%ns, %packet, "sent packet");

        Some(Message::Text(packet.encode(ns).into()))
    }
}
