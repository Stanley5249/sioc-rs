//! Every namespace name the client has used.

use std::collections::BTreeMap;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::Message;

use crate::manager::core::decoder::Decoded;
use crate::manager::core::input_output::ManagerOutput;
use crate::manager::core::namespace::Namespace;
use crate::packet::ServerPacket;

/// Every namespace name, like socket.io-client's `Manager.nsps`, which keeps
/// one `Socket` per name and never prunes it.
#[derive(Debug)]
pub struct Namespaces {
    /// The send buffer capacity of each new namespace, like the `opts` that
    /// `Manager.socket` passes on.
    capacity: usize,
    /// One namespace per name, like `Manager.nsps`.
    map: BTreeMap<ByteString, Namespace>,
    /// The namespace that sent last, so the next message comes from the next
    /// name. JS has no counterpart, because each `Socket` writes at once.
    cursor: Option<ByteString>,
}

impl Namespaces {
    /// Creates an empty set whose namespaces buffer `capacity` packets, like
    /// `Manager.nsps = {}`.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: BTreeMap::new(),
            cursor: None,
        }
    }

    /// Returns the namespace of `ns`, adding it if the name is new, like
    /// `Manager.socket`.
    pub fn entry(&mut self, ns: &ByteString) -> &mut Namespace {
        self.map
            .entry(ns.clone())
            .or_insert_with(|| Namespace::new(self.capacity))
    }

    /// Returns the namespace of `ns`, if the client used the name, like
    /// `Manager.nsps[nsp]`.
    pub fn get_mut(&mut self, ns: &str) -> Option<&mut Namespace> {
        self.map.get_mut(ns)
    }

    /// Returns the namespace of `ns`, if the client used the name, like
    /// `Manager.nsps[nsp]`.
    #[must_use]
    pub fn get(&self, ns: &str) -> Option<&Namespace> {
        self.map.get(ns)
    }

    /// Returns every name with its namespace, like iterating
    /// `Manager.nsps`.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&ByteString, &mut Namespace)> {
        self.map.iter_mut()
    }

    /// Returns whether any namespace needs an engine, like `Manager._destroy`
    /// checking every socket.
    #[must_use]
    pub fn needs_engine(&self) -> bool {
        self.map.values().any(Namespace::needs_engine)
    }

    /// Returns whether every namespace is inactive, like `Manager._destroy`
    /// finding no active socket.
    #[must_use]
    pub fn is_inactive(&self) -> bool {
        self.map.values().all(Namespace::is_inactive)
    }

    /// Returns whether any namespace has a message for the engine. JS has no
    /// counterpart, because each `Socket` writes at once.
    #[must_use]
    pub fn has_message(&self) -> bool {
        self.map
            .values()
            .any(|namespace| namespace.has_control() || namespace.has_data())
    }

    /// Routes one decoded server packet to its namespace, like
    /// `Socket.onpacket`. Returns the packet if its receiver is full.
    ///
    /// The server may still send packets for a namespace the client left, so
    /// packets for an unknown or inactive namespace are discarded.
    pub fn receive(&mut self, ns: &str, decoded: Decoded) -> Option<ServerPacket> {
        let Some(namespace) = self.map.get_mut(ns) else {
            tracing::debug!(%ns, "discarded server packet for an unknown namespace");
            return None;
        };

        let packet = match decoded {
            Decoded::Ack { id, ack } => {
                namespace.resolve_ack(ns, id, ack);
                return None;
            }
            Decoded::Packet(packet) => packet,
        };

        match &packet {
            ServerPacket::Connect(connect) => {
                tracing::debug!(%ns, sid = %connect.sid, "connected");

                namespace.confirm();
            }
            ServerPacket::Disconnect | ServerPacket::ConnectError(_) => {
                tracing::debug!(%ns, %packet, "namespace ended by the server");

                // The server refused the namespace or left it, so both end it.
                namespace.end(ns, Some(packet));
                return None;
            }
            ServerPacket::Event(_) => {}
        }

        namespace.deliver(ns, packet)
    }

    /// Takes the next message for the engine: CONNECT and DISCONNECT first,
    /// then one buffered packet from the next namespace in turn. Returns the
    /// attachments that must follow it. JS has no counterpart, because each
    /// `Socket` writes at once.
    pub fn pop(&mut self) -> Option<(Message, Vec<Bytes>)> {
        if let Some(ns) = self.pick(Namespace::has_control) {
            let message = self.map.get_mut(&ns)?.pop_control(&ns)?;
            self.cursor = Some(ns);
            return Some((message, Vec::new()));
        }

        let ns = self.pick(Namespace::has_data)?;
        let packet = self.map.get_mut(&ns)?.pop(&ns)?;
        self.cursor = Some(ns);
        Some(packet)
    }

    /// Takes the inbox of one namespace that just became active, if any. JS
    /// has no counterpart, because its handles call the `Socket` directly.
    ///
    /// Deriving this from the namespaces keeps the output queue short when
    /// several requests take their names in one transition.
    pub fn take_listen(&mut self) -> Option<ManagerOutput> {
        self.map.iter_mut().find_map(|(ns, namespace)| {
            let (client_packet_rx, closed, room) = namespace.take_listen()?;
            Some(ManagerOutput::Listen {
                ns: ns.clone(),
                client_packet_rx,
                closed,
                room,
            })
        })
    }

    /// Picks the first namespace after the cursor that satisfies `ready`.
    /// JS has no counterpart, because each `Socket` writes at once.
    fn pick(&self, ready: impl Fn(&Namespace) -> bool) -> Option<ByteString> {
        let cursor = self.cursor.as_ref();
        let after = self
            .map
            .iter()
            .filter(|(ns, _)| cursor.is_none_or(|cursor| *ns > cursor));
        let before = self
            .map
            .iter()
            .filter(|(ns, _)| cursor.is_some_and(|cursor| *ns <= cursor));

        after
            .chain(before)
            .find(|(_, namespace)| ready(namespace))
            .map(|(ns, _)| ns.clone())
    }
}
