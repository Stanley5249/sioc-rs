use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{Handshake, Message, ServerMessage};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::{ManagerError, SocketError};
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::{ConnectHandles, ConnectRequest};
use crate::manager::namespace::Read;
use crate::manager::state::{ManagerCore, ManagerInput, ManagerOutput};
use crate::packet::{ClientPacket, DynAck, ServerPacket};

/// An effect for the engine or the timer. `Listen` is not one, because the rig
/// keeps the channel as the actor would.
#[derive(Debug, PartialEq, Eq)]
pub enum Effect {
    Open,
    Close,
    Schedule(Duration),
    Cancel,
}

/// What a test keeps of a namespace's handles. The rig keeps the sender, so it
/// can end the channel the way the last `SocketSender` does.
pub struct Handles {
    pub closed: CancellationToken,
    pub server_packet_rx: mpsc::Receiver<ServerPacket>,
    pub terminal_packet_rx: oneshot::Receiver<ServerPacket>,
    pub reply_rx: oneshot::Receiver<Result<(), SocketError>>,
}

/// A channel the core handed out through `ManagerOutput::Listen`.
struct Channel {
    ns: ByteString,
    client_packet_rx: mpsc::Receiver<ClientPacket>,
    closed: CancellationToken,
}

/// What one look at the channels found, for a caller that holds a permit.
enum Pulled {
    Packet(ByteString, ClientPacket),
    Ended(ByteString),
    Nothing,
}

/// Plays the actor for a core without a runtime. It reads a channel only when
/// a test asks, so tests control the order of events.
pub struct Rig {
    pub core: ManagerCore,
    client_packet_capacity: usize,
    channels: Vec<Channel>,
    /// Where the next read starts, so namespaces take turns.
    next_channel: usize,
    /// The senders of every request, oldest first. The live one for a name is
    /// the oldest whose channel the rig or the core has not dropped.
    client_packet_txs: HashMap<ByteString, Vec<mpsc::Sender<ClientPacket>>>,
    effects: Vec<Effect>,
}

impl Rig {
    /// A rig whose namespaces queue `capacity` packets and whose engine never
    /// reconnects.
    pub fn new(capacity: usize) -> Self {
        Self::with_backoff(capacity, None)
    }

    pub fn with_backoff(capacity: usize, backoff: Option<Backoff>) -> Self {
        Self::seeded(capacity, backoff, 1)
    }

    /// A rig whose reconnection jitter comes from `seed`.
    pub fn seeded(capacity: usize, backoff: Option<Backoff>, seed: u64) -> Self {
        let core = ManagerCore::new(backoff, fastrand::Rng::with_seed(seed));
        let mut rig = Self {
            core,
            client_packet_capacity: capacity,
            channels: Vec::new(),
            next_channel: 0,
            client_packet_txs: HashMap::new(),
            effects: Vec::new(),
        };
        rig.pump();
        rig
    }

    /// A rig that reconnects at once.
    pub fn reconnecting(capacity: usize) -> Self {
        Self::with_backoff(
            capacity,
            Some(Backoff::new(Duration::ZERO, Duration::ZERO, 0.0, None)),
        )
    }

    /// Drains the core's outputs, as the actor does after every call.
    pub fn pump(&mut self) {
        while let Some(output) = self.core.next_output() {
            match output {
                ManagerOutput::Open => self.effects.push(Effect::Open),
                ManagerOutput::Close => self.effects.push(Effect::Close),
                ManagerOutput::Schedule(delay) => self.effects.push(Effect::Schedule(delay)),
                ManagerOutput::Cancel => self.effects.push(Effect::Cancel),
                ManagerOutput::Listen {
                    ns,
                    client_packet_rx,
                    closed,
                } => {
                    assert!(
                        self.channels.iter().all(|channel| channel.ns != ns),
                        "a name gets a new channel only after its old one ended"
                    );
                    self.channels.push(Channel {
                        ns,
                        client_packet_rx,
                        closed,
                    });
                }
            }
        }
    }

    pub fn try_input(&mut self, input: ManagerInput) -> Result<(), ManagerError> {
        let result = self.core.handle(input);
        self.pump();
        result
    }

    pub fn input(&mut self, input: ManagerInput) {
        self.try_input(input).unwrap();
    }

    /// Ends every namespace as the actor does after an internal error, which
    /// drops the channels it reads.
    pub fn shutdown(&mut self) {
        self.core.shutdown();
        self.channels.clear();
        self.pump();
    }

    /// Takes the effects since the last call.
    pub fn effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }

    /// Builds a request for `ns` and keeps its sender, without passing it to
    /// the core.
    pub fn request(
        &mut self,
        ns: &str,
        auth: &str,
        server_capacity: usize,
    ) -> (ConnectRequest, Handles) {
        let (request, handles) = ConnectRequest::new(
            ns.into(),
            auth.into(),
            self.client_packet_capacity,
            server_capacity,
        );
        let ConnectHandles {
            client_packet_tx,
            closed,
            server_packet_rx,
            terminal_packet_rx,
            reply_rx,
        } = handles;
        self.client_packet_txs
            .entry(ns.into())
            .or_default()
            .push(client_packet_tx);
        let handles = Handles {
            closed,
            server_packet_rx,
            terminal_packet_rx,
            reply_rx,
        };
        (request, handles)
    }

    pub fn open(&mut self, ns: &str, auth: &str) -> Handles {
        self.open_with(ns, auth, 32)
    }

    pub fn open_with(&mut self, ns: &str, auth: &str, server_capacity: usize) -> Handles {
        let (request, handles) = self.request(ns, auth, server_capacity);
        self.input(ManagerInput::Connect(request));
        handles
    }

    fn live_senders(&mut self, ns: &str) -> &mut Vec<mpsc::Sender<ClientPacket>> {
        let senders = self
            .client_packet_txs
            .get_mut(ns)
            .expect("the test opened the name");
        senders.retain(|sender| !sender.is_closed());
        senders
    }

    /// Puts a packet into the oldest live channel of `ns`, as a handle does.
    /// The core hears nothing until a read.
    pub fn send(&mut self, ns: &str, packet: ClientPacket) {
        self.live_senders(ns)
            .first()
            .expect("the name has live handles")
            .try_send(packet)
            .expect("the channel has space");
    }

    pub fn emit(&mut self, ns: &str, payload: &'static str) {
        self.send(ns, event(payload, None, None));
    }

    pub fn emit_ack(&mut self, ns: &str, payload: &'static str) -> oneshot::Receiver<DynAck> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.send(ns, event(payload, Some(ack_tx), None));
        ack_rx
    }

    pub fn reply_ack(&mut self, ns: &str, id: u64) {
        self.send(
            ns,
            ClientPacket::Ack {
                payload: ByteString::from_static("[]"),
                id,
                attachments: None,
            },
        );
    }

    /// Drops the oldest live sender of `ns` and cancels its token, as the last
    /// `SocketSender` does, then reads what the actor reads without a permit.
    pub fn disconnect(&mut self, ns: &str) {
        let senders = self.live_senders(ns);
        assert!(!senders.is_empty(), "the name has live handles");
        senders.remove(0);
        let channel = self
            .channels
            .iter()
            .find(|channel| &*channel.ns == ns)
            .expect("the actor reads the channel of live handles");
        channel.closed.cancel();
        self.poll(ns);
    }

    /// Reads the channel of `ns` as `poll_client_packets` does without a
    /// permit: packets to discard, and the end once the token is cancelled.
    pub fn poll(&mut self, ns: &str) {
        loop {
            let Some(index) = self.channels.iter().position(|c| &*c.ns == ns) else {
                return;
            };
            let channel = &mut self.channels[index];
            if channel.closed.is_cancelled() {
                channel.client_packet_rx.close();
            }
            if self.core.read(ns, false) != Read::Discard {
                return;
            }

            match channel.client_packet_rx.try_recv() {
                Ok(packet) => self.input(ManagerInput::Packet {
                    ns: ns.into(),
                    packet,
                }),
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.channels.remove(index);
                    self.input(ManagerInput::Disconnect(ns.into()));
                    return;
                }
            }
        }
    }

    /// Returns how many packets wait in the channel of `ns`.
    pub fn channel_len(&self, ns: &str) -> usize {
        self.channels
            .iter()
            .find(|channel| &*channel.ns == ns)
            .map_or(0, |channel| channel.client_packet_rx.len())
    }

    pub fn server(&mut self, text: &str) {
        self.input(ManagerInput::Message(ServerMessage::Text(text.into())));
    }

    pub fn server_binary(&mut self, bytes: &'static [u8]) {
        self.input(ManagerInput::Message(ServerMessage::Binary(
            Bytes::from_static(bytes),
        )));
    }

    pub fn handshake(&mut self) {
        self.input(ManagerInput::Message(ServerMessage::Open(Handshake {
            sid: "engine".into(),
            upgrades: Vec::new(),
            ping_interval: 25_000,
            ping_timeout: 20_000,
            max_payload: 1_000_000,
        })));
    }

    /// The server confirms the namespace.
    pub fn confirm(&mut self, ns: &str) {
        let prefix = if ns == "/" {
            String::new()
        } else {
            format!("{ns},")
        };
        self.server(&format!("0{prefix}{{\"sid\":\"test\"}}"));
    }

    /// The engine finishes without an error.
    pub fn close(&mut self) {
        self.input(ManagerInput::Closed(Ok(())));
    }

    /// Opens `ns`, runs the handshake, sends CONNECT, and has the server
    /// confirm it.
    pub fn connected(&mut self, ns: &str) -> Handles {
        let handles = self.open(ns, "{}");
        self.handshake();
        assert_eq!(self.pull().len(), 1);
        self.confirm(ns);
        self.effects();
        handles
    }

    /// Looks for a channel the actor reads with a permit, in turn.
    fn read_with_permit(&mut self) -> Pulled {
        let len = self.channels.len();
        for offset in 0..len {
            let index = (self.next_channel + offset) % len;
            let channel = &mut self.channels[index];
            if channel.closed.is_cancelled() {
                channel.client_packet_rx.close();
            }
            if self.core.read(&channel.ns, true) != Read::Send {
                continue;
            }

            match channel.client_packet_rx.try_recv() {
                Ok(packet) => {
                    self.next_channel = index + 1;
                    return Pulled::Packet(channel.ns.clone(), packet);
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    return Pulled::Ended(self.channels.remove(index).ns);
                }
            }
        }
        Pulled::Nothing
    }

    /// Models one engine permit: takes a waiting message, or reads the first
    /// channel in turn that can send, then takes the message that follows.
    pub fn pull_one(&mut self) -> Option<String> {
        loop {
            if self.core.has_message() {
                let message = self.core.next_message().expect("has_message promised one");
                self.pump();
                return Some(match message {
                    Message::Text(text) => text.to_string(),
                    Message::Binary(bytes) => format!("bin:{}", String::from_utf8_lossy(&bytes)),
                });
            }

            match self.read_with_permit() {
                Pulled::Packet(ns, packet) => self.input(ManagerInput::Packet { ns, packet }),
                Pulled::Ended(ns) => self.input(ManagerInput::Disconnect(ns)),
                Pulled::Nothing => {
                    assert!(self.core.next_message().is_none());
                    return None;
                }
            }
        }
    }

    /// Takes every message the engine would accept.
    pub fn pull(&mut self) -> Vec<String> {
        std::iter::from_fn(|| self.pull_one()).collect()
    }
}

pub fn event(
    payload: &'static str,
    ack_tx: Option<oneshot::Sender<DynAck>>,
    attachments: Option<Vec<Bytes>>,
) -> ClientPacket {
    ClientPacket::Event {
        payload: ByteString::from_static(payload),
        ack_tx,
        attachments,
    }
}
