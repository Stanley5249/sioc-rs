use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{Handshake, Message, ServerMessage};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::config::ChannelConfig;
use crate::error::{ManagerError, SocketError};
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::{ConnectHandles, ConnectRequest};
use crate::manager::core::input_output::{ManagerInput, ManagerOutput};
use crate::manager::core::manager_core::ManagerCore;
use crate::packet::{ClientPacket, DynAck, ServerPacket};

/// An effect for the engine or the timer. `Listen` is not one, because the rig
/// keeps the inbox as the actor would poll it.
#[derive(Debug, PartialEq, Eq)]
pub enum Effect {
    Open,
    Close,
    Schedule(Duration),
    Cancel,
}

/// What a test keeps of a namespace's handles. The rig keeps the sender, so it
/// can end the inbox the way the last `SocketSender` does.
pub struct Handles {
    pub closed: CancellationToken,
    pub server_packet_rx: mpsc::Receiver<ServerPacket>,
    pub terminal_packet_rx: oneshot::Receiver<ServerPacket>,
    pub reply_rx: oneshot::Receiver<Result<(), SocketError>>,
}

/// An inbox the core handed out, as the actor polls it.
struct Listen {
    client_packet_rx: mpsc::Receiver<ClientPacket>,
    closed: CancellationToken,
    room: Arc<Semaphore>,
}

/// The caller of a core. It polls the inboxes the core hands out only when a
/// test asks, the way the actor's `select!` would, so tests control the order
/// of events.
pub struct Rig {
    pub core: ManagerCore,
    capacity: usize,
    listens: HashMap<ByteString, Listen>,
    /// The senders of every request, oldest first. The live one for a name is
    /// the oldest whose inbox the core has not dropped.
    client_packet_txs: HashMap<ByteString, Vec<mpsc::Sender<ClientPacket>>>,
    effects: Vec<Effect>,
}

impl Rig {
    /// A rig whose namespaces buffer `capacity` packets and whose engine never
    /// reconnects.
    pub fn new(capacity: usize) -> Self {
        Self::with_backoff(capacity, None)
    }

    pub fn with_backoff(capacity: usize, backoff: Option<Backoff>) -> Self {
        Self::seeded(capacity, backoff, 1)
    }

    /// A rig whose reconnection jitter comes from `seed`.
    pub fn seeded(capacity: usize, backoff: Option<Backoff>, seed: u64) -> Self {
        let core = ManagerCore::new(
            ChannelConfig::from(capacity),
            backoff,
            fastrand::Rng::with_seed(seed),
        );
        let mut rig = Self {
            core,
            capacity,
            listens: HashMap::new(),
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

    fn pump(&mut self) {
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
                    room,
                } => {
                    let listen = Listen {
                        client_packet_rx,
                        closed,
                        room,
                    };
                    assert!(
                        self.listens.insert(ns, listen).is_none(),
                        "a name gets new handles only after its old inbox ended"
                    );
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
    /// drops the inboxes it polls.
    pub fn shutdown(&mut self) {
        self.core.shutdown();
        self.listens.clear();
        self.pump();
    }

    /// Takes the effects since the last call.
    pub fn effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }

    pub fn open(&mut self, ns: &str, auth: &str) -> Handles {
        self.open_with(ns, auth, 32)
    }

    pub fn open_with(&mut self, ns: &str, auth: &str, server_capacity: usize) -> Handles {
        let (request, handles) =
            ConnectRequest::new(ns.into(), auth.into(), self.capacity, server_capacity);
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
        self.input(ManagerInput::Connect(request));
        Handles {
            closed,
            server_packet_rx,
            terminal_packet_rx,
            reply_rx,
        }
    }

    fn live_sender(&mut self, ns: &str) -> &mut Vec<mpsc::Sender<ClientPacket>> {
        let senders = self
            .client_packet_txs
            .get_mut(ns)
            .expect("the test opened the name");
        senders.retain(|sender| !sender.is_closed());
        senders
    }

    /// Sends a packet as a handle does, then lets the actor read the inbox.
    pub fn send(&mut self, ns: &str, packet: ClientPacket) {
        self.live_sender(ns)
            .first()
            .expect("the name has live handles")
            .try_send(packet)
            .expect("the inbox has space");
        self.poll(ns);
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

    /// Drops the oldest live handles of `ns`, as the last `SocketSender` does,
    /// then lets the actor read the inbox to its end.
    pub fn disconnect(&mut self, ns: &str) {
        let senders = self.live_sender(ns);
        assert!(!senders.is_empty(), "the name has live handles");
        senders.remove(0);
        let listen = self
            .listens
            .get(ns)
            .expect("the actor polls the inbox of live handles");
        // `SocketSender` cancels the shared token once its last clone drops.
        listen.closed.cancel();
        self.poll(ns);
    }

    /// Reads the inbox of `ns` as the actor does until it would wait: with
    /// one room permit per packet, or to its end once `closed` is cancelled.
    pub fn poll(&mut self, ns: &str) {
        loop {
            let Some(listen) = self.listens.get_mut(ns) else {
                return;
            };

            let received = if listen.closed.is_cancelled() {
                listen.client_packet_rx.close();
                listen.client_packet_rx.try_recv()
            } else {
                let Ok(permit) = listen.room.clone().try_acquire_owned() else {
                    return;
                };
                let received = listen.client_packet_rx.try_recv();
                if received.is_ok() {
                    permit.forget();
                }
                received
            };

            match received {
                Ok(packet) => self.input(ManagerInput::Packet {
                    ns: ns.into(),
                    packet,
                }),
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.listens.remove(ns);
                    self.input(ManagerInput::Disconnect(ns.into()));
                    return;
                }
            }
        }
    }

    /// Returns how many packets wait in the inbox of `ns`.
    pub fn inbox_len(&self, ns: &str) -> usize {
        self.listens
            .get(ns)
            .map_or(0, |listen| listen.client_packet_rx.len())
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
        self.input(ManagerInput::Close(Ok(())));
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

    /// Takes the next message for the engine, as the actor does with a permit.
    pub fn pull_one(&mut self) -> Option<String> {
        if !self.core.has_message() {
            assert!(self.core.next_message().is_none());
            return None;
        }
        let message = self.core.next_message().expect("has_message promised one");
        self.pump();
        Some(match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => format!("bin:{}", String::from_utf8_lossy(&bytes)),
        })
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
