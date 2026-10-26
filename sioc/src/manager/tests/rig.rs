use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{ClientMessage, Handshake, ServerMessage};
use tokio::sync::oneshot;

use crate::manager::backoff::Backoff;
use crate::manager::connect_request::{ConnectHandles, ConnectRequest};
use crate::manager::namespace::Read;
use crate::manager::state::{ManagerCore, ManagerInput, ManagerOutput};
use crate::packet::{ClientPacket, DynAck};

/// An effect for the engine or the timer. `Listen` keeps the name only.
#[derive(Debug, PartialEq, Eq)]
pub enum Effect {
    Open,
    Close,
    Schedule(Duration),
    Cancel,
    Listen { ns: ByteString },
    ListenAcks { ns: ByteString },
}

pub fn listen(ns: &str) -> Effect {
    Effect::Listen { ns: ns.into() }
}

/// A core and the helpers that feed it inputs and collect what it decides.
pub struct Rig {
    pub core: ManagerCore,
}

impl Rig {
    /// A core that never reconnects.
    pub fn new() -> Self {
        Self::with_backoff(None, 1)
    }

    /// A core that reconnects at once.
    pub fn reconnecting() -> Self {
        let backoff = Backoff::new(Duration::ZERO, Duration::ZERO, 0.0, None);
        Self::with_backoff(Some(backoff), 1)
    }

    /// A core whose reconnection jitter comes from `seed`.
    pub fn with_backoff(backoff: Option<Backoff>, seed: u64) -> Self {
        let core = ManagerCore::new(backoff, fastrand::Rng::with_seed(seed));
        Self { core }
    }

    /// Takes the effects since the last call.
    pub fn effects(&mut self) -> Vec<Effect> {
        let outputs = std::iter::from_fn(|| self.core.next_output());
        outputs
            .map(|output| match output {
                ManagerOutput::Open => Effect::Open,
                ManagerOutput::Close => Effect::Close,
                ManagerOutput::Schedule(delay) => Effect::Schedule(delay),
                ManagerOutput::Cancel => Effect::Cancel,
                ManagerOutput::Listen { ns, .. } => Effect::Listen { ns },
                ManagerOutput::ListenAcks { ns, .. } => Effect::ListenAcks { ns },
            })
            .collect()
    }

    /// Applies an input and returns the effects since the last call.
    pub fn step(&mut self, input: ManagerInput) -> Vec<Effect> {
        self.core.handle(input).unwrap();
        self.effects()
    }

    /// Takes every message the engine would accept, as text or `bin:<bytes>`.
    pub fn messages(&mut self) -> Vec<String> {
        let mut messages = Vec::new();
        while self.core.has_message() {
            let message = self.core.next_message().expect("has_message promised one");
            messages.push(match message {
                ClientMessage::Text(text) => text.to_string(),
                ClientMessage::Binary(bytes) => format!("bin:{}", String::from_utf8_lossy(&bytes)),
            });
        }
        messages
    }

    /// Opens `ns` and returns the handles. The effects stay for `effects`.
    pub fn connect(&mut self, ns: &str, auth: &str, server_capacity: usize) -> ConnectHandles {
        let (request, handles) = ConnectRequest::new(ns.into(), auth.into(), 8, server_capacity);
        self.core.handle(ManagerInput::Connect(request)).unwrap();
        handles
    }

    /// Opens `ns`, runs the handshake, sends CONNECT, and has the server
    /// confirm it. Leaves no effect behind.
    pub fn connected(&mut self, ns: &str) -> ConnectHandles {
        let handles = self.connect(ns, "{}", 32);
        self.handshake();
        assert_eq!(self.messages().len(), 1);
        self.confirm(ns);
        self.effects();
        handles
    }

    pub fn handshake(&mut self) {
        self.message(ServerMessage::Open(Handshake {
            sid: "engine".into(),
            upgrades: Vec::new(),
            ping_interval: 25_000,
            ping_timeout: 20_000,
            max_payload: 1_000_000,
        }));
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

    pub fn server(&mut self, text: &str) {
        self.message(ServerMessage::Text(text.into()));
    }

    pub fn server_binary(&mut self, bytes: &'static [u8]) {
        self.message(ServerMessage::Binary(Bytes::from_static(bytes)));
    }

    /// A handle's packet, which the core must ask for with `Read::Send`.
    pub fn send(&mut self, ns: &str, packet: ClientPacket) {
        assert_eq!(self.core.read(ns, true), Read::Send);
        let input = ManagerInput::Packet {
            ns: ns.into(),
            packet,
        };
        self.core.handle(input).unwrap();
    }

    fn message(&mut self, message: ServerMessage) {
        self.core.handle(ManagerInput::Message(message)).unwrap();
    }
}

pub fn event(payload: &'static str) -> ClientPacket {
    ClientPacket::Event {
        payload: ByteString::from_static(payload),
        ack_tx: None,
        attachments: None,
    }
}

/// An event with a waiter for its ack.
pub fn event_ack(payload: &'static str) -> (ClientPacket, oneshot::Receiver<DynAck>) {
    let (ack_tx, ack_rx) = oneshot::channel();
    let packet = ClientPacket::Event {
        payload: ByteString::from_static(payload),
        ack_tx: Some(ack_tx),
        attachments: None,
    };
    (packet, ack_rx)
}

/// An ack the handle sends for the server's event `id`.
pub fn ack(id: u64) -> ClientPacket {
    ClientPacket::Ack {
        payload: ByteString::from_static("[]"),
        id,
        attachments: None,
    }
}

/// A handle's packet as the actor hands it over, without the read check.
pub fn packet(ns: &str, packet: ClientPacket) -> ManagerInput {
    ManagerInput::Packet {
        ns: ns.into(),
        packet,
    }
}

/// The end of a namespace's input channels.
pub fn disconnect(ns: &str) -> ManagerInput {
    ManagerInput::Disconnect(ns.into())
}
