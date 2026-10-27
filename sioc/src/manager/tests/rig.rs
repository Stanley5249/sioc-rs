use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{ClientMessage, Handshake, ServerMessage};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::SocketError;
use crate::manager::backoff::Backoff;
use crate::manager::socket::{Auth, Blocked, Handles, Release};
use crate::manager::state::{ManagerCore, ManagerOutput};
use crate::packet::{ClientPacket, DynAck, ServerPacket};

/// An effect for the engine or the timer.
#[derive(Debug, PartialEq, Eq)]
pub enum Effect {
    Open,
    Close,
    Schedule(Duration),
    Cancel,
}

/// The application's ends of one `connect`, and its result.
pub struct TestHandles {
    /// `Ok(Some(release))` while the previous handles still flush.
    pub result: Result<Option<CancellationToken>, SocketError>,
    /// Cancelled once the core drops these handles.
    pub release: CancellationToken,
    pub capacity: Arc<Semaphore>,
    pub reply_capacity: Arc<Semaphore>,
    pub closed: CancellationToken,
    pub server_packet_rx: mpsc::Receiver<ServerPacket>,
    pub terminal_packet_rx: oneshot::Receiver<ServerPacket>,
}

/// A core and the helpers that feed it transitions and collect what it
/// decides. It remembers the capacities of the last handles per name, as the
/// handles themselves would.
pub struct Rig {
    pub core: ManagerCore,
    capacities: HashMap<String, (Arc<Semaphore>, Arc<Semaphore>)>,
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
        Self {
            core,
            capacities: HashMap::new(),
        }
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
            })
            .collect()
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

    /// Opens `ns` with room for 8 events and 8 replies, and returns the
    /// handles. The effects stay for `effects`.
    pub fn connect(&mut self, ns: &str, auth: &str, server_capacity: usize) -> TestHandles {
        let capacity = Arc::new(Semaphore::new(8));
        let reply_capacity = Arc::new(Semaphore::new(8));
        let closed = CancellationToken::new();
        let (server_packet_tx, server_packet_rx) = mpsc::channel(server_capacity);
        let (terminal_packet_tx, terminal_packet_rx) = oneshot::channel();
        let release = CancellationToken::new();
        let handles = Handles {
            auth: Auth(auth.into()),
            capacity: Arc::clone(&capacity),
            reply_capacity: Arc::clone(&reply_capacity),
            closed: closed.clone(),
            server_packet_tx,
            terminal_packet_tx,
            release: Release(release.clone()),
        };
        let result = self.core.connect(ns.into(), handles);
        if matches!(result, Ok(None)) {
            self.capacities.insert(
                ns.into(),
                (Arc::clone(&capacity), Arc::clone(&reply_capacity)),
            );
        }
        TestHandles {
            result,
            release,
            capacity,
            reply_capacity,
            closed,
            server_packet_rx,
            terminal_packet_rx,
        }
    }

    /// Opens `ns`, runs the handshake, sends CONNECT, and has the server
    /// confirm it. Leaves no effect behind.
    pub fn connected(&mut self, ns: &str) -> TestHandles {
        let handles = self.connect(ns, "{}", 32);
        self.handshake();
        assert_eq!(self.messages().len(), 1);
        self.confirm(ns);
        self.effects();
        handles
    }

    pub fn handshake(&mut self) {
        self.receive(ServerMessage::Open(Handshake {
            sid: "engine".into(),
            upgrades: Vec::new(),
            ping_interval: 25_000,
            ping_timeout: 20_000,
            max_payload: 1_000_000,
        }));
    }

    /// The server confirms the namespace.
    pub fn confirm(&mut self, ns: &str) -> Option<Blocked> {
        let prefix = if ns == "/" {
            String::new()
        } else {
            format!("{ns},")
        };
        self.server(&format!("0{prefix}{{\"sid\":\"test\"}}"))
    }

    /// A server text message. Returns the packet a full receiver blocked.
    pub fn server(&mut self, text: &str) -> Option<Blocked> {
        self.receive(ServerMessage::Text(text.into()))
    }

    pub fn server_binary(&mut self, bytes: &'static [u8]) {
        self.receive(ServerMessage::Binary(Bytes::from_static(bytes)));
    }

    /// A packet from the last handles of `ns`, as `SocketSender::send`
    /// submits it: an event waits for its socket, and a reply is discarded
    /// unless the socket is connected.
    pub fn send(&mut self, ns: &str, packet: ClientPacket) {
        self.try_send(ns, packet).expect("the handles are live");
    }

    pub fn try_send(&mut self, ns: &str, packet: ClientPacket) -> Result<(), SocketError> {
        let (capacity, reply_capacity) = self.capacities[ns].clone();
        let ns = ByteString::from(ns);
        if matches!(packet, ClientPacket::Ack { .. }) {
            let Some(connection) = self.core.connection(&ns, &capacity) else {
                return Ok(());
            };
            let slot = reply_capacity
                .try_acquire_owned()
                .expect("the rig never fills the reply capacity");
            self.core.acknowledge(&ns, packet, slot, &connection);
            Ok(())
        } else {
            let slot = capacity
                .try_acquire_owned()
                .map_err(|_| SocketError::Closed)?;
            self.core.emit(&ns, packet, slot)
        }
    }

    /// The last handles of `ns` disconnect, by `disconnect` or by dropping.
    pub fn disconnect(&mut self, ns: &str) -> Vec<Effect> {
        let (capacity, _) = self.capacities[ns].clone();
        self.core.disconnect(&ns.into(), &capacity);
        self.effects()
    }

    /// The client handle drops.
    pub fn drop_client(&mut self) -> Vec<Effect> {
        self.core.drop_client();
        self.effects()
    }

    /// The engine finishes without an error.
    pub fn close(&mut self) -> Vec<Effect> {
        self.core.close(Ok(())).unwrap();
        self.effects()
    }

    /// The reconnection delay is over.
    pub fn reconnect(&mut self) -> Vec<Effect> {
        self.core.reconnect();
        self.effects()
    }

    fn receive(&mut self, message: ServerMessage) -> Option<Blocked> {
        self.core.receive(message)
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
