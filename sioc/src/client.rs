//! Socket.IO client and namespace handles.

use crate::ack::AckType;
use crate::error::Result;
use crate::marker::{AckId, AckMarker, BinaryMarker};
use bytes::Bytes;

use sioc_core::error::Result as CoreResult;
use sioc_core::manager::{CommandSender, Manager, ManagerAction};
use sioc_core::packet::{Command, Ns, Packet};
use sioc_engine::engine::Engine;
use sioc_engine::transport::TransportStrategy;
use sioc_engine::websocket::{DefaultWebSocketConnector, WebSocketConnector};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use url::Url;

/// Converts a typed event into a [`Command`] for emission.
///
/// The associated `Output` type is `()` for fire-and-forget events and
/// [`AckHandle<A>`](crate::ack::AckHandle) for events that expect an acknowledgement.
pub trait Emit<A, B>
where
    A: AckMarker,
    B: BinaryMarker,
{
    /// The value returned to the caller after the command is sent.
    type Output;

    /// Serializes into a [`Command`] and the caller's output handle.
    fn prepare(self) -> Result<(Command, Self::Output)>;
}

/// Converts a typed acknowledgement into a [`Command`] for transmission.
pub trait Acknowledge<A, B>
where
    A: AckType,
    B: BinaryMarker,
{
    /// Serializes into an ack [`Command`] ready to send.
    fn into_command(self, id: u64) -> Result<Command>;
}

/// Builder for a [`Client`] connection.
///
/// # Example
///
/// ```rust,no_run
/// # async fn run() -> sioc::error::Result<()> {
/// use sioc::prelude::*;
/// use url::Url;
///
/// let url = Url::parse("http://localhost:3000").unwrap();
/// let client = ClientBuilder::new(url).open()?;
/// let (tx, mut rx) = client.connect("/").await?;
/// # Ok(())
/// # }
/// ```
pub struct ClientBuilder<C = DefaultWebSocketConnector> {
    url: Url,
    path: String,
    http_client: Option<reqwest::Client>,
    websocket_connector: C,
    transport_strategy: TransportStrategy,
}

impl ClientBuilder<DefaultWebSocketConnector> {
    /// Creates a builder targeting `url`.
    pub fn new(url: impl Into<Url>) -> Self {
        Self {
            url: url.into(),
            path: "socket.io/".to_string(),
            http_client: None,
            websocket_connector: DefaultWebSocketConnector,
            transport_strategy: TransportStrategy::default(),
        }
    }
}

impl<C> ClientBuilder<C>
where
    C: WebSocketConnector,
{
    /// Override the Engine.IO path segment (default: `"socket.io"`).
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Override the HTTP client used for polling.
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Override the WebSocket connector used for transport upgrade.
    ///
    /// Pass any type implementing [`WebSocketConnector`], including async closures.
    ///
    /// ```rust,no_run
    /// # async fn run() -> sioc::error::Result<()> {
    /// use sioc::prelude::*;
    /// use url::Url;
    ///
    /// // Example: wrap the default connector to add logging.
    /// let client = ClientBuilder::new(Url::parse("http://localhost:3000").unwrap())
    ///     .websocket_connector(|url| async move {
    ///         // add custom logging or TLS config here
    ///         DefaultWebSocketConnector.connect(url).await
    ///     })
    ///     .open()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn websocket_connector<C2>(self, connector: C2) -> ClientBuilder<C2>
    where
        C2: WebSocketConnector,
    {
        ClientBuilder {
            url: self.url,
            path: self.path,
            http_client: self.http_client,
            websocket_connector: connector,
            transport_strategy: self.transport_strategy,
        }
    }

    /// Override the initial transport strategy (default: HTTP long-polling with WebSocket upgrade).
    pub fn transport(mut self, strategy: TransportStrategy) -> Self {
        self.transport_strategy = strategy;
        self
    }

    /// Connect to the Engine.IO server and return a [`Client`].
    ///
    /// Spawns the engine and transport tasks in the background. Hold the
    /// returned [`Client`] and call [`Client::join`] to await clean shutdown.
    pub fn open(self) -> Result<Client> {
        let http_client = self.http_client.unwrap_or_default();
        let websocket_connector = self.websocket_connector;
        let url = self.url.join(&self.path)?;

        let (manager_tx, manager_rx) = mpsc::channel::<ManagerAction>(32);

        let engine = Engine::connect(
            url,
            http_client,
            websocket_connector,
            self.transport_strategy,
            manager_tx.clone(),
        );

        let manager = Manager::new(manager_rx);

        let manager_handle = tokio::spawn(manager.run(engine));

        let manager_tx = CommandSender(manager_tx);

        Ok(Client {
            manager_tx,
            manager_handle,
        })
    }
}

/// A connected Socket.IO client.
///
/// Created by [`ClientBuilder::open`]. Use [`connect`](Self::connect) to open
/// namespace handles, and [`join`](Self::join) to await the background tasks.
#[derive(Debug)]
pub struct Client {
    manager_tx: CommandSender,
    manager_handle: JoinHandle<CoreResult<()>>,
}

impl Client {
    /// Returns a [`ClientBuilder`] targeting `url`.
    pub fn builder(url: impl Into<Url>) -> ClientBuilder {
        ClientBuilder::new(url)
    }

    /// Opens a Socket.IO namespace and returns a sender/receiver pair.
    ///
    /// Sends a Socket.IO `Connect` packet to the server. The namespace
    /// is not yet confirmed — await a [`Packet::Connect`] from the
    /// [`SocketReceiver`] before emitting events.
    pub async fn connect(&self, ns: impl Into<String>) -> Result<(SocketSender, SocketReceiver)> {
        self.connect_with(ns, Bytes::from_static(b"")).await
    }

    /// Opens a namespace with an initial connection payload.
    pub async fn connect_with(
        &self,
        ns: impl Into<String>,
        data: impl Into<Bytes>,
    ) -> Result<(SocketSender, SocketReceiver)> {
        let (tx, rx) = mpsc::channel(32);

        let socket_tx = SocketSender {
            ns: ns.into(),
            manager_tx: self.manager_tx.clone(),
        };
        let socket_rx = SocketReceiver { rx };

        let command = Command::Connect {
            tx,
            data: data.into(),
        };
        socket_tx.send(command).await?;

        Ok((socket_tx, socket_rx))
    }

    /// Await the background tasks.
    ///
    /// Drops all namespace senders to signal the manager to stop, then awaits
    /// the manager task (which in turn awaits engine and transport).
    pub async fn join(self) -> Result<()> {
        // TODO: drop sender does not make manager stop
        // since engine also holds manager sender,
        // we need to disconnect all namespaces
        drop(self.manager_tx);
        self.manager_handle.await??;
        Ok(())
    }
}

/// Clonable sender half of a Socket.IO namespace handle.
///
/// Provides typed [`emit`](Self::emit) and [`ack`](Self::ack) methods.
/// Obtained from [`Client::connect`] alongside a [`SocketReceiver`].
#[derive(Debug, Clone)]
pub struct SocketSender {
    ns: String,
    manager_tx: CommandSender,
}

impl SocketSender {
    async fn send(&self, packet: Command) -> Result<()> {
        let packet = Ns(self.ns.clone(), packet);
        Ok(self.manager_tx.send(packet).await?)
    }

    /// Emits an event and returns the output determined by the event's ack
    /// policy: `()` for fire-and-forget, [`AckHandle`](crate::ack::AckHandle)
    /// for events that expect an acknowledgement.
    pub async fn emit<E, A, B>(&self, event: E) -> Result<E::Output>
    where
        E: Emit<A, B>,
        A: AckMarker,
        B: BinaryMarker,
    {
        let (command, output) = event.prepare()?;
        self.send(command).await?;
        Ok(output)
    }

    /// Sends an acknowledgement for a previously received event.
    pub async fn ack<T, A, B>(&self, id: AckId<A>, data: T) -> Result<()>
    where
        T: Acknowledge<A, B>,
        A: AckType,
        B: BinaryMarker,
    {
        self.send(data.into_command(id.get())?).await
    }

    /// Sends a disconnect packet, closing this namespace on the server.
    pub async fn disconnect(&self) -> Result<()> {
        self.send(Command::Disconnect).await
    }
}

/// Receiver half of a Socket.IO namespace handle.
///
/// Yields inbound [`Packet`]s. Obtained from [`Client::connect`]
/// alongside a [`SocketSender`].
#[derive(Debug)]
pub struct SocketReceiver {
    rx: mpsc::Receiver<Packet>,
}

impl SocketReceiver {
    /// Receives the next packet from the server for this namespace.
    ///
    /// Returns `None` when the manager shuts down.
    pub async fn recv(&mut self) -> Option<Packet> {
        self.rx.recv().await
    }
}
