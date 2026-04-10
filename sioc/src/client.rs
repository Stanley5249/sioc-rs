//! Socket.IO client and namespace handles.

use crate::ack::AckType;
use crate::error::Result;
use crate::marker::{AckId, AckMarker, BinaryMarker};
use bytes::Bytes;

use sioc_engine::engine::Engine;
use sioc_engine::transport::TransportStrategy;
use sioc_engine::websocket::{DefaultWebSocketConnector, WebSocketConnector};
use sioc_socket::error::Result as CoreResult;
use sioc_socket::manager::{Manager, ManagerAction, ManagerSender, manager_sink};
use sioc_socket::packet::{Directive, Signal};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use url::Url;

/// Converts a typed event into a [`Directive`] for emission.
///
/// `Output` is `()` for fire-and-forget events and [`AckHandle`](crate::ack::AckHandle)
/// for events that expect an acknowledgement.
pub trait Emit<A, B>
where
    A: AckMarker,
    B: BinaryMarker,
{
    /// Return value after the directive is sent.
    type Output;

    /// Serializes into a [`Directive`] and the output handle.
    fn prepare(self) -> Result<(Directive, Self::Output)>;
}

/// Converts a typed acknowledgement into an ack [`Directive`].
pub trait Acknowledge<A, B>
where
    A: AckType,
    B: BinaryMarker,
{
    /// Serializes into an ack [`Directive`].
    fn into_directive(self, id: u64) -> Result<Directive>;
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

    /// Connects to the Engine.IO server and returns a [`Client`].
    ///
    /// Spawns the engine and transport tasks in the background.
    #[must_use = "dropping the Client stops the background tasks"]
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
            manager_sink(manager_tx.clone()),
        );

        let manager = Manager::new(manager_rx);

        let manager_handle = tokio::spawn(manager.run(engine));

        Ok(Client {
            manager_tx: ManagerSender(manager_tx),
            manager_handle,
        })
    }
}

/// A connected Socket.IO client.
#[derive(Debug)]
pub struct Client {
    manager_tx: ManagerSender,
    manager_handle: JoinHandle<CoreResult<()>>,
}

impl Client {
    /// Returns a [`ClientBuilder`] targeting `url`.
    pub fn builder(url: impl Into<Url>) -> ClientBuilder {
        ClientBuilder::new(url)
    }

    /// Opens a namespace and returns a sender/receiver pair.
    ///
    /// The namespace is not confirmed until a [`Packet::Connect`] arrives on the [`SocketReceiver`].
    pub async fn connect(&self, ns: impl Into<String>) -> Result<(SocketSender, SocketReceiver)> {
        self.connect_with(ns, Bytes::from_static(b"")).await
    }

    /// Opens a namespace with a connection payload.
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

        let directive = Directive::Connect {
            tx,
            data: data.into(),
        };
        socket_tx.send(directive).await?;

        Ok((socket_tx, socket_rx))
    }

    /// Awaits the background manager task.
    ///
    /// All [`SocketSender`] clones must be dropped (via [`SocketSender::disconnect`])
    /// before calling this. The manager exits only when the last sender is dropped.
    pub async fn join(self) -> Result<()> {
        drop(self.manager_tx);
        self.manager_handle.await??;
        Ok(())
    }
}

/// Cloneable sender for a Socket.IO namespace.
#[derive(Debug, Clone)]
pub struct SocketSender {
    ns: String,
    manager_tx: ManagerSender,
}

impl SocketSender {
    async fn send(&self, directive: Directive) -> Result<()> {
        Ok(self.manager_tx.send(self.ns.clone(), directive).await?)
    }

    /// Emits an event; returns `()` or an [`AckHandle`](crate::ack::AckHandle) depending on the ack policy.
    pub async fn emit<E, A, B>(&self, event: E) -> Result<E::Output>
    where
        E: Emit<A, B>,
        A: AckMarker,
        B: BinaryMarker,
    {
        let (directive, output) = event.prepare()?;
        self.send(directive).await?;
        Ok(output)
    }

    /// Sends an acknowledgement for a received event.
    pub async fn ack<T, A, B>(&self, id: AckId<A>, data: T) -> Result<()>
    where
        T: Acknowledge<A, B>,
        A: AckType,
        B: BinaryMarker,
    {
        self.send(data.into_directive(id.get())?).await
    }

    /// Sends a disconnect packet, closing this namespace on the server.
    pub async fn disconnect(&self) -> Result<()> {
        self.send(Directive::Disconnect).await
    }
}

/// Receiver for a Socket.IO namespace.
#[derive(Debug)]
pub struct SocketReceiver {
    rx: mpsc::Receiver<Signal>,
}

impl SocketReceiver {
    /// Returns the next inbound packet, or `None` when the router shuts down.
    pub async fn recv(&mut self) -> Option<Signal> {
        self.rx.recv().await
    }
}
