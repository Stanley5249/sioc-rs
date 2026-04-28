//! Socket.IO client and namespace handles.

use crate::ack::AckType;
use crate::error::{ClientBuilderError, ClientError, PayloadError, SocketError};
use crate::marker::{AckId, AckMarker, BinaryMarker};
use bytestring::ByteString;
use sioc_engine::engine::Engine;
use sioc_engine::transport::TransportStrategy;
use sioc_engine::websocket::{DefaultWebSocketConnector, WebSocketConnector};
use sioc_socket::error::ManagerError;
use sioc_socket::manager::{Manager, ManagerAction, ManagerSender, manager_sink};
use sioc_socket::packet::{Directive, Signal};
use std::sync::atomic::{AtomicBool, Ordering};
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
    fn prepare(self) -> Result<(Directive, Self::Output), PayloadError>;
}

/// Converts a typed acknowledgement into an ack [`Directive`].
pub trait Acknowledge<A, B>
where
    A: AckType,
    B: BinaryMarker,
{
    /// Serializes into an ack [`Directive`].
    fn into_directive(self, id: u64) -> Result<Directive, PayloadError>;
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
    pub fn open(self) -> Result<Client, ClientBuilderError> {
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
            tx: ManagerSender::new(manager_tx),
            handle: manager_handle,
        })
    }
}

/// A connected Socket.IO client.
#[derive(Debug)]
pub struct Client {
    tx: ManagerSender,
    handle: JoinHandle<Result<(), ManagerError>>,
}

impl Client {
    /// Returns a [`ClientBuilder`] targeting `url`.
    pub fn builder(url: impl Into<Url>) -> ClientBuilder {
        ClientBuilder::new(url)
    }

    /// Opens a namespace and returns a sender/receiver pair.
    ///
    /// The namespace is not confirmed until a [`Signal::Connect`] arrives on the [`SocketReceiver`].
    pub async fn connect<S>(&self, ns: S) -> Result<(SocketSender, SocketReceiver), SocketError>
    where
        S: Into<ByteString>,
    {
        self.connect_with(ns, ByteString::new()).await
    }

    /// Opens a namespace with a connection payload.
    pub async fn connect_with<S, B>(
        &self,
        ns: S,
        data: B,
    ) -> Result<(SocketSender, SocketReceiver), SocketError>
    where
        S: Into<ByteString>,
        B: Into<ByteString>,
    {
        let (tx, rx) = mpsc::channel(32);

        let socket_tx = SocketSender::new(ns.into(), self.tx.clone());

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
    /// The [`SocketSender`] must be dropped or explicitly disconnected before calling this.
    /// The manager exits only when the sender is dropped.
    pub async fn join(self) -> Result<(), ClientError> {
        drop(self.tx);
        self.handle.await??;
        Ok(())
    }
}

/// Sender for a Socket.IO namespace.
///
/// Owns the connection lifetime: dropping sends a disconnect packet automatically.
/// Wrap in [`Arc`](std::sync::Arc) to share across tasks.
#[derive(Debug)]
pub struct SocketSender {
    ns: ByteString,
    tx: ManagerSender,
    is_connected: AtomicBool,
}

impl SocketSender {
    fn new(ns: ByteString, tx: ManagerSender) -> Self {
        Self {
            ns,
            tx,
            is_connected: AtomicBool::new(true),
        }
    }

    async fn send(&self, directive: Directive) -> Result<(), SocketError> {
        self.tx
            .send(self.ns.clone(), directive)
            .await
            .map_err(SocketError::Send)
    }

    /// Emits an event; returns `()` or an [`AckHandle`](crate::ack::AckHandle) depending on the ack policy.
    pub async fn emit<E, A, B>(&self, event: E) -> Result<E::Output, SocketError>
    where
        E: Emit<A, B>,
        A: AckMarker,
        B: BinaryMarker,
    {
        let (directive, output) = event.prepare()?;
        self.send(directive).await?;
        Ok(output)
    }

    /// Acknowledges a received event.
    pub async fn acknowledge<T, A, B>(&self, id: AckId<A>, data: T) -> Result<(), SocketError>
    where
        T: Acknowledge<A, B>,
        A: AckType,
        B: BinaryMarker,
    {
        let directive = data.into_directive(id.get())?;
        self.send(directive).await
    }

    /// Closes this namespace; no-op if already disconnected.
    pub async fn disconnect(&self) -> Result<(), SocketError> {
        // Relaxed suffices: ns and manager_tx are immutable after construction,
        // so no other shared data needs synchronizing through this flag.
        if self.is_connected.swap(false, Ordering::Relaxed) {
            self.send(Directive::Disconnect).await?;
        }
        Ok(())
    }
}

impl Drop for SocketSender {
    fn drop(&mut self) {
        // Same reasoning as disconnect(): Relaxed is sufficient.
        if self.is_connected.swap(false, Ordering::Relaxed) {
            let type_name = std::any::type_name::<Self>();

            tracing::warn!(ns = %self.ns, "{type_name} dropped while connected");

            // try_send is non-blocking; if the channel is full or closed the
            // disconnect packet is lost, but we've already logged the warning.
            let _ = self.tx.try_send(self.ns.clone(), Directive::Disconnect);
        }
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
