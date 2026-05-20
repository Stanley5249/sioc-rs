//! Socket.IO client and namespace handles.

use crate::ack::AckType;
use crate::error::ManagerError;
use crate::error::{ClientBuilderError, ClientError, PayloadError, SocketError};
use crate::manager::{DirectiveSender, Manager, ManagerAction, message_sink};
use crate::marker::{AckId, AckMarker, BinaryMarker};
use crate::packet::{Directive, DynEvent, Signal};
use bytestring::ByteString;
use eioc::engine::{FrameSender, MessageSender};
use eioc::transport::TransportStrategy;
use eioc::websocket::WebSocketConnector;
use futures_util::TryFutureExt;
use std::sync::Arc;
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
    ///
    /// # Errors
    ///
    /// Returns an error if payload serialization fails.
    fn prepare(self) -> Result<(Directive, Self::Output), PayloadError>;
}

/// Converts a typed acknowledgement into an ack [`Directive`].
pub trait Acknowledge<A, B>
where
    A: AckType,
    B: BinaryMarker,
{
    /// Serializes into an ack [`Directive`].
    ///
    /// # Errors
    ///
    /// Returns an error if payload serialization fails.
    fn into_directive(self, id: u64) -> Result<Directive, PayloadError>;
}

/// Channel buffer capacities for each internal MPSC queue.
///
/// Construct via [`From<()>`] for defaults, [`From<usize>`] for uniform sizing,
/// or build manually for per-channel control.
#[derive(Clone, Copy, Debug)]
pub struct ChannelConfig {
    /// Engine task inbox: frames from the transport and messages from the Socket.IO layer.
    pub engine: usize,
    /// Transport channel: encoded frames to send to the transport.
    pub transport: usize,
    /// Manager task inbox: directives from all namespace senders.
    pub manager: usize,
    /// Per-namespace inbox: signals delivered to each [`SocketReceiver`].
    pub socket: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            engine: 32,
            transport: 32,
            manager: 32,
            socket: 32,
        }
    }
}

impl From<()> for ChannelConfig {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

impl From<usize> for ChannelConfig {
    fn from(n: usize) -> Self {
        Self {
            engine: n,
            transport: n,
            manager: n,
            socket: n,
        }
    }
}

/// Builder for a [`Client`] connection.
#[must_use = "call open() to connect"]
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
pub struct ClientBuilder<C = ()> {
    url: Url,
    path: String,
    http_client: Option<reqwest::Client>,
    websocket_connector: C,
    transport_strategy: TransportStrategy,
    channels: ChannelConfig,
}

impl ClientBuilder<()> {
    /// Creates a builder targeting `url`.
    pub fn new(url: impl Into<Url>) -> Self {
        Self {
            url: url.into(),
            path: "socket.io/".to_string(),
            http_client: None,
            websocket_connector: (),
            transport_strategy: TransportStrategy::default(),
            channels: ChannelConfig::default(),
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
    ///         ().connect(url).await
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
            channels: self.channels,
        }
    }

    /// Override the initial transport strategy (default: HTTP long-polling with WebSocket upgrade).
    pub fn transport(mut self, strategy: TransportStrategy) -> Self {
        self.transport_strategy = strategy;
        self
    }

    /// Override the channel buffer capacities (default: 32 for all channels).
    ///
    /// Accepts `()` for defaults, a `usize` for uniform sizing, or a [`ChannelConfig`] for
    /// per-channel control.
    pub fn channels(mut self, config: impl Into<ChannelConfig>) -> Self {
        self.channels = config.into();
        self
    }

    /// Connects to the Engine.IO server and returns a [`Client`].
    ///
    /// Spawns the manager task, which drives the engine and transport concurrently.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid.
    #[must_use = "dropping the Client stops the background tasks"]
    pub fn open(self) -> Result<Client, ClientBuilderError> {
        let http_client = self.http_client.unwrap_or_default();
        let websocket_connector = self.websocket_connector;
        let url = self.url.join(&self.path)?;

        let (manager_tx, manager_rx) = mpsc::channel::<ManagerAction>(self.channels.manager);

        let (engine_tx, engine_rx) = mpsc::channel(self.channels.engine);

        let frame_tx = FrameSender(engine_tx.clone());

        let message_tx = MessageSender(engine_tx);

        let manager = Manager::new(manager_rx);

        let sio_future = manager.socket_io(message_tx);

        let eio_future = eioc::engine::connect(
            url,
            http_client,
            websocket_connector,
            self.transport_strategy,
            message_sink(manager_tx.clone()),
            engine_rx,
            frame_tx,
            self.channels.transport,
        );

        let eio_future = eio_future.map_err(ManagerError::Engine);

        let handle = tokio::spawn(async {
            tokio::try_join!(sio_future, eio_future)?;
            Ok(())
        });

        Ok(Client {
            tx: DirectiveSender::new(manager_tx),
            handle,
            socket_capacity: self.channels.socket,
        })
    }
}

/// A connected Socket.IO client.
#[derive(Debug)]
pub struct Client {
    tx: DirectiveSender,
    handle: JoinHandle<Result<(), ManagerError>>,
    socket_capacity: usize,
}

impl Client {
    /// Returns a [`ClientBuilder`] targeting `url`.
    pub fn builder(url: impl Into<Url>) -> ClientBuilder {
        ClientBuilder::new(url)
    }

    /// Opens a namespace and returns a sender/receiver pair.
    ///
    /// The namespace is not confirmed until a [`Signal::Connect`] arrives on the [`SocketReceiver`].
    ///
    /// # Errors
    ///
    /// Returns an error if the manager channel is closed.
    pub async fn connect<S>(&self, ns: S) -> Result<(SocketSender, SocketReceiver), SocketError>
    where
        S: Into<ByteString>,
    {
        self.connect_with(ns, ByteString::new()).await
    }

    /// Opens a namespace with a connection payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the manager channel is closed.
    pub async fn connect_with<S, B>(
        &self,
        ns: S,
        payload: B,
    ) -> Result<(SocketSender, SocketReceiver), SocketError>
    where
        S: Into<ByteString>,
        B: Into<ByteString>,
    {
        let (tx, rx) = mpsc::channel(self.socket_capacity);

        let socket_tx = SocketSender::new(ns.into(), self.tx.clone());

        let socket_rx = SocketReceiver { rx };

        let directive = Directive::Connect {
            tx,
            payload: payload.into(),
        };
        socket_tx.0.send(directive).await?;

        Ok((socket_tx, socket_rx))
    }

    /// Awaits the background manager task.
    ///
    /// The [`SocketSender`] must be dropped or explicitly disconnected before calling this.
    /// The manager exits only when the sender is dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the manager task fails or panics.
    pub async fn join(self) -> Result<(), ClientError> {
        drop(self.tx);
        self.handle.await??;
        Ok(())
    }
}

#[derive(Debug)]
struct SocketSenderInner {
    ns: ByteString,
    tx: DirectiveSender,
    disconnected: AtomicBool,
}

impl SocketSenderInner {
    async fn send(&self, directive: Directive) -> Result<(), SocketError> {
        self.tx
            .send(self.ns.clone(), directive)
            .await
            .map_err(SocketError::Send)
    }
}

impl Drop for SocketSenderInner {
    fn drop(&mut self) {
        if !self.disconnected.swap(true, Ordering::Relaxed) {
            let _ = self.tx.try_send(self.ns.clone(), Directive::Dropped);
        }
    }
}

/// Sender for a Socket.IO namespace.
///
/// Cloning is cheap — all clones share the same connection. The disconnect
/// packet is sent automatically when the last clone is dropped.
#[derive(Clone, Debug)]
pub struct SocketSender(Arc<SocketSenderInner>);

impl SocketSender {
    fn new(ns: ByteString, tx: DirectiveSender) -> Self {
        Self(Arc::new(SocketSenderInner {
            ns,
            tx,
            disconnected: AtomicBool::new(false),
        }))
    }

    /// Emits an event; returns `()` or an [`AckHandle`](crate::ack::AckHandle) depending on the ack policy.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the manager channel is closed.
    pub async fn emit<E, A, B>(&self, event: E) -> Result<E::Output, SocketError>
    where
        E: Emit<A, B>,
        A: AckMarker,
        B: BinaryMarker,
    {
        let (directive, output) = event.prepare()?;
        self.0.send(directive).await?;
        Ok(output)
    }

    /// Acknowledges a received event.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the manager channel is closed.
    pub async fn acknowledge<T, A, B>(&self, id: AckId<A>, payload: T) -> Result<(), SocketError>
    where
        T: Acknowledge<A, B>,
        A: AckType,
        B: BinaryMarker,
    {
        let directive = payload.into_directive(id.get())?;
        self.0.send(directive).await
    }

    /// Sends a graceful disconnect packet and marks this sender as disconnected.
    ///
    /// Idempotent: subsequent calls and calls made after a server-initiated disconnect
    /// both return immediately. Prefer this over dropping when you need a guaranteed
    /// async send rather than the fire-and-forget `try_send` in `Drop`.
    pub async fn disconnect(&self) {
        if !self.0.disconnected.swap(true, Ordering::Relaxed) {
            // Ignore SendError: a closed channel means the manager already handled
            // the disconnect (server-initiated), so we are already disconnected.
            let _ = self.0.send(Directive::Disconnect).await;
        }
    }
}

/// Receiver for a Socket.IO namespace.
#[derive(Debug)]
pub struct SocketReceiver {
    rx: mpsc::Receiver<Signal>,
}

impl SocketReceiver {
    /// Returns the next application event. [`Signal::Connect`], [`Signal::Disconnect`], and
    /// [`Signal::ConnectError`] are silently dropped; they do not close the receiver.
    /// Returns `None` only when the channel closes (router shut down).
    ///
    /// Cancel safe: the only suspend point is `recv`; skipped protocol signals have no
    /// suspend point after consumption, so no events are lost on cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be converted into `E`.
    pub async fn listen<E>(&mut self) -> Result<Option<E>, E::Error>
    where
        E: TryFrom<DynEvent>,
    {
        loop {
            match self.rx.recv().await {
                None => return Ok(None),
                Some(Signal::Event(e)) => return E::try_from(e).map(Some),
                Some(_) => continue,
            }
        }
    }
}

impl std::ops::Deref for SocketReceiver {
    type Target = mpsc::Receiver<Signal>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl std::ops::DerefMut for SocketReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}
