//! Connection setup and manager lifecycle, like socket.io-client's `Manager`.
//! The builder and client stay together so opening transfers the owned task
//! and shared manager directly into a handle with private fields.

use std::sync::Arc;
use std::time::Duration;

use bytestring::ByteString;
use eioc::engine::EngineOptions;
use eioc::prelude::{TransportStrategy, WebSocketConnector};
use tokio::task::JoinHandle;
use url::Url;

use crate::client::{SocketReceiver, SocketSender};
use crate::config::{ChannelConfig, ReconnectionConfig};
use crate::error::{ClientBuilderError, ClientError, ManagerError, SocketError};
use crate::manager::backoff::Backoff;
use crate::manager::driver::Manager;
use crate::manager::state::ManagerCore;

/// Connection options, like socket.io-client's `Manager` options.
/// The builder creates a [`Client`] and its manager task.
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
#[must_use = "call open() to connect"]
pub struct ClientBuilder<C = ()> {
    url: Url,
    path: String,
    http_client: Option<reqwest::Client>,
    websocket_connector: C,
    transport_strategy: TransportStrategy,
    channels: ChannelConfig,
    reconnection: Option<ReconnectionConfig>,
    timeout: Option<Duration>,
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
            reconnection: Some(ReconnectionConfig::default()),
            timeout: Some(Duration::from_secs(20)),
        }
    }
}

impl<C> ClientBuilder<C>
where
    C: WebSocketConnector + Clone,
{
    /// Override the Engine.IO path, joined to the URL (default:
    /// `"socket.io/"`).
    ///
    /// Keep the trailing slash: by default the server matches its path with
    /// one, as engine.io's `addTrailingSlash` option does.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Override the HTTP client used for polling.
    ///
    /// Closing a polling session waits for the server to answer the Close
    /// request, unlike the JS client, which does not wait. The default client
    /// has no timeout, so a server that accepts the request and never answers
    /// holds the close, and [`Client::join`], open. Set a timeout on this
    /// client to bound it.
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Override the WebSocket connector used for transport upgrade.
    ///
    /// Pass any type implementing [`WebSocketConnector`], including async
    /// closures. Each Engine.IO session uses its own clone.
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
        C2: WebSocketConnector + Clone,
    {
        ClientBuilder {
            url: self.url,
            path: self.path,
            http_client: self.http_client,
            websocket_connector: connector,
            transport_strategy: self.transport_strategy,
            channels: self.channels,
            reconnection: self.reconnection,
            timeout: self.timeout,
        }
    }

    /// Override the initial transport strategy (default: HTTP long-polling with
    /// WebSocket upgrade).
    pub fn transport(mut self, strategy: TransportStrategy) -> Self {
        self.transport_strategy = strategy;
        self
    }

    /// Override the positive channel buffer capacities (default: 32 for all
    /// channels).
    ///
    /// Accepts `()` for defaults, a `usize` for uniform sizing, or a
    /// [`ChannelConfig`] for per-channel control.
    pub fn channels(mut self, config: impl Into<ChannelConfig>) -> Self {
        self.channels = config.into();
        self
    }

    /// Override the reconnection settings, or pass `None` to close every
    /// namespace when an Engine.IO session drops instead (default:
    /// [`ReconnectionConfig::default`]).
    ///
    /// The client stays usable either way: once the namespaces close, the next
    /// [`Client::connect`] opens a new session.
    pub fn reconnection(mut self, config: Option<ReconnectionConfig>) -> Self {
        self.reconnection = config;
        self
    }

    /// Override how long each connection attempt waits for the server to
    /// accept the Engine.IO handshake, or pass `None` to wait without a limit,
    /// like socket.io-client's `timeout` option (default: 20 seconds).
    ///
    /// An attempt that times out counts as failed, so the client reconnects as
    /// after a dropped session. This limit covers only the handshake, unlike
    /// [`reqwest::ClientBuilder::timeout`], which limits every HTTP request,
    /// including each long poll.
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// Connects to the Engine.IO server and returns a [`Client`].
    ///
    /// Spawns the manager task, which opens a new engine after the backoff
    /// delay when a connection drops while namespaces are open.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid or any channel capacity is zero.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    #[must_use = "call join() to observe the background task result"]
    pub fn open(self) -> Result<Client, ClientBuilderError> {
        for (channel, capacity) in [
            ("engine", self.channels.engine),
            ("transport", self.channels.transport),
            ("manager", self.channels.manager),
            ("socket", self.channels.socket),
        ] {
            if capacity == 0 {
                return Err(ClientBuilderError::ZeroCapacity { channel });
            }
        }

        let http_client = self.http_client.unwrap_or_default();
        let websocket_connector = self.websocket_connector;
        let transport_strategy = self.transport_strategy;
        let channels = self.channels;
        let timeout = self.timeout;
        let backoff = self.reconnection.map(|config| {
            Backoff::new(
                config.delay,
                config.delay_max,
                config.randomization_factor,
                config.attempts,
            )
        });
        let mut url = self.url.join(&self.path)?;
        // The endpoint path replaces the URL path, while auth and other
        // connection query parameters belong to every transport attempt.
        if self.url.query().is_some() {
            url.query_pairs_mut().extend_pairs(self.url.query_pairs());
        }

        let manager = Manager::new(ManagerCore::new(backoff, fastrand::Rng::new()));

        // Each engine gets its own connector.
        let connect_engine = move || {
            let mut options = EngineOptions::new(url.clone(), websocket_connector.clone());
            options.http_client = http_client.clone();
            options.transport = transport_strategy;
            options.handshake_timeout = timeout;
            options.server_message_capacity = channels.manager;
            options.client_message_capacity = channels.engine;
            options.server_frame_capacity = channels.engine;
            options.client_frame_capacity = channels.transport;
            eioc::engine::open(options).into()
        };

        let task = tokio::spawn(crate::manager::driver::run(
            Arc::clone(&manager),
            connect_engine,
        ));

        Ok(Client {
            handle: ClientHandle(manager),
            task,
            channels: self.channels,
        })
    }
}

/// A Socket.IO manager handle, like socket.io-client's `Manager`.
#[derive(Debug)]
pub struct Client {
    handle: ClientHandle,
    task: JoinHandle<Result<(), ManagerError>>,
    channels: ChannelConfig,
}

/// Tells the manager that the client handle is gone once it drops, so the
/// manager ends with its last socket. Rust cleanup.
#[derive(Debug)]
struct ClientHandle(Arc<Manager>);

impl Drop for ClientHandle {
    fn drop(&mut self) {
        self.0.with_core(ManagerCore::drop_client);
    }
}

impl Client {
    /// Returns a [`ClientBuilder`] targeting `url`.
    pub fn builder(url: impl Into<Url>) -> ClientBuilder {
        ClientBuilder::new(url)
    }

    /// Opens a namespace and returns a sender/receiver pair.
    ///
    /// The namespace is not confirmed until a
    /// [`ServerPacket::Connect`](crate::packet::ServerPacket::Connect) arrives
    /// on the [`SocketReceiver`].
    ///
    /// # Errors
    ///
    /// Returns an error if the client has ended.
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
    /// Returns [`SocketError::NamespaceConflict`] if the namespace is already
    /// open, or [`SocketError::Closed`] if the client has ended.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "the public API stays async, although the manager now accepts a namespace synchronously"
    )]
    pub async fn connect_with<S, B>(
        &self,
        ns: S,
        payload: B,
    ) -> Result<(SocketSender, SocketReceiver), SocketError>
    where
        S: Into<ByteString>,
        B: Into<ByteString>,
    {
        crate::client::connect(
            &self.handle.0,
            ns.into(),
            payload.into(),
            self.channels.manager,
            self.channels.socket,
        )
    }

    /// Drops the client handle and waits for the client to end.
    ///
    /// The client ends once no namespace is open. A namespace closes once every
    /// [`SocketSender`] of it is dropped or disconnected, or when the server
    /// closes it. A dropped Engine.IO session does not close it: the client
    /// reconnects and sends CONNECT again.
    ///
    /// # Errors
    ///
    /// Returns an error if the manager task fails with a library bug or panics.
    /// Call this method to collect those errors; dropping the handle detaches
    /// the task, which continues while namespace senders remain alive.
    pub async fn join(self) -> Result<(), ClientError> {
        let Self { handle, task, .. } = self;
        drop(handle);
        task.await??;
        Ok(())
    }
}
