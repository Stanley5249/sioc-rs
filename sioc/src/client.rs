//! Socket.IO client and namespace handles.

use crate::ack::AckType;
use crate::error::ManagerError;
use crate::error::{ClientBuilderError, ClientError, PayloadError, SocketError};
use crate::manager::{self, ConnectRequest};
use crate::marker::{AckId, AckMarker, BinaryMarker};
use crate::packet::{Directive, DynEvent, Signal};
use bytestring::ByteString;
use eioc::transport::TransportStrategy;
use eioc::websocket::WebSocketConnector;
use futures_util::TryFutureExt;
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
    /// Engine task inboxes: frames from the transport and messages from the manager.
    pub engine: usize,
    /// Transport channel: encoded frames to send to the transport.
    pub transport: usize,
    /// Manager inboxes: messages from the engine, new namespaces, and each namespace's directives.
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
    fn from((): ()) -> Self {
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

        let (connect_request_tx, connect_request_rx) = mpsc::channel(self.channels.manager);

        let (server_message_tx, server_message_rx) = mpsc::channel(self.channels.manager);

        let (client_message_tx, client_message_rx) = mpsc::channel(self.channels.engine);

        let manager_future = manager::run(connect_request_rx, server_message_rx, client_message_tx);

        let engine_future = eioc::engine::connect(
            url,
            http_client,
            websocket_connector,
            self.transport_strategy,
            server_message_tx,
            client_message_rx,
            self.channels.engine,
            self.channels.transport,
        );

        let engine_future = engine_future.map_err(ManagerError::Engine);

        let task = tokio::spawn(async {
            tokio::try_join!(manager_future, engine_future)?;
            Ok(())
        });

        Ok(Client {
            connect_request_tx,
            task,
            channels: self.channels,
        })
    }
}

/// A connected Socket.IO client.
#[derive(Debug)]
pub struct Client {
    connect_request_tx: mpsc::Sender<ConnectRequest>,
    task: JoinHandle<Result<(), ManagerError>>,
    channels: ChannelConfig,
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
    /// Returns an error if the session has ended.
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
    /// Returns an error if the session has ended.
    pub async fn connect_with<S, B>(
        &self,
        ns: S,
        payload: B,
    ) -> Result<(SocketSender, SocketReceiver), SocketError>
    where
        S: Into<ByteString>,
        B: Into<ByteString>,
    {
        let (directive_tx, directive_rx) = mpsc::channel(self.channels.manager);

        let (signal_tx, signal_rx) = mpsc::channel(self.channels.socket);

        let connect_request = ConnectRequest {
            ns: ns.into(),
            payload: payload.into(),
            directive_rx,
            signal_tx,
        };

        self.connect_request_tx
            .send(connect_request)
            .await
            .map_err(|_| SocketError::Closed)?;

        Ok((SocketSender { directive_tx }, SocketReceiver { signal_rx }))
    }

    /// Drops the client handle and waits for the session to end.
    ///
    /// The session ends once every [`SocketSender`] is dropped or disconnected,
    /// or when the server closes it.
    ///
    /// # Errors
    ///
    /// Returns an error if the manager task fails or panics.
    pub async fn join(self) -> Result<(), ClientError> {
        drop(self.connect_request_tx);
        self.task.await??;
        Ok(())
    }
}

/// Sender for a Socket.IO namespace.
///
/// Cloning is cheap, and all clones share the namespace. The namespace
/// disconnects when the last clone is dropped.
#[derive(Clone, Debug)]
pub struct SocketSender {
    directive_tx: mpsc::Sender<Directive>,
}

impl SocketSender {
    async fn send(&self, directive: Directive) -> Result<(), SocketError> {
        self.directive_tx
            .send(directive)
            .await
            .map_err(|_| SocketError::Closed)
    }

    /// Emits an event; returns `()` or an [`AckHandle`](crate::ack::AckHandle) depending on the ack policy.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the namespace has closed.
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
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the namespace has closed.
    pub async fn acknowledge<T, A, B>(&self, id: AckId<A>, payload: T) -> Result<(), SocketError>
    where
        T: Acknowledge<A, B>,
        A: AckType,
        B: BinaryMarker,
    {
        let directive = payload.into_directive(id.get())?;
        self.send(directive).await
    }

    /// Sends a graceful disconnect packet for the namespace.
    ///
    /// Idempotent: once the namespace has closed, by either side, the call returns immediately.
    pub async fn disconnect(&self) {
        // A closed channel means the namespace has already closed.
        let _ = self.send(Directive::Disconnect).await;
    }
}

/// Receiver for a Socket.IO namespace.
#[derive(Debug)]
pub struct SocketReceiver {
    signal_rx: mpsc::Receiver<Signal>,
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
            match self.signal_rx.recv().await {
                None => return Ok(None),
                Some(Signal::Event(e)) => return E::try_from(e).map(Some),
                Some(_) => {}
            }
        }
    }
}

impl std::ops::Deref for SocketReceiver {
    type Target = mpsc::Receiver<Signal>;

    fn deref(&self) -> &Self::Target {
        &self.signal_rx
    }
}

impl std::ops::DerefMut for SocketReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.signal_rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PayloadError;
    use crate::marker::{HasAck, NoAck, NoBinary};
    use crate::packet::{Connect, ConnectError, Directive, DynEvent, Signal};
    use eioc::transport::TransportStrategy;
    use serde_json::Map;
    use tokio::sync::mpsc;
    use url::Url;

    struct TestEmit;

    impl Emit<NoAck, NoBinary> for TestEmit {
        type Output = ();

        fn prepare(self) -> Result<(Directive, ()), PayloadError> {
            Ok((
                Directive::Event {
                    payload: r#"["test"]"#.into(),
                    ack_tx: None,
                    attachments: None,
                },
                (),
            ))
        }
    }

    fn socket_sender() -> (SocketSender, mpsc::Receiver<Directive>) {
        let (directive_tx, directive_rx) = mpsc::channel(8);
        (SocketSender { directive_tx }, directive_rx)
    }

    struct Pass(DynEvent);

    impl From<DynEvent> for Pass {
        fn from(e: DynEvent) -> Self {
            Self(e)
        }
    }

    #[test]
    fn channel_config_default_is_32() {
        let c = ChannelConfig::default();
        assert_eq!(
            (c.engine, c.transport, c.manager, c.socket),
            (32, 32, 32, 32)
        );
    }

    #[test]
    fn channel_config_from_unit_matches_default() {
        let c = ChannelConfig::from(());
        assert_eq!(
            (c.engine, c.transport, c.manager, c.socket),
            (32, 32, 32, 32)
        );
    }

    #[test]
    fn channel_config_from_usize_uniform() {
        let c = ChannelConfig::from(8_usize);
        assert_eq!((c.engine, c.transport, c.manager, c.socket), (8, 8, 8, 8));
    }

    #[tokio::test]
    async fn listen_skips_protocol_signals() {
        let (tx, rx) = mpsc::channel(8);
        let mut receiver = SocketReceiver { signal_rx: rx };
        tx.send(Signal::Connect(Connect {
            sid: ByteString::default(),
            extra: Map::default(),
        }))
        .await
        .unwrap();
        tx.send(Signal::Disconnect).await.unwrap();
        tx.send(Signal::ConnectError(ConnectError {
            message: "err".into(),
            extra: Map::default(),
        }))
        .await
        .unwrap();
        tx.send(Signal::Event(DynEvent::new(r#"["hi"]"#, None)))
            .await
            .unwrap();
        let Pass(event) = receiver.listen::<Pass>().await.unwrap().unwrap();
        assert_eq!(event.payload, r#"["hi"]"#);
    }

    #[tokio::test]
    async fn listen_returns_none_on_closed_channel() {
        let (tx, rx) = mpsc::channel::<Signal>(4);
        let mut receiver = SocketReceiver { signal_rx: rx };
        drop(tx);
        assert!(receiver.listen::<Pass>().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dropping_last_clone_closes_directive_channel() {
        let (sender, mut directive_rx) = socket_sender();
        let clone = sender.clone();
        drop(sender);
        directive_rx.try_recv().unwrap_err();
        drop(clone);
        assert!(directive_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn disconnect_sends_disconnect_directive() {
        let (sender, mut directive_rx) = socket_sender();
        sender.disconnect().await;
        assert!(matches!(
            directive_rx.try_recv().unwrap(),
            Directive::Disconnect
        ));
    }

    #[tokio::test]
    async fn disconnect_after_close_returns() {
        let (sender, directive_rx) = socket_sender();
        drop(directive_rx);
        sender.disconnect().await;
    }

    #[tokio::test]
    async fn open_returns_client() {
        let url = Url::parse("http://localhost:9999/").unwrap();
        ClientBuilder::new(url).open().unwrap();
    }

    #[tokio::test]
    async fn client_builder_alias() {
        let url = Url::parse("http://localhost:9999/").unwrap();
        Client::builder(url).open().unwrap();
    }

    #[tokio::test]
    async fn builder_path_channels_transport() {
        let url = Url::parse("http://localhost:9999/").unwrap();
        let result = ClientBuilder::new(url)
            .path("socket.io/")
            .channels(16_usize)
            .transport(TransportStrategy::WebSocket)
            .open();
        result.unwrap();
    }

    #[tokio::test]
    async fn builder_http_client() {
        let url = Url::parse("http://localhost:9999/").unwrap();
        let result = ClientBuilder::new(url)
            .http_client(reqwest::Client::new())
            .open();
        result.unwrap();
    }

    #[tokio::test]
    async fn emit_sends_event_directive() {
        let (sender, mut rx) = socket_sender();
        sender.emit(TestEmit).await.unwrap();
        assert!(matches!(rx.try_recv().unwrap(), Directive::Event { .. }));
    }

    #[tokio::test]
    async fn emit_returns_error_on_closed_channel() {
        let (sender, rx) = socket_sender();
        drop(rx);
        assert!(matches!(
            sender.emit(TestEmit).await,
            Err(SocketError::Closed)
        ));
    }

    #[tokio::test]
    async fn acknowledge_sends_ack_directive_with_correct_id() {
        let (sender, mut rx) = socket_sender();
        let id = HasAck::<()>::parse(Some(5)).unwrap();
        sender.acknowledge(id, ()).await.unwrap();
        let Directive::Ack { id, .. } = rx.try_recv().unwrap() else {
            panic!("expected Ack directive");
        };
        assert_eq!(id, 5);
    }

    #[test]
    fn socket_receiver_deref_gives_inner_receiver() {
        let (_tx, rx) = mpsc::channel::<Signal>(4);
        let receiver = SocketReceiver { signal_rx: rx };
        let _ = &*receiver;
    }
}
