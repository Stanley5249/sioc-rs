//! Socket.IO client and namespace handles.

use std::time::Duration;

use bytestring::ByteString;
use eioc::connector::WebSocketConnector;
use eioc::transport::TransportStrategy;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::ack::AckType;
use crate::error::{ClientBuilderError, ClientError, ManagerError, PayloadError, SocketError};
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::supervisor::Supervisor;
use crate::marker::{AckId, AckMarker, BinaryMarker};
use crate::packet::{ClientPacket, DynEvent, ServerAckId, ServerPacket};

/// Converts a typed event into a [`ClientPacket`] for emission.
///
/// `Output` is `()` for fire-and-forget events and
/// [`AckHandle`](crate::ack::AckHandle) for events that expect an
/// acknowledgement.
pub trait Emit<A, B>
where
    A: AckMarker,
    B: BinaryMarker,
{
    /// Return value after the packet is sent.
    type Output;

    /// Serializes into a [`ClientPacket`] and the output handle.
    ///
    /// # Errors
    ///
    /// Returns an error if payload serialization fails.
    fn prepare(self) -> Result<(ClientPacket, Self::Output), PayloadError>;
}

/// Converts a typed acknowledgement into an ack [`ClientPacket`].
pub trait Acknowledge<A, B>
where
    A: AckType,
    B: BinaryMarker,
{
    /// Serializes into an ack [`ClientPacket`].
    ///
    /// # Errors
    ///
    /// Returns an error if payload serialization fails.
    fn into_client_packet(self, id: ServerAckId) -> Result<ClientPacket, PayloadError>;
}

/// Channel buffer capacities for each internal MPSC queue.
///
/// Construct via [`From<()>`] for defaults, [`From<usize>`] for uniform sizing,
/// or build manually for per-channel control.
#[derive(Clone, Copy, Debug)]
pub struct ChannelConfig {
    /// Engine task inboxes: frames from the transport and messages from the
    /// manager.
    pub engine: usize,
    /// Transport channel: encoded frames to send to the transport.
    pub transport: usize,
    /// Manager inboxes: messages from the engine, new namespaces, and each
    /// namespace's client packets.
    pub manager: usize,
    /// Per-namespace inbox: server packets delivered to each
    /// [`SocketReceiver`].
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

/// Reconnection settings, with the defaults of socket.io-client's `Manager`.
///
/// After an Engine.IO session drops while namespaces are open, attempt `n`
/// waits `delay * 2^n`, moved up or down by a random share of up to
/// `randomization_factor` of itself, and capped at `delay_max`. A session the
/// server answers restarts the count.
///
/// Pass it to [`ClientBuilder::reconnection`].
#[derive(Clone, Copy, Debug)]
pub struct ReconnectionConfig {
    /// The number of attempts before giving up, or `None` for no limit, like
    /// `reconnectionAttempts` (default: `None`).
    pub attempts: Option<u32>,
    /// The delay before the first attempt, like `reconnectionDelay` (default:
    /// 1 second).
    pub delay: Duration,
    /// The longest delay, like `reconnectionDelayMax` (default: 5 seconds).
    pub delay_max: Duration,
    /// The largest random share of each delay to add or subtract, like
    /// `randomizationFactor` (default: 0.5). A value outside 0 to 1 means no
    /// jitter, as in JS.
    pub randomization_factor: f64,
}

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            attempts: None,
            delay: Duration::from_secs(1),
            delay_max: Duration::from_secs(5),
            randomization_factor: 0.5,
        }
    }
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
#[must_use = "call open() to connect"]
pub struct ClientBuilder<C = ()> {
    url: Url,
    path: String,
    http_client: Option<reqwest::Client>,
    websocket_connector: C,
    transport_strategy: TransportStrategy,
    channels: ChannelConfig,
    reconnection: Option<ReconnectionConfig>,
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
        }
    }
}

impl<C> ClientBuilder<C>
where
    C: WebSocketConnector + Clone,
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
        }
    }

    /// Override the initial transport strategy (default: HTTP long-polling with
    /// WebSocket upgrade).
    pub fn transport(mut self, strategy: TransportStrategy) -> Self {
        self.transport_strategy = strategy;
        self
    }

    /// Override the channel buffer capacities (default: 32 for all channels).
    ///
    /// Accepts `()` for defaults, a `usize` for uniform sizing, or a
    /// [`ChannelConfig`] for per-channel control.
    pub fn channels(mut self, config: impl Into<ChannelConfig>) -> Self {
        self.channels = config.into();
        self
    }

    /// Override the reconnection settings, or pass `None` to end the client
    /// when an Engine.IO session drops instead (default:
    /// [`ReconnectionConfig::default`]).
    pub fn reconnection(mut self, config: Option<ReconnectionConfig>) -> Self {
        self.reconnection = config;
        self
    }

    /// Connects to the Engine.IO server and returns a [`Client`].
    ///
    /// Spawns the manager task, which opens one Engine.IO session after
    /// another while namespaces are open, so a dropped connection reconnects.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    #[must_use = "call join() to observe the background task result"]
    pub fn open(self) -> Result<Client, ClientBuilderError> {
        let http_client = self.http_client.unwrap_or_default();
        let websocket_connector = self.websocket_connector;
        let transport_strategy = self.transport_strategy;
        let channels = self.channels;
        let backoff = self.reconnection.map(|config| {
            Backoff::new(
                config.delay,
                config.delay_max,
                config.randomization_factor,
                config.attempts,
            )
        });
        let url = self.url.join(&self.path)?;

        let (connect_request_tx, connect_request_rx) = mpsc::channel(channels.manager);

        // Each session opens a new engine with its own connector.
        let connect_engine = move |server_message_tx, client_message_rx| {
            eioc::engine::session::connect(
                url.clone(),
                http_client.clone(),
                websocket_connector.clone(),
                transport_strategy,
                server_message_tx,
                client_message_rx,
                channels.engine,
                channels.transport,
            )
        };

        let supervisor = Supervisor {
            server_message_capacity: channels.manager,
            client_message_capacity: channels.engine,
            backoff,
            connect_engine,
        };

        let task = tokio::spawn(crate::manager::session::run(connect_request_rx, supervisor));

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
    /// The namespace is not confirmed until a [`ServerPacket::Connect`] arrives
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
    /// Returns an error if the client has ended.
    pub async fn connect_with<S, B>(
        &self,
        ns: S,
        payload: B,
    ) -> Result<(SocketSender, SocketReceiver), SocketError>
    where
        S: Into<ByteString>,
        B: Into<ByteString>,
    {
        let (connect_request, handles) = ConnectRequest::new(
            ns.into(),
            payload.into(),
            self.channels.manager,
            self.channels.socket,
        );

        self.connect_request_tx
            .send(connect_request)
            .await
            .map_err(|_| SocketError::Closed)?;

        handles.reply_rx.await.map_err(|_| SocketError::Closed)??;

        Ok((
            SocketSender::new(handles.client_packet_tx, handles.closed),
            SocketReceiver {
                server_packet_rx: handles.server_packet_rx,
            },
        ))
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
        drop(self.connect_request_tx);
        self.task.await??;
        Ok(())
    }
}

/// Sender for a Socket.IO namespace.
///
/// Clone it to emit from several tasks: clones share the namespace.
/// Disconnecting any clone closes all of them, and dropping the last clone
/// also disconnects the namespace.
#[derive(Clone, Debug)]
pub struct SocketSender {
    /// Carries packets to the manager task, which encodes and sends them.
    client_packet_tx: mpsc::Sender<ClientPacket>,
    closed: CancellationToken,
}

impl SocketSender {
    /// Wraps the channel to a namespace and the token that marks it closed.
    #[must_use]
    pub fn new(client_packet_tx: mpsc::Sender<ClientPacket>, closed: CancellationToken) -> Self {
        Self {
            client_packet_tx,
            closed,
        }
    }

    /// Sends one raw client packet; [`emit`](Self::emit) and
    /// [`acknowledge`](Self::acknowledge) build theirs and call this.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError::Closed`] once the namespace is closed.
    pub async fn send(&self, packet: ClientPacket) -> Result<(), SocketError> {
        if self.closed.is_cancelled() {
            return Err(SocketError::Closed);
        }
        self.client_packet_tx
            .send(packet)
            .await
            .map_err(|_| SocketError::Closed)
    }
}

impl SocketSender {
    /// Emits an event; returns `()` or an [`AckHandle`](crate::ack::AckHandle)
    /// depending on the ack policy.
    ///
    /// The event waits until the server confirms the namespace, also after a
    /// reconnection. The [`AckHandle`](crate::ack::AckHandle) fails if the
    /// Engine.IO session drops after the event went out, like
    /// socket.io-client's `Socket._clearAcks`.
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
        let (client_packet, output) = event.prepare()?;
        self.send(client_packet).await?;
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
        let client_packet = payload.into_client_packet(id.get())?;
        self.send(client_packet).await
    }

    /// Disconnects the namespace for every clone.
    ///
    /// Events and acks sent before the call still go out, followed by a
    /// DISCONNECT packet. Later sends fail with [`SocketError::Closed`].
    /// Calling it after the namespace has closed, by either side, does
    /// nothing.
    pub fn disconnect(&self) {
        self.closed.cancel();
    }

    /// Waits until the namespace closes, by either side, or the client ends.
    ///
    /// Resolves as soon as the namespace stops accepting packets, which can be
    /// before its DISCONNECT packet reaches the server.
    pub async fn closed(&self) {
        self.closed.cancelled().await;
    }
}

/// Receiver for a Socket.IO namespace.
///
/// Read it continuously: bounded queues apply backpressure to the server.
/// A stalled consumer can delay heartbeat responses until the server times out.
///
/// # Items and reconnection
///
/// The receiver yields only what the server sends. When the Engine.IO session
/// drops, the client reconnects and sends CONNECT again, so a reconnection
/// shows up as another [`ServerPacket::Connect`] with a new `sid`. The server
/// forgets the socket's rooms in between.
///
/// | Server sends      | Item                           | Then                            |
/// |-------------------|--------------------------------|---------------------------------|
/// | `CONNECT`         | [`ServerPacket::Connect`]      | events, until the session drops |
/// | `EVENT`           | [`ServerPacket::Event`]        | more items                      |
/// | `CONNECT_ERROR`   | [`ServerPacket::ConnectError`] | `None`                          |
/// | `DISCONNECT`      | [`ServerPacket::Disconnect`]   | `None`, with no reconnection    |
///
/// `None` ends the receiver after a server `DISCONNECT`, a `CONNECT_ERROR`, a
/// client disconnect, an internal error, or when reconnection gives up.
/// [`Client::join`] reports why the client ended.
#[derive(Debug)]
pub struct SocketReceiver {
    server_packet_rx: mpsc::Receiver<ServerPacket>,
}

impl SocketReceiver {
    /// Returns the next application event. [`ServerPacket::Connect`],
    /// [`ServerPacket::Disconnect`], and [`ServerPacket::ConnectError`] are
    /// skipped. Returns `None` once the namespace closes, by either side,
    /// or the client ends.
    ///
    /// Cancel safe: the only suspend point is `recv`; skipped protocol packets
    /// have no suspend point after consumption, so no events are lost on
    /// cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be converted into `E`.
    pub async fn listen<E>(&mut self) -> Result<Option<E>, E::Error>
    where
        E: TryFrom<DynEvent>,
    {
        loop {
            match self.server_packet_rx.recv().await {
                None => return Ok(None),
                Some(ServerPacket::Event(e)) => return E::try_from(e).map(Some),
                Some(_) => {}
            }
        }
    }
}

impl std::ops::Deref for SocketReceiver {
    type Target = mpsc::Receiver<ServerPacket>;

    fn deref(&self) -> &Self::Target {
        &self.server_packet_rx
    }
}

impl std::ops::DerefMut for SocketReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.server_packet_rx
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use eioc::error::{Error as EngineIoError, TransportError, WebSocketError};
    use eioc::transport::TransportStrategy;
    use serde_json::Map;
    use tokio::sync::{Semaphore, mpsc};
    use url::Url;

    use super::*;
    use crate::error::PayloadError;
    use crate::marker::{HasAck, NoAck, NoBinary};
    use crate::packet::{ClientPacket, Connect, ConnectError, DynEvent, ServerPacket};

    struct TestEmit;

    impl Emit<NoAck, NoBinary> for TestEmit {
        type Output = ();

        fn prepare(self) -> Result<(ClientPacket, ()), PayloadError> {
            Ok((
                ClientPacket::Event {
                    payload: r#"["test"]"#.into(),
                    ack_tx: None,
                    attachments: None,
                },
                (),
            ))
        }
    }

    fn socket_sender() -> (SocketSender, mpsc::Receiver<ClientPacket>) {
        let (client_packet_tx, client_packet_rx) = mpsc::channel(8);
        let closed = CancellationToken::new();
        (
            SocketSender::new(client_packet_tx, closed),
            client_packet_rx,
        )
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
    async fn listen_skips_protocol_packets() {
        let (tx, rx) = mpsc::channel(8);
        let mut receiver = SocketReceiver {
            server_packet_rx: rx,
        };
        tx.send(ServerPacket::Connect(Connect {
            sid: ByteString::default(),
            extra: Map::default(),
        }))
        .await
        .unwrap();
        tx.send(ServerPacket::Disconnect).await.unwrap();
        tx.send(ServerPacket::ConnectError(ConnectError {
            message: "err".into(),
            extra: Map::default(),
        }))
        .await
        .unwrap();
        tx.send(ServerPacket::Event(DynEvent::new(r#"["hi"]"#, None)))
            .await
            .unwrap();
        let Pass(event) = receiver.listen::<Pass>().await.unwrap().unwrap();
        assert_eq!(event.payload, r#"["hi"]"#);
    }

    #[tokio::test]
    async fn listen_returns_none_on_closed_channel() {
        let (tx, rx) = mpsc::channel::<ServerPacket>(4);
        let mut receiver = SocketReceiver {
            server_packet_rx: rx,
        };
        drop(tx);
        assert!(receiver.listen::<Pass>().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dropping_last_clone_closes_client_packet_channel() {
        let (sender, mut client_packet_rx) = socket_sender();
        let clone = sender.clone();
        drop(sender);
        client_packet_rx.try_recv().unwrap_err();
        drop(clone);
        assert!(client_packet_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn disconnect_closes_every_clone_and_keeps_sent_packets() {
        let (sender, mut client_packet_rx) = socket_sender();
        let clone = sender.clone();
        sender.emit(TestEmit).await.unwrap();
        sender.disconnect();
        assert!(matches!(
            clone.emit(TestEmit).await,
            Err(SocketError::Closed)
        ));
        // The manager drains what was sent before;
        // `client_close_sends_earlier_packets_first` covers the rest.
        assert!(matches!(
            client_packet_rx.try_recv(),
            Ok(ClientPacket::Event { .. })
        ));
        client_packet_rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    async fn disconnect_after_close_returns() {
        let (sender, client_packet_rx) = socket_sender();
        drop(client_packet_rx);
        sender.disconnect();
        sender.disconnect();
    }

    #[tokio::test]
    async fn closed_resolves_for_every_clone_after_disconnect() {
        let (sender, _client_packet_rx) = socket_sender();
        let clone = sender.clone();
        sender.disconnect();
        clone.closed().await;
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

    #[test]
    fn builder_rejects_url_without_relative_path_support() {
        let url = Url::parse("mailto:client@example.com").unwrap();
        assert!(matches!(
            ClientBuilder::new(url).open(),
            Err(ClientBuilderError::Url(
                url::ParseError::RelativeUrlWithCannotBeABaseBase
            ))
        ));
    }

    #[tokio::test]
    async fn reconnection_gives_up_after_its_attempts() {
        let gate = Arc::new(Semaphore::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let connector_gate = Arc::clone(&gate);
        let counter = Arc::clone(&attempts);
        let client = ClientBuilder::new(Url::parse("http://localhost:3000/").unwrap())
            .path("custom/")
            .transport(TransportStrategy::WebSocket)
            .reconnection(Some(ReconnectionConfig {
                attempts: Some(2),
                delay: Duration::from_millis(1),
                delay_max: Duration::from_millis(1),
                ..ReconnectionConfig::default()
            }))
            .websocket_connector(move |url: Url| async move {
                // Wait for the namespace, so that every session reconnects.
                connector_gate.acquire().await.unwrap().forget();
                assert_eq!(url.scheme(), "ws");
                assert_eq!(url.path(), "/custom/");
                counter.fetch_add(1, Ordering::Relaxed);
                Err(std::io::Error::new(ErrorKind::ConnectionRefused, "connector refused").into())
            })
            .open()
            .unwrap();

        let (_tx, mut rx) = client.connect("/").await.unwrap();
        gate.add_permits(16);
        assert!(rx.recv().await.is_none());
        assert_eq!(attempts.load(Ordering::Relaxed), 3);

        assert!(matches!(
            client.connect("/chat").await,
            Err(SocketError::Closed)
        ));
        assert!(matches!(
            client.join().await,
            Err(ClientError::Manager(ManagerError::Engine(
                EngineIoError::Transport(TransportError::WebSocket(WebSocketError::Tungstenite(
                    error
                )))
            ))) if error.to_string().contains("connector refused")
        ));
    }

    #[tokio::test]
    async fn without_reconnection_the_first_failure_ends_the_client() {
        let client = ClientBuilder::new(Url::parse("http://localhost:3000/").unwrap())
            .transport(TransportStrategy::WebSocket)
            .reconnection(None)
            .websocket_connector(async |_| {
                Err(std::io::Error::from(ErrorKind::ConnectionRefused).into())
            })
            .open()
            .unwrap();

        let (_tx, mut rx) = client
            .connect_with("/chat", r#"{"token":"secret"}"#)
            .await
            .unwrap();
        assert!(rx.recv().await.is_none());
        client.join().await.unwrap_err();
    }

    #[tokio::test]
    async fn emit_sends_event_packet() {
        let (sender, mut rx) = socket_sender();
        sender.emit(TestEmit).await.unwrap();
        assert!(matches!(rx.try_recv().unwrap(), ClientPacket::Event { .. }));
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
    async fn acknowledge_sends_ack_packet_with_correct_id() {
        let (sender, mut rx) = socket_sender();
        let id = HasAck::<()>::parse(Some(ServerAckId::new(5))).unwrap();
        sender.acknowledge(id, ()).await.unwrap();
        let ClientPacket::Ack { id, .. } = rx.try_recv().unwrap() else {
            panic!("expected Ack packet");
        };
        assert_eq!(id.get(), 5);
    }

    #[test]
    fn socket_receiver_deref_gives_inner_receiver() {
        let (_tx, rx) = mpsc::channel::<ServerPacket>(4);
        let receiver = SocketReceiver {
            server_packet_rx: rx,
        };
        let _ = &*receiver;
    }
}
