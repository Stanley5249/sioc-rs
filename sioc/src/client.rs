//! Socket.IO client and namespace handles.

use crate::ack::AckType;
use crate::error::Result;
use crate::marker::{AckId, AckMarker, BinaryMarker};
use bytes::Bytes;
use futures_util::future::FutureExt;
use sioc_core::error::Result as CoreResult;
use sioc_core::manager::Manager;
use sioc_core::packet::{Command, Ns, Packet};
use sioc_engine::engine::Engine;
use sioc_engine::error::Result as EngineResult;
use sioc_engine::transports::Transport;
use sioc_engine::websocket::{
    WebSocketConnector, WebSocketError, WebSocketStream, default_connect,
};
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
/// let client = ClientBuilder::new(url).open().await?;
/// let (tx, mut rx) = client.connect("/").await?;
/// # Ok(())
/// # }
/// ```
pub struct ClientBuilder {
    url: Url,
    path: String,
    http_client: Option<reqwest::Client>,
    ws_connector: Option<WebSocketConnector>,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("url", &self.url)
            .field("path", &self.path)
            .field("http_client", &self.http_client)
            .field("ws_connector", &self.ws_connector.as_ref().map(|_| ".."))
            .finish()
    }
}

impl ClientBuilder {
    /// Creates a builder targeting `url`.
    pub fn new(url: impl Into<Url>) -> Self {
        Self {
            url: url.into(),
            path: "socket.io/".to_string(),
            http_client: None,
            ws_connector: None,
        }
    }

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
    /// Pass any `async fn(Url) -> Result<WebSocketStream>` — no boxing needed.
    ///
    /// ```rust,no_run
    /// # async fn run() -> sioc::error::Result<()> {
    /// use sioc::prelude::*;
    /// use url::Url;
    ///
    /// // Example: wrap the default connector to add logging.
    /// let client = ClientBuilder::new(Url::parse("http://localhost:3000").unwrap())
    ///     .ws_connector(|url| async move { default_connect(url).await })
    ///     .open()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn ws_connector<F, Fut>(mut self, f: F) -> Self
    where
        F: FnOnce(Url) -> Fut + Send + 'static,
        Fut: Future<Output = Result<WebSocketStream, WebSocketError>> + Send + 'static,
    {
        self.ws_connector = Some(Box::new(move |url| f(url).boxed()));
        self
    }

    /// Connect to the Engine.IO server and return a [`Client`].
    ///
    /// Opens the HTTP long-polling transport, completes the Engine.IO
    /// handshake, and spawns a background manager task. Hold the returned
    /// [`Client`] and call [`Client::join`] to await clean shutdown.
    pub async fn open(self) -> Result<Client> {
        let http_client = self.http_client.unwrap_or_default();

        let ws_connector = self
            .ws_connector
            .unwrap_or_else(|| Box::new(|url| default_connect(url).boxed()));

        let url = self.url.join(&self.path)?;

        let (transport, handshake, upgrade_handle) =
            Transport::connect_polling(url, http_client, ws_connector).await?;

        let engine = Engine::spawn(transport, handshake, upgrade_handle);
        let (tx, rx) = mpsc::channel(32);
        let manager_handle = tokio::spawn(Manager::new(rx).run(engine.rx, engine.tx));
        let engine_handle = engine.handle;

        Ok(Client {
            tx,
            manager_handle,
            engine_handle,
        })
    }
}

/// A connected Socket.IO client.
///
/// Created by [`ClientBuilder::open`]. Use [`connect`](Self::connect) to open
/// namespace handles, and [`join`](Self::join) to await the manager task.
#[derive(Debug)]
pub struct Client {
    tx: mpsc::Sender<Ns<Command>>,
    manager_handle: JoinHandle<CoreResult<()>>,
    engine_handle: JoinHandle<EngineResult<()>>,
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
        let ns = ns.into();
        let (remote_tx, remote_rx) = mpsc::channel(32);

        let socket_tx = SocketSender {
            ns: ns.clone(),
            tx: self.tx.clone(),
        };
        let socket_rx = SocketReceiver { rx: remote_rx };

        socket_tx
            .send(Command::Connect {
                sender: remote_tx,
                data: None,
            })
            .await?;

        Ok((socket_tx, socket_rx))
    }

    /// Opens a namespace with an initial connection payload.
    pub async fn connect_with<B: Into<Bytes>>(
        &self,
        ns: impl Into<String>,
        data: B,
    ) -> Result<(SocketSender, SocketReceiver)> {
        let ns = ns.into();
        let (remote_tx, remote_rx) = mpsc::channel(32);

        let socket_tx = SocketSender {
            ns: ns.clone(),
            tx: self.tx.clone(),
        };
        let socket_rx = SocketReceiver { rx: remote_rx };

        socket_tx
            .send(Command::Connect {
                sender: remote_tx,
                data: Some(data.into()),
            })
            .await?;

        Ok((socket_tx, socket_rx))
    }

    /// Await the background tasks.
    ///
    /// Resolves when both the manager and engine tasks exit. Drops all
    /// namespace senders first to signal the manager to stop.
    pub async fn join(self) -> Result<()> {
        drop(self.tx);
        let (manager_result, engine_result) = tokio::join!(self.manager_handle, self.engine_handle);
        manager_result??;
        engine_result??;
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
    tx: mpsc::Sender<Ns<Command>>,
}

impl SocketSender {
    async fn send(&self, packet: Command) -> Result<()> {
        Ok(self.tx.send(Ns(self.ns.clone(), packet)).await?)
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
