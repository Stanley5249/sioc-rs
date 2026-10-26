use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use url::Url;

use crate::engine::EngineOptions;
use crate::engine::state::EngineCore;
use crate::error::{EngineError, Error, TransportError, WebSocketError};
use crate::packet::{ClientMessage, Handshake, ServerMessage};
use crate::transport::{ClientFrame, ServerFrame, TransportStrategy};

fn handshake() -> Handshake {
    Handshake {
        sid: "sid".into(),
        upgrades: vec![],
        ping_interval: 25_000,
        ping_timeout: 5_000,
        max_payload: 1_000_000,
    }
}

fn text(payload: &str) -> ServerFrame {
    ServerFrame::Message(payload.into())
}

fn sent(payload: &str) -> ClientFrame {
    ClientFrame::Message(payload.into())
}

/// An engine task with a fake transport: the transport ends of the frame
/// channels, and a oneshot that finishes the transport future.
struct Harness {
    server_frame_tx: mpsc::Sender<ServerFrame>,
    client_frame_rx: mpsc::Receiver<ClientFrame>,
    server_message_rx: mpsc::Receiver<ServerMessage>,
    client_message_tx: mpsc::Sender<ClientMessage>,
    closed_rx: watch::Receiver<()>,
    /// Closed once the engine drops the transport future.
    transport_tx: oneshot::Sender<Result<(), TransportError>>,
    task: JoinHandle<Result<(), Error>>,
}

impl Harness {
    fn spawn(
        handshake_timeout: Option<Duration>,
        server_message_capacity: usize,
        client_frame_capacity: usize,
    ) -> Self {
        let (server_frame_tx, server_frame_rx) = mpsc::channel(4);
        let (client_frame_tx, client_frame_rx) = mpsc::channel(client_frame_capacity);
        let (server_message_tx, server_message_rx) = mpsc::channel(server_message_capacity);
        let (client_message_tx, client_message_rx) = mpsc::channel(4);
        let (closed_tx, closed_rx) = watch::channel(());
        let (transport_tx, transport_rx) = oneshot::channel();
        let transport = async move { transport_rx.await.unwrap_or(Ok(())) };
        let core = EngineCore::new(handshake_timeout, Instant::now());
        let task = tokio::spawn(crate::engine::actor::run(
            core,
            transport,
            server_frame_rx,
            client_frame_tx,
            server_message_tx,
            client_message_rx,
            closed_tx,
        ));

        Self {
            server_frame_tx,
            client_frame_rx,
            server_message_rx,
            client_message_tx,
            closed_rx,
            transport_tx,
            task,
        }
    }

    /// Like [`Harness::spawn`], past the Open message.
    async fn open(server_message_capacity: usize, client_frame_capacity: usize) -> Self {
        let mut harness = Self::spawn(None, server_message_capacity, client_frame_capacity);
        harness.send_frame(ServerFrame::Open(handshake())).await;
        assert_eq!(
            harness.server_message_rx.recv().await,
            Some(ServerMessage::Open(handshake()))
        );
        harness
    }

    async fn send_frame(&self, frame: ServerFrame) {
        send(&self.server_frame_tx, frame).await;
    }

    async fn send_message(&self, message: &str) {
        self.client_message_tx
            .send(ClientMessage::Text(message.into()))
            .await
            .unwrap();
    }

    /// Ends the transport like a real one: the server direction ends, and the
    /// future finishes with `result` once the engine closed the client one.
    async fn close_transport(mut self, result: Result<(), TransportError>) -> Remains {
        drop(self.server_frame_tx);
        let mut frames = Vec::new();
        while let Some(frame) = self.client_frame_rx.recv().await {
            frames.push(frame);
        }
        let _ = self.transport_tx.send(result);

        Remains {
            frames,
            server_message_rx: self.server_message_rx,
            client_message_tx: self.client_message_tx,
            task: self.task,
        }
    }
}

async fn send<T: std::fmt::Debug>(tx: &mpsc::Sender<T>, item: T) {
    tx.send(item).await.unwrap();
}

/// What a test still holds after the transport ended.
struct Remains {
    frames: Vec<ClientFrame>,
    server_message_rx: mpsc::Receiver<ServerMessage>,
    client_message_tx: mpsc::Sender<ClientMessage>,
    task: JoinHandle<Result<(), Error>>,
}

impl Remains {
    /// Reads the manager channel to its end, then hangs up.
    async fn join(mut self) -> (Vec<ServerMessage>, Result<(), Error>) {
        let mut messages = Vec::new();
        while let Some(message) = self.server_message_rx.recv().await {
            messages.push(message);
        }
        drop(self.client_message_tx);
        (messages, self.task.await.unwrap())
    }
}

// Delivery and order

#[tokio::test]
async fn server_frames_arrive_in_order_after_open() {
    let harness = Harness::open(4, 4).await;
    harness.send_frame(text("a")).await;
    harness
        .send_frame(ServerFrame::Binary(Bytes::from_static(b"b")))
        .await;
    harness.send_frame(ServerFrame::Noop).await;
    harness.send_frame(text("c")).await;

    let remains = harness.close_transport(Ok(())).await;
    assert_eq!(remains.frames, []);
    let (messages, result) = remains.join().await;
    assert_eq!(
        messages,
        [
            ServerMessage::Text("a".into()),
            ServerMessage::Binary(Bytes::from_static(b"b")),
            ServerMessage::Text("c".into()),
        ]
    );
    result.unwrap();
}

#[tokio::test]
async fn messages_before_open_wait_in_the_transport_channel() {
    let mut harness = Harness::spawn(None, 4, 4);
    harness.send_message("early").await;

    assert_eq!(harness.client_frame_rx.recv().await, Some(sent("early")));
}

// Backpressure and progress

#[tokio::test]
async fn ping_before_a_message_is_answered_while_the_manager_is_full() {
    let mut harness = Harness::open(1, 4).await;
    harness.send_frame(text("fills")).await;
    harness.send_frame(ServerFrame::Ping("".into())).await;

    assert_eq!(
        harness.client_frame_rx.recv().await,
        Some(ClientFrame::Pong("".into()))
    );
}

#[tokio::test]
async fn client_messages_flow_while_the_manager_is_full() {
    let mut harness = Harness::open(1, 4).await;
    for payload in ["fills", "pending", "waits"] {
        harness.send_frame(text(payload)).await;
    }
    harness.send_message("out").await;

    assert_eq!(harness.client_frame_rx.recv().await, Some(sent("out")));
    for payload in ["fills", "pending", "waits"] {
        assert_eq!(
            harness.server_message_rx.recv().await,
            Some(ServerMessage::Text(payload.into()))
        );
    }
}

#[tokio::test]
async fn server_frames_flow_while_the_transport_is_full() {
    let mut harness = Harness::open(4, 1).await;
    harness.send_message("fills").await;
    harness.send_message("waits").await;
    harness.send_frame(text("in")).await;

    assert_eq!(
        harness.server_message_rx.recv().await,
        Some(ServerMessage::Text("in".into()))
    );
    assert_eq!(harness.client_frame_rx.recv().await, Some(sent("fills")));
    assert_eq!(harness.client_frame_rx.recv().await, Some(sent("waits")));
}

/// With no client message, the client branch holds the transport's only slot
/// while it waits, and a pending pong must disable it. Without that, the two
/// reservations race in random `select!` order, so each round catches the bug
/// with a chance of one half. The rounds make a pass by luck unlikely, not
/// impossible. A missed pong does not hang: the paused clock runs out the
/// ping window, and the closed channel fails the assertion.
#[tokio::test(start_paused = true)]
async fn pong_takes_the_slot_a_waiting_client_branch_holds() {
    let mut harness = Harness::open(4, 1).await;

    for round in 0..32 {
        harness.send_frame(ServerFrame::Ping("".into())).await;
        assert_eq!(
            harness.client_frame_rx.recv().await,
            Some(ClientFrame::Pong("".into())),
            "round {round}"
        );
    }
    harness.send_message("after").await;
    assert_eq!(harness.client_frame_rx.recv().await, Some(sent("after")));
}

#[tokio::test(start_paused = true)]
async fn the_deadline_fires_while_the_manager_is_full() {
    let mut harness = Harness::open(1, 4).await;
    harness.send_frame(text("fills")).await;
    harness.send_frame(text("pending")).await;

    // Nothing reads the manager channel until the failure dropped the
    // transport. The session's end is visible ahead of the messages, which
    // still arrive.
    harness.transport_tx.closed().await;
    harness.closed_rx.changed().await.unwrap_err();
    for payload in ["fills", "pending"] {
        assert_eq!(
            harness.server_message_rx.recv().await,
            Some(ServerMessage::Text(payload.into()))
        );
    }
    assert_eq!(harness.server_message_rx.recv().await, None);
    drop(harness.client_message_tx);
    assert!(matches!(
        harness.task.await.unwrap(),
        Err(Error::Engine(EngineError::HeartbeatTimeout))
    ));
}

// Closing

#[tokio::test]
async fn a_local_close_ends_both_directions_and_waits_for_the_transport() {
    let mut harness = Harness::open(4, 4).await;
    harness.send_message("last").await;
    drop(harness.client_message_tx);

    // The accepted message reaches the transport before its channel ends.
    assert_eq!(harness.client_frame_rx.recv().await, Some(sent("last")));
    assert_eq!(harness.client_frame_rx.recv().await, None);
    assert_eq!(harness.server_message_rx.recv().await, None);

    // Frames after a local close are discarded.
    send(&harness.server_frame_tx, text("late")).await;
    drop(harness.server_frame_tx);
    let _ = harness.transport_tx.send(Ok(()));
    harness.task.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_peer_end_delivers_the_last_message_and_accepts_messages_until_hang_up() {
    let mut harness = Harness::open(1, 4).await;
    harness.send_frame(text("fills")).await;
    harness.send_frame(text("last")).await;
    drop(harness.server_frame_tx);

    // The engine sees the end only after the manager took the last message.
    for payload in ["fills", "last"] {
        assert_eq!(
            harness.server_message_rx.recv().await,
            Some(ServerMessage::Text(payload.into()))
        );
    }
    assert_eq!(harness.server_message_rx.recv().await, None);
    assert_eq!(harness.client_frame_rx.recv().await, None);
    // Sends succeed after the end, so the manager never sees a failed send.
    send(
        &harness.client_message_tx,
        ClientMessage::Text("late".into()),
    )
    .await;
    let _ = harness.transport_tx.send(Ok(()));
    drop(harness.client_message_tx);
    harness.task.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_transport_error_is_the_session_result() {
    let harness = Harness::open(4, 4).await;

    let remains = harness
        .close_transport(Err(WebSocketError::Closed.into()))
        .await;
    let (messages, result) = remains.join().await;
    assert_eq!(messages, []);
    assert!(matches!(
        result,
        Err(Error::Transport(TransportError::WebSocket(
            WebSocketError::Closed
        )))
    ));
}

#[tokio::test]
async fn a_hang_up_during_opening_drops_the_transport() {
    let mut harness = Harness::spawn(None, 4, 4);
    drop(harness.client_message_tx);

    // The server holds no session yet, so the handshake is not waited for,
    // even without a deadline.
    harness.transport_tx.closed().await;
    assert_eq!(harness.server_message_rx.recv().await, None);
    assert_eq!(harness.client_frame_rx.recv().await, None);
    harness.task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_handshake_timeout_drops_the_transport() {
    let mut harness = Harness::spawn(Some(Duration::from_secs(5)), 4, 4);

    harness.transport_tx.closed().await;
    assert_eq!(harness.server_message_rx.recv().await, None);
    assert_eq!(harness.client_frame_rx.recv().await, None);
    drop(harness.client_message_tx);
    assert!(matches!(
        harness.task.await.unwrap(),
        Err(Error::Engine(EngineError::HandshakeTimeout))
    ));
}

#[tokio::test]
async fn a_dropped_manager_receiver_is_an_internal_error() {
    let harness = Harness::spawn(None, 4, 4);
    drop(harness.server_message_rx);
    send(&harness.server_frame_tx, ServerFrame::Open(handshake())).await;

    let error = harness.task.await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        Error::Engine(EngineError::ServerMessageClosed)
    ));
    assert!(error.is_internal());
}

#[tokio::test]
async fn dropping_the_engine_aborts_its_task() {
    let (alive_tx, alive_rx) = oneshot::channel::<()>();
    let connector = async move |_| {
        let _alive_tx = alive_tx;
        std::future::pending().await
    };
    let engine = crate::engine::open(EngineOptions {
        url: Url::parse("http://localhost:3000/").unwrap(),
        http_client: reqwest::Client::new(),
        websocket_connector: connector,
        transport: TransportStrategy::WebSocket,
        handshake_timeout: None,
        server_message_capacity: 4,
        client_message_capacity: 4,
        server_frame_capacity: 4,
        client_frame_capacity: 4,
    });

    let crate::engine::Engine {
        client_message_tx: _client_message_tx,
        server_message_rx: _server_message_rx,
        mut closed_rx,
        task,
    } = engine;
    drop(task);

    // The connector's future drops with the aborted task, and so does the
    // session's closed channel, although the manager never hung up.
    assert!(alive_rx.await.is_err());
    closed_rx.changed().await.unwrap_err();
}

#[tokio::test]
async fn a_transport_error_still_delivers_the_messages_already_received() {
    let mut harness = Harness::open(4, 4).await;
    harness.send_frame(text("before")).await;
    drop(harness.server_frame_tx);
    let _ = harness
        .transport_tx
        .send(Err(WebSocketError::Closed.into()));

    assert_eq!(
        harness.server_message_rx.recv().await,
        Some(ServerMessage::Text("before".into()))
    );
    assert_eq!(harness.server_message_rx.recv().await, None);
    assert_eq!(harness.client_frame_rx.recv().await, None);
    drop(harness.client_message_tx);
    assert!(matches!(
        harness.task.await.unwrap(),
        Err(Error::Transport(TransportError::WebSocket(
            WebSocketError::Closed
        )))
    ));
}

#[tokio::test(start_paused = true)]
async fn the_first_failure_survives_the_cleanup_after_it() {
    let mut harness = Harness::open(4, 4).await;

    // The heartbeat fails with nothing to read, which drops the transport.
    harness.transport_tx.closed().await;
    assert_eq!(harness.server_message_rx.recv().await, None);
    // The manager keeps sending until it notices, and its sends succeed.
    harness.send_message("late").await;
    drop(harness.client_message_tx);

    assert!(matches!(
        harness.task.await.unwrap(),
        Err(Error::Engine(EngineError::HeartbeatTimeout))
    ));
}
