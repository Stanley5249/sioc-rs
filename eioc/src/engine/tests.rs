//! Component tests for the engine protocol.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use url::Url;

use crate::error::{EngineError, Error, TransportError};
use crate::packet::{Frame, Handshake, Message, Packet, ServerMessage};
use crate::transport::TransportStrategy;

fn make_handshake() -> Handshake {
    Handshake {
        sid: "sid".into(),
        upgrades: vec![],
        ping_interval: 25_000,
        ping_timeout: 5_000,
        max_payload: 1_000_000,
    }
}

/// A running engine with the transport and upper-layer ends of its
/// channels, past its `Open` event.
struct TestEngine {
    server_frame_tx: mpsc::Sender<Frame>,
    server_message_rx: mpsc::Receiver<ServerMessage>,
    client_message_tx: mpsc::Sender<Message>,
    client_frame_rx: mpsc::Receiver<Frame>,
    task: JoinHandle<Result<(), EngineError>>,
}

impl TestEngine {
    async fn spawn(handshake: Handshake, server_message_capacity: usize) -> Self {
        let (server_frame_tx, server_frame_rx) = mpsc::channel(4);
        let (server_message_tx, mut server_message_rx) = mpsc::channel(server_message_capacity);
        let (client_message_tx, client_message_rx) = mpsc::channel(4);
        let (client_frame_tx, client_frame_rx) = mpsc::channel(4);
        let (handshake_tx, handshake_rx) = oneshot::channel();
        handshake_tx.send(handshake).unwrap();
        let task = tokio::spawn(async move {
            let mut client_message_rx = client_message_rx;
            crate::engine::protocol::run_protocol(
                server_frame_rx,
                server_message_tx,
                &mut client_message_rx,
                client_frame_tx,
                handshake_rx,
                None,
            )
            .await
        });
        assert!(matches!(
            server_message_rx.recv().await,
            Some(ServerMessage::Open(_))
        ));
        Self {
            server_frame_tx,
            server_message_rx,
            client_message_tx,
            client_frame_rx,
            task,
        }
    }

    /// Ends the session from the transport, then hangs up the upper layer once
    /// the frames stop.
    ///
    /// Reads both directions together, as the transport and the upper layer
    /// do, so a full upper-layer queue cannot stall the frames. Returns the
    /// engine result, the frames sent to the transport, and the messages sent
    /// to the upper layer.
    async fn finish(self) -> (Result<(), EngineError>, Vec<Frame>, Vec<Message>) {
        let Self {
            server_frame_tx,
            mut server_message_rx,
            client_message_tx,
            mut client_frame_rx,
            task,
        } = self;
        drop(server_frame_tx);
        let frames = async move {
            let mut frames = Vec::new();
            while let Some(frame) = client_frame_rx.recv().await {
                frames.push(frame);
            }
            drop(client_message_tx);
            frames
        };
        let messages = async move {
            let mut messages = Vec::new();
            while let Some(server_message) = server_message_rx.recv().await {
                messages.push(match server_message {
                    ServerMessage::Text(text) => Message::Text(text),
                    ServerMessage::Binary(bytes) => Message::Binary(bytes),
                    ServerMessage::Open(_) => panic!("a second open"),
                });
            }
            messages
        };
        let (frames, messages) = tokio::join!(frames, messages);
        (task.await.unwrap(), frames, messages)
    }
}

#[tokio::test]
async fn protocol_handshake_dropped_is_error() {
    let (_server_frame_tx, server_frame_rx) = mpsc::channel(4);
    let (server_message_tx, _server_message_rx) = mpsc::channel(4);
    let (_client_message_tx, mut client_message_rx) = mpsc::channel(4);
    let (client_frame_tx, _) = mpsc::channel(4);
    let (handshake_tx, handshake_rx) = oneshot::channel::<Handshake>();
    drop(handshake_tx);
    let result = crate::engine::protocol::run_protocol(
        server_frame_rx,
        server_message_tx,
        &mut client_message_rx,
        client_frame_tx,
        handshake_rx,
        None,
    )
    .await;
    assert!(matches!(result, Err(EngineError::Handshake(_))));
}

#[tokio::test]
async fn protocol_handshake_times_out() {
    let (_server_frame_tx, server_frame_rx) = mpsc::channel(4);
    let (server_message_tx, mut server_message_rx) = mpsc::channel(4);
    let (_client_message_tx, mut client_message_rx) = mpsc::channel(4);
    let (client_frame_tx, _client_frame_rx) = mpsc::channel(4);
    let (_handshake_tx, handshake_rx) = oneshot::channel::<Handshake>();
    let result = crate::engine::protocol::run_protocol(
        server_frame_rx,
        server_message_tx,
        &mut client_message_rx,
        client_frame_tx,
        handshake_rx,
        Some(Duration::from_millis(10)),
    )
    .await;
    assert!(matches!(result, Err(EngineError::HandshakeTimeout)));
    assert!(server_message_rx.recv().await.is_none());
}

#[tokio::test]
async fn protocol_reports_open_before_messages() {
    let (server_frame_tx, server_frame_rx) = mpsc::channel(4);
    let (server_message_tx, mut server_message_rx) = mpsc::channel(4);
    let (client_message_tx, mut client_message_rx) = mpsc::channel(4);
    let (client_frame_tx, _client_frame_rx) = mpsc::channel(4);
    let (handshake_tx, handshake_rx) = oneshot::channel();
    server_frame_tx
        .send(Packet::Message("first".into()).into())
        .await
        .unwrap();
    drop(server_frame_tx);
    handshake_tx.send(make_handshake()).unwrap();
    let protocol = crate::engine::protocol::run_protocol(
        server_frame_rx,
        server_message_tx,
        &mut client_message_rx,
        client_frame_tx,
        handshake_rx,
        Some(Duration::from_secs(20)),
    );
    let server_messages = async {
        let mut server_messages = Vec::new();
        while let Some(server_message) = server_message_rx.recv().await {
            server_messages.push(server_message);
        }
        drop(client_message_tx);
        server_messages
    };
    let (result, server_messages) = tokio::join!(protocol, server_messages);
    result.unwrap();
    assert_eq!(
        server_messages,
        [
            ServerMessage::Open(make_handshake()),
            ServerMessage::Text("first".into()),
        ]
    );
}

#[tokio::test]
async fn protocol_transport_closed_ends_message_stream() {
    let TestEngine {
        server_frame_tx,
        mut server_message_rx,
        client_message_tx,
        mut client_frame_rx,
        task,
    } = TestEngine::spawn(make_handshake(), 4).await;
    drop(server_frame_tx);
    assert!(server_message_rx.recv().await.is_none());
    assert!(client_frame_rx.recv().await.is_none());
    drop(client_message_tx);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_client_close_ends_frame_stream() {
    let TestEngine {
        server_frame_tx,
        mut server_message_rx,
        client_message_tx,
        mut client_frame_rx,
        task,
    } = TestEngine::spawn(make_handshake(), 4).await;
    drop(client_message_tx);
    assert!(client_frame_rx.recv().await.is_none());

    // Frames keep arriving until the transport finishes.
    server_frame_tx
        .send(Packet::Message("late".into()).into())
        .await
        .unwrap();
    drop(server_frame_tx);
    assert!(matches!(
        server_message_rx.recv().await,
        Some(ServerMessage::Text(t)) if t == "late"
    ));
    assert!(server_message_rx.recv().await.is_none());
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_accepts_client_messages_until_sender_dropped() {
    let TestEngine {
        server_frame_tx,
        mut server_message_rx,
        client_message_tx,
        mut client_frame_rx,
        task,
    } = TestEngine::spawn(make_handshake(), 4).await;
    drop(server_frame_tx);
    assert!(server_message_rx.recv().await.is_none());
    assert!(client_frame_rx.recv().await.is_none());
    client_message_tx
        .send(Message::Text("late".into()))
        .await
        .unwrap();
    drop(client_message_tx);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_client_messages_flow_while_upper_layer_is_full() {
    let engine = TestEngine::spawn(make_handshake(), 1).await;
    for text in ["fills", "blocks"] {
        engine
            .server_frame_tx
            .send(Packet::Message(text.into()).into())
            .await
            .unwrap();
    }
    engine
        .client_message_tx
        .send(Message::Text("out".into()))
        .await
        .unwrap();
    let TestEngine {
        mut client_frame_rx,
        ..
    } = engine;
    let frame = tokio::time::timeout(Duration::from_secs(5), client_frame_rx.recv())
        .await
        .unwrap();
    assert!(matches!(frame, Some(Frame::Packet(Packet::Message(m))) if m == "out"));
}

#[tokio::test]
async fn protocol_ping_triggers_pong() {
    let engine = TestEngine::spawn(make_handshake(), 4).await;
    engine
        .server_frame_tx
        .send(Packet::Ping("probe".into()).into())
        .await
        .unwrap();
    let (result, frames, _) = engine.finish().await;
    result.unwrap();
    assert!(matches!(&frames[..], [Frame::Packet(Packet::Pong(p))] if p == "probe"));
}

#[tokio::test]
async fn protocol_noop_is_ignored() {
    let engine = TestEngine::spawn(make_handshake(), 4).await;
    engine
        .server_frame_tx
        .send(Packet::Noop.into())
        .await
        .unwrap();
    let (result, frames, messages) = engine.finish().await;
    result.unwrap();
    assert_eq!(frames, [] as [Frame; 0]);
    assert_eq!(messages, [] as [Message; 0]);
}

#[tokio::test]
async fn protocol_out_of_place_packets_are_ignored() {
    let engine = TestEngine::spawn(make_handshake(), 4).await;
    for packet in [
        Packet::Open(make_handshake()),
        Packet::Pong("probe".into()),
        Packet::Upgrade,
        Packet::Message("after".into()),
    ] {
        engine.server_frame_tx.send(packet.into()).await.unwrap();
    }
    let (result, frames, messages) = engine.finish().await;
    result.unwrap();
    assert_eq!(frames, [] as [Frame; 0]);
    assert!(matches!(&messages[..], [Message::Text(t)] if t == "after"));
}

#[tokio::test]
async fn protocol_server_messages_forwarded() {
    let engine = TestEngine::spawn(make_handshake(), 4).await;
    engine
        .server_frame_tx
        .send(Packet::Message("hello".into()).into())
        .await
        .unwrap();
    engine
        .server_frame_tx
        .send(Frame::Binary(Bytes::from_static(b"bin")))
        .await
        .unwrap();
    let (result, _, messages) = engine.finish().await;
    result.unwrap();
    assert!(matches!(
        &messages[..],
        [Message::Text(t), Message::Binary(b)] if t == "hello" && b.as_ref() == b"bin"
    ));
}

#[tokio::test]
async fn protocol_client_messages_sent_to_transport() {
    let TestEngine {
        server_frame_tx,
        server_message_rx: _server_message_rx,
        client_message_tx,
        mut client_frame_rx,
        task,
    } = TestEngine::spawn(make_handshake(), 4).await;
    client_message_tx
        .send(Message::Text("out".into()))
        .await
        .unwrap();
    client_message_tx
        .send(Message::Binary(Bytes::from_static(b"out")))
        .await
        .unwrap();
    drop(client_message_tx);
    let mut frames = Vec::new();
    while let Some(frame) = client_frame_rx.recv().await {
        frames.push(frame);
    }
    assert!(matches!(
        &frames[..],
        [Frame::Packet(Packet::Message(m)), Frame::Binary(b)]
            if m == "out" && b.as_ref() == b"out"
    ));
    drop(server_frame_tx);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_heartbeat_timeout_fires() {
    let handshake = Handshake {
        ping_interval: 1,
        ping_timeout: 1,
        ..make_handshake()
    };
    let engine = TestEngine::spawn(handshake, 4).await;
    assert!(matches!(
        engine.task.await.unwrap(),
        Err(EngineError::HeartbeatTimeout)
    ));
}

#[tokio::test]
async fn protocol_closed_message_receiver_is_error() {
    let TestEngine {
        server_frame_tx,
        server_message_rx,
        client_message_tx: _client_message_tx,
        client_frame_rx: _client_frame_rx,
        task,
    } = TestEngine::spawn(make_handshake(), 4).await;
    drop(server_message_rx);
    server_frame_tx
        .send(Packet::Message("x".into()).into())
        .await
        .unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(EngineError::ServerMessage(_))
    ));
}

#[tokio::test]
async fn session_failure_accepts_client_messages_until_hang_up() {
    let (server_message_tx, mut server_message_rx) = mpsc::channel(4);
    let (client_message_tx, client_message_rx) = mpsc::channel(4);
    let session = tokio::spawn(crate::engine::session::connect(
        Url::parse("http://localhost:3000/").unwrap(),
        reqwest::Client::new(),
        async |_| Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into()),
        TransportStrategy::WebSocket,
        server_message_tx,
        client_message_rx,
        None,
        4,
        4,
    ));

    assert!(server_message_rx.recv().await.is_none());
    client_message_tx
        .send(Message::Text("late".into()))
        .await
        .unwrap();
    drop(client_message_tx);
    assert!(matches!(
        session.await.unwrap(),
        Err(Error::Transport(TransportError::WebSocket(_)))
    ));
}
