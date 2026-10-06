//! Component tests for the engine protocol.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::error::EngineError;
use crate::packet::{Frame, Handshake, Message, Packet};

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
/// channels.
struct Harness {
    server_frame_tx: mpsc::Sender<Frame>,
    server_message_rx: mpsc::Receiver<Message>,
    client_message_tx: mpsc::Sender<Message>,
    client_frame_rx: mpsc::Receiver<Frame>,
    engine: JoinHandle<Result<(), EngineError>>,
}

fn spawn(handshake: Handshake, server_message_capacity: usize) -> Harness {
    let (server_frame_tx, server_frame_rx) = mpsc::channel(4);
    let (server_message_tx, server_message_rx) = mpsc::channel(server_message_capacity);
    let (client_message_tx, client_message_rx) = mpsc::channel(4);
    let (client_frame_tx, client_frame_rx) = mpsc::channel(4);
    let (handshake_tx, handshake_rx) = oneshot::channel();
    handshake_tx.send(handshake).unwrap();
    let engine = tokio::spawn(crate::engine::protocol::run_protocol(
        server_frame_rx,
        server_message_tx,
        client_message_rx,
        client_frame_tx,
        handshake_rx,
    ));
    Harness {
        server_frame_tx,
        server_message_rx,
        client_message_tx,
        client_frame_rx,
        engine,
    }
}

impl Harness {
    /// Ends the session from the transport, then hangs up the upper layer.
    ///
    /// Returns the engine result, the frames sent to the transport, and the
    /// messages sent to the upper layer.
    async fn finish(mut self) -> (Result<(), EngineError>, Vec<Frame>, Vec<Message>) {
        drop(self.server_frame_tx);
        let mut frames = Vec::new();
        while let Some(frame) = self.client_frame_rx.recv().await {
            frames.push(frame);
        }
        drop(self.client_message_tx);
        let mut messages = Vec::new();
        while let Some(message) = self.server_message_rx.recv().await {
            messages.push(message);
        }
        (self.engine.await.unwrap(), frames, messages)
    }
}

#[tokio::test]
async fn protocol_handshake_dropped_is_error() {
    let (_server_frame_tx, server_frame_rx) = mpsc::channel(4);
    let (server_message_tx, _server_message_rx) = mpsc::channel(4);
    let (_client_message_tx, client_message_rx) = mpsc::channel(4);
    let (client_frame_tx, _) = mpsc::channel(4);
    let (handshake_tx, handshake_rx) = oneshot::channel::<Handshake>();
    drop(handshake_tx);
    let result = crate::engine::protocol::run_protocol(
        server_frame_rx,
        server_message_tx,
        client_message_rx,
        client_frame_tx,
        handshake_rx,
    )
    .await;
    assert!(matches!(result, Err(EngineError::Handshake(_))));
}

#[tokio::test]
async fn protocol_transport_closed_ends_message_stream() {
    let Harness {
        server_frame_tx,
        mut server_message_rx,
        client_message_tx,
        mut client_frame_rx,
        engine,
    } = spawn(make_handshake(), 4);
    drop(server_frame_tx);
    assert!(server_message_rx.recv().await.is_none());
    assert!(client_frame_rx.recv().await.is_none());
    drop(client_message_tx);
    engine.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_client_close_ends_frame_stream() {
    let Harness {
        server_frame_tx,
        mut server_message_rx,
        client_message_tx,
        mut client_frame_rx,
        engine,
    } = spawn(make_handshake(), 4);
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
        Some(Message::Text(t)) if t == "late"
    ));
    assert!(server_message_rx.recv().await.is_none());
    engine.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_accepts_client_messages_until_sender_dropped() {
    let Harness {
        server_frame_tx,
        mut server_message_rx,
        client_message_tx,
        mut client_frame_rx,
        engine,
    } = spawn(make_handshake(), 4);
    drop(server_frame_tx);
    assert!(server_message_rx.recv().await.is_none());
    assert!(client_frame_rx.recv().await.is_none());
    client_message_tx
        .send(Message::Text("late".into()))
        .await
        .unwrap();
    drop(client_message_tx);
    engine.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_client_messages_flow_while_upper_layer_is_full() {
    let h = spawn(make_handshake(), 1);
    for text in ["fills", "blocks"] {
        h.server_frame_tx
            .send(Packet::Message(text.into()).into())
            .await
            .unwrap();
    }
    h.client_message_tx
        .send(Message::Text("out".into()))
        .await
        .unwrap();
    let Harness {
        mut client_frame_rx,
        ..
    } = h;
    let frame = tokio::time::timeout(Duration::from_secs(5), client_frame_rx.recv())
        .await
        .unwrap();
    assert!(matches!(frame, Some(Frame::Packet(Packet::Message(m))) if m == "out"));
}

#[tokio::test]
async fn protocol_ping_triggers_pong() {
    let h = spawn(make_handshake(), 4);
    h.server_frame_tx
        .send(Packet::Ping("probe".into()).into())
        .await
        .unwrap();
    let (result, frames, _) = h.finish().await;
    result.unwrap();
    assert!(matches!(&frames[..], [Frame::Packet(Packet::Pong(p))] if p == "probe"));
}

#[tokio::test]
async fn protocol_noop_is_ignored() {
    let h = spawn(make_handshake(), 4);
    h.server_frame_tx.send(Packet::Noop.into()).await.unwrap();
    let (result, frames, messages) = h.finish().await;
    result.unwrap();
    assert!(frames.is_empty());
    assert!(messages.is_empty());
}

#[tokio::test]
async fn protocol_unexpected_packet_is_server_error() {
    let h = spawn(make_handshake(), 4);
    h.server_frame_tx
        .send(Packet::Upgrade.into())
        .await
        .unwrap();
    let (result, _, _) = h.finish().await;
    assert!(matches!(
        result,
        Err(EngineError::UnexpectedPacket(Packet::Upgrade))
    ));
}

#[tokio::test]
async fn protocol_server_messages_forwarded() {
    let h = spawn(make_handshake(), 4);
    h.server_frame_tx
        .send(Packet::Message("hello".into()).into())
        .await
        .unwrap();
    h.server_frame_tx
        .send(Frame::Binary(Bytes::from_static(b"bin")))
        .await
        .unwrap();
    let (result, _, messages) = h.finish().await;
    result.unwrap();
    assert!(matches!(
        &messages[..],
        [Message::Text(t), Message::Binary(b)] if t == "hello" && b.as_ref() == b"bin"
    ));
}

#[tokio::test]
async fn protocol_client_messages_sent_to_transport() {
    let Harness {
        server_frame_tx,
        server_message_rx: _server_message_rx,
        client_message_tx,
        mut client_frame_rx,
        engine,
    } = spawn(make_handshake(), 4);
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
    engine.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_heartbeat_timeout_fires() {
    let handshake = Handshake {
        ping_interval: 1,
        ping_timeout: 1,
        ..make_handshake()
    };
    let h = spawn(handshake, 4);
    assert!(matches!(
        h.engine.await.unwrap(),
        Err(EngineError::HeartbeatTimeout)
    ));
}

#[tokio::test]
async fn protocol_closed_message_receiver_is_error() {
    let Harness {
        server_frame_tx,
        server_message_rx,
        client_message_tx: _client_message_tx,
        client_frame_rx: _client_frame_rx,
        engine,
    } = spawn(make_handshake(), 4);
    drop(server_message_rx);
    server_frame_tx
        .send(Packet::Message("x".into()).into())
        .await
        .unwrap();
    assert!(matches!(
        engine.await.unwrap(),
        Err(EngineError::ServerMessage(_))
    ));
}
