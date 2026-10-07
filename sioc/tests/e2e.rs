//! End-to-end tests against the TypeScript reference server, run through
//! `just test-e2e`.
//!
//! Each test starts its own server and runs its scenario over every transport
//! and channel capacity.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use bytes::Bytes;
use sioc::prelude::*;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use url::Url;

const FLOOD: u32 = 2_000;

#[derive(Debug, EventType, SerializePayload)]
struct Flood(u32);
#[derive(Debug, EventType, SerializePayload)]
struct Seen(u32);
#[derive(Debug, EventType, SerializePayload)]
struct Echo(u32);
#[derive(Debug, EventType, DeserializePayload)]
struct Item(u32);
#[derive(Debug, AckType, DeserializePayload)]
struct Total(u32);
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(ack = "Total"))]
struct Count;
#[derive(Debug, EventType, SerializePayload)]
struct Kick;
#[derive(Debug, AckType, DeserializePayload)]
#[sioc(ack(binary))]
struct BinaryAck(Placeholder);
#[derive(Debug, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(binary, ack = "BinaryAck"))]
struct Blob(Placeholder);
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "blob", binary))]
struct IncomingBlob(Placeholder);
#[derive(Debug, EventType, SerializePayload)]
struct Ask(u32);
#[derive(Debug, AckType, SerializePayload)]
struct Answer(u32);
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(ack = "Answer"))]
struct Question(u32);
#[derive(Debug, EventType, SerializePayload)]
struct CloseEngine;
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(ack = "Total"))]
struct CloseEngineLater;
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(ack = "Total"))]
struct HangUp;
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(ack = "Total"))]
struct Engines;
#[derive(Debug, EventRouter)]
enum Received {
    Item(Event<Item>),
    Blob(Event<IncomingBlob>),
    Question(Event<Question>),
}

/// Owns a reference server child, which exits once its stdin closes.
struct Server {
    child: Child,
    url: Url,
}

impl Server {
    async fn start() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let mut child = Command::new("bun")
            .arg("sioc/tests/fixtures/server.ts")
            .current_dir(root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        // The server prints its port once it listens on it.
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let port = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await
            .expect("the reference server listens within 10 seconds")
            .unwrap()
            .expect("the reference server prints its port");
        let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
        Self { child, url }
    }

    async fn stop(mut self) {
        drop(self.child.stdin.take());
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("the reference server exits within 10 seconds")
            .unwrap();
        assert!(status.success());
    }

    /// Opens a client that reconnects after 50 milliseconds, to keep the
    /// reconnection tests short.
    fn client(&self, transport: TransportStrategy, capacity: usize) -> Client {
        let reconnection = ReconnectionConfig {
            delay: Duration::from_millis(50),
            delay_max: Duration::from_millis(50),
            ..ReconnectionConfig::default()
        };
        ClientBuilder::new(self.url.clone())
            .transport(transport)
            .channels(capacity)
            .reconnection(Some(reconnection))
            .open()
            .unwrap()
    }
}

/// Capacities that put every bounded queue under pressure.
const PRESSURE: [usize; 3] = [1, 4, 32];
/// One ordinary capacity, for scenarios about protocol behavior.
const ORDINARY: [usize; 1] = [4];

/// Runs `scenario` against its own server over every transport and capacity.
async fn run(capacities: &[usize], scenario: impl AsyncFn(&Server, TransportStrategy, usize)) {
    let server = Server::start().await;
    for &capacity in capacities {
        for transport in [TransportStrategy::WebSocket, TransportStrategy::Polling] {
            scenario(&server, transport, capacity).await;
        }
    }
    server.stop().await;
}

async fn connected(client: &Client, ns: &str) -> (SocketSender, SocketReceiver) {
    let (tx, mut rx) = client.connect(ns).await.unwrap();
    assert!(matches!(rx.recv().await, Some(ServerPacket::Connect(_))));
    (tx, rx)
}

/// Returns the namespace's next item, which must be a CONNECT.
async fn next_connect(rx: &mut SocketReceiver) -> Connect {
    match rx.recv().await {
        Some(ServerPacket::Connect(connect)) => connect,
        other => panic!("expected a CONNECT, got {other:?}"),
    }
}

/// Disconnects the namespace and waits until the client ends.
async fn finish(client: Client, tx: SocketSender, mut rx: SocketReceiver) {
    tx.disconnect();
    while rx.recv().await.is_some() {}
    client.join().await.unwrap();
}

/// The server floods items while the client answers each with `seen`.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn flood_and_count() {
    run(&PRESSURE, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = connected(&client, "/").await;
        tx.emit(Flood(FLOOD)).await.unwrap();
        for seq in 0..FLOOD {
            let Some(Received::Item(event)) = rx.listen::<Received>().await.unwrap() else {
                panic!("expected item {seq}");
            };
            assert_eq!(event.payload.0, seq);
            tx.emit(Seen(seq)).await.unwrap();
            if seq % 50 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        assert_eq!(
            tx.emit(Count).await.unwrap().await.unwrap().payload.0,
            FLOOD
        );
        finish(client, tx, rx).await;
    })
    .await;
}

/// A separate task keeps sending while the receive direction fills up.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn echo_while_receiving_full() {
    run(&PRESSURE, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = connected(&client, "/").await;
        let sending = tx.clone();
        let feeder = tokio::spawn(async move {
            for seq in 0..FLOOD {
                sending.emit(Echo(seq)).await.unwrap();
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        for seq in 0..FLOOD {
            let Some(Received::Item(event)) = rx.listen::<Received>().await.unwrap() else {
                panic!("expected echo {seq}");
            };
            assert_eq!(event.payload.0, seq);
        }
        feeder.await.unwrap();
        assert_eq!(
            tx.emit(Count).await.unwrap().await.unwrap().payload.0,
            FLOOD
        );
        finish(client, tx, rx).await;
    })
    .await;
}

/// Dropping the only sender mid-flood still ends the client.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn drop_sender_mid_flood() {
    run(&PRESSURE, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = connected(&client, "/").await;
        tx.emit(Flood(FLOOD)).await.unwrap();
        assert!(matches!(rx.recv().await, Some(ServerPacket::Event(_))));
        drop(tx);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        client.join().await.unwrap();
        drain.await.unwrap();
    })
    .await;
}

/// A binary event comes back both as an event and as its ack.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn binary_roundtrip() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = connected(&client, "/").await;
        let data = Bytes::from_static(b"binary roundtrip");
        let expected = data.clone();
        let handle = tx
            .emit(|builder: &mut AttachmentsBuilder| Blob(builder.attach(data)))
            .await
            .unwrap();
        let Some(Received::Blob(event)) = rx.listen::<Received>().await.unwrap() else {
            panic!("expected blob");
        };
        assert_eq!(event.attachments[event.payload.0.slot()], expected);
        let ack = handle.await.unwrap();
        assert_eq!(ack.attachments[ack.payload.0.slot()], expected);
        finish(client, tx, rx).await;
    })
    .await;
}

/// Leaving one namespace keeps the session, and the server sees the leave.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn client_disconnect_notifies_observer() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (observer_tx, mut observer_rx) = connected(&client, "/observe").await;
        let (tx, mut rx) = connected(&client, "/").await;
        tx.disconnect();
        assert!(rx.recv().await.is_none());
        assert!(matches!(
            observer_rx.recv().await,
            Some(ServerPacket::Event(_))
        ));
        finish(client, observer_tx, observer_rx).await;
    })
    .await;
}

/// A server-side disconnect closes the namespace and its senders.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn kick_closes_namespace() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = connected(&client, "/").await;
        tx.emit(Kick).await.unwrap();
        assert!(matches!(rx.recv().await, Some(ServerPacket::Disconnect)));
        assert!(rx.recv().await.is_none());
        assert!(matches!(
            tx.emit(Count).await,
            Err(sioc::error::SocketError::Closed)
        ));
        client.join().await.unwrap();
    })
    .await;
}

/// A refused namespace closes, and the session lets the client try again.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn connect_error_closes_namespace() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        for _ in 0..2 {
            let (tx, mut rx) = client.connect("/denied").await.unwrap();
            assert!(matches!(
                rx.recv().await,
                Some(ServerPacket::ConnectError(_))
            ));
            assert!(rx.recv().await.is_none());
            assert!(matches!(
                tx.emit(Count).await,
                Err(sioc::error::SocketError::Closed)
            ));
        }
        client.join().await.unwrap();
    })
    .await;
}

/// The server asks the client back, and the client's ack reaches it.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn server_ack_reaches_server() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = connected(&client, "/").await;
        tx.emit(Ask(7)).await.unwrap();
        let Some(Received::Question(Event {
            payload: Question(n),
            id,
            ..
        })) = rx.listen::<Received>().await.unwrap()
        else {
            panic!("expected question");
        };
        tx.acknowledge(id, Answer(n * 2)).await.unwrap();
        let Some(Received::Item(event)) = rx.listen::<Received>().await.unwrap() else {
            panic!("expected the answer back");
        };
        assert_eq!(event.payload.0, 14);
        finish(client, tx, rx).await;
    })
    .await;
}

/// The server ends the Engine.IO session, and the namespace connects again.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn engine_close_reconnects() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = client.connect("/").await.unwrap();
        let first = next_connect(&mut rx).await;
        tx.emit(Count).await.unwrap().await.unwrap();

        tx.emit(CloseEngine).await.unwrap();
        let second = next_connect(&mut rx).await;
        assert_ne!(first.sid, second.sid);

        // The server sees a new socket, which has seen nothing yet.
        assert_eq!(tx.emit(Count).await.unwrap().await.unwrap().payload.0, 0);
        finish(client, tx, rx).await;
    })
    .await;
}

/// The server refuses the CONNECT resent after a reconnection, which closes
/// only that namespace.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn refused_reconnection_closes_namespace() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = connected(&client, "/").await;
        let auth = format!(r#"{{"token":"{transport:?}-{capacity}"}}"#);
        let (once_tx, mut once_rx) = client.connect_with("/once", auth).await.unwrap();
        next_connect(&mut once_rx).await;

        tx.emit(CloseEngine).await.unwrap();
        next_connect(&mut rx).await;
        assert!(matches!(
            once_rx.recv().await,
            Some(ServerPacket::ConnectError(error)) if error.message == "used"
        ));
        assert!(once_rx.recv().await.is_none());
        assert!(matches!(
            once_tx.emit(Count).await,
            Err(sioc::error::SocketError::Closed)
        ));
        finish(client, tx, rx).await;
    })
    .await;
}

/// An ack the server never sent fails when the session drops.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn pending_ack_fails_on_drop() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, mut rx) = connected(&client, "/").await;
        let handle = tx.emit(HangUp).await.unwrap();
        assert!(matches!(handle.await, Err(sioc::error::AckError::Recv(_))));
        next_connect(&mut rx).await;
        finish(client, tx, rx).await;
    })
    .await;
}

/// A session that drops without open namespaces is not reopened until a
/// namespace opens.
#[tokio::test]
#[ignore = "requires bun; run just test-e2e"]
async fn no_reconnection_without_namespaces() {
    run(&ORDINARY, async |server: &Server, transport, capacity| {
        let client = server.client(transport, capacity);
        let (tx, rx) = connected(&client, "/").await;
        // Wait for the ack, because the server ignores an event that arrives
        // with the DISCONNECT behind it in one polling request.
        let handle = tx.emit(CloseEngineLater).await.unwrap();
        let before = handle.await.unwrap().payload.0;
        tx.disconnect();
        drop(rx);

        // Wait past the drop and the reconnection delay.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let (tx, rx) = connected(&client, "/").await;
        let after = tx.emit(Engines).await.unwrap().await.unwrap().payload.0;
        assert_eq!(after, before + 1);
        finish(client, tx, rx).await;
    })
    .await;
}
