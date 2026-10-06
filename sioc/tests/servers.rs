//! Reference-server tests run through `just test-servers`; normal tests stay
//! self-contained.
//!
//! Each scenario runs as one test per backend, such as
//! `py::kick_closes_namespace`, against its own server and over every transport
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
#[derive(Debug, EventRouter)]
enum Received {
    Item(Event<Item>),
    Blob(Event<IncomingBlob>),
}

/// Owns a reference server child, which exits once its stdin closes.
struct Server {
    child: Child,
    url: Url,
}

impl Server {
    async fn start(backend: &str) -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let mut command = Command::new(if backend == "js" { "bun" } else { "uv" });
        if backend == "js" {
            command.arg("sioc/tests/servers/server.ts");
        } else {
            command.args(["run", "--locked", "python", "sioc/tests/servers/server.py"]);
        }
        let mut child = command
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

    fn client(&self, transport: TransportStrategy, capacity: usize) -> Client {
        ClientBuilder::new(self.url.clone())
            .transport(transport)
            .channels(capacity)
            .open()
            .unwrap()
    }
}

/// Capacities that put every bounded queue under pressure.
const PRESSURE: [usize; 3] = [1, 4, 32];
/// One ordinary capacity, for scenarios about protocol behavior.
const ORDINARY: [usize; 1] = [4];

/// Runs `scenario` against one backend over every transport and capacity.
async fn run(
    backend: &str,
    capacities: &[usize],
    scenario: impl AsyncFn(&Server, TransportStrategy, usize),
) {
    let server = Server::start(backend).await;
    for &capacity in capacities {
        for transport in [TransportStrategy::WebSocket, TransportStrategy::Polling] {
            scenario(&server, transport, capacity).await;
        }
    }
    server.stop().await;
}

macro_rules! scenarios {
    ($($scenario:ident: $capacities:ident),* $(,)?) => {
        mod js {
            $(
                #[tokio::test]
                #[ignore = "requires bun; run just test-servers"]
                async fn $scenario() {
                    super::run("js", &super::$capacities, super::$scenario).await;
                }
            )*
        }

        mod py {
            $(
                #[tokio::test]
                #[ignore = "requires uv; run just test-servers"]
                async fn $scenario() {
                    super::run("py", &super::$capacities, super::$scenario).await;
                }
            )*
        }
    };
}

scenarios! {
    flood_and_count: PRESSURE,
    echo_while_receiving_full: PRESSURE,
    drop_sender_mid_flood: PRESSURE,
    binary_roundtrip: ORDINARY,
    client_disconnect_notifies_observer: ORDINARY,
    kick_closes_namespace: ORDINARY,
    connect_error_closes_namespace: ORDINARY,
}

async fn connected(client: &Client, ns: &str) -> (SocketSender, SocketReceiver) {
    let (tx, mut rx) = client.connect(ns).await.unwrap();
    assert!(matches!(rx.recv().await, Some(ServerPacket::Connect(_))));
    (tx, rx)
}

/// Disconnects the namespace and waits until the session ends.
async fn finish(client: Client, tx: SocketSender, mut rx: SocketReceiver) {
    tx.disconnect();
    while rx.recv().await.is_some() {}
    client.join().await.unwrap();
}

/// The server floods items while the client answers each with `seen`.
async fn flood_and_count(server: &Server, transport: TransportStrategy, capacity: usize) {
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
}

/// A separate task keeps sending while the receive direction fills up.
async fn echo_while_receiving_full(server: &Server, transport: TransportStrategy, capacity: usize) {
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
}

/// Dropping the only sender mid-flood still ends the session.
async fn drop_sender_mid_flood(server: &Server, transport: TransportStrategy, capacity: usize) {
    let client = server.client(transport, capacity);
    let (tx, mut rx) = connected(&client, "/").await;
    tx.emit(Flood(FLOOD)).await.unwrap();
    assert!(matches!(rx.recv().await, Some(ServerPacket::Event(_))));
    drop(tx);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    client.join().await.unwrap();
    drain.await.unwrap();
}

/// A binary event comes back both as an event and as its ack.
async fn binary_roundtrip(server: &Server, transport: TransportStrategy, capacity: usize) {
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
}

/// Leaving one namespace keeps the session, and the server sees the leave.
async fn client_disconnect_notifies_observer(
    server: &Server,
    transport: TransportStrategy,
    capacity: usize,
) {
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
}

/// A server-side disconnect closes the namespace and its senders.
async fn kick_closes_namespace(server: &Server, transport: TransportStrategy, capacity: usize) {
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
}

/// A refused namespace closes, and the session lets the client try again.
async fn connect_error_closes_namespace(
    server: &Server,
    transport: TransportStrategy,
    capacity: usize,
) {
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
}
