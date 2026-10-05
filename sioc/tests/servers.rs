//! Reference-server tests run through `just test-servers`; normal tests stay self-contained.

use bytes::Bytes;
use sioc::prelude::*;
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
};
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

/// Owns the child, its readiness handshake, and shutdown through stdin EOF.
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
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let port = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
        Self { child, url }
    }

    async fn stop(mut self) {
        self.child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"stop\n")
            .await
            .unwrap();
        drop(self.child.stdin.take());
        assert!(
            tokio::time::timeout(Duration::from_secs(10), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }

    fn client(&self, transport: TransportStrategy, capacity: usize) -> Client {
        ClientBuilder::new(self.url.clone())
            .transport(transport)
            .channels(capacity)
            .open()
            .unwrap()
    }
}

async fn connected(client: &Client, ns: &str) -> (SocketSender, SocketReceiver) {
    let (tx, mut rx) = client.connect(ns).await.unwrap();
    assert!(matches!(rx.recv().await, Some(ServerPacket::Connect(_))));
    (tx, rx)
}

#[tokio::test]
#[ignore = "requires bun and uv; run just test-servers"]
async fn pressure_on_reference_servers() {
    for backend in ["js", "py"] {
        let server = Server::start(backend).await;
        for capacity in [1, 4, 32] {
            for transport in [TransportStrategy::WebSocket, TransportStrategy::Polling] {
                let websocket = matches!(transport, TransportStrategy::WebSocket);
                let client = server.client(transport, capacity);
                let (tx, mut rx) = connected(&client, "/").await;
                tx.emit(Flood(FLOOD)).await.unwrap();
                for seq in 0..FLOOD {
                    let Some(Received::Item(event)) = rx.listen::<Received>().await.unwrap() else {
                        panic!("expected item");
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

                // Fill the receive direction while a separate task keeps sending.
                let sending = tx.clone();
                let feeder = tokio::spawn(async move {
                    for seq in 0..FLOOD {
                        sending.emit(Echo(seq)).await.unwrap();
                    }
                });
                tokio::time::sleep(Duration::from_millis(10)).await;
                for seq in 0..FLOOD {
                    let Some(Received::Item(event)) = rx.listen::<Received>().await.unwrap() else {
                        panic!("expected echo");
                    };
                    assert_eq!(event.payload.0, seq);
                }
                feeder.await.unwrap();
                assert_eq!(
                    tx.emit(Count).await.unwrap().await.unwrap().payload.0,
                    FLOOD * 2
                );
                tx.disconnect();
                while rx.recv().await.is_some() {}
                client.join().await.unwrap();

                let client = server.client(
                    if websocket {
                        TransportStrategy::WebSocket
                    } else {
                        TransportStrategy::Polling
                    },
                    capacity,
                );
                let (tx, mut rx) = connected(&client, "/").await;
                tx.emit(Flood(FLOOD)).await.unwrap();
                assert!(matches!(rx.recv().await, Some(ServerPacket::Event(_))));
                drop(tx);
                let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
                client.join().await.unwrap();
                drain.await.unwrap();
            }
        }
        server.stop().await;
    }
}

#[tokio::test]
#[ignore = "requires bun and uv; run just test-servers"]
async fn protocol_on_reference_servers() {
    for backend in ["js", "py"] {
        let server = Server::start(backend).await;
        for transport in [TransportStrategy::WebSocket, TransportStrategy::Polling] {
            let client = server.client(transport, 4);
            let (observer_tx, mut observer_rx) = connected(&client, "/observe").await;
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
            tx.disconnect();
            assert!(rx.recv().await.is_none());
            assert!(matches!(
                observer_rx.recv().await,
                Some(ServerPacket::Event(_))
            ));

            let (tx, mut rx) = connected(&client, "/").await;
            tx.emit(Kick).await.unwrap();
            assert!(matches!(rx.recv().await, Some(ServerPacket::Disconnect)));
            assert!(rx.recv().await.is_none());
            assert!(matches!(
                tx.emit(Count).await,
                Err(sioc::error::SocketError::Closed)
            ));
            for _ in 0..2 {
                let (denied_tx, mut denied_rx) = client.connect("/denied").await.unwrap();
                assert!(matches!(
                    denied_rx.recv().await,
                    Some(ServerPacket::ConnectError(_))
                ));
                assert!(denied_rx.recv().await.is_none());
                assert!(matches!(
                    denied_tx.emit(Count).await,
                    Err(sioc::error::SocketError::Closed)
                ));
            }
            observer_tx.disconnect();
            while observer_rx.recv().await.is_some() {}
            client.join().await.unwrap();
        }
        server.stop().await;
    }
}
