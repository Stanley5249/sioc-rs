//! Client tests against an in-process socketioxide server over WebSocket and
//! polling.

use std::time::Duration;

use axum::Router;
use sioc::prelude::*;
use socketioxide::SocketIo;
use socketioxide::extract::{AckSender, Data, SocketRef};
use tokio::net::TcpListener;
use url::Url;

// sioc wire format: tuple struct fields become positional JSON args.
// Echo(42) -> ["echo", 42], Reply(42) -> ["reply", 42].
// On the socketioxide side we mirror this with (u32,) tuples.

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Echo(u32);

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Reply(u32);

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(ack = "Confirm"))]
struct EchoWithAck(u32);

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(ack = "Confirm"))]
struct ServerAsk(u32);

#[derive(Debug, PartialEq, AckType, SerializePayload, DeserializePayload)]
struct Confirm(bool);

#[derive(Debug, EventRouter)]
enum MyEvent {
    Reply(Event<Reply>),
    ServerAsk(Event<ServerAsk>),
}

async fn spawn_server(setup: impl FnOnce(&SocketIo)) -> u16 {
    let (layer, io) = SocketIo::new_layer();
    setup(&io);
    let app = Router::new().layer(layer);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    port
}

#[tokio::test]
async fn ws_connect() {
    let port = spawn_server(|io| {
        io.ns("/", async |_socket: SocketRef| {});
    })
    .await;
    let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
    let client = ClientBuilder::new(url).open().unwrap();
    let (_tx, mut rx) = client.connect("/").await.unwrap();
    assert!(matches!(rx.recv().await.unwrap(), ServerPacket::Connect(_)));
}

#[tokio::test]
async fn ws_emit_echo() {
    let port = spawn_server(|io| {
        io.ns("/", async |socket: SocketRef| {
            socket.on("echo", async |socket: SocketRef, Data::<(u32,)>((seq,))| {
                socket.emit("reply", &(seq,)).ok();
            });
        });
    })
    .await;
    let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
    let client = ClientBuilder::new(url).open().unwrap();
    let (tx, mut rx) = client.connect("/").await.unwrap();
    rx.recv().await.unwrap();
    tx.emit(Echo(7)).await.unwrap();
    let event = rx.listen::<MyEvent>().await.unwrap().unwrap();
    assert!(matches!(
        event,
        MyEvent::Reply(Event {
            payload: Reply(7),
            ..
        })
    ));
}

#[tokio::test]
async fn ws_client_ack() {
    let port = spawn_server(|io| {
        io.ns("/", async |socket: SocketRef| {
            socket.on(
                "echo_with_ack",
                async |_: SocketRef, Data::<(u32,)>(_), ack: AckSender| {
                    ack.send(&(true,)).ok();
                },
            );
        });
    })
    .await;
    let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
    let client = ClientBuilder::new(url).open().unwrap();
    let (tx, mut rx) = client.connect("/").await.unwrap();
    rx.recv().await.unwrap();
    let handle = tx.emit(EchoWithAck(1)).await.unwrap();
    let Ack {
        payload: Confirm(ok),
        ..
    } = handle.timeout(Duration::from_secs(5)).await.unwrap();
    assert!(ok);
}

#[tokio::test]
async fn ws_server_ack() {
    let port = spawn_server(|io| {
        io.ns("/", async |socket: SocketRef| {
            let _ = socket
                .emit_with_ack::<(u32,), (bool,)>("server_ask", &(99u32,))
                .unwrap()
                .await;
        });
    })
    .await;
    let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
    let client = ClientBuilder::new(url).open().unwrap();
    let (tx, mut rx) = client.connect("/").await.unwrap();
    rx.recv().await.unwrap();
    let event = rx.listen::<MyEvent>().await.unwrap().unwrap();
    let MyEvent::ServerAsk(Event { id, .. }) = event else {
        panic!("wrong event");
    };
    tx.acknowledge(id, Confirm(true)).await.unwrap();
}

#[tokio::test]
async fn polling_connect() {
    let port = spawn_server(|io| {
        io.ns("/", async |_socket: SocketRef| {});
    })
    .await;
    let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
    let client = ClientBuilder::new(url)
        .transport(TransportStrategy::Polling)
        .open()
        .unwrap();
    let (_tx, mut rx) = client.connect("/").await.unwrap();
    assert!(matches!(rx.recv().await.unwrap(), ServerPacket::Connect(_)));
}

#[tokio::test]
async fn polling_emit_echo() {
    let port = spawn_server(|io| {
        io.ns("/", async |socket: SocketRef| {
            socket.on("echo", async |socket: SocketRef, Data::<(u32,)>((seq,))| {
                socket.emit("reply", &(seq,)).ok();
            });
        });
    })
    .await;
    let url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
    let client = ClientBuilder::new(url)
        .transport(TransportStrategy::Polling)
        .open()
        .unwrap();
    let (tx, mut rx) = client.connect("/").await.unwrap();
    rx.recv().await.unwrap();
    tx.emit(Echo(99)).await.unwrap();
    let event = rx.listen::<MyEvent>().await.unwrap().unwrap();
    assert!(matches!(
        event,
        MyEvent::Reply(Event {
            payload: Reply(99),
            ..
        })
    ));
}
