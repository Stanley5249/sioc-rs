//! Backpressure tests: a server floods events while the client keeps up slowly.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use axum::Router;
use sioc::prelude::*;
use socketioxide::SocketIo;
use socketioxide::extract::{Data, SocketRef};
use tokio::net::TcpListener;
use url::Url;

const FLOOD: u32 = 2000;
const CAPACITIES: [usize; 3] = [1, 4, 32];

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Echo(u32);

#[derive(Debug, PartialEq, EventType, SerializePayload, DeserializePayload)]
struct Reply(u32);

#[derive(Debug, EventRouter)]
enum FloodEvent {
    Reply(Event<Reply>),
}

/// Serves a namespace that floods `reply` events and counts the `echo`s it gets
/// back.
async fn flood_server(echoes: Arc<AtomicU32>) -> Url {
    let (layer, io) = SocketIo::new_layer();
    io.ns("/", async move |socket: SocketRef| {
        socket.on("echo", async move |_: SocketRef, Data::<(u32,)>(_)| {
            echoes.fetch_add(1, Ordering::Relaxed);
        });
        tokio::spawn(async move {
            for i in 0..FLOOD {
                if socket.emit("reply", &(i,)).is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        });
    });
    let app = Router::new().layer(layer);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Url::parse(&format!("http://127.0.0.1:{port}")).unwrap()
}

/// Echoes every event from the receive loop while reading slower than the
/// server floods.
async fn echo_flood(capacity: usize) {
    let echoes = Arc::new(AtomicU32::new(0));
    let url = flood_server(echoes.clone()).await;
    let client = ClientBuilder::new(url).channels(capacity).open().unwrap();
    let (tx, mut rx) = client.connect("/").await.unwrap();

    let mut received = 0;
    while received < FLOOD {
        if let Some(FloodEvent::Reply(Event {
            payload: Reply(i), ..
        })) = rx.listen::<FloodEvent>().await.unwrap()
        {
            received += 1;
            if received % 50 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            tx.emit(Echo(i)).await.unwrap();
        }
    }

    while echoes.load(Ordering::Relaxed) < FLOOD {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Drops the namespace sender mid-flood; the session must still end.
async fn drop_during_flood(capacity: usize) {
    let url = flood_server(Arc::new(AtomicU32::new(0))).await;
    let client = ClientBuilder::new(url).channels(capacity).open().unwrap();
    let (tx, mut rx) = client.connect("/").await.unwrap();
    rx.recv().await.unwrap();
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    drop(tx);
    client.join().await.unwrap();
}

#[tokio::test]
async fn echo_under_flood() {
    for capacity in CAPACITIES {
        tokio::time::timeout(Duration::from_secs(20), echo_flood(capacity))
            .await
            .unwrap_or_else(|_| panic!("stalled with capacity {capacity}"));
    }
}

#[tokio::test]
async fn drop_sender_under_flood() {
    for capacity in CAPACITIES {
        tokio::time::timeout(Duration::from_secs(20), drop_during_flood(capacity))
            .await
            .unwrap_or_else(|_| panic!("join hung with capacity {capacity}"));
    }
}
