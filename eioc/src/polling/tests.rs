//! Tests for the polling transport.

use std::time::Duration;

use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::error::WebSocketError;
use crate::packet::{Frame, Packet};
use crate::polling::forward::Stop;

#[tokio::test]
async fn forward_client_frames_respects_limit_and_closes_last() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for _ in 0..3 {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let (request, body_start) = read_http_request(&mut tcp).await.unwrap();
            bodies.push(String::from_utf8(request[body_start..].to_vec()).unwrap());
            tcp.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                .await
                .unwrap();
        }
        bodies
    });
    let (tx, mut rx) = mpsc::channel(4);
    for payload in ["aaaa", "bbbb", "cccc"] {
        tx.send(Packet::Message(payload.into()).into())
            .await
            .unwrap();
    }
    drop(tx);
    crate::polling::forward::forward_client_frames(
        &Client::new(),
        &url,
        &mut rx,
        &CancellationToken::new(),
        11,
    )
    .await
    .unwrap();
    assert_eq!(server.await.unwrap(), ["4aaaa\x1e4bbbb", "4cccc", "1"]);
}

/// Owns the listener and its connections. POSTs complete immediately; GETs
/// wait for the test to release `respond`, after notifying `get_received`.
struct HttpServer {
    url: Url,
    task: JoinHandle<()>,
    get_received: CancellationToken,
    respond: CancellationToken,
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        // Aborting the listener drops its JoinSet and aborts every connection.
        self.task.abort();
    }
}

async fn http_server(body: &'static str) -> HttpServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let get_received = CancellationToken::new();
    let respond = CancellationToken::new();
    let received = get_received.clone();
    let response_ready = respond.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            // accept and join_next are cancel-safe. Both handlers only manage
            // tasks, so neither waits on a connection's I/O.
            tokio::select! {
                result = listener.accept() => {
                    let (mut tcp, _) = result.unwrap();
                    let received = received.clone();
                    let response_ready = response_ready.clone();
                    connections.spawn(async move {
                        let Some((request, _)) = read_http_request(&mut tcp).await else { return };
                        let body = if request.starts_with(b"POST") {
                            "ok"
                        } else {
                            received.cancel();
                            response_ready.cancelled().await;
                            body
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        // A client may close before the pending GET completes.
                        let _ = tcp.write_all(response.as_bytes()).await;
                    });
                }
                Some(result) = connections.join_next() => result.unwrap(),
            }
        }
    });
    HttpServer {
        url,
        task,
        get_received,
        respond,
    }
}

/// Reads one complete request, including a body fragmented across TCP reads.
async fn read_http_request(tcp: &mut TcpStream) -> Option<(Vec<u8>, usize)> {
    let mut request = Vec::new();
    loop {
        let mut chunk = [0; 1024];
        let count = tcp.read(&mut chunk).await.unwrap();
        if count == 0 {
            return None;
        }
        request.extend_from_slice(&chunk[..count]);
        if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
            let length: usize = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .map_or(0, |length| length.parse().unwrap());
            if request.len() >= end + 4 + length {
                request.truncate(end + 4 + length);
                return Some((request, end + 4));
            }
        }
    }
}

#[tokio::test]
async fn forward_server_frames_finishes_get_in_flight_when_paused() {
    let server = http_server("4data").await;
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let pause = CancellationToken::new();
    let pause_in_flight = async {
        server.get_received.cancelled().await;
        pause.cancel();
        server.respond.cancel();
    };
    let client = Client::new();
    let (stop, ()) = tokio::join!(
        crate::polling::forward::forward_server_frames(
            &client,
            &server.url,
            &server_frame_tx,
            &pause
        ),
        pause_in_flight,
    );
    let stop = stop.unwrap();
    assert!(matches!(stop, Stop::Paused));
    assert!(matches!(
        server_frame_rx.recv().await.unwrap(),
        Frame::Packet(Packet::Message(m)) if m == "data"
    ));
}

#[tokio::test]
async fn transport_forwards_packets_batched_with_handshake() {
    let server = http_server(concat!(
        r#"0{"sid":"s","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}"#,
        "4data"
    )).await;
    server.respond.cancel();
    let (handshake_tx, handshake_rx) = oneshot::channel();
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let (_client_frame_tx, client_frame_rx) = mpsc::channel(4);
    let task = tokio::spawn(crate::polling::session::run(
        Client::new(),
        server.url.clone(),
        (),
        handshake_tx,
        server_frame_tx,
        client_frame_rx,
    ));

    assert_eq!(handshake_rx.await.unwrap().sid, "s");
    assert!(matches!(
        server_frame_rx.recv().await.unwrap(),
        Frame::Packet(Packet::Message(m)) if m == "data"
    ));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn forward_server_frames_ends_at_server_close() {
    let server = http_server("1").await;
    server.respond.cancel();
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let client = Client::new();
    let stop = crate::polling::forward::forward_server_frames(
        &client,
        &server.url,
        &server_frame_tx,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(matches!(stop, Stop::Ended));
    server_frame_rx.try_recv().unwrap_err();
}

#[tokio::test]
async fn forward_frames_abandons_get_when_engine_closes() {
    let server = http_server("4blocked").await;
    let (server_frame_tx, _frame_rx) = mpsc::channel(4);
    let (client_frame_tx, mut client_frame_rx) = mpsc::channel(4);
    drop(client_frame_tx);
    let client = Client::new();
    let pause = CancellationToken::new();
    let forward = crate::polling::forward::forward_frames(
        &client,
        &server.url,
        &server_frame_tx,
        &mut client_frame_rx,
        &pause,
        1_000_000,
    );
    let stop = tokio::time::timeout(Duration::from_secs(5), forward)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(stop, Stop::Ended));
}

#[tokio::test]
async fn failed_upgrade_falls_back_to_long_polling() {
    let server = http_server("1").await;
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let (_transport_tx, mut client_frame_rx) = mpsc::channel(4);
    let client = Client::new();
    let upgrade = async {
        server.get_received.cancelled().await;
        server.respond.cancel();
        Err(WebSocketError::Closed)
    };
    let stream = crate::polling::forward::forward_frames_until_upgrade(
        &client,
        &server.url,
        &server_frame_tx,
        &mut client_frame_rx,
        upgrade,
        1_000_000,
    )
    .await
    .unwrap();
    assert!(stream.is_none());
    server_frame_rx.try_recv().unwrap_err();
}
