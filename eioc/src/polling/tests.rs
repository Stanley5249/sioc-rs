//! Tests for the polling transport.

use std::time::Duration;

use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
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
            let mut request = Vec::new();
            let (header_end, length) = loop {
                let mut chunk = [0; 1024];
                let count = tcp.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&chunk[..count]);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break (end + 4, length);
                    }
                }
            };
            bodies.push(
                String::from_utf8(request[header_end..header_end + length].to_vec()).unwrap(),
            );
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

/// Answers every HTTP request with `body` after `delay`, or never when
/// `body` is `None`.
async fn http_server(body: Option<&'static str>, delay: Duration) -> Url {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    tokio::spawn(async move {
        loop {
            let (mut tcp, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buffer = [0; 4096];
                while tcp.read(&mut buffer).await.is_ok_and(|n| n > 0) {
                    // POSTs always succeed; GETs answer `body` after `delay`, or never.
                    let body = if buffer.starts_with(b"POST") {
                        "ok"
                    } else {
                        let Some(body) = body else { continue };
                        tokio::time::sleep(delay).await;
                        body
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    if tcp.write_all(response.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    url
}

#[tokio::test]
async fn forward_server_frames_finishes_get_in_flight_when_paused() {
    let url = http_server(Some("4data"), Duration::from_millis(100)).await;
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let pause = CancellationToken::new();
    let pause_later = pause.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        pause_later.cancel();
    });
    let client = Client::new();
    let stop =
        crate::polling::forward::forward_server_frames(&client, &url, &server_frame_tx, &pause)
            .await
            .unwrap();
    assert!(matches!(stop, Stop::Paused));
    assert!(matches!(
        server_frame_rx.recv().await.unwrap(),
        Frame::Packet(Packet::Message(m)) if m == "data"
    ));
}

#[tokio::test]
async fn transport_forwards_packets_batched_with_handshake() {
    let url = http_server(
        Some(concat!(
            r#"0{"sid":"s","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}"#,
            "4data"
        )),
        Duration::ZERO,
    )
    .await;
    let (handshake_tx, handshake_rx) = oneshot::channel();
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let (_client_frame_tx, client_frame_rx) = mpsc::channel(4);
    let task = tokio::spawn(crate::polling::session::run(
        Client::new(),
        url,
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
}

#[tokio::test]
async fn forward_server_frames_ends_at_server_close() {
    let url = http_server(Some("1"), Duration::ZERO).await;
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let client = Client::new();
    let stop = crate::polling::forward::forward_server_frames(
        &client,
        &url,
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
    let url = http_server(None, Duration::ZERO).await;
    let (server_frame_tx, _frame_rx) = mpsc::channel(4);
    let (client_frame_tx, mut client_frame_rx) = mpsc::channel(4);
    drop(client_frame_tx);
    let client = Client::new();
    let pause = CancellationToken::new();
    let forward = crate::polling::forward::forward_frames(
        &client,
        &url,
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
    let url = http_server(Some("1"), Duration::from_millis(50)).await;
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let (_transport_tx, mut client_frame_rx) = mpsc::channel(4);
    let client = Client::new();
    let upgrade = async { Err(WebSocketError::Closed) };
    let stream = crate::polling::forward::forward_frames_until_upgrade(
        &client,
        &url,
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
