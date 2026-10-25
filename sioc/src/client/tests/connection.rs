//! Builder options and client connection lifecycle through public handles.

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use eioc::prelude::TransportStrategy;
use tokio::sync::{Semaphore, mpsc};
use url::Url;

use crate::client::{Client, ClientBuilder};
use crate::config::{ChannelConfig, ReconnectionConfig};
use crate::error::ClientBuilderError;

#[tokio::test]
async fn builder_preserves_connection_query_parameters() {
    let (url_tx, mut url_rx) = mpsc::channel(1);
    let client = ClientBuilder::new(
        Url::parse("https://localhost/base?token=hello%20world&tag=a&tag=b").unwrap(),
    )
    .path("/custom/")
    .transport(TransportStrategy::WebSocket)
    .websocket_connector(move |url: Url| {
        let url_tx = url_tx.clone();
        async move {
            url_tx.send(url).await.unwrap();
            Err(std::io::Error::from(ErrorKind::ConnectionRefused).into())
        }
    })
    .open()
    .unwrap();
    let url = url_rx.recv().await.unwrap();
    assert_eq!(url.scheme(), "wss");
    assert_eq!(url.path(), "/custom/");
    let query: Vec<_> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert_eq!(
        query,
        [
            ("token".into(), "hello world".into()),
            ("tag".into(), "a".into()),
            ("tag".into(), "b".into()),
            ("EIO".into(), "4".into()),
            ("transport".into(), "websocket".into()),
        ]
    );
    client.join().await.unwrap();
}

#[tokio::test]
async fn builder_rejects_zero_channel_capacities() {
    for config in [
        ChannelConfig {
            engine: 0,
            ..ChannelConfig::default()
        },
        ChannelConfig {
            transport: 0,
            ..ChannelConfig::default()
        },
        ChannelConfig {
            manager: 0,
            ..ChannelConfig::default()
        },
        ChannelConfig {
            socket: 0,
            ..ChannelConfig::default()
        },
    ] {
        ClientBuilder::new(Url::parse("http://localhost/").unwrap())
            .channels(config)
            .open()
            .unwrap_err();
    }
}

#[tokio::test]
async fn open_returns_client() {
    let url = Url::parse("http://localhost:9999/").unwrap();
    ClientBuilder::new(url).open().unwrap();
}

#[tokio::test]
async fn client_builder_alias() {
    let url = Url::parse("http://localhost:9999/").unwrap();
    Client::builder(url).open().unwrap();
}

#[tokio::test]
async fn builder_path_channels_transport() {
    let url = Url::parse("http://localhost:9999/").unwrap();
    let result = ClientBuilder::new(url)
        .path("socket.io/")
        .channels(16_usize)
        .transport(TransportStrategy::WebSocket)
        .open();
    result.unwrap();
}

#[tokio::test]
async fn builder_http_client() {
    let url = Url::parse("http://localhost:9999/").unwrap();
    let result = ClientBuilder::new(url)
        .http_client(reqwest::Client::new())
        .open();
    result.unwrap();
}

#[test]
fn builder_rejects_url_without_relative_path_support() {
    let url = Url::parse("mailto:client@example.com").unwrap();
    assert!(matches!(
        ClientBuilder::new(url).open(),
        Err(ClientBuilderError::Url(
            url::ParseError::RelativeUrlWithCannotBeABaseBase
        ))
    ));
}

#[tokio::test]
async fn reconnection_gives_up_after_its_attempts() {
    let gate = Arc::new(Semaphore::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let connector_gate = Arc::clone(&gate);
    let counter = Arc::clone(&attempts);
    let client = ClientBuilder::new(Url::parse("http://localhost:3000/").unwrap())
        .path("custom/")
        .transport(TransportStrategy::WebSocket)
        .reconnection(Some(ReconnectionConfig {
            attempts: Some(2),
            delay: Duration::from_millis(1),
            delay_max: Duration::from_millis(1),
            ..ReconnectionConfig::default()
        }))
        .websocket_connector(move |url: Url| async move {
            // Wait for the namespace, so that every session reconnects.
            connector_gate.acquire().await.unwrap().forget();
            assert_eq!(url.scheme(), "ws");
            assert_eq!(url.path(), "/custom/");
            counter.fetch_add(1, Ordering::Relaxed);
            Err(std::io::Error::new(ErrorKind::ConnectionRefused, "connector refused").into())
        })
        .open()
        .unwrap();

    let (_tx, mut rx) = client.connect("/").await.unwrap();
    gate.add_permits(16);
    assert!(rx.recv().await.is_none());
    assert_eq!(attempts.load(Ordering::Relaxed), 3);

    // The client outlives giving up, and the next namespace starts over.
    let (_chat_tx, mut chat_rx) = client.connect("/chat").await.unwrap();
    assert!(chat_rx.recv().await.is_none());
    assert_eq!(attempts.load(Ordering::Relaxed), 6);
    client.join().await.unwrap();
}

#[tokio::test]
async fn without_reconnection_the_first_failure_closes_the_namespace() {
    let client = ClientBuilder::new(Url::parse("http://localhost:3000/").unwrap())
        .transport(TransportStrategy::WebSocket)
        .reconnection(None)
        .websocket_connector(async |_| {
            Err(std::io::Error::from(ErrorKind::ConnectionRefused).into())
        })
        .open()
        .unwrap();

    let (_tx, mut rx) = client
        .connect_with("/chat", r#"{"token":"secret"}"#)
        .await
        .unwrap();
    assert!(rx.recv().await.is_none());
    client.join().await.unwrap();
}

#[tokio::test]
async fn handshake_timeout_fails_the_attempt() {
    let client = ClientBuilder::new(Url::parse("http://localhost:3000/").unwrap())
        .transport(TransportStrategy::WebSocket)
        .reconnection(None)
        .timeout(Some(Duration::from_millis(10)))
        .websocket_connector(async |_| std::future::pending().await)
        .open()
        .unwrap();

    let (_tx, mut rx) = client.connect("/").await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(matches!(closed, Ok(None)), "the attempt never timed out");
    client.join().await.unwrap();
}
