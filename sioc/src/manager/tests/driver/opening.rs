//! Cancelling opening reclaims the real engine future without a handshake.

use std::sync::Arc;
use std::time::Duration;

use eioc::prelude::TransportStrategy;
use tokio::sync::Notify;
use url::Url;

use crate::client::ClientBuilder;
use crate::manager::tests::rig::event;

#[tokio::test(start_paused = true)]
async fn cancellation_during_opening_needs_neither_handshake_nor_deadline() {
    for with_namespace in [false, true] {
        let started = Arc::new(Notify::new());
        let connector_started = Arc::clone(&started);
        let client = ClientBuilder::new(Url::parse("http://localhost:3000/").unwrap())
            .transport(TransportStrategy::WebSocket)
            .timeout(None)
            .channels(1)
            .websocket_connector(move |_| async move {
                connector_started.notify_one();
                std::future::pending().await
            })
            .open()
            .unwrap();
        let handles = if with_namespace {
            let (tx, rx) = client.connect("/").await.unwrap();
            tx.send(event(r#"["queued"]"#)).await.unwrap();
            Some((tx, rx))
        } else {
            None
        };
        started.notified().await;
        if let Some((tx, _rx)) = &handles {
            tx.disconnect();
        }
        tokio::time::timeout(Duration::from_secs(1), client.join())
            .await
            .expect("opening cancellation releases the engine without a peer response")
            .unwrap();
        if let Some((_tx, mut rx)) = handles {
            assert!(rx.recv().await.is_none());
        }
    }
}
