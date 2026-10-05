# sioc

A type-safe, async [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) client for Rust.

## What is the Socket.IO protocol?

The [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) is built on top of [Engine.IO protocol v4](https://socket.io/docs/v4/engine-io-protocol/).

It adds _namespaces_ for multiple channels over a single connection, _events_ as named messages with structured data, _acknowledgements_ for request/response patterns, and support for _binary attachments_.

## Design

- Async networking built on [`tokio`](https://docs.rs/tokio), [`reqwest`](https://docs.rs/reqwest), and [`tokio-tungstenite`](https://docs.rs/tokio-tungstenite).
- Actor-model client, communicating through channels, no internal locks.
- Derive macros for events and acks.
- Event handling lives in match arms, no callbacks with boxed futures.
- State lives in the enclosing scope, no Arc or Mutex.
- Zero-copy packet parsing via [`bytestring`](https://docs.rs/bytestring) and [`bytes`](https://docs.rs/bytes).
- Reconnection policy belongs to the application. Call `Client::connect` again for a closed namespace on a live session, or open a new `Client` after the session ends. Use `tokio::time::sleep` for application-controlled retry delays.

An application can reopen a session after its receiver ends. This example retries
session failures with a fixed delay; applications choose their own limit and policy.

```rust,no_run
# async fn reconnect(url: url::Url) -> sioc::error::Result<()> {
use sioc::prelude::*;
use std::time::Duration;

loop {
    let client = ClientBuilder::new(url.clone()).open()?;
    let (tx, mut rx) = client.connect("/").await?;
    while let Some(packet) = rx.recv().await {
        // Dispatch packets in the application's event loop.
        tracing::debug!(?packet, "received packet");
    }
    drop(tx);
    if let Err(error) = client.join().await {
        tracing::warn!(%error, "session ended with an error");
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
}
# }
```

## License

MIT OR Apache-2.0.
