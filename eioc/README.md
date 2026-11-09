# eioc

Async [Engine.IO protocol revision 4](https://socket.io/docs/v4/engine-io-protocol/) client for Rust, the transport layer of Socket.IO 3.x and 4.x.

## Engine.IO protocol

The [Engine.IO protocol](https://socket.io/docs/v4/engine-io-protocol/) provides bidirectional communication over HTTP long-polling and WebSocket. By default, this client starts with polling and upgrades to WebSocket when the server offers it. It also supports connecting directly over WebSocket.

## Usage

The higher-level [`sioc`](https://docs.rs/sioc) crate adds namespaces, events, and acknowledgements.

`eioc::engine::open` spawns a session and returns its channels. Wait for the handshake before sending, and close the session by dropping `client_message_tx`:

```rust,no_run
use eioc::engine::{ClientMessage, Engine, EngineOptions, ServerMessage};
use url::Url;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let url = Url::parse("http://localhost:3000/engine.io/")?;
let Engine {
    client_message_tx,
    mut server_message_rx,
    task,
    ..
} = eioc::engine::open(EngineOptions::new(url, ()));

if let Some(ServerMessage::Open(handshake)) = server_message_rx.recv().await {
    println!("session {}", handshake.sid);
    client_message_tx.send(ClientMessage::Text("hello".into())).await?;
}

drop(client_message_tx);
while let Some(message) = server_message_rx.recv().await {
    println!("{message:?}");
}
task.await??;
# Ok(())
# }
```

## License

MIT OR Apache-2.0.
