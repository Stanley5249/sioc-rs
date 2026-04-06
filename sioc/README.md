# sioc

An async [Socket.IO v4](https://socket.io/docs/v4/socket-io-protocol/) client for Rust.

## Features

- Async, built on Tokio.
- Derive macros for compile-time event and ack schemas.
- Compile-time binary attachment and acknowledgement markers.
- Zero-copy packet parsing via `bytes::Bytes`.

## Quick Start

```rust,no_run
use sioc::prelude::*;
use url::Url;

#[derive(Debug, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(name = "ping"))]
struct Ping { data: i64 }

#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "pong"))]
struct Pong { data: i64 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = Url::parse("http://localhost:3000")?;
    let client = ClientBuilder::new(url).open()?;
    let (tx, mut rx) = client.connect("/").await?;

    while let Some(packet) = rx.recv().await {
        match packet {
            Packet::Connect(info) => println!("connected: {}", info.sid),
            Packet::Disconnect => break,
            Packet::ConnectError(e) => { eprintln!("{e}"); break }
            Packet::Event(ev) => {
                let ping = ev.cast::<Event<Ping>>()?.payload;
                tx.emit(Pong { data: ping.data }).await?;
            }
        }
    }

    Ok(())
}
```

## Examples

See [`examples/`](../examples) for runnable examples including a ping-pong server and a generals.io bot.

## License

Licensed under MIT OR Apache-2.0.
