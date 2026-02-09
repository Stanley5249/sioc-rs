# Sioc

An async Socket.IO client for Rust.

## Features

- Asynchronous by default, built on Tokio.
- Lock-free design using channels.
- Provides derive macros for compile-time packet schemas.

## Quick Start

```rust
use sioc::prelude::*;
use url::Url;

#[derive(Debug, Clone, Event)]
#[sioc(event = "ping")]
struct Ping {
    data: i64,
}

#[derive(Debug, Clone, Event)]
#[sioc(event = "pong")]
struct Pong {
    data: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the server
    let url = Url::parse("http://localhost:3000")?;
    let client = ClientBuilder::new(url).open().await?;

    // Open a namespace
    let mut ns = client.connect("/").await?;

    while let Some(packet) = ns.recv().await {
        match packet {
            Packet::Connect(connection) => {
                println!("{connection:?}");
            }
            Packet::Disconnect => {
                println!("Disconnected");
                break;
            }
            Packet::Event(event) => {
                let event_in: Event<Ping> = Event::from_dyn(event)?;
                println!("{:?}", event_in.payload);

                let pong = Pong { data: event_in.payload.data };
                let event_out = Pong { data: pong.data };
                ns.emit(event_out).await?;
            }
            Packet::ConnectError(error) => {
                eprintln!("Error: {error}");
                break;
            }
        }
    }

    Ok(())
}
```

## Examples

See [`examples/`](../examples) for more details.

## License

Licensed under MIT OR Apache-2.0.

## Resources

- [The Socket.IO Protocol](https://socket.io/docs/v4/socket-io-protocol/)
