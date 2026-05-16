# sioc

Typed, ergonomic [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) client for Rust.

## What is Socket.IO?

The [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) is a real-time, bidirectional event-based communication protocol built on top of the [Engine.IO protocol v4](https://socket.io/docs/v4/engine-io-protocol/).

It adds features such as _namespaces_, which allow multiple channels over a single connection; _events_, which are named messages with structured data; _acknowledgements_, enabling request/response patterns over events; and support for _binary attachments_.

## Features

- Supports Socket.IO protocol v5 and Engine.IO protocol v4.
- Async networking built on [Tokio](https://tokio.rs), [reqwest](https://docs.rs/reqwest), and [tokio-tungstenite](https://docs.rs/tokio-tungstenite).
- No boxed futures and a lock-free actor model. Channel-based event loops avoid callback borrow issues and fit stateful applications naturally.
- Derive macros for events, acks, serialization, and routing.
- Zero-copy packet parsing via [`bytestring`](https://docs.rs/bytestring) and [`bytes`](https://docs.rs/bytes).

## Quick start

This example requires a running Socket.IO server. See the `quick-start` example for a self-contained demo.

```rust,no_run
use sioc::prelude::*;
use std::time::Duration;
use url::Url;

#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "greeting"))]
struct Greeting {
    message: String,
}

#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "reply"))]
struct Reply {
    text: String,
}

#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "poll", ack = "Vote"))]
struct Survey {
    question: String,
    options: Vec<String>,
}

#[derive(Debug, AckType, SerializePayload)]
struct Vote(usize);

#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "join", ack = "RoomInfo"))]
struct Join {
    room: String,
}

#[derive(Debug, AckType, DeserializePayload)]
struct RoomInfo {
    count: u32,
}

#[derive(Debug, EventRouter)]
enum AppEvent {
    Greeting(Event<Greeting>),
    Survey(Event<Survey>),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = Url::parse("http://localhost:3000")?;

    let client = ClientBuilder::new(url).open()?;
    let (tx, mut rx) = client.connect("/").await?;

    let Ack { payload: RoomInfo { count }, .. } = tx
        .emit(Join { room: "lobby".into() })
        .await?
        .timeout(Duration::from_secs(5))
        .await?;

    println!("joined lobby with {count} members");

    while let Some(event) = rx.listen::<AppEvent>().await? {
        match event {
            AppEvent::Greeting(Event { payload: Greeting { message }, .. }) => {
                println!("greeting: {message}");
                tx.emit(Reply { text: "glad to be here!".into() }).await?;
            }

            AppEvent::Survey(Event { payload: Survey { question, options }, id, .. }) => {
                println!("poll: {question} ({} options)", options.len());
                tx.acknowledge(id, Vote(0)).await?;
            }
        }
    }

    tx.disconnect().await?;

    client.join().await?;

    Ok(())
}
```

## License

Licensed under MIT OR Apache-2.0.
