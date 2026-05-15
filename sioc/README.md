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

This example requires a running Socket.IO server. See the `ping-pong` example for a self-contained demo.

```rust,no_run
use sioc::prelude::*;
use std::time::Duration;
use url::Url;

// Server to client event
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "greeting"))]
struct Greeting {
    name: String,
}

// Client to server event
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "reply"))]
struct Reply {
    text: String,
}

// Server to client event that requires a client ack (Sum)
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "add", ack = "Sum"))]
struct Add {
    a: i32,
    b: i32,
}

// Client to server ack in response to Add
#[derive(Debug, AckType, SerializePayload)]
struct Sum(i32);

// Client to server event that requires a server ack (RoomInfo)
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "join", ack = "RoomInfo"))]
struct Join {
    room: String,
}

// Server to client ack in response to Join
#[derive(Debug, AckType, DeserializePayload)]
struct RoomInfo {
    count: u32,
}

// Router for server to client events. Implements TryFrom<DynEvent> to dispatch on the event name.
#[derive(Debug, EventRouter)]
enum AppEvent {
    Greeting(Event<Greeting>),
    Add(Event<Add>),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = Url::parse("http://localhost:3000")?;

    let client: Client = ClientBuilder::new(url).open()?;

    // Each namespace gets its own SocketSender and SocketReceiver.
    let (tx, mut rx): (SocketSender, SocketReceiver) = client.connect("/").await?;

    // Emitting Join returns AckHandle<RoomInfo>.
    let ack: Ack<RoomInfo> = tx.emit(Join { room: "lobby".into() })
        .await?
        .timeout(Duration::from_secs(5))
        .await?;

    println!("joined lobby with {} members", ack.payload.count);

    // listen() filters out protocol signals (Connect, ConnectError, Disconnect) and returns
    // typed app events. Event<E> and EventRouter enums both implement TryFrom<DynEvent>.
    while let Some(event) = rx.listen::<AppEvent>().await? {
        match event {
            // Emit requires EventType + SerializePayload.
            AppEvent::Greeting(Event { payload: Greeting { name }, .. }) => {
                tx.emit(Reply { text: format!("hello, {name}!") }).await?;
            }

            // Acknowledge requires AckType + SerializePayload.
            // id: AckId<Sum> ensures only Sum is accepted.
            AppEvent::Add(Event { payload: Add { a, b }, id, .. }) => {
                tx.acknowledge(id, Sum(a + b)).await?;
            }
        }
    }

    // SocketSender warns when dropped while still connected.
    tx.disconnect().await?;

    // Awaits the background engine and socket tasks.
    client.join().await?;

    Ok(())
}
```

## License

Licensed under MIT OR Apache-2.0.
