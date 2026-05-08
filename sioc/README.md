# sioc

Typed, ergonomic [Socket.IO v4](https://socket.io/docs/v4/socket-io-protocol/) client for Rust.

## What is Socket.IO?

[Socket.IO](https://socket.io/docs/v4/socket-io-protocol/) is a real-time, bidirectional
event-based communication protocol built on top of
[Engine.IO](https://socket.io/docs/v4/engine-io-protocol/). It adds **namespaces**
(multiplexed channels on a single connection), **typed events** (named messages with structured
payloads), **acknowledgements** (request/response over events), and **binary attachments** to
the underlying transport.

## Crate overview

- **`sioc`** (this crate) - derive macros (`EventType`, `AckType`, `SerializePayload`,
  `DeserializePayload`, `EventRouter`), typed `Event` / `Ack` / `AckHandle`, marker traits
  - **`sioc-socket`** - protocol logic: routing, ack tracking, binary reassembly;
    `Signal` / `Directive` / `Packet`
    - **`sioc-engine`** - Engine.IO v4 transport: HTTP long-polling, WebSocket, heartbeat

## Quick start

```rust,no_run
use sioc::prelude::*;
use url::Url;

#[derive(Debug, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(name = "greeting"))]
struct Greeting { text: String }

#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "reply"))]
struct Reply { text: String }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = Url::parse("http://localhost:3000")?;
    let client = ClientBuilder::new(url).open()?;
    let (tx, mut rx) = client.connect("/").await?;

    // Wait for the server to confirm the connection.
    let Some(Signal::Connect(info)) = rx.recv().await else {
        panic!("connection rejected");
    };
    println!("connected with sid={}", info.sid);

    // Receive typed events and respond.
    while let Some(signal) = rx.listen::<Event<Greeting>>().await? {
        if let Signal::Event(event) = signal {
            println!("got: {}", event.payload.text);
            tx.emit(Reply { text: "hello back".into() }).await?;
        }
    }

    Ok(())
}
```

## Key concepts

| Concept | Type | Description |
|---------|------|-------------|
| **Event** | [`EventType`](event::EventType) | Named message: wire format `["name", arg0, …]`. Derive with `#[derive(EventType)]`. |
| **Ack** | [`AckType`](ack::AckType) | Response to an event: wire format `[arg0, …]`. Derive with `#[derive(AckType)]`. |
| **Namespace** | [`SocketSender`](client::SocketSender) / [`SocketReceiver`](client::SocketReceiver) | Multiplexed channel on a single connection (e.g. `"/"`, `"/chat"`). |
| **Binary** | [`HasBinary`](marker::HasBinary) / [`NoBinary`](marker::NoBinary) | Compile-time marker for packets with or without binary attachments. |
| **Ack policy** | [`HasAck`](marker::HasAck) / [`NoAck`](marker::NoAck) | Compile-time marker for packets that do or don't expect an acknowledgement. |

## Features

- Fully async, built on [Tokio](https://tokio.rs).
- Derive macros for zero-boilerplate event and ack definitions.
- Compile-time enforcement of binary attachment and acknowledgement policies.
- Zero-copy packet parsing via [`bytestring::ByteString`](https://docs.rs/bytestring) and [`bytes::Bytes`](https://docs.rs/bytes).

## Examples

See [`examples/`](../examples) for runnable examples including a ping-pong server and a
generals.io bot.

## License

Licensed under MIT OR Apache-2.0.
