# sioc-rs

[![crates.io](https://img.shields.io/crates/v/sioc.svg)](https://crates.io/crates/sioc)
[![docs.rs](https://docs.rs/sioc/badge.svg)](https://docs.rs/sioc)

A type-safe, async [Socket.IO](https://socket.io) client for Rust.

## Quick Start

### Open and Connect

Create a client and connect to a namespace. The default namespace is `"/"`. `tx` is cheap to clone and share across tasks.

```rust
use sioc::prelude::*;
use url::Url;

let url = Url::parse("http://localhost:3000")?;
let client = ClientBuilder::new(url).open()?;
let (tx, mut rx) = client.connect("/").await?;
```

### Emit

Derive `EventType` and `SerializePayload` on your type. `EventType` provides the event name for routing and defaults to the struct name in snake case. `SerializePayload` serializes the fields as a JSON array to match the Socket.IO wire format: `42["reply","Hello World!"]`.

```rust
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "reply"))]
struct Reply {
    text: String,
}

tx.emit(Reply { text: "Hello World!".into() }).await?;
```

### Listen

Derive `EventType` and `DeserializePayload` on each event type, then collect them into an enum that derives `EventRouter`. Call `rx.listen()` in a loop to receive and dispatch incoming events. The `Event<E>` wrapper carries the payload alongside ack ID and binary attachments if present.

```rust
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "greeting"))]
struct Greeting {
    message: String,
}

#[derive(Debug, EventRouter)]
enum MyEvent {
    Greeting(Event<Greeting>),
}

while let Some(event) = rx.listen::<MyEvent>().await? {
    match event {
        MyEvent::Greeting(Event { payload: Greeting { message }, .. }) => {
            println!("greeting: {message}");
        }
    }
}
```

### Acks

Socket.IO acks let the two sides confirm receipt of an event. sioc models this through the type system so the compiler catches mismatches at build time.

**Client requests an ack.** Associate an ack type with an event using `ack = "TypeName"`. When you emit that event, `emit` returns `Ack<A>` instead of `()`, and you can await the response with an optional timeout.

```rust
use std::time::Duration;

#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "join", ack = "RoomInfo"))]
struct Join {
    room: String,
}

#[derive(Debug, AckType, DeserializePayload)]
struct RoomInfo {
    count: u32,
}

let Ack { payload: RoomInfo { count }, .. } = tx
    .emit(Join { room: "lobby-1".into() })
    .await?
    .timeout(Duration::from_secs(5))
    .await?;

println!("joined lobby-1 with {count} members");
```

**Server requests an ack.** When the server sends an event that expects a reply, the `Event<E>` carries an `AckId<A>`. Pass it to `tx.acknowledge` with a value of the expected type.

```rust
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "poll", ack = "Vote"))]
struct Survey {
    question: String,
    options: Vec<String>,
}

#[derive(Debug, AckType, SerializePayload)]
struct Vote(usize);

#[derive(Debug, EventRouter)]
enum MyEvent {
    Survey(Event<Survey>),
}

while let Some(event) = rx.listen::<MyEvent>().await? {
    match event {
        MyEvent::Survey(Event { payload: Survey { question, options }, id, .. }) => {
            println!("{question}\n{options:?}");
            tx.acknowledge(id, Vote(0)).await?;
        }
    }
}
```

To summarize the derive traits: outgoing packets need `SerializePayload` and incoming packets need `DeserializePayload`. Their fields must implement `Serialize` and `Deserialize` from serde respectively.

## Design

sioc is built as an actor-model client. Tasks communicate through channels and no lock mechanism is used internally. It is built on top of async from the ground up and uses zero-copy parsing throughout.

The key insight is that proc macros and Rust's type system can do the work that most Socket.IO clients push onto runtime callbacks. Event names, payload shapes, and ack associations are all resolved at compile time.

## Comparison

[rust-socketio](https://github.com/1c3t3a/rust-socketio) is the first Socket.IO client for Rust, but its callback-based model creates friction in async Rust:

1. Callbacks must be `Send + Sync + 'static`, forcing `Arc<Mutex<T>>` for any shared state.
2. Storing async callbacks requires boxing: `async move { ... }.boxed()` and `futures_util::FutureExt`.

sioc replaces callbacks with channels and enums. Event handling lives in match arms and state lives in the enclosing scope, with no boxing or shared-state boilerplate.

## Examples

[`quick-start`](examples/quick-start) is a minimal setup paired with a Python Socket.IO server.

[`generals-io`](examples/generals-io) is the client for [generals.io](https://generals.io), the online strategy game that motivated this crate.

## Status

Early development. Expect breaking changes. Benchmarks and test coverage are not yet comprehensive. This does not attempt to pass the JavaScript Socket.IO test suite.

## Origin

`sioc-rs` was written in the first year of learning Rust, finishing in May 2026. The goal was to build a generals.io bot, but existing clients made it frustrating enough to justify writing one from scratch. That turned out to be the fun part.

AI helped with the learning journey, docstrings, and unit tests, but the design is entirely the author's own.

## License

MIT OR Apache-2.0.
