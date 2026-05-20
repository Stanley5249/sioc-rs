# sioc-rs

[![crates.io](https://img.shields.io/crates/v/sioc.svg)](https://crates.io/crates/sioc)
[![docs.rs](https://docs.rs/sioc/badge.svg)](https://docs.rs/sioc)
[![CI](https://github.com/Stanley5249/sioc-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Stanley5249/sioc-rs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Stanley5249/sioc-rs/graph/badge.svg)](https://codecov.io/gh/Stanley5249/sioc-rs)
[![License](https://img.shields.io/crates/l/sioc.svg)](https://crates.io/crates/sioc)
[![MSRV](https://img.shields.io/badge/rustc-1.85+-blue.svg)](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html)

A type-safe, async [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/) client for Rust.

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

## Status

Early development. Expect breaking changes. Benchmarks and test coverage are not yet comprehensive. This does not attempt to pass the JavaScript Socket.IO test suite.

## Comparison

### Rust-socketio-client

[rust-socketio](https://github.com/1c3t3a/rust-socketio) is another Socket.IO client for Rust, but its callback-based model creates friction in async Rust:

1. Callbacks must be `Send + Sync + 'static`, forcing smart pointers and interior mutability for any shared state.
2. Storing async callbacks requires boxed futures, which adds heap allocation and dynamic dispatch overhead.

`sioc` replaces callbacks with channels and enums. Event handling lives in match arms and state lives in the enclosing scope, with no boxing or shared-state boilerplate.

### Socketioxide

[socketioxide](https://github.com/Totodore/socketioxide) is a Socket.IO server implementation that integrates with [Tower](https://github.com/tower-rs/tower) and [Tokio](https://github.com/tokio-rs/tokio) stack.

Server and client use fundamentally different architectures, so `socketioxide`'s design doesn't translate to a client. `sioc` uses `tokio` as well, with networking built on top of `reqwest` and `tokio-tungstenite`.

`socketioxide` uses callbacks for event handling, which is less of an issue for stateless servers. `sioc` offers stronger type safety for events and acks through its derive macros and type system.

## Examples

[`quick-start`](examples/quick-start) is a minimal setup paired with a Python Socket.IO server.

[`generals-io`](examples/generals-io) is the client for [generals.io](https://generals.io), the online strategy game that motivated this crate.

## Origin

`sioc` was written in the first year of learning Rust. It took 4 months to reach 0.1 and was published in May 2026. The goal was to build a generals.io bot, but existing clients made it frustrating enough to justify writing one from scratch. That turned out to be the fun part.

AI helped with the learning journey, docstrings, and unit tests, but the design is entirely the author's own.

## License

MIT OR Apache-2.0.
