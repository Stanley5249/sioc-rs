# sioc-rs

[![crates.io](https://img.shields.io/crates/v/sioc.svg)](https://crates.io/crates/sioc)
[![docs.rs](https://docs.rs/sioc/badge.svg)](https://docs.rs/sioc)
[![CI](https://github.com/Stanley5249/sioc-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Stanley5249/sioc-rs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Stanley5249/sioc-rs/graph/badge.svg)](https://codecov.io/gh/Stanley5249/sioc-rs)
[![License](https://img.shields.io/crates/l/sioc.svg)](https://crates.io/crates/sioc)
[![MSRV](https://img.shields.io/badge/rustc-1.88+-blue.svg)](https://blog.rust-lang.org/2025/06/05/Rust-1.88.0.html)

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

Derive `EventType` and `SerializePayload` on your type. `EventType` provides the event name for routing and defaults to the struct name in snake case. `SerializePayload` serializes the fields as a JSON array to match the Socket.IO wire format: `42["message","Hello World!"]`.

```rust
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "message"))]
struct Say {
    text: String,
}

tx.emit(Say { text: "Hello World!".into() }).await?;
```

### Listen

Derive `EventType` and `DeserializePayload` on each event type, then collect them into an enum that derives `EventRouter`. Call `rx.listen()` in a loop to receive and dispatch incoming events. The `Event<E>` wrapper carries the payload alongside ack ID and binary attachments if present.

```rust
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "message"))]
struct Message {
    from: String,
    text: String,
}

#[derive(Debug, EventRouter)]
enum ChatEvent {
    Message(Event<Message>),
}

while let Some(event) = rx.listen::<ChatEvent>().await? {
    match event {
        ChatEvent::Message(Event { payload: Message { from, text }, .. }) => {
            println!("{from}: {text}");
        }
    }
}
```

### Acks

Socket.IO acks let the two sides confirm receipt of an event. `sioc` models this through the type system so the compiler catches mismatches at build time.

**Client requests an ack.** Associate an ack type with an event using `ack = "TypeName"`. When you emit that event, `emit` returns `Ack<A>` instead of `()`, and you can await the response with an optional timeout.

```rust
use std::time::Duration;

#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "join", ack = "RoomInfo"))]
struct Join {
    room: String,
    name: String,
}

#[derive(Debug, AckType, DeserializePayload)]
struct RoomInfo {
    members: u32,
}

let Ack { payload: RoomInfo { members }, .. } = tx
    .emit(Join { room: "rust".into(), name: "ferris".into() })
    .await?
    .timeout(Duration::from_secs(5))
    .await?;

println!("joined rust with {members} members");
```

**Server requests an ack.** When the server sends an event that expects a reply, the `Event<E>` carries an `AckId<A>`. Pass it to `tx.acknowledge` with a value of the expected type.

```rust
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "confirm", ack = "Answer"))]
struct Confirm {
    question: String,
}

#[derive(Debug, AckType, SerializePayload)]
struct Answer(bool);

#[derive(Debug, EventRouter)]
enum ChatEvent {
    Confirm(Event<Confirm>),
}

while let Some(event) = rx.listen::<ChatEvent>().await? {
    match event {
        ChatEvent::Confirm(Event { payload: Confirm { question }, id, .. }) => {
            println!("? {question}");
            tx.acknowledge(id, Answer(true)).await?;
        }
    }
}
```

### Binary

JSON cannot represent binary data directly, so Socket.IO sends it as _binary attachments_, separate frames that accompany the JSON packet. The Socket.IO JS library finds and replaces binary objects automatically at runtime. `sioc` requires you to register binary data via an `AttachmentsBuilder` closure and embed the returned `Placeholder` in your struct. On the receiving side, use `data.get(&attachments)` for checked access. Binary packets carry between one and `MAX_ATTACHMENTS` attachments, matching the JavaScript parser's default limit.

One type can derive both `SerializePayload` and `DeserializePayload` when the event flows both ways.

```rust
use bytes::Bytes;

#[derive(Debug, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(name = "image", binary))]
struct Image {
    name: String,
    data: Placeholder,
}
```

**Send**

```rust
let png = Bytes::from_static(b"\x89PNG\r\n\x1a\n");
tx.emit(|a: &mut AttachmentsBuilder| Image {
    name: "crab.png".into(),
    data: a.attach(png), // slot 0
})
.await?;
```

**Receive**

```rust
#[derive(Debug, EventRouter)]
enum ChatEvent {
    Image(Event<Image>),
}

while let Some(event) = rx.listen::<ChatEvent>().await? {
    match event {
        ChatEvent::Image(Event {
            payload: Image { name, data },
            attachments, // Vec<Bytes>
            ..
        }) => {
            let bytes = data.get(&attachments)?;
            println!("image {name}: {} bytes", bytes.len());
        }
    }
}
```

### Reconnection

When the connection drops, the client reconnects with the same backoff as the JavaScript client and connects every open namespace again. Each reconnection shows up as another `ServerPacket::Connect` on the receiver. Events emitted meanwhile wait until the server confirms the namespace, and acks of events that already went out fail.

Pass a `ReconnectionConfig` to tune it, or `None` to close the namespaces instead. When reconnection gives up, the namespaces close too, and the client stays usable for the next `connect`. Configure each attempt's handshake deadline with `timeout`.

```rust
let client = ClientBuilder::new(url)
    .reconnection(Some(ReconnectionConfig {
        attempts: Some(5),
        ..ReconnectionConfig::default()
    }))
    .open()?;
```

### Namespaces share one connection

Every namespace of a client shares one connection, as in the JavaScript client, so a receiver that stops reading also holds up the other namespaces of that client. Open a separate `Client` for a namespace that must stay independent, like `multiplex: false` in the JavaScript client.

## Status

Early development. Expect breaking changes. End-to-end tests run against the reference JavaScript server, and benchmarks are not yet in place.

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

[`quick-start`](examples/quick-start) is a minimal chat client with JavaScript and Python Socket.IO server alternatives.

[`generals-io`](examples/generals-io) is the client for [generals.io](https://generals.io), the online strategy game that motivated this crate.

## Development

Run `just --list` to browse commands. Unsuffixed aggregate recipes cover Rust; `-all` variants add other languages or end-to-end tests. See [`justfile`](justfile) for recipe details.

### Required

For basic Rust development:

- [just](https://just.systems/man/en/packages.html) to run recipes.
- [rustup](https://rustup.rs/) for stable Rust and the components declared in [`rust-toolchain.toml`](rust-toolchain.toml).

Run `just install` to set up the pinned nightly formatter, then `just check` for Rust formatting and lint checks.

### Optional

Install tools for the workflows needed:

- **Tests:** [cargo-nextest](https://nexte.st/docs/installation/pre-built-binaries/) for `just test` and `just test-e2e`.
- **Dependency checks:** [cargo-deny](https://embarkstudios.github.io/cargo-deny/cli/index.html) for `just deny`.
- **JavaScript tooling and end-to-end tests:** [Bun](https://bun.sh/docs/installation).
- **Python example and tooling:** [uv](https://docs.astral.sh/uv/getting-started/installation/).
- **Coverage:** [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov#installation), cargo-nextest, and `rustup component add llvm-tools-preview`.
- **MSRV checks:** The Rust toolchain specified by `check-msrv` in `justfile`.

Run `just install-all` to also install JavaScript and Python dependencies. Install the Rust CLI tools separately. `just ci` runs the Rust merge gate with cargo-nextest; `just ci-all` also requires Bun and uv. Dependency policy checks remain separate through `just deny` and are enforced in GitHub Actions.

## Origin

`sioc` was written in the first year of learning Rust. It took 4 months to reach 0.1 and was published in May 2026. The goal was to build a generals.io bot, but existing clients made it frustrating enough to justify writing one from scratch. That turned out to be the fun part.

AI helped with the learning journey, docstrings, and unit tests, but the design is entirely the author's own.

## License

MIT OR Apache-2.0.
