//! Typed, ergonomic [Socket.IO v4][spec] client for Rust.
//!
//! # What is Socket.IO?
//!
//! [Socket.IO][spec] is a real-time, bidirectional event-based communication
//! protocol built on top of [Engine.IO][eio-spec].  It adds **namespaces**
//! (multiplexed channels on a single connection), **typed events** (named
//! messages with structured payloads), **acknowledgements** (request/response
//! over events), and **binary attachments** to the underlying transport.
//!
//! # Crate overview
//!
//! This crate is the main entry point for users.  It re-exports the wire-level
//! types from `sioc-core` and layers typed, compile-time–checked events and
//! acknowledgements on top.
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │  sioc  (this crate)                          │
//! │  • derive macros: EventType, AckType,        │
//! │    SerializePayload, DeserializePayload, EventRouter │
//! │  • typed Event / Ack / AckHandle             │
//! │  • marker traits: BinaryMarker, AckMarker    │
//! ├──────────────────────────────────────────────┤
//! │  sioc-core — protocol logic                  │
//! │  • Manager: routing, ack tracking, binary    │
//! │    reassembly                                │
//! │  • Packet / Command / SioPacket              │
//! ├──────────────────────────────────────────────┤
//! │  sioc-engine — Engine.IO v4 transport        │
//! │  • HTTP long-polling & WebSocket             │
//! │  • heartbeat, ping/pong                      │
//! └──────────────────────────────────────────────┘
//! ```
//!
//! # Quick start
//!
//! ```rust,no_run
//! use sioc::prelude::*;
//!
//! #[derive(Debug, EventType, SerializePayload, DeserializePayload)]
//! #[sioc(event(name = "greeting"))]
//! struct Greeting { text: String }
//!
//! #[derive(Debug, EventType, SerializePayload, DeserializePayload)]
//! #[sioc(event(name = "reply"))]
//! struct Reply { text: String }
//!
//! # async fn run() -> sioc::error::Result<()> {
//! // 1. Connect to the server and open the default namespace.
//! let url: url::Url = "http://localhost:3000".parse().unwrap();
//! let client = ClientBuilder::new(url).open().await?;
//! let (ns, mut rx) = client.connect("/").await?;
//!
//! // 2. Wait for the server to confirm the connection.
//! let Some(Packet::Connect(info)) = rx.recv().await else {
//!     panic!("connection rejected");
//! };
//! println!("connected with sid={}", info.sid);
//!
//! // 3. Receive events and respond.
//! while let Some(packet) = rx.recv().await {
//!     if let Packet::Event(event) = packet.cast::<Event<Greeting>>()? {
//!         println!("got: {}", event.payload.text);
//!
//!         let reply = Reply { text: "hello back".into() };
//!         ns.emit(reply).await?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Key concepts
//!
//! | Concept | Type | Description |
//! |---------|------|-------------|
//! | **Event** | [`EventType`](event::EventType) | Named message: wire format `["name", arg0, …]`. Derive with `#[derive(EventType)]`. |
//! | **Ack** | [`AckType`](ack::AckType) | Response to an event: wire format `[arg0, …]`. Derive with `#[derive(AckType)]`. |
//! | **Namespace** | [`SocketSender`](client::SocketSender) / [`SocketReceiver`](client::SocketReceiver) | Multiplexed channel on a single connection (e.g. `"/"`, `"/chat"`). |
//! | **Binary** | [`HasBinary`](marker::HasBinary) / [`NoBinary`](marker::NoBinary) | Compile-time marker for packets with or without binary attachments. |
//! | **Ack policy** | [`HasAck`](marker::HasAck) / [`NoAck`](marker::NoAck) | Compile-time marker for packets that do or don't expect an acknowledgement. |
//!
//! # Features
//!
//! - Fully async, built on [Tokio](https://tokio.rs).
//! - Derive macros for zero-boilerplate event and ack definitions.
//! - Compile-time enforcement of binary attachment and acknowledgement policies.
//! - Zero-copy packet parsing via [`bytes::Bytes`].
//!
//! [spec]: https://socket.io/docs/v4/socket-io-protocol/
//! [eio-spec]: https://socket.io/docs/v4/engine-io-protocol/

pub mod ack;
pub mod binary;
pub mod client;
pub mod error;
pub mod event;
pub mod marker;
pub mod payload;

pub mod prelude {
    pub use crate::ack::{Ack, AckHandle, AckType};
    pub use crate::binary::{AttachmentBuilder, Placeholder};
    pub use crate::client::{
        Acknowledge, Client, ClientBuilder, Emit, SocketReceiver, SocketSender,
    };
    pub use crate::event::{Event, EventHandler, EventType};
    pub use crate::marker::{AckId, AckMarker, BinaryMarker, HasAck, HasBinary, NoAck, NoBinary};
    pub use crate::payload::{
        DeserializePayload, SerializePayload, deserialize_ack, deserialize_event, serialize_ack,
        serialize_event,
    };

    pub use sioc_core::packet::{Connect, ConnectError, DynAck, DynEvent, Ns, Packet};

    pub use sioc_engine::prelude::{
        WebSocketConnector, WebSocketError, WebSocketStream, default_connect,
    };

    pub use sioc_macros::{AckType, DeserializePayload, EventRouter, EventType, SerializePayload};
}
