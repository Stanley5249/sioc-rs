//! Demonstrates a Socket.IO client exchanging typed events, acknowledgements,
//! and binary attachments with a Python server. `sioc` derive macros enforce
//! event schemas at compile time: [`EventType`]/[`AckType`] define the event
//! contract, [`SerializePayload`]/[`DeserializePayload`] handle wire encoding,
//! and [`EventRouter`] dispatches incoming events by name.

use bytes::Bytes;
use miette::{IntoDiagnostic, Result};
use sioc::prelude::*;
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use url::Url;

/// Carries a welcome message from the server to the client.
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "greeting"))]
pub struct Greeting {
    pub message: String,
}

/// Carries a text reply from the client to the server.
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "reply"))]
pub struct Reply {
    pub text: String,
}

/// Carries a poll question from the server, expecting a [`Vote`] ack.
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "poll", ack = "Vote"))]
pub struct Survey {
    pub question: String,
    pub options: Vec<String>,
}

/// Ack for the [`Survey`] event.
#[derive(Debug, AckType, SerializePayload)]
pub struct Vote(pub usize);

/// Carries a room join request from the client, expecting a [`RoomInfo`] ack.
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "join", ack = "RoomInfo"))]
pub struct Join {
    pub room: String,
}

/// Ack for the [`Join`] event.
#[derive(Debug, AckType, DeserializePayload)]
pub struct RoomInfo {
    pub count: u32,
}

/// Carries upload data from the client to the server.
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "upload", binary))]
pub struct Upload {
    pub name: String,
    pub header: Placeholder,
    pub body: Placeholder,
}

/// Carries a download chunk from the server to the client.
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "chunk", binary))]
pub struct Chunk {
    pub name: String,
    pub data: Placeholder,
}

/// Routes incoming server events by name.
#[derive(Debug, EventRouter)]
pub enum MyEvent {
    Greeting(Event<Greeting>),
    Survey(Event<Survey>),
    Chunk(Event<Chunk>),
}

async fn run() -> Result<()> {
    let url = Url::parse("http://localhost:3000").into_diagnostic()?;

    let client = ClientBuilder::new(url).open()?;
    let (tx, mut rx) = client.connect("/").await?;

    let room = "lobby".to_string();

    let Ack {
        payload: RoomInfo { count },
        ..
    } = tx
        .emit(Join { room: room.clone() })
        .await?
        .timeout(Duration::from_secs(5))
        .await?;

    println!("joined {room} with {count} members");

    let header = Bytes::from_static(b"PNG\r\n\x1a\n");
    let body = Bytes::from(vec![0u8; 256]);
    tx.emit(|a: &mut AttachmentsBuilder| Upload {
        name: "photo.png".into(),
        header: a.attach(header), // slot 0
        body: a.attach(body),     // slot 1
    })
    .await?;

    while let Some(event) = rx.listen::<MyEvent>().await? {
        match event {
            MyEvent::Greeting(Event {
                payload: Greeting { message },
                ..
            }) => {
                println!("greeting: {message}");

                tx.emit(Reply {
                    text: "glad to be here!".into(),
                })
                .await?;
            }
            MyEvent::Survey(Event {
                payload: Survey { question, options },
                id,
                ..
            }) => {
                let i = options
                    .iter()
                    .position(|s| s.eq_ignore_ascii_case("Rust"))
                    .unwrap_or_default();

                println!("{question}\n{options:?}");

                tx.acknowledge(id, Vote(i)).await?;
            }
            MyEvent::Chunk(Event {
                payload: Chunk { name, data },
                attachments,
                ..
            }) => {
                let bytes = &attachments[data.slot()];
                println!("chunk {name}: {} bytes", bytes.len());
            }
        }
    }

    tx.disconnect().await;

    client.join().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    run().await
}
