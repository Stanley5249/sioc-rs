//! Demonstrates a Socket.IO chat client exchanging typed events,
//! acknowledgements, and binary attachments with a server and its bot.
//! `sioc` derive macros enforce event schemas at compile time:
//! [`EventType`]/[`AckType`] define the event contract,
//! [`SerializePayload`]/[`DeserializePayload`] handle wire encoding, and
//! [`EventRouter`] dispatches incoming events by name.

use bytes::Bytes;
use miette::{IntoDiagnostic, Result};
use sioc::prelude::*;
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use url::Url;

/// Asks the server to join a chat room, expecting a [`RoomInfo`] ack.
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "join", ack = "RoomInfo"))]
pub struct Join {
    pub room: String,
    pub name: String,
}

/// Ack for the [`Join`] event, counting the bot as a member.
#[derive(Debug, AckType, DeserializePayload)]
pub struct RoomInfo {
    pub members: u32,
}

/// Sends a chat message to the room.
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "message"))]
pub struct Say {
    pub text: String,
}

/// Carries a chat message from another room member.
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "message"))]
pub struct Message {
    pub from: String,
    pub text: String,
}

/// Carries a room announcement from the server.
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "notice"))]
pub struct Notice {
    pub text: String,
}

/// Carries a yes-or-no question from the server, expecting an [`Answer`] ack.
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "confirm", ack = "Answer"))]
pub struct Confirm {
    pub question: String,
}

/// Ack for the [`Confirm`] event.
#[derive(Debug, AckType, SerializePayload)]
pub struct Answer(pub bool);

/// Carries an image as a binary attachment, both to and from the room.
#[derive(Debug, EventType, SerializePayload, DeserializePayload)]
#[sioc(event(name = "image", binary))]
pub struct Image {
    pub name: String,
    pub data: Placeholder,
}

/// Routes incoming server events by name.
#[derive(Debug, EventRouter)]
pub enum ChatEvent {
    Message(Event<Message>),
    Notice(Event<Notice>),
    Confirm(Event<Confirm>),
    Image(Event<Image>),
}

async fn run() -> Result<()> {
    let url = Url::parse("http://localhost:3000").into_diagnostic()?;

    let client = ClientBuilder::new(url).open()?;
    let (tx, mut rx) = client.connect("/").await?;

    let room = "rust".to_string();

    let Ack {
        payload: RoomInfo { members },
        ..
    } = tx
        .emit(Join {
            room: room.clone(),
            name: "ferris".into(),
        })
        .await?
        .timeout(Duration::from_secs(5))
        .await?;

    println!("client <- ack       join: {members} members");

    let png = Bytes::from_static(b"\x89PNG\r\n\x1a\n");
    tx.emit(|a: &mut AttachmentsBuilder| Image {
        name: "crab.png".into(),
        data: a.attach(png), // slot 0
    })
    .await?;

    tx.emit(Say {
        text: "hello from Rust!".into(),
    })
    .await?;

    while let Some(event) = rx.listen::<ChatEvent>().await? {
        match event {
            ChatEvent::Message(Event {
                payload: Message { from, text },
                ..
            }) => {
                println!("client <- message   {from}: {text}");
            }
            ChatEvent::Notice(Event {
                payload: Notice { text },
                ..
            }) => {
                println!("client <- notice    {text}");
            }
            ChatEvent::Confirm(Event {
                payload: Confirm { question },
                id,
                ..
            }) => {
                println!("client <- confirm   {question}");

                tx.acknowledge(id, Answer(true)).await?;
            }
            ChatEvent::Image(Event {
                payload: Image { name, data },
                attachments,
                ..
            }) => {
                let bytes = &attachments[data.slot()];
                println!("client <- image     {name} ({} bytes)", bytes.len());
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
