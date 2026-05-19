//! Demonstrates a Socket.IO client exchanging typed events and acknowledgements
//! with a Python server. `sioc` derive macros enforce event schemas at compile
//! time: [`EventType`]/[`AckType`] define the event contract,
//! [`SerializePayload`]/[`DeserializePayload`] handle wire encoding, and
//! [`EventRouter`] dispatches incoming events by name.

use miette::{IntoDiagnostic, Result};
use sioc::prelude::*;
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use url::Url;

/// Server -> Client event
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "greeting"))]
pub struct Greeting {
    pub message: String,
}

/// Client -> Server event
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "reply"))]
pub struct Reply {
    pub text: String,
}

/// Server -> Client event with ack
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "poll", ack = "Vote"))]
pub struct Survey {
    pub question: String,
    pub options: Vec<String>,
}

/// Ack for [`Survey`] event from client
#[derive(Debug, AckType, SerializePayload)]
pub struct Vote(pub usize);

/// Client -> Server event with ack
#[derive(Debug, EventType, SerializePayload)]
#[sioc(event(name = "join", ack = "RoomInfo"))]
pub struct Join {
    pub room: String,
}

/// Ack for [`Join`] event from server
#[derive(Debug, AckType, DeserializePayload)]
pub struct RoomInfo {
    pub count: u32,
}

/// Routes incoming server events by name.
#[derive(Debug, EventRouter)]
pub enum MyEvent {
    Greeting(Event<Greeting>),
    Survey(Event<Survey>),
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
