//! Generals.io bot that plays through the `sioc` Socket.IO client.
//!
//! `client` and `server` type every client-to-server and server-to-client
//! event in the generals.io protocol with the `sioc` derive macros.

mod bot;
#[expect(
    dead_code,
    reason = "the schema covers the whole generals.io protocol, and the bot uses only some of it"
)]
mod client;
#[expect(
    dead_code,
    reason = "the schema covers the whole generals.io protocol, and the bot uses only some of it"
)]
mod constants;
#[expect(
    dead_code,
    reason = "the schema covers the whole generals.io protocol, and the bot uses only some of it"
)]
mod server;
mod session;

use bytestring::ByteString;
use miette::{IntoDiagnostic, Result, WrapErr};
use sioc::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use url::Url;

use crate::client::socket::GeneralsIoSender;
use crate::session::Session;

const ENDPOINT: &str = "https://ws.generals.io";

async fn disconnect(tx: SocketSender) -> Result<()> {
    tokio::signal::ctrl_c().await.into_diagnostic()?;
    tx.disconnect();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let url = Url::parse(ENDPOINT).into_diagnostic()?;

    let client = ClientBuilder::new(url)
        .transport(TransportStrategy::WebSocket)
        .open()?;

    let (socket_tx, rx) = client.connect(ByteString::from_static("/")).await?;

    let gio_tx = GeneralsIoSender::new(socket_tx.clone());

    let user_id = std::env::var("GENERALS_IO_USER_ID")
        .into_diagnostic()
        .wrap_err("GENERALS_IO_USER_ID not set")?;

    let session = Session::new(gio_tx, rx, user_id);

    tokio::try_join!(session.run(), disconnect(socket_tx))?;

    client.join().await?;

    Ok(())
}
