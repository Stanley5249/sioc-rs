mod bot;
mod session;

use crate::session::Session;
use bytestring::ByteString;
use generals_io::prelude::*;
use miette::{IntoDiagnostic, Result, WrapErr};
use sioc::prelude::*;
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};
use url::Url;

const ENDPOINT: &str = "https://ws.generals.io";

async fn disconnect(tx: SocketSender) -> Result<()> {
    tokio::signal::ctrl_c().await.into_diagnostic()?;
    tx.disconnect().await;
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
