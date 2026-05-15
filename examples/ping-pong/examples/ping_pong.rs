use miette::{IntoDiagnostic, Result};
use sioc::prelude::*;
use std::path::PathBuf;
use tokio::process::{Child, Command};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use url::Url;

#[derive(Debug, EventType, DeserializePayload)]
struct Ping {
    data: i64,
}

#[derive(Debug, EventType, SerializePayload)]
struct Pong {
    data: i64,
}

fn get_manifest_dir() -> std::io::Result<PathBuf> {
    match option_env!("CARGO_MANIFEST_DIR") {
        Some(dir) => Ok(PathBuf::from(dir)),
        None => std::env::current_dir(),
    }
}

async fn run_server() -> std::io::Result<Child> {
    let current_dir = get_manifest_dir()?;

    let child = Command::new("uv")
        .args(["run", "server.py"])
        .current_dir(current_dir)
        .spawn()?;

    Ok(child)
}

async fn disconnect(tx: &SocketSender) -> Result<()> {
    tokio::signal::ctrl_c().await.into_diagnostic()?;
    tracing::info!("ctrl-c signal");
    tx.disconnect().await?;
    Ok(())
}

async fn ping_loop(tx: &SocketSender, rx: &mut SocketReceiver) -> Result<()> {
    while let Some(ping) = rx.listen::<Event<Ping>>().await? {
        tracing::debug!(?ping, "received ping");
        tx.emit(Pong {
            data: ping.payload.data,
        })
        .await?;
    }
    Ok(())
}

#[tracing::instrument(skip_all, err)]
async fn run_client() -> Result<()> {
    let url = Url::parse("http://localhost:3000").into_diagnostic()?;

    let client = ClientBuilder::new(url).open()?;

    let (tx, mut rx) = client.connect("/").await?;

    tokio::try_join!(ping_loop(&tx, &mut rx), disconnect(&tx))?;

    client.join().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut server = run_server().await.into_diagnostic()?;

    run_client().await?;

    server.kill().await.into_diagnostic()?;

    Ok(())
}
