use miette::IntoDiagnostic;
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

#[tracing::instrument(skip_all, err)]
async fn run_client() -> miette::Result<()> {
    let url = Url::parse("http://localhost:3000").into_diagnostic()?;

    let client = ClientBuilder::new(url).open()?;

    let (tx, mut rx) = client.connect("/").await?;

    loop {
        let signal = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("ctrl-c signal");
                break;
            }
            result = rx.listen::<Event<Ping>>() => {
                match result? {
                    Some(signal) => signal,
                    _ => break,
                }
            }
        };

        tracing::debug!(?signal, "received signal");

        match signal {
            Signal::ConnectError(error) => return Err(error.into()),

            Signal::Event(ping) => {
                let data = ping.payload.data;
                let pong = Pong { data };
                tx.emit(pong).await?;
            }
            _ => {}
        }
    }

    tx.disconnect().await?;

    client.join().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> miette::Result<()> {
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
