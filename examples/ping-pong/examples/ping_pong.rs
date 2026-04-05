use miette::IntoDiagnostic;
use sioc::prelude::*;
use std::path::PathBuf;
use tokio::process::Command;
use tokio::signal;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use url::Url;

#[derive(Debug, Clone, EventType, SerializePayload, DeserializePayload)]
struct Ping {
    data: i64,
}

#[derive(Debug, Clone, EventType, SerializePayload, DeserializePayload)]
struct Pong {
    data: i64,
}

fn get_manifest_dir() -> std::io::Result<PathBuf> {
    match option_env!("CARGO_MANIFEST_DIR") {
        Some(dir) => Ok(PathBuf::from(dir)),
        None => std::env::current_dir(),
    }
}

async fn run_server() -> std::io::Result<tokio::process::Child> {
    let current_dir = get_manifest_dir()?;
    let child = Command::new("uv")
        .args(["run", "server.py"])
        .current_dir(current_dir)
        .spawn()?;
    Ok(child)
}

async fn run_client() -> miette::Result<()> {
    let url = Url::parse("http://localhost:3000").into_diagnostic()?;

    let client = ClientBuilder::new(url).open()?;

    let (tx, mut rx) = client.connect("/").await?;

    loop {
        tokio::select! {
            Some(packet) = rx.recv() => {
                if !handle_packet(&tx, packet).await? {
                    break;
                }
            }
            _ = signal::ctrl_c() => {
                println!("Received shutdown signal");
                tx.disconnect().await?;
                break;
            }
        }
    }

    client.join().await?;

    Ok(())
}

async fn handle_packet(sender: &SocketSender, packet: Packet) -> miette::Result<bool> {
    match packet.cast::<Event<Ping>>()? {
        Packet::Connect(connection) => {
            println!("{connection:?}");
            Ok(true)
        }
        Packet::Disconnect => {
            println!("Disconnected");
            Ok(false)
        }
        Packet::ConnectError(error) => {
            eprintln!("Connection error: {error}");
            Err(error).into_diagnostic()
        }
        Packet::Event(ping) => {
            println!("{:?}", ping.payload);

            let pong = Pong {
                data: ping.payload.data,
            };

            sender.emit(pong).await?;

            Ok(true)
        }
    }
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut handle = run_server().await.into_diagnostic()?;

    run_client().await?;

    handle.kill().await.into_diagnostic()?;

    Ok(())
}
