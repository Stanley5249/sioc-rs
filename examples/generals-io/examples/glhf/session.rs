#![allow(dead_code)]

use crate::bot::Bot;
use generals_io::prelude::*;
use miette::{IntoDiagnostic, Result, WrapErr, bail};
use sioc::prelude::*;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::instrument;

#[derive(Debug, Clone)]
pub enum GameMode {
    /// Join a private custom game.
    Private(String),
    /// Join the public 1v1 matchmaking queue.
    OneVsOne,
}

impl GameMode {
    pub fn random() -> Self {
        let queue_id = std::iter::repeat_with(fastrand::alphanumeric)
            .take(4)
            .collect();

        GameMode::Private(queue_id)
    }
}

enum Phase {
    Queue {
        mode: GameMode,
    },
    Game {
        mode: GameMode,
        bot_tx: mpsc::Sender<GameUpdateData>,
        bot_handle: JoinHandle<Result<()>>,
    },
    GameOver {
        mode: GameMode,
    },
    Rematch {
        mode: GameMode,
    },
}

pub struct Session {
    tx: GeneralsIoSender,
    rx: SocketReceiver,
    user_id: String,
    username: Option<String>,
    mode: GameMode,
}

impl Session {
    pub fn new(tx: GeneralsIoSender, rx: SocketReceiver, user_id: String) -> Self {
        Self {
            tx,
            rx,
            user_id,
            username: None,
            mode: GameMode::random(),
        }
    }

    pub fn with_username(mut self, username: String) -> Self {
        self.username = Some(username);
        self
    }

    pub fn with_mode(mut self, mode: GameMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn run(self) -> impl Future<Output = Result<()>> {
        session(self.tx, self.rx, self.user_id, self.username, self.mode)
    }
}

async fn update_username(
    tx: &GeneralsIoSender,
    user_id: String,
    username: Option<String>,
) -> Result<String> {
    match (tx.get_username(user_id.clone()).await?, username) {
        (Some(old), _) => {
            tracing::info!(%old, "username already set");
            Ok(old)
        }
        (None, Some(new)) => {
            tracing::info!(%new, "set username");
            tx.set_username(user_id, new.clone()).await?;
            Ok(new)
        }
        (None, None) => bail!("username not set"),
    }
}

async fn join_private(tx: &GeneralsIoSender, user_id: String, queue_id: String) -> Result<()> {
    let url = format!("https://generals.io/games/{}", queue_id);

    tracing::info!(%url, "joining private");

    tx.join_private(queue_id.clone(), user_id).await?;

    tracing::info!("set force start");

    tx.set_force_start(queue_id).await?;

    Ok(())
}

async fn join_1v1(tx: &GeneralsIoSender, user_id: String) -> Result<()> {
    tracing::info!("joining 1v1");

    tx.join_1v1(user_id, None, true).await?;

    Ok(())
}

async fn join_game(tx: &GeneralsIoSender, user_id: String, mode: GameMode) -> Result<()> {
    match mode {
        GameMode::Private(queue_id) => join_private(tx, user_id, queue_id).await,
        GameMode::OneVsOne => join_1v1(tx, user_id).await,
    }
}

fn game_start(
    tx: &GeneralsIoSender,
    payload: GameStart,
) -> (mpsc::Sender<GameUpdateData>, JoinHandle<Result<()>>) {
    let (bot_tx, bot_rx) = mpsc::channel(8);

    let bot_handle = tokio::spawn(Bot::new(tx.clone(), bot_rx).run(payload.data.chat_room));

    (bot_tx, bot_handle)
}

fn handle_queue(mode: GameMode, event: GeneralsIoEvent, tx: &GeneralsIoSender) -> Phase {
    match event {
        GeneralsIoEvent::QueueUpdate(Event { payload, .. }) => {
            tracing::info!(?payload.data);
            Phase::Queue { mode }
        }
        GeneralsIoEvent::GameStart(Event { payload, .. }) => {
            let (bot_tx, bot_handle) = game_start(tx, payload);

            Phase::Game {
                mode,
                bot_tx,
                bot_handle,
            }
        }
        _ => Phase::Queue { mode },
    }
}

async fn handle_game(
    mode: GameMode,
    bot_tx: mpsc::Sender<GameUpdateData>,
    bot_handle: JoinHandle<Result<()>>,
    event: GeneralsIoEvent,
    tx: &GeneralsIoSender,
) -> Result<Phase> {
    match event {
        GeneralsIoEvent::GameUpdate(Event { payload, .. }) => {
            match bot_tx.send(payload.data).await {
                Ok(()) => Ok(Phase::Game {
                    mode,
                    bot_tx,
                    bot_handle,
                }),
                Err(_) => {
                    bot_handle
                        .await
                        .into_diagnostic()
                        .wrap_err("bot task failed")??;
                    Ok(Phase::GameOver { mode })
                }
            }
        }
        GeneralsIoEvent::GameOver(Event { payload, .. }) => {
            tracing::info!(?payload, "game over");
            tx.rematch(true).await?;
            Ok(Phase::Rematch { mode })
        }
        _ => Ok(Phase::Game {
            mode,
            bot_tx,
            bot_handle,
        }),
    }
}

async fn handle_game_over(
    mode: GameMode,
    event: GeneralsIoEvent,
    tx: &GeneralsIoSender,
) -> Result<Phase> {
    match event {
        GeneralsIoEvent::GameOver(Event { payload, .. }) => {
            tracing::info!(?payload, "game over");
            tx.rematch(true).await?;
            Ok(Phase::Rematch { mode })
        }
        _ => Ok(Phase::GameOver { mode }),
    }
}

async fn handle_rematch(
    mode: GameMode,
    event: GeneralsIoEvent,
    tx: &GeneralsIoSender,
    user_id: &str,
) -> Result<Phase> {
    match event {
        GeneralsIoEvent::GameStart(Event { payload, .. }) => {
            let (bot_tx, bot_handle) = game_start(tx, payload);

            Ok(Phase::Game {
                mode,
                bot_tx,
                bot_handle,
            })
        }
        GeneralsIoEvent::DisableRematch(_) => {
            join_game(tx, user_id.to_owned(), mode.clone()).await?;
            Ok(Phase::Queue { mode })
        }
        _ => Ok(Phase::Rematch { mode }),
    }
}

#[instrument(skip(tx, rx), err)]
async fn session(
    tx: GeneralsIoSender,
    mut rx: SocketReceiver,
    user_id: String,
    username: Option<String>,
    mode: GameMode,
) -> Result<()> {
    update_username(&tx, user_id.clone(), username).await?;

    join_game(&tx, user_id.clone(), mode.clone()).await?;

    let mut phase = Phase::Queue { mode };

    while let Some(event) = rx.listen::<GeneralsIoEvent>().await? {
        tracing::info!(event.name = event.name());

        phase = match phase {
            Phase::Queue { mode } => handle_queue(mode, event, &tx),
            Phase::Game {
                mode,
                bot_tx,
                bot_handle,
            } => handle_game(mode, bot_tx, bot_handle, event, &tx).await?,
            Phase::GameOver { mode } => handle_game_over(mode, event, &tx).await?,
            Phase::Rematch { mode } => handle_rematch(mode, event, &tx, &user_id).await?,
        };
    }

    Ok(())
}
