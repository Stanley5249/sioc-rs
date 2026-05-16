use generals_io::prelude::*;
use miette::Result;
use tokio::sync::mpsc;
use tracing::instrument;

pub struct Bot {
    tx: GeneralsIoSender,
    rx: mpsc::Receiver<GameUpdateData>,
}

impl Bot {
    pub fn new(tx: GeneralsIoSender, rx: mpsc::Receiver<GameUpdateData>) -> Self {
        Self { tx, rx }
    }

    pub fn run(self, chat_room: String) -> impl Future<Output = Result<()>> {
        bot(self.tx, self.rx, chat_room)
    }
}

#[instrument(skip(tx, rx), err)]
pub async fn bot(
    tx: GeneralsIoSender,
    mut rx: mpsc::Receiver<GameUpdateData>,
    chat_room: String,
) -> Result<()> {
    tx.chat_message(
        chat_room,
        "glhf! just a test bot here, will surrender shortly, thanks for bearing with me".into(),
        None,
    )
    .await?;

    while let Some(update) = rx.recv().await {
        if update.turn >= 10 {
            tracing::info!(turn = update.turn, "surrendering");
            tx.surrender().await?;
            break;
        }
    }

    Ok(())
}
