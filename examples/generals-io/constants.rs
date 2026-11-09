use serde::Serialize;

const FORCE_FLAG: &str = "sd09fjdZ03i0ejwi_changeme";

/// Force flag type required by many requests.
#[derive(Clone, Copy)]
pub struct ForceFlag;

impl Serialize for ForceFlag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(FORCE_FLAG)
    }
}

/// Spectator player index sentinel.
pub const INDEX_SPECTATOR: i32 = -1;

/// Well-known chat room identifiers.
pub mod chat_rooms {
    pub const MAIN_MENU: &str = "main_menu";
    pub const FFA_QUEUE: &str = "ffa_queue";
    pub const TWO_VS_TWO_QUEUE: &str = "2v2_queue";
    pub const BIG_TEAM_QUEUE: &str = "big_team_queue";
    pub const PARTY_PREFIX: &str = "party_";
}

/// Well-known queue identifiers.
pub mod queues {
    pub const ONE_VS_ONE: &str = "1v1";
    pub const FFA: &str = "ffa";
    pub const TWO_VS_TWO: &str = "2v2";
    pub const BIG_TEAM: &str = "big_team";
}

/// Replay URL prefixes.
pub mod replay_urls {
    pub const NA: &str = "https://generalsio-replays-na.s3.amazonaws.com/";
    pub const BOT: &str = "https://generalsio-replays-bot.s3.amazonaws.com/";
}
