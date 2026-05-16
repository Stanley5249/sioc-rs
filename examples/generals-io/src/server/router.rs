//! Server-to-client event router.

use crate::prelude::*;
use miette::Result;
use sioc::prelude::*;

/// Dispatches a server-to-client event by name.
#[derive(Debug, EventRouter)]
pub enum GeneralsIoEvent {
    // Game state
    PreGameStart(Event<PreGameStart>),
    GameStart(Event<GameStart>),
    GameUpdate(Event<GameUpdate>),
    GameOver(Event<GameOver>),
    GameLost(Event<GameLost>),
    GameWon(Event<GameWon>),

    // Game mechanics
    DisableRematch(Event<DisableRematch>),
    RematchUpdate(Event<RematchUpdate>),
    AfkWarning(Event<AfkWarning>),
    WarnPopup(Event<WarnPopup>),
    PingTile(Event<PingTileOn>),

    // Queue and matchmaking
    QueueUpdate(Event<QueueUpdate>),
    TeamUpdate(Event<TeamUpdate>),
    BigTeamUpdate(Event<BigTeamUpdate>),
    TeamJoinedQueue(Event<TeamJoinedQueue>),
    RemovedFromQueue(Event<RemovedFromQueue>),
    QueueLeft(Event<QueueLeft>),
    QueueWaiting(Event<QueueWaiting>),
    QueueWaitingOver(Event<QueueWaitingOver>),
    PartyJoinedQueue(Event<PartyJoinedQueue>),

    // Chat
    RecvChatMessage(Event<RecvChatMessage>),
    ChatRedaction(Event<ChatRedaction>),
    ChatColorChange(Event<ChatColorChange>),
    QueueChatHistory(Event<QueueChatHistory>),
    PartyChatHistory(Event<PartyChatHistory>),
    MainMenuChatUserLeft(Event<MainMenuChatUserLeft>),

    // Connectivity
    PongServer(Event<PongServer>),
    PongWorker(Event<PongWorker>),

    // Party
    JoinedParty(Event<JoinedParty>),
    LeftParty(Event<LeftParty>),
    PartyUpdate(Event<PartyUpdate>),
    PartyUserLeft(Event<PartyUserLeft>),
    PartyInvite(Event<PartyInvite>),
    PartyInviteRequest(Event<PartyInviteRequest>),
    StayPartyUpdate(Event<StayPartyUpdate>),

    // Friends
    FriendOnline(Event<FriendOnline>),
    FriendOffline(Event<FriendOffline>),
    FriendAdded(Event<FriendAdded>),
    FriendRemoved(Event<FriendRemoved>),
    FriendRequestReceived(Event<FriendRequestReceived>),
    FriendPresence(Event<FriendPresence>),

    // Stats and leaderboards
    Stars(Event<Stars>),
    Rank(Event<Rank>),
    Teammates2v2(Event<Teammates2v2>),

    // Public customs
    PublicCustomsUpdate(Event<PublicCustomsUpdate>),

    // Server status and notifications
    Notify(Event<Notify>),
    ServerRestart(Event<ServerRestart>),
    ServerDown(Event<ServerDown>),

    // Errors
    ErrorSetUsername(Event<ErrorSetUsername>),
    ErrorUserId(Event<ErrorUserId>),
    ErrorQueueFull(Event<ErrorQueueFull>),
    ErrorBanned(Event<ErrorBanned>),
    ErrorKicked(Event<ErrorKicked>),
    GioError(Event<GioError>),
    ErrorJoinQueue(Event<ErrorJoinQueue>),
    ModerationWarning(Event<ModerationWarning>),
    PartyJoinError(Event<PartyJoinError>),
    DisableStayParty(Event<DisableStayParty>),
}

impl GeneralsIoEvent {
    /// Converts error events into a [`miette::Report`]; passes non-error events through as `Ok`.
    pub fn into_result(self) -> Result<Self> {
        match self {
            GeneralsIoEvent::ErrorSetUsername(e) => Err(e.payload.into()),
            GeneralsIoEvent::ErrorUserId(e) => Err(e.payload.into()),
            GeneralsIoEvent::ErrorQueueFull(e) => Err(e.payload.into()),
            GeneralsIoEvent::ErrorBanned(e) => Err(e.payload.into()),
            GeneralsIoEvent::ErrorKicked(e) => Err(e.payload.into()),
            GeneralsIoEvent::GioError(e) => Err(e.payload.into()),
            GeneralsIoEvent::ErrorJoinQueue(e) => Err(e.payload.into()),
            GeneralsIoEvent::ModerationWarning(e) => Err(e.payload.into()),
            GeneralsIoEvent::PartyJoinError(e) => Err(e.payload.into()),
            GeneralsIoEvent::DisableStayParty(e) => Err(e.payload.into()),
            other => Ok(other),
        }
    }
}
