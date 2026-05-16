//! Server-to-client event definitions.

use crate::prelude::*;
use miette::Diagnostic;
use serde::Deserialize;
use sioc::prelude::*;
use thiserror::Error;

// Game state

/// Game start metadata. Mixed casing matches the server wire format.
#[derive(Debug, Deserialize)]
pub struct GameStartData {
    #[serde(rename = "playerIndex")]
    pub player_index: i32,
    pub replay_id: Option<String>,
    pub chat_room: String,
    pub team_chat_room: Option<String>,
    pub usernames: Vec<String>,
    #[serde(default)]
    pub teams: Option<Vec<i32>>,
    pub game_type: String,
    #[serde(default, rename = "playerColors")]
    pub player_colors: Option<Vec<i32>>,
    #[serde(default)]
    pub queue_chat: Option<Vec<ChatMessageData>>,
    #[serde(default)]
    pub swamps: Option<Vec<i32>>,
    #[serde(default)]
    pub deserts: Option<Vec<i32>>,
    #[serde(default)]
    pub lights: Option<Vec<i32>>,
    #[serde(default)]
    pub lookouts: Option<Vec<i32>>,
    #[serde(default)]
    pub observatories: Option<Vec<i32>>,
    pub options: CustomGameOptions,
}

/// Countdown before the game starts.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PreGameStart;

/// Game started with initial metadata.
#[derive(Debug, EventType, DeserializePayload)]
pub struct GameStart {
    pub data: Box<GameStartData>,
}

/// Per-player score entry in a game update.
#[derive(Debug, Deserialize)]
pub struct Score {
    pub total: i32,
    pub tiles: i32,
    pub i: i32,
    pub color: i32,
    pub has_kill: bool,
    #[serde(default)]
    pub dead: bool,
    #[serde(default)]
    pub neutralize_in: Option<i32>,
}

/// Incremental game state update payload.
#[derive(Debug, Deserialize)]
pub struct GameUpdateData {
    pub turn: i32,
    pub scores: Vec<Score>,
    #[serde(rename = "attackIndex")]
    pub attack_index: i32,
    pub generals: Vec<i32>,
    pub map_diff: Vec<i32>,
    pub cities_diff: Vec<i32>,
    #[serde(default)]
    pub deserts_diff: Option<Vec<i32>>,
    #[serde(default)]
    pub stars: Option<Vec<i32>>,
    #[serde(default)]
    pub uncertainties: Option<Vec<i32>>,
    #[serde(default)]
    pub alt_stars: Option<Vec<Option<serde_json::Value>>>,
    #[serde(default, rename = "rawVisible")]
    pub raw_visible: Option<Vec<i32>>,
}

/// Incremental game state update.
#[derive(Debug, EventType, DeserializePayload)]
pub struct GameUpdate {
    pub data: GameUpdateData,
}

/// Game has ended.
#[derive(Debug, EventType, DeserializePayload)]
pub struct GameOver;

/// Game lost payload.
#[derive(Debug, Deserialize)]
pub struct GameLostData {
    #[serde(default)]
    pub surrender: bool,
    pub killer: Option<i32>,
}

/// The local player was eliminated.
#[derive(Debug, EventType, DeserializePayload)]
pub struct GameLost {
    pub data: GameLostData,
}

/// The local player won the game.
#[derive(Debug, EventType, DeserializePayload)]
pub struct GameWon;

// Game mechanics

/// Rematch is no longer available.
#[derive(Debug, EventType, DeserializePayload)]
pub struct DisableRematch;

/// Rematch preference update from another player.
#[derive(Debug, EventType, DeserializePayload)]
pub struct RematchUpdate {
    pub num_rematch: i32,
    pub is_rematching: bool,
}

/// Player is about to be removed for inactivity.
#[derive(Debug, EventType, DeserializePayload)]
pub struct AfkWarning;

/// Warning popup with optional ping-blocking and auto-dismiss.
#[derive(Debug, EventType, DeserializePayload)]
pub struct WarnPopup {
    pub warn_message: String,
    pub block_ping_period_ms: i32,
    pub show_duration_ms: i32,
}

/// A teammate pinged a tile (server-to-client).
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "ping_tile"))]
pub struct PingTileOn {
    pub tile_index: i32,
    pub player_color: i32,
}

// Queue and matchmaking

/// Queue/lobby state update payload.
#[derive(Debug, Clone, Deserialize)]
pub struct QueueUpdateData {
    #[serde(default)]
    pub num_players: Option<i32>,
    #[serde(default)]
    pub num_spectators: Option<i32>,
    #[serde(default)]
    pub usernames: Option<Vec<String>>,
    #[serde(default)]
    pub teams: Option<Vec<i32>>,
    #[serde(default, rename = "playerColors")]
    pub player_colors: Option<Vec<i32>>,
    #[serde(default, rename = "playerIndices")]
    pub player_indices: Option<Vec<i32>>,
    #[serde(default, rename = "lobbyIndex")]
    pub lobby_index: Option<i32>,
    #[serde(default)]
    pub owner: Option<i32>,
    #[serde(default, rename = "numForce")]
    pub num_force: Option<Vec<i32>>,
    #[serde(default)]
    pub options: Option<CustomGameOptions>,
    #[serde(default, rename = "queueTimeLeft")]
    pub queue_time_left: Option<f64>,
    #[serde(default)]
    pub wait: Option<i64>,
    #[serde(default)]
    pub waiting: Option<bool>,
    #[serde(default, rename = "isForcing")]
    pub is_forcing: Option<bool>,
    #[serde(default, rename = "playerCap")]
    pub player_cap: Option<i32>,
    #[serde(default, rename = "teamSize")]
    pub team_size: Option<i32>,
    #[serde(default)]
    pub bans: Option<Vec<i32>>,
    #[serde(default)]
    pub kicks: Option<Vec<i32>>,
}

/// Queue/lobby state update.
#[derive(Debug, EventType, DeserializePayload)]
pub struct QueueUpdate {
    pub data: Box<QueueUpdateData>,
}

/// 2v2 team queue player-count update.
#[derive(Debug, EventType, DeserializePayload)]
pub struct TeamUpdate {
    pub num_players: i32,
}

/// Big-team queue player-count update.
#[derive(Debug, EventType, DeserializePayload)]
pub struct BigTeamUpdate {
    pub num_players: i32,
}

/// The 2v2 team successfully joined the matchmaking queue.
#[derive(Debug, EventType, DeserializePayload)]
pub struct TeamJoinedQueue;

/// A player was removed from the 2v2 team queue.
#[derive(Debug, EventType, DeserializePayload)]
pub struct RemovedFromQueue {
    pub username: String,
}

/// The client was removed from the queue.
#[derive(Debug, EventType, DeserializePayload)]
pub struct QueueLeft;

/// The party is in the queue but waiting for opponents.
#[derive(Debug, EventType, DeserializePayload)]
pub struct QueueWaiting;

/// The waiting period ended; matchmaking resumed.
#[derive(Debug, EventType, DeserializePayload)]
pub struct QueueWaitingOver;

/// The party entered a matchmaking queue.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PartyJoinedQueue {
    pub queue_id: String,
    pub is_waiting: bool,
}

// Chat

/// Chat message data received from the server.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatMessageData {
    #[serde(default)]
    pub username: Option<String>,
    pub text: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default, rename = "playerColor")]
    pub player_color: Option<i32>,
    #[serde(default, rename = "playerIndex")]
    pub player_index: Option<i32>,
    #[serde(default, rename = "isPrevGame")]
    pub is_prev_game: Option<bool>,
    #[serde(default)]
    pub turn: Option<i32>,
    #[serde(default, rename = "multiText")]
    pub multi_text: Option<Vec<String>>,
    #[serde(default, rename = "multiColors")]
    pub multi_colors: Option<Vec<serde_json::Value>>,
}

/// A chat message was received in a room.
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "chat_message"))]
pub struct RecvChatMessage {
    pub chat_room: String,
    pub data: ChatMessageData,
}

/// A previously sent chat message was redacted.
#[derive(Debug, EventType, DeserializePayload)]
pub struct ChatRedaction {
    pub chat_room: String,
    pub message_id: String,
}

/// Color change entry for `chat_color_change`.
#[derive(Debug, Clone, Deserialize)]
pub struct ColorChange {
    #[serde(rename = "oldColor")]
    pub old_color: i32,
    #[serde(rename = "newColor")]
    pub new_color: i32,
}

/// A player's color in chat was updated.
#[derive(Debug, EventType, DeserializePayload)]
pub struct ChatColorChange {
    pub chat_room: String,
    pub changes: Vec<ColorChange>,
}

/// Batch of historical chat messages for a queue room.
#[derive(Debug, EventType, DeserializePayload)]
pub struct QueueChatHistory {
    pub chat_room: String,
    pub messages: Vec<ChatMessageData>,
}

/// Batch of historical chat messages for a party room.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PartyChatHistory {
    pub party_id: String,
    pub messages: Vec<ChatMessageData>,
}

/// A user disconnected from the main menu chat.
#[derive(Debug, EventType, DeserializePayload)]
pub struct MainMenuChatUserLeft {
    pub chat_room: String,
    pub username: String,
}

// Connectivity

/// Response to `ping_server`.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PongServer;

/// Response to `ping_worker` with the server-to-worker latency.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PongWorker {
    pub worker_ping_ms: i32,
}

// Party

/// Party member entry.
#[derive(Debug, Clone, Deserialize)]
pub struct PartyMember {
    pub username: String,
    pub is_ready: bool,
}

/// Full party state.
#[derive(Debug, Clone, Deserialize)]
pub struct PartyData {
    pub party_id: String,
    pub members: Vec<PartyMember>,
    pub leader: String,
    pub in_queue: bool,
    #[serde(default)]
    pub queue_id: Option<String>,
}

/// The client successfully joined a party.
#[derive(Debug, EventType, DeserializePayload)]
pub struct JoinedParty {
    pub party_id: String,
    pub data: PartyData,
}

/// The client left or was removed from a party.
#[derive(Debug, EventType, DeserializePayload)]
pub struct LeftParty {
    pub party_id: String,
}

/// Current state of the party changed.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PartyUpdate {
    pub data: PartyData,
}

/// A specific member left the party.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PartyUserLeft {
    pub username: String,
}

/// Party invite payload.
#[derive(Debug, Clone, Deserialize)]
pub struct PartyInviteData {
    pub party_id: String,
    pub inviter_username: String,
}

/// The client received an invitation to join a party.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PartyInvite {
    pub data: PartyInviteData,
}

/// Party invite request payload.
#[derive(Debug, Clone, Deserialize)]
pub struct PartyInviteRequestData {
    pub requester_username: String,
    pub friend_username: String,
}

/// Another player is requesting an invite to the party.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PartyInviteRequest {
    pub data: PartyInviteRequestData,
}

// Friends

/// Username-only payload for friend events.
#[derive(Debug, Clone, Deserialize)]
pub struct UsernamePayload {
    pub username: String,
    #[serde(default, rename = "inParty")]
    pub in_party: Option<bool>,
}

/// A friend came online.
#[derive(Debug, EventType, DeserializePayload)]
pub struct FriendOnline {
    pub data: UsernamePayload,
}

/// A friend went offline.
#[derive(Debug, EventType, DeserializePayload)]
pub struct FriendOffline {
    pub data: UsernamePayload,
}

/// Friend added payload.
#[derive(Debug, Clone, Deserialize)]
pub struct FriendAddedData {
    pub username: String,
    pub online: bool,
    #[serde(default, rename = "inParty")]
    pub in_party: Option<bool>,
}

/// A new friend was added.
#[derive(Debug, EventType, DeserializePayload)]
pub struct FriendAdded {
    pub data: FriendAddedData,
}

/// A friend was removed from the list.
#[derive(Debug, EventType, DeserializePayload)]
pub struct FriendRemoved {
    pub data: UsernamePayload,
}

/// Received a new incoming friend request.
#[derive(Debug, EventType, DeserializePayload)]
pub struct FriendRequestReceived {
    pub data: UsernamePayload,
}

/// Friend presence payload.
#[derive(Debug, Clone, Deserialize)]
pub struct FriendPresenceData {
    pub username: String,
    pub presence: String,
    #[serde(default, rename = "inParty")]
    pub in_party: Option<bool>,
}

/// A friend's presence/status changed.
#[derive(Debug, EventType, DeserializePayload)]
pub struct FriendPresence {
    pub data: FriendPresenceData,
}

// Stats and leaderboards

/// Stars (rating) update per mode.
#[derive(Debug, EventType, DeserializePayload)]
pub struct Stars {
    pub data: serde_json::Value,
}

/// Rank update per mode.
#[derive(Debug, EventType, DeserializePayload)]
pub struct Rank {
    pub data: serde_json::Value,
}

/// Suggested or historical 2v2 teammates.
#[derive(Debug, EventType, DeserializePayload)]
#[sioc(event(name = "2v2_teammates"))]
pub struct Teammates2v2 {
    pub usernames: Vec<String>,
    pub is_all_time: bool,
}

// Public customs

/// Public custom game listing entry.
#[derive(Debug, Clone, Deserialize)]
pub struct PublicCustomGame {
    pub game_id: String,
    pub num_players: i32,
    pub max_players: i32,
    pub usernames: Vec<String>,
}

/// Updated list of publicly visible custom games.
#[derive(Debug, EventType, DeserializePayload)]
pub struct PublicCustomsUpdate {
    pub games: Vec<PublicCustomGame>,
}

// Server status and notifications

/// Generic in-app notification.
#[derive(Debug, EventType, DeserializePayload)]
pub struct Notify {
    pub title: String,
    pub body: String,
}

/// Server is about to restart.
#[derive(Debug, EventType, DeserializePayload)]
pub struct ServerRestart;

/// Server is going down for maintenance.
#[derive(Debug, EventType, DeserializePayload)]
pub struct ServerDown {
    pub message: String,
    pub reboot_timestamp_ms: Option<i64>,
}

// Errors

/// Emitted when `set_username` fails.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("set username failed")]
#[diagnostic(code(generals_io::error::set_username))]
pub struct ErrorSetUsername {
    #[help]
    pub message: Option<String>,
}

/// The user ID is already connected.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("user ID is already connected")]
#[diagnostic(code(generals_io::error::user_id))]
pub struct ErrorUserId;

/// The target game or queue is full.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("queue is full")]
#[diagnostic(code(generals_io::error::queue_full))]
pub struct ErrorQueueFull;

/// The player is temporarily banned.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("temporarily banned")]
#[diagnostic(code(generals_io::error::banned))]
pub struct ErrorBanned {
    #[help]
    pub message: Option<String>,
}

/// The player was kicked from a game or queue.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("kicked from game or queue")]
#[diagnostic(code(generals_io::error::kicked))]
pub struct ErrorKicked {
    #[help]
    pub message: Option<String>,
}

/// Generic server-side error; special values trigger specific client actions.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("server error")]
#[diagnostic(code(generals_io::error::gio_error))]
pub struct GioError {
    #[help]
    pub message: String,
}

/// Unable to join the requested queue.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("unable to join queue")]
#[diagnostic(code(generals_io::error::join_queue))]
pub struct ErrorJoinQueue {
    #[help]
    pub message: Option<String>,
}

/// Moderation warning from the server.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("moderation warning")]
#[diagnostic(code(generals_io::error::moderation_warning), severity(Warning))]
pub struct ModerationWarning {
    #[help]
    pub message: Option<String>,
}

/// Unable to join a party via invite link.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("party join error")]
#[diagnostic(code(generals_io::error::party_join))]
pub struct PartyJoinError {
    #[help]
    pub message: Option<String>,
}

/// Rematch/stay-party option is no longer available.
#[derive(Debug, Error, Diagnostic, EventType, DeserializePayload)]
#[error("stay-party option is no longer available")]
#[diagnostic(code(generals_io::error::disable_stay_party))]
pub struct DisableStayParty;

/// Stay-party preference update from players (semantics unconfirmed).
#[derive(Debug, EventType, DeserializePayload)]
pub struct StayPartyUpdate {
    #[sioc(flatten)]
    pub data: Vec<serde_json::Value>,
}
