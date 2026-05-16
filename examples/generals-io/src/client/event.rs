//! Client-to-server event definitions.

use crate::prelude::*;
use serde::{Deserialize, Serialize};
use sioc::prelude::*;

/// Game options sent when joining a queue.
#[derive(Debug, Serialize)]
pub struct GameOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub game_speed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub map: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fog_of_war: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spectate: Option<bool>,
}

/// Custom game options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomGameOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub map: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub game_speed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_players: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fog_of_war: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mountain_density: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city_density: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swamp_density: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookout_density: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observatory_density: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desert_density: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city_fairness: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spawn_fairness: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modifiers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tunnel_density: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tunnel_limit_min: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tunnel_limit_max: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stronghold_density: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stronghold_strength_min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stronghold_strength_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defeat_spectate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spectate_chat: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public: Option<bool>,
    #[serde(
        rename = "chatRecordingDisabled",
        skip_serializing_if = "Option::is_none"
    )]
    pub chat_recording_disabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modifier_options: Option<serde_json::Value>,
    #[serde(rename = "eventId", skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

/// Retrieves the current username associated with the user ID.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "GetUsernameAck"))]
pub struct GetUsername {
    pub user_id: String,
}

/// Sets a new username for the player.
#[derive(EventType, SerializePayload)]
pub struct SetUsername {
    pub user_id: String,
    pub username: String,
    pub force_flag: ForceFlag,
}

/// Checks whether the player is a supporter.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "IsSupporterAck"))]
pub struct IsSupporter {
    pub user_id: String,
}

/// Checks the moderation status for the user.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "CheckModerationAck"))]
pub struct CheckModeration {
    pub user_id: String,
}

/// Joins the FFA queue.
#[derive(EventType, SerializePayload)]
pub struct Play {
    pub user_id: String,
    pub force_flag: ForceFlag,
    pub options: Option<GameOptions>,
}

/// Joins the 1v1 queue.
#[derive(EventType, SerializePayload)]
#[sioc(event(name = "join_1v1"))]
pub struct Join1v1 {
    pub user_id: String,
    pub force_flag: ForceFlag,
    pub options: Option<GameOptions>,
    /// Reserved null slot in the wire format.
    pub reserved: (),
    /// Whether the player allows bots in their queue (`queue_1v1_allow_bots` feature flag).
    pub allow_bots: bool,
}

/// Joins the 2v2 team queue.
#[derive(EventType, SerializePayload)]
pub struct JoinTeam {
    pub partner_username: Option<String>,
    pub user_id: String,
    pub force_flag: ForceFlag,
    pub options: Option<GameOptions>,
}

/// Leaves the 2v2 team queue.
#[derive(EventType, SerializePayload)]
pub struct LeaveTeam {
    pub partner_username: Option<String>,
}

/// Joins the big-team queue.
#[derive(EventType, SerializePayload)]
pub struct PlayBigTeam {
    pub user_id: String,
    pub force_flag: ForceFlag,
    pub options: Option<GameOptions>,
}

/// Joins the big-team queue with a specific partner.
#[derive(EventType, SerializePayload)]
pub struct JoinBigTeam {
    pub partner_username: Option<String>,
    pub user_id: String,
    pub force_flag: ForceFlag,
    pub options: Option<GameOptions>,
}

/// Joins a private custom game.
#[derive(EventType, SerializePayload)]
pub struct JoinPrivate {
    pub queue_id: String,
    pub user_id: String,
    pub force_flag: ForceFlag,
}

/// Cancels the current queue search.
#[derive(EventType, SerializePayload)]
pub struct Cancel;

/// Toggles force-start for the current queue slot.
#[derive(EventType, SerializePayload)]
pub struct SetForceStart {
    pub queue_id: String,
    pub enabled: bool,
}

/// Requests the current player count for a queue.
#[derive(EventType, SerializePayload)]
pub struct QueueCount {
    pub queue_id: String,
}

/// Leaves the current game.
#[derive(EventType, SerializePayload)]
pub struct LeaveGame;

/// Queues an attack/move from one tile to an adjacent tile.
#[derive(EventType, SerializePayload)]
pub struct Attack {
    pub from_index: i32,
    pub to_index: i32,
    pub is_50_percent: bool,
    pub priority_half: Option<i32>,
}

/// Pings a tile to highlight it for teammates (client emit).
#[derive(EventType, SerializePayload)]
pub struct PingTile {
    pub tile_index: i32,
}

/// Surrenders the current game.
#[derive(EventType, SerializePayload)]
pub struct Surrender;

/// Undoes the last queued move.
#[derive(EventType, SerializePayload)]
pub struct UndoMove;

/// Clears all queued moves.
#[derive(EventType, SerializePayload)]
pub struct ClearMoves;

/// Sets the player's rematch preference.
#[derive(EventType, SerializePayload)]
pub struct Rematch {
    pub wants_rematch: bool,
}

/// Sends a chat message to a room.
#[derive(EventType, SerializePayload)]
#[sioc(event(name = "chat_message"))]
pub struct EmitChatMessage {
    pub chat_room: String,
    pub text: String,
    pub prefix: Option<String>,
}

/// Sets options for a custom game.
#[derive(EventType, SerializePayload)]
pub struct SetCustomOptions {
    pub game_id: String,
    pub options: CustomGameOptions,
}

/// Makes a custom game publicly visible.
#[derive(EventType, SerializePayload)]
pub struct MakeCustomPublic {
    pub game_id: String,
    pub options: CustomGameOptions,
}

/// Makes a custom game private.
#[derive(EventType, SerializePayload)]
pub struct MakeCustomPrivate {
    pub game_id: String,
    pub options: CustomGameOptions,
}

/// Updates whether chat is recorded for a custom game.
#[derive(EventType, SerializePayload)]
pub struct UpdateCustomChatRecording {
    pub game_id: String,
    pub enabled: bool,
}

/// Transfers host privileges in a custom game.
#[derive(EventType, SerializePayload)]
pub struct SetCustomHost {
    pub game_id: String,
    pub username: String,
}

/// Kicks a player from a custom game lobby.
#[derive(EventType, SerializePayload)]
pub struct KickFromCustom {
    pub game_id: String,
    pub username: String,
}

/// Reverses a kick from a custom game lobby.
#[derive(EventType, SerializePayload)]
pub struct UnkickFromCustom {
    pub game_id: String,
    pub username: String,
}

/// Sets the team number for the player in a custom game.
#[derive(EventType, SerializePayload)]
pub struct SetCustomTeam {
    pub game_id: String,
    pub team: i32,
}

/// Sets the player color in a custom game.
#[derive(EventType, SerializePayload)]
pub struct SetColor {
    pub game_id: String,
    pub player_index: i32,
    pub color: i32,
}

/// Joins an existing party or creates a new one.
#[derive(EventType, SerializePayload)]
pub struct JoinParty {
    pub party_id: Option<String>,
    pub user_id: String,
}

/// Leaves the current party.
#[derive(EventType, SerializePayload)]
pub struct LeaveParty;

/// Invites another player to the current party.
#[derive(EventType, SerializePayload)]
pub struct InviteToParty {
    pub party_id: String,
    pub username: String,
}

/// Requests an invite to another player's party.
#[derive(EventType, SerializePayload)]
pub struct RequestPartyInvite {
    pub party_id: String,
    pub username: String,
}

/// Responds to a party invite request from another player.
#[derive(EventType, SerializePayload)]
pub struct RespondPartyInviteRequest {
    pub party_id: String,
    pub accepted: bool,
    pub username: String,
}

/// Loads friends lists and pending friend requests.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "LoadSocialDataAck"))]
pub struct LoadSocialData {
    pub user_id: String,
}

/// Sends a friend request to another player.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "SendFriendRequestAck"))]
pub struct SendFriendRequest {
    pub user_id: String,
    pub username: String,
}

/// Accepts an incoming friend request.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "AcceptFriendRequestAck"))]
pub struct AcceptFriendRequest {
    pub username: String,
}

/// Declines an incoming friend request.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "GenericSuccessAck"))]
pub struct DeclineFriendRequest {
    pub username: String,
}

/// Cancels an outgoing friend request.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "GenericSuccessAck"))]
pub struct CancelFriendRequest {
    pub username: String,
}

/// Removes a friend from the friends list.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "GenericSuccessAck"))]
pub struct RemoveFriend {
    pub username: String,
}

/// Requests the list of suggested/previous 2v2 teammates.
#[derive(EventType, SerializePayload)]
#[sioc(event(name = "get_2v2_teammates"))]
pub struct Get2v2Teammates {
    pub user_id: String,
    pub all_time: bool,
}

/// Fetches the leaderboard for a given mode.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "LeaderboardAck"))]
pub struct Leaderboard {
    pub leaderboard_id: String,
}

/// Fetches all pending notifications for the user.
#[derive(EventType, SerializePayload)]
#[sioc(event(ack = "GetNotifsAck"))]
pub struct GetNotifs {
    pub user_id: String,
}

/// Clears a single notification by ID.
#[derive(EventType, SerializePayload)]
pub struct ClearNotif {
    pub user_id: String,
    pub notif_id: String,
}

/// Requests season data for a given season identifier.
#[derive(EventType, SerializePayload)]
pub struct GetSeason {
    pub season_id: serde_json::Value,
}

/// Requests a stars/rank refresh; server responds with `stars` and `rank` events.
#[derive(EventType, SerializePayload)]
pub struct StarsAndRank {
    pub user_id: String,
}

/// Links an email address to the user account for recovery.
#[derive(EventType, SerializePayload)]
pub struct LinkEmail {
    pub user_id: String,
    pub email: String,
    pub token: String,
}

/// Starts account recovery using a previously linked email.
#[derive(EventType, SerializePayload)]
pub struct RecoverAccount {
    pub email: String,
    pub token: String,
}

/// Pings the server for latency measurement.
#[derive(EventType, SerializePayload)]
pub struct PingServer;

/// Pings the game worker for round-trip latency measurement.
#[derive(EventType, SerializePayload)]
pub struct PingWorker;

/// Joins the main menu chat room.
#[derive(EventType, SerializePayload)]
pub struct JoinMainMenuChat {
    pub user_id: String,
}

/// Leaves the main menu chat room.
#[derive(EventType, SerializePayload)]
pub struct LeaveMainMenuChat;

/// Subscribes to `public_customs_update` events.
#[derive(EventType, SerializePayload)]
pub struct ListenPublicCustoms;

/// Unsubscribes from `public_customs_update` events.
#[derive(EventType, SerializePayload)]
pub struct StopListenPublicCustoms;

/// Sets the player's stay-party preference after game over.
#[derive(EventType, SerializePayload)]
pub struct StayParty {
    pub wants_stay: bool,
}

/// Reports the game-over state to the server.
#[derive(EventType, SerializePayload)]
pub struct GameOverState;

/// Reports main-menu activity to keep the session alive.
#[derive(EventType, SerializePayload)]
pub struct MainMenuActivity;
