//! Server acknowledgement types.

use serde::Deserialize;
use sioc::prelude::*;

/// Ack for `get_username`: returns the username or null.
#[derive(Debug, AckType, DeserializePayload)]
pub struct GetUsernameAck {
    pub username: Option<String>,
}

/// Ack for `is_supporter`: returns whether the player is a supporter.
#[derive(Debug, AckType, DeserializePayload)]
pub struct IsSupporterAck {
    pub is_supporter: bool,
}

/// Ack for `check_moderation`: `(is_muted, is_disabled, warning)`.
#[derive(Debug, AckType, DeserializePayload)]
pub struct CheckModerationAck {
    pub is_muted: bool,
    pub is_disabled: bool,
    pub warning: Option<String>,
}

/// Friend entry.
#[derive(Debug, Clone, Deserialize)]
pub struct Friend {
    pub username: String,
    pub online: bool,
    #[serde(default, rename = "inParty")]
    pub in_party: Option<bool>,
}

/// Friend request entry.
#[derive(Debug, Clone, Deserialize)]
pub struct FriendRequest {
    pub username: String,
}

/// Inner payload for `load_social_data` ack.
#[derive(Debug, Clone, Deserialize)]
pub struct LoadSocialDataAckInner {
    #[serde(default, rename = "onlineFriends")]
    pub online_friends: Vec<Friend>,
    #[serde(default, rename = "offlineFriends")]
    pub offline_friends: Vec<Friend>,
    #[serde(default, rename = "incomingRequests")]
    pub incoming_requests: Vec<FriendRequest>,
    #[serde(default, rename = "outgoingRequests")]
    pub outgoing_requests: Vec<FriendRequest>,
    #[serde(default)]
    pub error: Option<String>,
}

/// Ack for `load_social_data`.
#[derive(Debug, AckType, DeserializePayload)]
pub struct LoadSocialDataAck {
    pub data: LoadSocialDataAckInner,
}

/// Inner payload for `send_friend_request` ack.
#[derive(Debug, Clone, Deserialize)]
pub struct SendFriendRequestAckInner {
    #[serde(default, rename = "friendAdded")]
    pub friend_added: Option<Friend>,
    #[serde(default)]
    pub error: Option<String>,
}

/// Ack for `send_friend_request`.
#[derive(Debug, AckType, DeserializePayload)]
pub struct SendFriendRequestAck {
    pub data: SendFriendRequestAckInner,
}

/// Inner payload for `accept_friend_request` ack.
#[derive(Debug, Clone, Deserialize)]
pub struct AcceptFriendRequestAckInner {
    #[serde(default)]
    pub success: Option<bool>,
    #[serde(default)]
    pub friend: Option<Friend>,
    #[serde(default)]
    pub error: Option<String>,
}

/// Ack for `accept_friend_request`.
#[derive(Debug, AckType, DeserializePayload)]
pub struct AcceptFriendRequestAck {
    pub data: AcceptFriendRequestAckInner,
}

/// Inner payload for generic success/error acks.
#[derive(Debug, Clone, Deserialize)]
pub struct GenericSuccessAckInner {
    #[serde(default)]
    pub success: Option<bool>,
    #[serde(default)]
    pub error: Option<String>,
}

/// Generic success/error ack used by `decline_friend_request`, `cancel_friend_request`, and `remove_friend`.
#[derive(Debug, AckType, DeserializePayload)]
pub struct GenericSuccessAck {
    pub data: GenericSuccessAckInner,
}

/// Leaderboard entry.
#[derive(Debug, Clone, Deserialize)]
pub struct LeaderboardEntry {
    pub name: String,
    pub stars: i32,
    pub rank: i32,
}

/// Ack for `leaderboard`.
#[derive(Debug, AckType, DeserializePayload)]
pub struct LeaderboardAck {
    pub entries: Vec<LeaderboardEntry>,
}

/// Notification entry.
#[derive(Debug, Clone, Deserialize)]
pub struct Notification {
    pub id: String,
    pub message: String,
    pub notification_type: String,
    pub timestamp: i64,
}

/// Ack for `get_notifs`.
#[derive(Debug, AckType, DeserializePayload)]
pub struct GetNotifsAck {
    pub notifications: Vec<Notification>,
}
