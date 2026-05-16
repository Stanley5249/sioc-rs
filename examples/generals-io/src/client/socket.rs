use crate::prelude::*;
use sioc::error::Result;
use sioc::prelude::*;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct GeneralsIoSender {
    tx: SocketSender,
    ack_timeout: Duration,
}

impl GeneralsIoSender {
    pub fn new(tx: SocketSender) -> Self {
        Self {
            tx,
            ack_timeout: Duration::from_secs(5),
        }
    }

    pub fn with_ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    // Account

    pub async fn get_username(&self, user_id: String) -> Result<Option<String>> {
        let ack = self
            .tx
            .emit(GetUsername { user_id })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.username)
    }

    pub async fn set_username(&self, user_id: String, username: String) -> Result<()> {
        self.tx
            .emit(SetUsername {
                user_id,
                username,
                force_flag: ForceFlag,
            })
            .await?;
        Ok(())
    }

    pub async fn is_supporter(&self, user_id: String) -> Result<bool> {
        let ack = self
            .tx
            .emit(IsSupporter { user_id })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.is_supporter)
    }

    pub async fn check_moderation(&self, user_id: String) -> Result<CheckModerationAck> {
        let ack = self
            .tx
            .emit(CheckModeration { user_id })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload)
    }

    pub async fn link_email(&self, user_id: String, email: String, token: String) -> Result<()> {
        self.tx
            .emit(LinkEmail {
                user_id,
                email,
                token,
            })
            .await?;
        Ok(())
    }

    pub async fn recover_account(&self, email: String, token: String) -> Result<()> {
        self.tx.emit(RecoverAccount { email, token }).await?;
        Ok(())
    }

    // Queue

    pub async fn play(&self, user_id: String, options: Option<GameOptions>) -> Result<()> {
        self.tx
            .emit(Play {
                user_id,
                force_flag: ForceFlag,
                options,
            })
            .await?;
        Ok(())
    }

    pub async fn join_1v1(
        &self,
        user_id: String,
        options: Option<GameOptions>,
        allow_bots: bool,
    ) -> Result<()> {
        self.tx
            .emit(Join1v1 {
                user_id,
                force_flag: ForceFlag,
                options,
                reserved: (),
                allow_bots,
            })
            .await?;
        Ok(())
    }

    pub async fn join_team(
        &self,
        partner_username: Option<String>,
        user_id: String,
        options: Option<GameOptions>,
    ) -> Result<()> {
        self.tx
            .emit(JoinTeam {
                partner_username,
                user_id,
                force_flag: ForceFlag,
                options,
            })
            .await?;
        Ok(())
    }

    pub async fn leave_team(&self, partner_username: Option<String>) -> Result<()> {
        self.tx.emit(LeaveTeam { partner_username }).await?;
        Ok(())
    }

    pub async fn play_big_team(&self, user_id: String, options: Option<GameOptions>) -> Result<()> {
        self.tx
            .emit(PlayBigTeam {
                user_id,
                force_flag: ForceFlag,
                options,
            })
            .await?;
        Ok(())
    }

    pub async fn join_big_team(
        &self,
        partner_username: Option<String>,
        user_id: String,
        options: Option<GameOptions>,
    ) -> Result<()> {
        self.tx
            .emit(JoinBigTeam {
                partner_username,
                user_id,
                force_flag: ForceFlag,
                options,
            })
            .await?;
        Ok(())
    }

    pub async fn join_private(&self, queue_id: String, user_id: String) -> Result<()> {
        self.tx
            .emit(JoinPrivate {
                queue_id,
                user_id,
                force_flag: ForceFlag,
            })
            .await?;
        Ok(())
    }

    pub async fn cancel(&self) -> Result<()> {
        self.tx.emit(Cancel).await?;
        Ok(())
    }

    pub async fn set_force_start(&self, queue_id: String) -> Result<()> {
        self.tx
            .emit(SetForceStart {
                queue_id,
                enabled: true,
            })
            .await?;
        Ok(())
    }

    pub async fn queue_count(&self, queue_id: String) -> Result<()> {
        self.tx.emit(QueueCount { queue_id }).await?;
        Ok(())
    }

    pub async fn leave_game(&self) -> Result<()> {
        self.tx.emit(LeaveGame).await?;
        Ok(())
    }

    // Game actions

    pub async fn attack(
        &self,
        from_index: i32,
        to_index: i32,
        is_50_percent: bool,
        priority_half: Option<i32>,
    ) -> Result<()> {
        self.tx
            .emit(Attack {
                from_index,
                to_index,
                is_50_percent,
                priority_half,
            })
            .await?;
        Ok(())
    }

    pub async fn ping_tile(&self, tile_index: i32) -> Result<()> {
        self.tx.emit(PingTile { tile_index }).await?;
        Ok(())
    }

    pub async fn surrender(&self) -> Result<()> {
        self.tx.emit(Surrender).await?;
        Ok(())
    }

    pub async fn undo_move(&self) -> Result<()> {
        self.tx.emit(UndoMove).await?;
        Ok(())
    }

    pub async fn clear_moves(&self) -> Result<()> {
        self.tx.emit(ClearMoves).await?;
        Ok(())
    }

    pub async fn rematch(&self, wants_rematch: bool) -> Result<()> {
        self.tx.emit(Rematch { wants_rematch }).await?;
        Ok(())
    }

    pub async fn stay_party(&self, wants_stay: bool) -> Result<()> {
        self.tx.emit(StayParty { wants_stay }).await?;
        Ok(())
    }

    pub async fn game_over_state(&self) -> Result<()> {
        self.tx.emit(GameOverState).await?;
        Ok(())
    }

    // Chat

    pub async fn chat_message(
        &self,
        chat_room: String,
        text: String,
        prefix: Option<String>,
    ) -> Result<()> {
        self.tx
            .emit(EmitChatMessage {
                chat_room,
                text,
                prefix,
            })
            .await?;
        Ok(())
    }

    pub async fn join_main_menu_chat(&self, user_id: String) -> Result<()> {
        self.tx.emit(JoinMainMenuChat { user_id }).await?;
        Ok(())
    }

    pub async fn leave_main_menu_chat(&self) -> Result<()> {
        self.tx.emit(LeaveMainMenuChat).await?;
        Ok(())
    }

    // Custom games

    pub async fn set_custom_options(
        &self,
        game_id: String,
        options: CustomGameOptions,
    ) -> Result<()> {
        self.tx.emit(SetCustomOptions { game_id, options }).await?;
        Ok(())
    }

    pub async fn make_custom_public(
        &self,
        game_id: String,
        options: CustomGameOptions,
    ) -> Result<()> {
        self.tx.emit(MakeCustomPublic { game_id, options }).await?;
        Ok(())
    }

    pub async fn make_custom_private(
        &self,
        game_id: String,
        options: CustomGameOptions,
    ) -> Result<()> {
        self.tx.emit(MakeCustomPrivate { game_id, options }).await?;
        Ok(())
    }

    pub async fn update_custom_chat_recording(&self, game_id: String, enabled: bool) -> Result<()> {
        self.tx
            .emit(UpdateCustomChatRecording { game_id, enabled })
            .await?;
        Ok(())
    }

    pub async fn set_custom_host(&self, game_id: String, username: String) -> Result<()> {
        self.tx.emit(SetCustomHost { game_id, username }).await?;
        Ok(())
    }

    pub async fn kick_from_custom(&self, game_id: String, username: String) -> Result<()> {
        self.tx.emit(KickFromCustom { game_id, username }).await?;
        Ok(())
    }

    pub async fn unkick_from_custom(&self, game_id: String, username: String) -> Result<()> {
        self.tx.emit(UnkickFromCustom { game_id, username }).await?;
        Ok(())
    }

    pub async fn set_custom_team(&self, game_id: String, team: i32) -> Result<()> {
        self.tx.emit(SetCustomTeam { game_id, team }).await?;
        Ok(())
    }

    pub async fn set_color(&self, game_id: String, player_index: i32, color: i32) -> Result<()> {
        self.tx
            .emit(SetColor {
                game_id,
                player_index,
                color,
            })
            .await?;
        Ok(())
    }

    pub async fn listen_public_customs(&self) -> Result<()> {
        self.tx.emit(ListenPublicCustoms).await?;
        Ok(())
    }

    pub async fn stop_listen_public_customs(&self) -> Result<()> {
        self.tx.emit(StopListenPublicCustoms).await?;
        Ok(())
    }

    // Party

    pub async fn join_party(&self, party_id: Option<String>, user_id: String) -> Result<()> {
        self.tx.emit(JoinParty { party_id, user_id }).await?;
        Ok(())
    }

    pub async fn leave_party(&self) -> Result<()> {
        self.tx.emit(LeaveParty).await?;
        Ok(())
    }

    pub async fn invite_to_party(&self, party_id: String, username: String) -> Result<()> {
        self.tx.emit(InviteToParty { party_id, username }).await?;
        Ok(())
    }

    pub async fn request_party_invite(&self, party_id: String, username: String) -> Result<()> {
        self.tx
            .emit(RequestPartyInvite { party_id, username })
            .await?;
        Ok(())
    }

    pub async fn respond_party_invite_request(
        &self,
        party_id: String,
        accepted: bool,
        username: String,
    ) -> Result<()> {
        self.tx
            .emit(RespondPartyInviteRequest {
                party_id,
                accepted,
                username,
            })
            .await?;
        Ok(())
    }

    // Friends

    pub async fn load_social_data(&self, user_id: String) -> Result<LoadSocialDataAckInner> {
        let ack = self
            .tx
            .emit(LoadSocialData { user_id })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.data)
    }

    pub async fn send_friend_request(
        &self,
        user_id: String,
        username: String,
    ) -> Result<SendFriendRequestAckInner> {
        let ack = self
            .tx
            .emit(SendFriendRequest { user_id, username })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.data)
    }

    pub async fn accept_friend_request(
        &self,
        username: String,
    ) -> Result<AcceptFriendRequestAckInner> {
        let ack = self
            .tx
            .emit(AcceptFriendRequest { username })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.data)
    }

    pub async fn decline_friend_request(&self, username: String) -> Result<GenericSuccessAckInner> {
        let ack = self
            .tx
            .emit(DeclineFriendRequest { username })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.data)
    }

    pub async fn cancel_friend_request(&self, username: String) -> Result<GenericSuccessAckInner> {
        let ack = self
            .tx
            .emit(CancelFriendRequest { username })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.data)
    }

    pub async fn remove_friend(&self, username: String) -> Result<GenericSuccessAckInner> {
        let ack = self
            .tx
            .emit(RemoveFriend { username })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.data)
    }

    // Stats

    pub async fn get_2v2_teammates(&self, user_id: String, all_time: bool) -> Result<()> {
        self.tx.emit(Get2v2Teammates { user_id, all_time }).await?;
        Ok(())
    }

    pub async fn leaderboard(&self, leaderboard_id: String) -> Result<Vec<LeaderboardEntry>> {
        let ack = self
            .tx
            .emit(Leaderboard { leaderboard_id })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.entries)
    }

    pub async fn get_notifs(&self, user_id: String) -> Result<Vec<Notification>> {
        let ack = self
            .tx
            .emit(GetNotifs { user_id })
            .await?
            .timeout(self.ack_timeout)
            .await?;
        Ok(ack.payload.notifications)
    }

    pub async fn clear_notif(&self, user_id: String, notif_id: String) -> Result<()> {
        self.tx.emit(ClearNotif { user_id, notif_id }).await?;
        Ok(())
    }

    pub async fn get_season(&self, season_id: serde_json::Value) -> Result<()> {
        self.tx.emit(GetSeason { season_id }).await?;
        Ok(())
    }

    pub async fn stars_and_rank(&self, user_id: String) -> Result<()> {
        self.tx.emit(StarsAndRank { user_id }).await?;
        Ok(())
    }

    pub async fn main_menu_activity(&self) -> Result<()> {
        self.tx.emit(MainMenuActivity).await?;
        Ok(())
    }

    // Connectivity

    pub async fn ping_server(&self) -> Result<()> {
        self.tx.emit(PingServer).await?;
        Ok(())
    }

    pub async fn ping_worker(&self) -> Result<()> {
        self.tx.emit(PingWorker).await?;
        Ok(())
    }
}

impl std::ops::Deref for GeneralsIoSender {
    type Target = SocketSender;

    fn deref(&self) -> &SocketSender {
        &self.tx
    }
}
