use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;

use crate::engine::state::EngineCore;
use crate::error::EngineError;
use crate::packet::{Handshake, Message, ServerMessage};
use crate::transport::{ClientFrame, ServerFrame};

const PING_WINDOW: Duration = Duration::from_secs(30);

fn handshake() -> Handshake {
    Handshake {
        sid: "sid".into(),
        upgrades: vec![],
        ping_interval: 25_000,
        ping_timeout: 5_000,
        max_payload: 1_000_000,
    }
}

fn open_frame() -> ServerFrame {
    ServerFrame::Open(handshake())
}

/// A core past its Open packet at `now`.
fn open_core(now: Instant) -> EngineCore {
    let mut core = EngineCore::new(None, now);
    assert_eq!(
        core.on_frame(open_frame(), now),
        Some(ServerMessage::Open(handshake()))
    );
    core
}

// Opening

#[test]
fn open_frame_delivers_open_and_arms_the_ping_window() {
    let now = Instant::now();
    let core = open_core(now);

    assert_eq!(core.deadline(), Some(now + PING_WINDOW));
    assert!(core.delivers());
    assert!(core.sends());
    assert!(!core.has_pong());
}

#[test]
fn opening_without_a_timeout_has_no_deadline() {
    let core = EngineCore::new(None, Instant::now());

    assert_eq!(core.deadline(), None);
}

#[test]
fn handshake_timeout_fails_the_opening_and_drops_the_transport() {
    let now = Instant::now();
    let timeout = Duration::from_secs(5);
    let mut core = EngineCore::new(Some(timeout), now);
    assert_eq!(core.deadline(), Some(now + timeout));

    core.on_timeout(now + timeout);

    assert_eq!(core.deadline(), None);
    assert!(!core.delivers());
    assert!(!core.sends());
    assert!(!core.keeps_transport());
    assert!(matches!(core.finish(), Err(EngineError::HandshakeTimeout)));
}

#[test]
fn messages_during_opening_are_encoded_for_the_transport_channel() {
    let mut core = EngineCore::new(None, Instant::now());

    assert_eq!(
        core.on_message(Message::Text("hi".into())),
        Some(ClientFrame::Message("hi".into()))
    );
}

#[test]
fn client_close_during_opening_cancels_it_and_drops_the_transport() {
    let mut core = EngineCore::new(Some(Duration::from_secs(5)), Instant::now());

    core.on_client_closed();

    assert_eq!(core.deadline(), None);
    assert!(!core.delivers());
    assert!(!core.sends());
    assert!(!core.keeps_transport());
    core.finish().unwrap();
}

#[test]
fn transport_end_during_opening_keeps_the_transport_result() {
    let mut core = EngineCore::new(None, Instant::now());

    core.on_transport_ended();

    assert!(core.keeps_transport());
    assert!(!core.sends());
    core.finish().unwrap();
}

// Open session

#[test]
fn message_and_binary_frames_become_server_messages() {
    let now = Instant::now();
    let mut core = open_core(now);

    assert_eq!(
        core.on_frame(ServerFrame::Message("hi".into()), now),
        Some(ServerMessage::Text("hi".into()))
    );
    assert_eq!(
        core.on_frame(ServerFrame::Binary(Bytes::from_static(b"\x01")), now),
        Some(ServerMessage::Binary(Bytes::from_static(b"\x01")))
    );
}

#[test]
fn client_messages_become_frames() {
    let mut core = open_core(Instant::now());

    assert_eq!(
        core.on_message(Message::Text("hi".into())),
        Some(ClientFrame::Message("hi".into()))
    );
    assert_eq!(
        core.on_message(Message::Binary(Bytes::from_static(b"\x01"))),
        Some(ClientFrame::Binary(Bytes::from_static(b"\x01")))
    );
}

#[test]
fn ping_queues_a_pong_and_resets_the_deadline() {
    let now = Instant::now();
    let mut core = open_core(now);
    let later = now + Duration::from_secs(10);

    assert_eq!(core.on_frame(ServerFrame::Ping("".into()), later), None);

    assert_eq!(core.deadline(), Some(later + PING_WINDOW));
    assert!(core.has_pong());
    assert_eq!(core.take_pong(), Some(ClientFrame::Pong("".into())));
    assert!(!core.has_pong());
    assert_eq!(core.take_pong(), None);
}

#[test]
fn a_repeated_ping_replaces_the_unsent_pong() {
    let now = Instant::now();
    let mut core = open_core(now);

    core.on_frame(ServerFrame::Ping("a".into()), now);
    core.on_frame(ServerFrame::Ping("b".into()), now);

    assert_eq!(core.take_pong(), Some(ClientFrame::Pong("b".into())));
    assert_eq!(core.take_pong(), None);
}

#[test]
fn noop_and_out_of_place_packets_are_ignored() {
    let now = Instant::now();
    let mut core = open_core(now);

    for frame in [
        ServerFrame::Noop,
        ServerFrame::Open(handshake()),
        ServerFrame::Pong("".into()),
        ServerFrame::Upgrade,
    ] {
        assert_eq!(core.on_frame(frame, now), None);
    }
    assert!(!core.has_pong());
    assert_eq!(core.deadline(), Some(now + PING_WINDOW));
    assert!(core.delivers());
}

#[test]
fn heartbeat_timeout_fails_the_session_and_drops_the_transport() {
    let now = Instant::now();
    let mut core = open_core(now);
    core.on_frame(ServerFrame::Ping("".into()), now);

    core.on_timeout(now + PING_WINDOW);

    assert!(!core.has_pong());
    assert!(!core.delivers());
    assert!(!core.sends());
    assert!(!core.keeps_transport());
    assert!(matches!(core.finish(), Err(EngineError::HeartbeatTimeout)));
}

#[test]
fn an_early_timeout_changes_nothing() {
    let now = Instant::now();
    let mut core = open_core(now);

    core.on_timeout(now + PING_WINDOW - Duration::from_millis(1));

    assert!(core.delivers());
    assert!(core.sends());
}

// Closing

#[test]
fn client_close_stops_sending_and_discards_server_frames() {
    let now = Instant::now();
    let mut core = open_core(now);
    core.on_frame(ServerFrame::Ping("".into()), now);

    core.on_client_closed();

    assert!(!core.has_pong());
    assert!(!core.sends());
    assert!(!core.delivers());
    assert!(core.keeps_transport());
    assert_eq!(
        core.on_frame(ServerFrame::Message("late".into()), now),
        None
    );
    assert_eq!(core.on_frame(ServerFrame::Ping("".into()), now), None);
    assert!(!core.has_pong());
    assert_eq!(core.on_message(Message::Text("late".into())), None);
}

#[test]
fn the_last_ping_window_bounds_a_local_close() {
    let now = Instant::now();
    let mut core = open_core(now);
    core.on_client_closed();
    assert_eq!(core.deadline(), Some(now + PING_WINDOW));

    core.on_timeout(now + PING_WINDOW);

    assert!(!core.keeps_transport());
    assert!(matches!(core.finish(), Err(EngineError::HeartbeatTimeout)));
}

#[test]
fn transport_end_after_a_local_close_ends_the_session() {
    let mut core = open_core(Instant::now());
    core.on_client_closed();

    core.on_transport_ended();

    assert_eq!(core.deadline(), None);
    assert!(!core.delivers());
    assert!(core.keeps_transport());
    core.finish().unwrap();
}

#[test]
fn transport_end_keeps_delivering_and_stops_sending() {
    let now = Instant::now();
    let mut core = open_core(now);
    core.on_frame(ServerFrame::Ping("".into()), now);

    core.on_transport_ended();

    assert_eq!(core.deadline(), None);
    assert!(!core.has_pong());
    assert!(core.delivers());
    assert!(!core.sends());
    assert!(core.keeps_transport());
    assert_eq!(core.on_message(Message::Text("late".into())), None);
    core.finish().unwrap();
}

#[test]
fn repeated_end_reports_are_accepted() {
    let mut core = open_core(Instant::now());

    core.on_transport_ended();
    core.on_transport_ended();
    core.on_client_closed();

    assert!(core.delivers());
    core.finish().unwrap();
}

#[test]
fn a_frame_before_open_is_out_of_place() {
    let now = Instant::now();
    let mut core = EngineCore::new(None, now);

    assert_eq!(
        core.on_frame(ServerFrame::Message("early".into()), now),
        None
    );

    assert_eq!(
        core.on_frame(open_frame(), now),
        Some(ServerMessage::Open(handshake()))
    );
}
