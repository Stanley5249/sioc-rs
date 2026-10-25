use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;

use crate::engine::state::EngineCore;
use crate::error::EngineError;
use crate::packet::{ClientMessage, Handshake, ServerMessage};
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
    assert!(!core.is_closed());
    assert!(!core.has_pong());
}

#[test]
fn opening_without_a_timeout_has_no_deadline() {
    let core = EngineCore::new(None, Instant::now());

    assert_eq!(core.deadline(), None);
}

#[test]
fn handshake_timeout_fails_and_closes_the_opening() {
    let now = Instant::now();
    let timeout = Duration::from_secs(5);
    let mut core = EngineCore::new(Some(timeout), now);
    assert_eq!(core.deadline(), Some(now + timeout));

    assert!(matches!(
        core.on_timeout(now + timeout),
        Err(EngineError::HandshakeTimeout)
    ));

    assert!(core.is_closed());
    assert_eq!(core.deadline(), None);
}

#[test]
fn messages_during_opening_are_encoded_for_the_transport_channel() {
    let mut core = EngineCore::new(None, Instant::now());

    assert_eq!(
        core.on_message(ClientMessage::Text("hi".into())),
        Some(ClientFrame::Message("hi".into()))
    );
}

#[test]
fn closing_an_opening_ends_its_deadline() {
    let mut core = EngineCore::new(Some(Duration::from_secs(5)), Instant::now());
    assert!(core.is_opening());

    core.close();

    assert!(!core.is_opening());
    assert!(core.is_closed());
    assert_eq!(core.deadline(), None);
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
        core.on_message(ClientMessage::Text("hi".into())),
        Some(ClientFrame::Message("hi".into()))
    );
    assert_eq!(
        core.on_message(ClientMessage::Binary(Bytes::from_static(b"\x01"))),
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
    assert!(!core.is_closed());
}

#[test]
fn heartbeat_timeout_fails_and_closes_the_session() {
    let now = Instant::now();
    let mut core = open_core(now);
    core.on_frame(ServerFrame::Ping("".into()), now);

    assert!(matches!(
        core.on_timeout(now + PING_WINDOW),
        Err(EngineError::HeartbeatTimeout)
    ));

    assert!(core.is_closed());
    assert!(!core.has_pong());
    assert_eq!(core.deadline(), None);
}

#[test]
fn an_early_timeout_changes_nothing() {
    let now = Instant::now();
    let mut core = open_core(now);

    core.on_timeout(now + PING_WINDOW - Duration::from_millis(1))
        .unwrap();

    assert!(!core.is_closed());
    assert_eq!(core.deadline(), Some(now + PING_WINDOW));
}

// Closed session

#[test]
fn a_closed_session_discards_frames_and_refuses_messages() {
    let now = Instant::now();
    let mut core = open_core(now);
    core.on_frame(ServerFrame::Ping("".into()), now);

    core.close();

    assert!(!core.has_pong());
    assert_eq!(
        core.on_frame(ServerFrame::Message("late".into()), now),
        None
    );
    assert_eq!(core.on_frame(ServerFrame::Ping("".into()), now), None);
    assert!(!core.has_pong());
    assert_eq!(core.on_message(ClientMessage::Text("late".into())), None);
}

#[test]
fn a_closed_session_never_times_out() {
    let now = Instant::now();
    let mut core = open_core(now);

    core.close();

    assert_eq!(core.deadline(), None);
    core.on_timeout(now + PING_WINDOW * 2).unwrap();
}

#[test]
fn closing_again_is_accepted() {
    let mut core = open_core(Instant::now());

    core.close();
    core.close();

    assert!(core.is_closed());
}
