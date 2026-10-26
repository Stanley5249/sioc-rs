//! When the core opens, closes, and reopens the engine, and how it shuts down.

use std::time::Duration;

use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig, event, event_ack};
use crate::error::{ManagerError, SocketError};
use crate::manager::backoff::Backoff;

#[test]
fn engine_loss_after_the_last_namespace_ended_schedules_no_retry() {
    let mut rig = Rig::reconnecting();
    rig.connected("/");
    rig.disconnect("/");
    rig.messages();

    // The engine ends with no retry timer and no new engine.
    assert_eq!(rig.effects(), [Effect::Close]);
    assert_eq!(rig.close(), []);
    assert_eq!(rig.reconnect(), []);
    assert!(!rig.core.is_finished());
    rig.drop_client();
    assert!(rig.core.is_finished());
}

#[test]
fn the_first_engine_opens_at_once_and_stays_without_a_namespace() {
    let mut rig = Rig::new();
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.effects(), []);
    assert!(!rig.core.is_finished());
}

#[test]
fn dropping_the_client_with_no_namespace_closes_the_first_engine() {
    let mut rig = Rig::new();
    assert_eq!(rig.drop_client(), [Effect::Open, Effect::Close]);
    assert!(!rig.core.is_finished());

    assert_eq!(rig.close(), []);
    assert!(rig.core.is_finished());
}

#[test]
fn dropping_the_client_keeps_the_engine_until_the_last_namespace_disconnects() {
    let mut rig = Rig::new();
    rig.connected("/");

    assert_eq!(rig.drop_client(), []);
    rig.send("/", event("[\"e\"]"));
    assert_eq!(rig.messages(), ["2[\"e\"]"]);

    assert_eq!(rig.disconnect("/"), []);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);
    assert_eq!(rig.close(), []);
    assert!(rig.core.is_finished());
}

#[test]
fn no_engine_opens_while_one_runs_or_closes() {
    let mut rig = Rig::reconnecting();
    rig.connect("/", "{}", 32);
    rig.connect("/b", "{}", 32);
    rig.handshake();
    assert_eq!(rig.effects(), [Effect::Open]);

    rig.server("garbage");
    assert_eq!(rig.effects(), [Effect::Close]);
    rig.connect("/c", "{}", 32);
    assert_eq!(rig.effects(), []);

    assert_eq!(rig.close(), [Effect::Schedule(Duration::ZERO)]);
    rig.connect("/d", "{}", 32);
    assert_eq!(rig.effects(), []);

    assert_eq!(rig.reconnect(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}", "0/b,{}", "0/c,{}", "0/d,{}"]);
}

#[test]
fn a_namespace_opened_while_the_client_closes_the_engine_opens_the_next_at_once() {
    let mut rig = Rig::new();
    rig.connected("/");
    assert_eq!(rig.disconnect("/"), []);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);

    rig.connect("/b", "{}", 32);
    assert_eq!(rig.effects(), []);

    assert_eq!(rig.close(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0/b,{}"]);
}

#[test]
fn a_namespace_connected_while_flushing_uses_the_same_engine_once() {
    let mut rig = Rig::new();
    rig.connected("/");
    assert_eq!(rig.disconnect("/"), []);
    // DISCONNECT has not been handed off, so this engine is still flushing.
    let new = rig.connect("/new", "{}", 32);
    new.result.unwrap();
    assert_eq!(rig.effects(), []);
    assert_eq!(rig.messages(), ["1", "0/new,{}"]);
    assert_eq!(rig.drop_client(), []);
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn the_last_namespace_ending_during_a_wait_cancels_it() {
    let mut rig = Rig::reconnecting();
    rig.connected("/");
    assert_eq!(rig.close(), [Effect::Schedule(Duration::ZERO)]);

    assert_eq!(rig.disconnect("/"), [Effect::Cancel]);
    rig.drop_client();
    assert!(rig.core.is_finished());

    // A timer that fired before the cancel arrived changes nothing.
    assert_eq!(rig.reconnect(), []);
}

#[test]
fn exhaustion_ends_namespaces_and_the_core_takes_new_ones() {
    let backoff = Backoff::new(Duration::ZERO, Duration::ZERO, 0.0, Some(1));
    let mut rig = Rig::with_backoff(Some(backoff), 1);
    let mut handles = rig.connected("/");

    let mut effects = rig.close();
    effects.extend(rig.reconnect());
    assert_eq!(effects, [Effect::Schedule(Duration::ZERO), Effect::Open]);

    assert_eq!(rig.close(), []);
    assert_eq!(
        handles.terminal_packet_rx.try_recv().unwrap_err(),
        TryRecvError::Closed
    );
    // The Connect packet stays readable, then the receiver ends.
    handles.server_packet_rx.try_recv().unwrap();
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Err(MpscTryRecvError::Disconnected)
    ));
    assert!(handles.closed.is_cancelled());
    assert!(handles.capacity.is_closed());

    // The name is free at once.
    let again = rig.connect("/", "{}", 32);
    again.result.unwrap();
    assert_eq!(rig.effects(), [Effect::Open]);
}

#[test]
fn without_reconnection_the_namespaces_end() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");
    assert_eq!(rig.close(), []);
    assert!(handles.closed.is_cancelled());
}

#[test]
fn delays_use_the_seeded_rng_and_restart_after_a_handshake() {
    let new_backoff = || Backoff::new(Duration::from_secs(1), Duration::from_secs(5), 0.5, None);

    let mut rng = fastrand::Rng::with_seed(42);
    let mut mirror = new_backoff();
    let first = mirror.next_delay(&mut rng).unwrap();
    let second = mirror.next_delay(&mut rng).unwrap();
    mirror.reset();
    let third = mirror.next_delay(&mut rng).unwrap();

    let mut rig = Rig::with_backoff(Some(new_backoff()), 42);
    rig.connected("/");

    let mut effects = rig.close();
    effects.extend(rig.reconnect());
    // No handshake: the attempt count keeps growing.
    effects.extend(rig.close());
    effects.extend(rig.reconnect());
    rig.handshake();
    effects.extend(rig.close());

    assert_eq!(
        effects,
        [
            Effect::Schedule(first),
            Effect::Open,
            Effect::Schedule(second),
            Effect::Open,
            Effect::Schedule(third),
        ]
    );
}

#[test]
fn an_internal_error_is_returned_and_shutdown_ends_every_handle() {
    let mut rig = Rig::reconnecting();
    let mut handles = rig.connected("/");
    let (sent_packet, mut sent) = event_ack("[\"e\"]");
    rig.send("/", sent_packet);
    rig.messages();
    let (queued_packet, mut queued) = event_ack("[\"q\"]");
    rig.send("/", queued_packet);

    let result = rig.core.close(Err(ManagerError::EngineEnded));
    assert!(matches!(result, Err(ManagerError::EngineEnded)));

    rig.core.shutdown();
    assert_eq!(sent.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(queued.try_recv().unwrap_err(), TryRecvError::Closed);
    // The Connect packet stays readable, then the receiver ends.
    handles.server_packet_rx.try_recv().unwrap();
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Err(MpscTryRecvError::Disconnected)
    ));
    assert!(handles.closed.is_cancelled());
    assert!(handles.capacity.is_closed());
    assert_eq!(rig.effects(), []);
    assert!(matches!(
        rig.try_send("/", event("[\"late\"]")),
        Err(SocketError::Closed)
    ));
    assert!(matches!(
        rig.connect("/", "{}", 32).result,
        Err(SocketError::Closed)
    ));

    // Nothing waits on the application, so the core is finished once the
    // client handle is gone.
    rig.drop_client();
    assert!(rig.core.is_finished());
}

#[test]
fn without_a_namespace_a_lost_engine_is_not_reopened() {
    let mut rig = Rig::reconnecting();
    assert_eq!(rig.effects(), [Effect::Open]);

    assert_eq!(rig.close(), []);
    assert!(!rig.core.is_finished());

    let _handles = rig.connect("/", "{}", 32);
    assert_eq!(rig.effects(), [Effect::Open]);
}
