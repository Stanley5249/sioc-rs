//! When the core opens, closes, and reopens the engine, and how it shuts down.

use std::time::Duration;

use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig, disconnect, event_ack, listen};
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::namespace::Read;
use crate::manager::state::ManagerInput;

#[test]
fn engine_loss_after_the_last_namespace_ended_schedules_no_retry() {
    let mut rig = Rig::reconnecting();
    let handles = rig.connected("/");
    handles.closed.cancel();
    rig.step(disconnect("/"));
    rig.messages();

    // The engine ends with no retry timer and no new engine.
    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), [Effect::Close]);
    assert_eq!(rig.step(ManagerInput::Reconnect), []);
    assert!(rig.core.is_idle());
}

#[test]
fn the_first_engine_opens_at_once_and_stays_without_a_namespace() {
    let mut rig = Rig::new();
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.effects(), []);
    assert!(!rig.core.is_idle());
}

#[test]
fn release_with_no_namespace_closes_the_first_engine() {
    let mut rig = Rig::new();
    assert_eq!(
        rig.step(ManagerInput::Release),
        [Effect::Open, Effect::Close]
    );
    assert!(!rig.core.is_idle());

    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
    assert!(rig.core.is_idle());
}

#[test]
fn release_keeps_the_engine_until_the_last_namespace_disconnects() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");

    assert_eq!(rig.step(ManagerInput::Release), []);
    assert_eq!(rig.messages(), Vec::<String>::new());
    assert_eq!(rig.core.read("/", true), Read::Send);

    handles.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), []);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);
    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
    assert!(rig.core.is_idle());
}

#[test]
fn no_engine_opens_while_one_runs_or_closes() {
    let mut rig = Rig::reconnecting();
    rig.connect("/", "{}", 32);
    rig.connect("/b", "{}", 32);
    rig.handshake();
    assert_eq!(rig.effects(), [Effect::Open, listen("/"), listen("/b")]);

    rig.server("garbage");
    assert_eq!(rig.effects(), [Effect::Close]);
    rig.connect("/c", "{}", 32);
    assert_eq!(rig.effects(), [listen("/c")]);

    let effects = rig.step(ManagerInput::Closed(Ok(())));
    assert_eq!(effects, [Effect::Schedule(Duration::ZERO)]);
    rig.connect("/d", "{}", 32);
    assert_eq!(rig.effects(), [listen("/d")]);

    assert_eq!(rig.step(ManagerInput::Reconnect), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0{}", "0/b,{}", "0/c,{}", "0/d,{}"]);
}

#[test]
fn a_namespace_opened_while_the_client_closes_the_engine_opens_the_next_at_once() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");
    handles.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), []);
    assert_eq!(rig.messages(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);

    rig.connect("/b", "{}", 32);
    assert_eq!(rig.effects(), [listen("/b")]);

    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.messages(), ["0/b,{}"]);
}

#[test]
fn a_namespace_activated_while_flushing_uses_the_same_engine_once() {
    let mut rig = Rig::new();
    let old = rig.connected("/");
    old.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), []);
    // DISCONNECT has not been handed off, so this engine is still flushing.
    let mut new = rig.connect("/new", "{}", 32);
    new.reply_rx.try_recv().unwrap().unwrap();
    assert_eq!(rig.effects(), [listen("/new")]);
    assert_eq!(rig.messages(), ["1", "0/new,{}"]);
    assert_eq!(rig.step(ManagerInput::Release), []);
    assert_eq!(rig.messages(), Vec::<String>::new());
}

#[test]
fn the_last_namespace_ending_during_a_wait_cancels_it() {
    let mut rig = Rig::reconnecting();
    let handles = rig.connected("/");
    let effects = rig.step(ManagerInput::Closed(Ok(())));
    assert_eq!(effects, [Effect::Schedule(Duration::ZERO)]);

    handles.closed.cancel();
    assert_eq!(rig.step(disconnect("/")), [Effect::Cancel]);
    assert!(rig.core.is_idle());

    // A timer that fired before the cancel arrived changes nothing.
    assert_eq!(rig.step(ManagerInput::Reconnect), []);
}

#[test]
fn exhaustion_ends_namespaces_and_the_core_takes_new_ones() {
    let backoff = Backoff::new(Duration::ZERO, Duration::ZERO, 0.0, Some(1));
    let mut rig = Rig::with_backoff(Some(backoff), 1);
    let mut handles = rig.connected("/");

    let mut effects = rig.step(ManagerInput::Closed(Ok(())));
    effects.extend(rig.step(ManagerInput::Reconnect));
    assert_eq!(effects, [Effect::Schedule(Duration::ZERO), Effect::Open]);

    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
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

    rig.step(disconnect("/"));
    assert!(rig.core.is_idle());

    let mut again = rig.connect("/", "{}", 32);
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), [Effect::Open, listen("/")]);
}

#[test]
fn without_reconnection_the_namespaces_end() {
    let mut rig = Rig::new();
    let handles = rig.connected("/");
    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
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

    let mut effects = rig.step(ManagerInput::Closed(Ok(())));
    effects.extend(rig.step(ManagerInput::Reconnect));
    // No handshake: the attempt count keeps growing.
    effects.extend(rig.step(ManagerInput::Closed(Ok(()))));
    effects.extend(rig.step(ManagerInput::Reconnect));
    rig.handshake();
    effects.extend(rig.step(ManagerInput::Closed(Ok(()))));

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
fn an_internal_error_is_returned_and_shutdown_leaves_the_core_idle() {
    let mut rig = Rig::reconnecting();
    let mut handles = rig.connected("/");
    let (sent_packet, mut sent) = event_ack("[\"e\"]");
    rig.send("/", sent_packet);
    rig.messages();

    let result = rig
        .core
        .handle(ManagerInput::Closed(Err(ManagerError::EngineEnded)));
    assert!(matches!(result, Err(ManagerError::EngineEnded)));

    rig.core.shutdown();
    assert_eq!(sent.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(rig.core.read("/", true), Read::Discard);
    // The Connect packet stays readable, then the receiver ends.
    handles.server_packet_rx.try_recv().unwrap();
    assert!(matches!(
        handles.server_packet_rx.try_recv(),
        Err(MpscTryRecvError::Disconnected)
    ));
    assert!(handles.closed.is_cancelled());
    assert_eq!(rig.effects(), []);

    // Nothing waits on the application, so the core is idle at once.
    assert!(rig.core.is_idle());
}

#[test]
fn without_a_namespace_a_lost_engine_is_not_reopened() {
    let mut rig = Rig::reconnecting();
    assert_eq!(rig.effects(), [Effect::Open]);

    assert_eq!(rig.step(ManagerInput::Closed(Ok(()))), []);
    assert!(rig.core.is_idle());

    let _handles = rig.connect("/", "{}", 32);
    assert_eq!(rig.effects(), [Effect::Open, listen("/")]);
}
