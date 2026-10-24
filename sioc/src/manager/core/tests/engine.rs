use std::time::Duration;

use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig};
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::core::input_output::ManagerInput;

#[test]
fn no_engine_opens_while_one_runs_or_closes() {
    let mut rig = Rig::reconnecting(8);
    rig.open("/", "{}");
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.open("/b", "{}");
    rig.handshake();
    assert_eq!(rig.effects(), []);

    rig.server("garbage");
    assert_eq!(rig.effects(), [Effect::Close]);
    rig.open("/c", "{}");
    assert_eq!(rig.effects(), []);

    rig.close();
    assert_eq!(rig.effects(), [Effect::Schedule(Duration::ZERO)]);
    rig.open("/d", "{}");
    assert_eq!(rig.effects(), []);

    rig.input(ManagerInput::Reconnect);
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.pull(), ["0{}", "0/b,{}", "0/c,{}", "0/d,{}"]);
}

#[test]
fn a_namespace_opened_while_the_client_closes_the_engine_opens_the_next_at_once() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.disconnect("/");
    assert_eq!(rig.pull(), ["1"]);
    assert_eq!(rig.effects(), [Effect::Close]);

    rig.open("/b", "{}");
    assert_eq!(rig.effects(), []);

    rig.close();
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.pull(), ["0/b,{}"]);
}

#[test]
fn the_last_namespace_ending_during_a_wait_cancels_it() {
    let mut rig = Rig::reconnecting(8);
    rig.connected("/");
    rig.close();
    assert_eq!(rig.effects(), [Effect::Schedule(Duration::ZERO)]);

    rig.disconnect("/");
    assert_eq!(rig.effects(), [Effect::Cancel]);
    assert!(rig.core.is_idle());

    // A timer that fired before the cancel arrived changes nothing.
    rig.input(ManagerInput::Reconnect);
    assert_eq!(rig.effects(), []);
}

#[test]
fn exhaustion_ends_namespaces_and_the_core_takes_new_ones() {
    let backoff = Backoff::new(Duration::ZERO, Duration::ZERO, 0.0, Some(1));
    let mut rig = Rig::with_backoff(8, Some(backoff));
    let mut handles = rig.connected("/");

    rig.close();
    rig.input(ManagerInput::Reconnect);
    assert_eq!(
        rig.effects(),
        [Effect::Schedule(Duration::ZERO), Effect::Open]
    );

    rig.close();
    assert_eq!(rig.effects(), []);
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

    rig.disconnect("/");
    assert!(rig.core.is_idle());

    let mut again = rig.open("/", "{}");
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), [Effect::Open]);
}

#[test]
fn without_reconnection_the_namespaces_end() {
    let mut rig = Rig::new(8);
    let handles = rig.connected("/");
    rig.close();
    assert_eq!(rig.effects(), []);
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

    let mut rig = Rig::seeded(8, Some(new_backoff()), 42);
    rig.connected("/");

    rig.close();
    rig.input(ManagerInput::Reconnect);
    // No handshake: the attempt count keeps growing.
    rig.close();
    rig.input(ManagerInput::Reconnect);
    rig.handshake();
    rig.close();

    assert_eq!(
        rig.effects(),
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
fn an_internal_error_is_returned_and_shutdown_ends_everything() {
    let mut rig = Rig::reconnecting(8);
    let mut handles = rig.connected("/");
    let mut buffered = rig.emit_ack("/", "[\"e\"]");

    let result = rig.try_input(ManagerInput::Close(Err(ManagerError::EngineEnded)));
    assert!(matches!(result, Err(ManagerError::EngineEnded)));

    rig.shutdown();
    assert_eq!(buffered.try_recv().unwrap_err(), TryRecvError::Closed);
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
fn shutdown_closes_the_running_engine() {
    let mut rig = Rig::reconnecting(8);
    rig.connected("/");
    rig.shutdown();
    assert_eq!(rig.effects(), [Effect::Close]);

    assert!(!rig.core.is_idle());
    rig.close();
    assert_eq!(rig.effects(), []);
    assert!(rig.core.is_idle());
}

#[test]
fn the_first_engine_opens_at_once_and_stays_without_a_namespace() {
    let mut rig = Rig::new(8);
    assert_eq!(rig.effects(), [Effect::Open]);
    rig.handshake();
    assert_eq!(rig.effects(), []);
    assert!(!rig.core.is_idle());
}

#[test]
fn a_server_ended_namespace_keeps_the_engine_until_its_handles_end() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.server("1");
    assert_eq!(rig.effects(), []);
    rig.poll("/");
    assert_eq!(rig.effects(), [Effect::Close]);
}
