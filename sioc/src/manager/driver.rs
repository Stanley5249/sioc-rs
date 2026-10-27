//! The shared manager and the one task that drives its I/O: the engine, the
//! reconnection timer, and server packets that wait for a full receiver.
//!
//! The socket handles and the driver share [`ManagerCore`] behind a mutex.
//! Every lock runs one synchronous transition and never spans an `.await`,
//! so a transition is atomic for everyone. Handles wake the driver after each
//! transition, and the driver re-reads the core on every wake.

use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;
use tokio::time::Sleep;

use crate::error::ManagerError;
use crate::manager::engine::{Drain, Engine, EngineStep};
use crate::manager::socket::Blocked;
use crate::manager::state::{ManagerCore, ManagerOutput};

/// The state every handle and the driver share, like socket.io-client's
/// `Manager` object that its sockets call.
#[derive(Debug)]
pub struct Manager {
    core: Mutex<ManagerCore>,
    /// Wakes the driver after a handle changed the core. A permit stored
    /// while the driver is busy completes its next wait, so no wake is lost.
    wake: Notify,
}

impl Manager {
    #[must_use]
    pub fn new(core: ManagerCore) -> Arc<Self> {
        Arc::new(Self {
            core: Mutex::new(core),
            wake: Notify::new(),
        })
    }

    /// Runs one transition for a handle and wakes the driver. Returns `None`
    /// if a transition panicked earlier, which ended the manager.
    pub fn with_core<T>(&self, transition: impl FnOnce(&mut ManagerCore) -> T) -> Option<T> {
        let _wake = WakeGuard(&self.wake);
        let mut core = self.core.lock().ok()?;
        Some(transition(&mut core))
    }

    /// Locks the core for the driver, which needs no wake of its own.
    fn lock(&self) -> MutexGuard<'_, ManagerCore> {
        self.core
            .lock()
            .expect("a core transition panicked, which ends the manager")
    }
}

struct WakeGuard<'a>(&'a Notify);

impl Drop for WakeGuard<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

/// Runs the manager until the client handle and every socket are gone, like
/// socket.io-client's `Manager` for as long as its sockets live.
///
/// # Errors
///
/// Returns an internal error, which is a library bug.
pub fn run<F>(
    manager: Arc<Manager>,
    connect_engine: F,
) -> impl Future<Output = Result<(), ManagerError>>
where
    F: FnMut() -> Engine,
{
    // Construct before the future is polled, so aborting a newly spawned
    // driver still closes handles that connected before its first turn.
    let shutdown = ShutdownGuard(Arc::clone(&manager));
    async move {
        let mut driver = Driver {
            _shutdown: shutdown,
            manager,
            engine: None,
            sleep: None,
            blocked: None,
            connect_engine,
        };
        let result = driver.drive().await;
        driver.shutdown(result.is_err()).await;
        result
    }
}

/// Ends every handle when the driver stops for any reason, including a panic
/// or an aborted task, so no handle waits on a manager that is gone. Rust
/// cleanup.
struct ShutdownGuard(Arc<Manager>);

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        // After a panicked transition the state may be inconsistent, but
        // ending the handles only closes their channels and capacities, which
        // wakes every sender that waits for capacity.
        let mut core = self.0.core.lock().unwrap_or_else(PoisonError::into_inner);
        core.shutdown();
    }
}

struct Driver<F> {
    _shutdown: ShutdownGuard,
    manager: Arc<Manager>,
    /// The running engine, like `Manager.engine`.
    engine: Option<Engine>,
    /// The reconnection delay, like the timer in `Manager.reconnect`.
    sleep: Option<Pin<Box<Sleep>>>,
    /// One server packet waiting for its full receiver. While it waits, the
    /// driver reads no engine message, except to drain an ended engine.
    blocked: Option<Blocked>,
    /// Starts one engine with new channels, like the `Engine` constructor in
    /// `Manager.open`.
    connect_engine: F,
}

impl<F> Driver<F>
where
    F: FnMut() -> Engine,
{
    async fn drive(&mut self) -> Result<(), ManagerError> {
        loop {
            self.apply_outputs();
            let has_message = {
                let manager = Arc::clone(&self.manager);
                let mut core = manager.lock();
                if core.is_finished() && self.engine.is_none() {
                    return Ok(());
                }
                if self.send_message(&mut core) {
                    continue;
                }
                core.has_message()
            };
            self.turn(has_message).await?;
        }
    }

    /// Carries out the effects of the last transitions.
    fn apply_outputs(&mut self) {
        loop {
            let output = self.manager.lock().next_output();
            let Some(output) = output else { return };
            match output {
                ManagerOutput::Open => {
                    assert!(
                        self.engine.is_none(),
                        "the core opens an engine only after the last one closed"
                    );
                    self.engine = Some((self.connect_engine)());
                }
                ManagerOutput::Close => {
                    if let Some(engine) = &mut self.engine {
                        engine.close(Drain::Discard);
                    }
                }
                ManagerOutput::Schedule(delay) => {
                    self.sleep = Some(Box::pin(tokio::time::sleep(delay)));
                }
                ManagerOutput::Cancel => self.sleep = None,
            }
        }
    }

    /// Sends one waiting message on a held engine permit, like
    /// `engine.write`. The message leaves the core only with the permit in
    /// hand, so nothing can revoke the slot after the core let it go.
    fn send_message(&mut self, core: &mut ManagerCore) -> bool {
        let Some(engine) = &mut self.engine else {
            return false;
        };
        let Some(permit) = engine.take_permit() else {
            return false;
        };
        if let Some(message) = core.next_message() {
            permit.send(message);
            true
        } else {
            engine.return_permit(permit);
            false
        }
    }

    /// Waits for one input and applies it. Each branch future is cancel safe:
    /// the wake keeps a permit, the engine owns its future and acquired slot,
    /// the blocked packet stays in `self.blocked`, and the sleep is pinned.
    async fn turn(&mut self, has_message: bool) -> Result<(), ManagerError> {
        let Self {
            manager,
            engine,
            sleep,
            blocked,
            ..
        } = self;
        let blocked_wait = blocked
            .as_ref()
            .map(|blocked| (blocked.server_packet_tx.clone(), blocked.closed.clone()));
        let can_receive_server_message = blocked.is_none();
        let has_engine = engine.is_some();
        let waits = sleep.is_some();

        tokio::select! {
            () = manager.wake.notified() => {}

            permit = async {
                let (server_packet_tx, closed) =
                    blocked_wait.expect("the guard checked the blocked packet");
                tokio::select! {
                    permit = server_packet_tx.reserve_owned() => permit.ok(),
                    () = closed.cancelled() => None,
                }
            }, if !can_receive_server_message => {
                let Blocked { ns, packet, closed, .. } =
                    blocked.take().expect("the guard checked the blocked packet");
                match permit {
                    Some(permit) if !closed.is_cancelled() => {
                        permit.send(packet);
                    }
                    _ => tracing::debug!(%ns, "discarded server packet for a closed namespace"),
                }
            },

            step = async {
                engine.as_mut()
                    .expect("the guard checked the engine")
                    .next(can_receive_server_message, has_message).await
            }, if has_engine => match step {
                EngineStep::Message { message, draining } => {
                    debug_assert!(
                        can_receive_server_message || draining,
                        "the driver reads no live message while a packet is blocked"
                    );
                    let held = manager.lock().receive(message);
                    if let Some(held) = held {
                        if blocked.is_none() {
                            tracing::debug!(ns = %held.ns, "holding server packet for a full receiver");
                            *blocked = Some(held);
                        } else {
                            // Only a draining engine reaches a full receiver
                            // while another packet waits. Keep that first one,
                            // and discard later overflow without waiting.
                            tracing::debug!(ns = %held.ns, "discarded overflow during engine cleanup");
                        }
                    }
                }
                EngineStep::Closed => {
                    let result = engine.take().expect("the guard checked the engine").into_result();
                    manager.lock().close(result)?;
                }
                EngineStep::Progress => {}
            },

            () = async {
                sleep.as_mut().expect("the guard checked the sleep").await;
            }, if waits => {
                *sleep = None;
                manager.lock().reconnect();
            },
        }
        Ok(())
    }

    /// Releases every resource. After an internal error, ends every socket
    /// without waiting for the application. Then closes the engine and waits
    /// for its task, reading what the server still sends, so it never sees a
    /// failed send.
    async fn shutdown(mut self, failed: bool) {
        self.blocked = None;
        if failed {
            self.manager.lock().shutdown();
        }
        tracing::debug!("client ended");
        if let Some(mut engine) = self.engine.take() {
            engine.close(Drain::Discard);
            while !matches!(engine.next(false, false).await, EngineStep::Closed) {}
            if let Err(error) = engine.into_result() {
                tracing::warn!(%error, "engine failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use futures_util::FutureExt;

    use super::*;

    #[test]
    fn a_panicking_transition_stores_a_driver_wakeup() {
        let manager = Manager::new(ManagerCore::new(None, fastrand::Rng::new()));
        assert!(manager.wake.notified().now_or_never().is_none());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                manager.with_core(|_| panic!("transition failed"));
            }))
            .is_err()
        );
        assert!(manager.core.is_poisoned());
        assert!(manager.wake.notified().now_or_never().is_some());
    }

    #[test]
    fn connector_runs_outside_the_core_lock_and_its_panic_does_not_poison_it() {
        let manager = Manager::new(ManagerCore::new(None, fastrand::Rng::new()));
        let mut constructed = false;
        let connector = || {
            // try_lock makes a regression fail rather than deadlock the test.
            drop(
                manager
                    .core
                    .try_lock()
                    .expect("connector does not hold the core lock"),
            );
            manager.with_core(|_| ()).unwrap();
            constructed = true;
            panic!("connector failed");
        };
        let driver = run(Arc::clone(&manager), connector);
        catch_unwind(AssertUnwindSafe(|| driver.now_or_never())).unwrap_err();
        assert!(constructed);
        assert!(!manager.core.is_poisoned());
    }

    #[test]
    fn dropping_an_unpolled_driver_closes_its_handles() {
        let manager = Manager::new(ManagerCore::new(None, fastrand::Rng::new()));
        let (tx, _rx) = crate::client::connect(&manager, "/".into(), "{}".into(), 1, 1)
            .now_or_never()
            .unwrap()
            .unwrap();
        let driver = run(Arc::clone(&manager), || panic!("driver must not be polled"));
        drop(driver);
        assert!(tx.closed().now_or_never().is_some());
    }

    #[tokio::test]
    async fn the_shutdown_guard_wakes_a_waiting_connect_after_a_panicked_transition() {
        let manager = Manager::new(ManagerCore::new(None, fastrand::Rng::new()));
        let (tx, _rx) = crate::client::connect(&manager, "/".into(), "{}".into(), 1, 1)
            .now_or_never()
            .unwrap()
            .unwrap();
        manager.with_core(|core| {
            core.receive(eioc::prelude::ServerMessage::Open(
                eioc::prelude::Handshake {
                    sid: "engine".into(),
                    upgrades: Vec::new(),
                    ping_interval: 25_000,
                    ping_timeout: 20_000,
                    max_payload: 1_000_000,
                },
            ));
            core.next_message();
            core.receive(eioc::prelude::ServerMessage::Text(r#"0{"sid":"s"}"#.into()));
        });
        tx.disconnect();
        let mut waiting = Box::pin(crate::client::connect(
            &manager,
            "/".into(),
            "{}".into(),
            1,
            1,
        ));
        assert!(waiting.as_mut().now_or_never().is_none());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                manager.with_core(|_| panic!("transition failed"));
            }))
            .is_err()
        );
        drop(run(Arc::clone(&manager), || {
            panic!("driver must not be polled")
        }));
        assert!(matches!(
            waiting.await,
            Err(crate::error::SocketError::Closed)
        ));
    }
}
