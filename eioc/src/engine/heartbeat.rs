//! The deadline by which the server must ping again.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::error::EngineError;
use crate::packet::Frame;

pub struct Heartbeat {
    deadline: Instant,
    ping_window: Duration,
}

impl Heartbeat {
    pub fn new(ping_window: Duration) -> Self {
        Self {
            deadline: Instant::now() + ping_window,
            ping_window,
        }
    }

    pub fn reset(&mut self) {
        self.deadline = Instant::now() + self.ping_window;
    }

    /// Receives the next frame, failing once the server misses its ping window.
    pub async fn next_server_frame(
        &self,
        server_frame_rx: &mut mpsc::Receiver<Frame>,
    ) -> Result<Option<Frame>, EngineError> {
        tokio::time::timeout_at(self.deadline, server_frame_rx.recv())
            .await
            .map_err(|_| EngineError::HeartbeatTimeout)
    }
}
