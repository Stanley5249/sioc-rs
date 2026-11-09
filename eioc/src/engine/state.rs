//! The session rules, like the state of engine.io-client's `Socket`.

use std::time::Duration;

use bytestring::ByteString;
use tokio::time::Instant;

use crate::error::EngineError;
use crate::packet::{ClientMessage, Handshake, ServerMessage};
use crate::transport::{ClientFrame, ServerFrame};

/// What the session can do, like `Socket.readyState`.
#[derive(Debug)]
enum SessionState {
    /// Waiting for the Open packet. The deadline is the handshake timeout,
    /// like the `timeout` option of socket.io-client's `Manager`.
    Opening { deadline: Option<Instant> },
    /// Running. The deadline ends the ping window, like `_pingTimeoutTimer`.
    Open {
        ping_window: Duration,
        deadline: Instant,
    },
    /// Over: frames are discarded, messages refused, and nothing times out.
    /// The transport may still be sending the Close packet, which only it can
    /// bound, so no heartbeat deadline applies to it.
    Closed,
}

/// The Engine.IO session state, like engine.io-client's `Socket` without its
/// transport. Never awaits: the caller passes each input once the matching
/// channel permit is in hand, and sends the returned output on that permit.
#[derive(Debug)]
pub struct EngineCore {
    state: SessionState,
    /// The pong owed to the server's last ping. A later ping replaces it,
    /// because the server needs one pong within its window, not one per ping.
    /// JS writes each pong at once and never holds one.
    pong: Option<ByteString>,
}

impl EngineCore {
    /// Starts opening, like the `Socket` constructor.
    #[must_use]
    pub fn new(handshake_timeout: Option<Duration>, now: Instant) -> Self {
        Self {
            state: SessionState::Opening {
                deadline: handshake_timeout.map(|timeout| now + timeout),
            },
            pong: None,
        }
    }

    /// Handles one server frame, like `Socket._onPacket`, and returns the
    /// message to deliver. A valid frame out of place is ignored, like the JS
    /// client does, so a quirky server cannot end the session. After the
    /// session closed, frames are discarded.
    pub fn on_frame(&mut self, frame: ServerFrame, now: Instant) -> Option<ServerMessage> {
        let (packet, bytes) = frame.summary();
        tracing::trace!(packet, bytes, "received packet");

        match (&mut self.state, frame) {
            (SessionState::Opening { .. }, ServerFrame::Open(handshake)) => {
                tracing::debug!(sid = %handshake.sid, "received handshake");

                let ping_window = ping_window(&handshake);
                self.state = SessionState::Open {
                    ping_window,
                    deadline: now + ping_window,
                };

                Some(ServerMessage::Open(handshake))
            }
            (
                SessionState::Open {
                    ping_window,
                    deadline,
                },
                ServerFrame::Ping(payload),
            ) => {
                self.pong = Some(payload);
                *deadline = now + *ping_window;

                None
            }
            (SessionState::Open { .. }, ServerFrame::Message(payload)) => {
                Some(ServerMessage::Text(payload))
            }
            (SessionState::Open { .. }, ServerFrame::Binary(bytes)) => {
                Some(ServerMessage::Binary(bytes))
            }
            (SessionState::Open { .. }, ServerFrame::Noop) | (SessionState::Closed, _) => None,
            (SessionState::Opening { .. } | SessionState::Open { .. }, _) => {
                tracing::warn!(packet, bytes, "ignored out-of-place packet");

                None
            }
        }
    }

    /// Encodes one manager message, like `Socket.write`. During opening, the
    /// frame waits in the transport channel, which the transport reads only
    /// after the handshake, like `writeBuffer`. After the session closed, the
    /// message is discarded.
    pub fn on_message(&mut self, message: ClientMessage) -> Option<ClientFrame> {
        if self.is_closed() {
            return None;
        }

        let frame = match message {
            ClientMessage::Text(payload) => ClientFrame::Message(payload),
            ClientMessage::Binary(bytes) => ClientFrame::Binary(bytes),
        };
        let (packet, bytes) = frame.summary();
        tracing::trace!(packet, bytes, "sent packet");

        Some(frame)
    }

    /// Ends the session, like `Socket._onClose`, once either side's channel
    /// ended. Closing again is accepted.
    pub fn close(&mut self) {
        if !self.is_closed() {
            tracing::debug!("session closed");
        }

        // The server needs no pong once either side has hung up.
        self.pong = None;
        self.state = SessionState::Closed;
    }

    /// Fails the session once its deadline has passed, like the timers of
    /// `_resetPingTimeout` and of the `timeout` option.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::HandshakeTimeout`] or
    /// [`EngineError::HeartbeatTimeout`] when the deadline has passed, and the
    /// session is then closed.
    pub fn on_timeout(&mut self, now: Instant) -> Result<(), EngineError> {
        let error = match self.state {
            SessionState::Opening {
                deadline: Some(deadline),
            } if now >= deadline => EngineError::HandshakeTimeout,
            SessionState::Open { deadline, .. } if now >= deadline => EngineError::HeartbeatTimeout,
            _ => return Ok(()),
        };

        self.close();

        Err(error)
    }

    /// Whether a pong waits for a transport permit.
    #[must_use]
    pub fn has_pong(&self) -> bool {
        self.pong.is_some()
    }

    /// Takes the pong to send on a transport permit already in hand.
    pub fn take_pong(&mut self) -> Option<ClientFrame> {
        let frame = ClientFrame::Pong(self.pong.take()?);
        let (packet, bytes) = frame.summary();
        tracing::trace!(packet, bytes, "sent packet");

        Some(frame)
    }

    /// When the session fails if nothing else happens first.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        match self.state {
            SessionState::Opening { deadline } => deadline,
            SessionState::Open { deadline, .. } => Some(deadline),
            SessionState::Closed => None,
        }
    }

    /// Whether the handshake is still pending, so the server holds no session
    /// yet.
    #[must_use]
    pub fn is_opening(&self) -> bool {
        matches!(self.state, SessionState::Opening { .. })
    }

    /// Whether the session is over, so the caller stops sending frames.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(self.state, SessionState::Closed)
    }
}

/// How long the server may stay silent, `pingInterval + pingTimeout`, like the
/// timer of `_resetPingTimeout`.
fn ping_window(handshake: &Handshake) -> Duration {
    Duration::from_millis(handshake.ping_interval + handshake.ping_timeout)
}
