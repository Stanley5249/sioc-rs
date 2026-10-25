//! The session rules, like the state of engine.io-client's `Socket`.

use std::time::Duration;

use bytestring::ByteString;
use tokio::time::Instant;

use crate::error::EngineError;
use crate::packet::{Message, ServerMessage};
use crate::transport::{ClientFrame, ServerFrame};

/// The session's stage, like `Socket.readyState`.
#[derive(Debug)]
enum ReadyState {
    /// Before the Open packet, like `"opening"`. The deadline is the
    /// handshake timeout, like the `timeout` option.
    Opening { deadline: Option<Instant> },
    /// After it, like `"open"`. The deadline ends the ping window, like
    /// `_pingTimeoutTimer`.
    Open {
        ping_window: Duration,
        deadline: Instant,
    },
    /// The manager hung up, and the transport is sending its last frames and
    /// the Close packet, like `"closing"`. The last ping window still bounds
    /// the close. Rust cleanup: JS closes the transport synchronously.
    Closing { deadline: Instant },
    /// The session ended, like `"closed"`.
    Closed(CloseReason),
}

/// Why the session ended, like the reason of `Socket.onClose`.
#[derive(Debug)]
pub enum CloseReason {
    /// The manager hung up before the Open packet, like `Socket.close` while
    /// opening. The opening is dropped, because a handshake may have no
    /// deadline.
    Cancelled,
    /// The manager hung up, like `Socket.close`.
    Local,
    /// The transport ended the server direction, like `"transport close"`.
    Peer,
    /// The core failed the session, like `"ping timeout"`.
    Failed(EngineError),
}

/// The Engine.IO session state, like engine.io-client's `Socket` without its
/// transport. Never awaits: the caller passes each input once the matching
/// channel permit is in hand, and sends the returned output on that permit.
#[derive(Debug)]
pub struct EngineCore {
    state: ReadyState,
    /// The pong owed to the server's last ping. A later ping replaces it,
    /// because the server needs one pong within its window, not one per ping.
    /// Rust bound: JS writes each pong at once.
    pong: Option<ByteString>,
}

impl EngineCore {
    /// Starts opening, like the `Socket` constructor.
    #[must_use]
    pub fn new(handshake_timeout: Option<Duration>, now: Instant) -> Self {
        Self {
            state: ReadyState::Opening {
                deadline: handshake_timeout.map(|timeout| now + timeout),
            },
            pong: None,
        }
    }

    /// Handles one server frame, like `Socket._onPacket`, and returns the
    /// message to deliver. A valid frame out of place is ignored, like the JS
    /// client does, so a quirky server cannot end the session. After a local
    /// close, frames are discarded.
    pub fn on_frame(&mut self, frame: ServerFrame, now: Instant) -> Option<ServerMessage> {
        match &frame {
            ServerFrame::Binary(bytes) => {
                tracing::trace!(bytes = bytes.len(), "received binary frame");
            }
            frame => tracing::trace!(?frame, "received packet"),
        }

        match (&mut self.state, frame) {
            (ReadyState::Opening { .. }, ServerFrame::Open(handshake)) => {
                tracing::debug!(sid = %handshake.sid, "received handshake");

                let ping_window = handshake.ping_window();
                self.state = ReadyState::Open {
                    ping_window,
                    deadline: now + ping_window,
                };

                Some(ServerMessage::Open(handshake))
            }
            (
                ReadyState::Open {
                    ping_window,
                    deadline,
                },
                ServerFrame::Ping(payload),
            ) => {
                self.pong = Some(payload);
                *deadline = now + *ping_window;

                None
            }
            (ReadyState::Open { .. }, ServerFrame::Message(payload)) => {
                Some(ServerMessage::Text(payload))
            }
            (ReadyState::Open { .. }, ServerFrame::Binary(bytes)) => {
                Some(ServerMessage::Binary(bytes))
            }
            (ReadyState::Open { .. }, ServerFrame::Noop)
            | (ReadyState::Closing { .. } | ReadyState::Closed(_), _) => None,
            (ReadyState::Opening { .. } | ReadyState::Open { .. }, frame) => {
                tracing::warn!(?frame, "ignored out-of-place packet");

                None
            }
        }
    }

    /// Encodes one manager message, like `Socket.write`. During opening, the
    /// frame waits in the transport channel, which the transport reads only
    /// after the handshake, like `writeBuffer`. After the session ended, the
    /// message is discarded.
    pub fn on_message(&mut self, message: Message) -> Option<ClientFrame> {
        if !self.sends() {
            return None;
        }

        let frame = match message {
            Message::Text(payload) => ClientFrame::Message(payload),
            Message::Binary(bytes) => ClientFrame::Binary(bytes),
        };
        tracing::trace!(?frame, "sent packet");

        Some(frame)
    }

    /// The transport ended the server direction, like `Socket._onClose` on
    /// `"transport close"`. A repeated report is accepted.
    pub fn on_transport_ended(&mut self) {
        self.pong = None;
        self.state = match std::mem::replace(&mut self.state, ReadyState::Closed(CloseReason::Peer))
        {
            ReadyState::Opening { .. } | ReadyState::Open { .. } => {
                tracing::debug!("transport finished");

                ReadyState::Closed(CloseReason::Peer)
            }
            ReadyState::Closing { .. } => ReadyState::Closed(CloseReason::Local),
            closed @ ReadyState::Closed(_) => closed,
        };
    }

    /// The manager hung up, like `Socket.close`. A repeated report is accepted.
    pub fn on_client_closed(&mut self) {
        tracing::debug!("client closed");

        // The server needs no pong after the Close packet.
        self.pong = None;
        self.state =
            match std::mem::replace(&mut self.state, ReadyState::Closed(CloseReason::Local)) {
                ReadyState::Opening { .. } => ReadyState::Closed(CloseReason::Cancelled),
                ReadyState::Open { deadline, .. } => ReadyState::Closing { deadline },
                state @ (ReadyState::Closing { .. } | ReadyState::Closed(_)) => state,
            };
    }

    /// Fails the session once its deadline has passed, like the timer of
    /// `_resetPingTimeout` or of the `timeout` option.
    pub fn on_timeout(&mut self, now: Instant) {
        if self.deadline().is_none_or(|deadline| now < deadline) {
            return;
        }

        let error = match self.state {
            ReadyState::Opening { .. } => EngineError::HandshakeTimeout,
            _ => EngineError::HeartbeatTimeout,
        };
        tracing::debug!(%error, "session timed out");

        self.pong = None;
        self.state = ReadyState::Closed(CloseReason::Failed(error));
    }

    /// Whether a pong waits for a transport permit.
    #[must_use]
    pub fn has_pong(&self) -> bool {
        self.pong.is_some()
    }

    /// Takes the pong to send on a transport permit already in hand.
    pub fn take_pong(&mut self) -> Option<ClientFrame> {
        let payload = self.pong.take()?;
        tracing::trace!("sent pong");

        Some(ClientFrame::Pong(payload))
    }

    /// When the session fails if nothing else happens first.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        match self.state {
            ReadyState::Opening { deadline } => deadline,
            ReadyState::Open { deadline, .. } | ReadyState::Closing { deadline } => Some(deadline),
            ReadyState::Closed(_) => None,
        }
    }

    /// Whether messages still go to the manager. Only a peer end delivers
    /// the last message; a local close or a failure discards it, because the
    /// manager no longer wants it.
    #[must_use]
    pub fn delivers(&self) -> bool {
        matches!(
            self.state,
            ReadyState::Opening { .. }
                | ReadyState::Open { .. }
                | ReadyState::Closed(CloseReason::Peer)
        )
    }

    /// Whether frames still go to the transport. Once this turns false, the
    /// caller drops its transport sender, which makes the transport send the
    /// Close packet.
    #[must_use]
    pub fn sends(&self) -> bool {
        matches!(
            self.state,
            ReadyState::Opening { .. } | ReadyState::Open { .. }
        )
    }

    /// Whether the transport future still runs. A cancelled opening or a
    /// failure drops it, because the server either has no session yet or no
    /// longer answers.
    #[must_use]
    pub fn keeps_transport(&self) -> bool {
        !matches!(
            self.state,
            ReadyState::Closed(CloseReason::Cancelled | CloseReason::Failed(_))
        )
    }

    /// Returns the session's own failure, if any.
    ///
    /// # Errors
    ///
    /// Returns the error that failed the session.
    pub fn finish(self) -> Result<(), EngineError> {
        match self.state {
            ReadyState::Closed(CloseReason::Failed(error)) => Err(error),
            _ => Ok(()),
        }
    }
}
