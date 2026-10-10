//! Client settings: channel capacities and reconnection.

use std::time::Duration;

/// Channel buffer capacities for each internal MPSC queue.
///
/// Construct via [`From<()>`] for defaults, [`From<usize>`] for uniform sizing,
/// or build manually for per-channel control.
#[derive(Clone, Copy, Debug)]
pub struct ChannelConfig {
    /// Engine task inboxes: frames from the transport and messages from the
    /// manager.
    pub engine: usize,
    /// Transport channel: encoded frames to send to the transport.
    pub transport: usize,
    /// Manager inboxes: messages from the engine, new namespaces, and each
    /// namespace's client packets and buffered outgoing events.
    pub manager: usize,
    /// Per-namespace inbox: server packets delivered to each
    /// [`SocketReceiver`](crate::client::SocketReceiver), plus one separate
    /// slot for the terminal packet.
    pub socket: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            engine: 32,
            transport: 32,
            manager: 32,
            socket: 32,
        }
    }
}

impl From<()> for ChannelConfig {
    fn from((): ()) -> Self {
        Self::default()
    }
}

impl From<usize> for ChannelConfig {
    fn from(n: usize) -> Self {
        Self {
            engine: n,
            transport: n,
            manager: n,
            socket: n,
        }
    }
}

/// Reconnection settings, with the defaults of socket.io-client's `Manager`.
///
/// After an Engine.IO session drops while namespaces are open, attempt `n`
/// waits `delay * 2^n`, moved up or down by a random share of up to
/// `randomization_factor` of itself, and capped at `delay_max`. The count
/// restarts once an Engine.IO handshake succeeds, like socket.io-client, so it
/// counts only failed connection attempts in a row.
///
/// Pass it to
/// [`ClientBuilder::reconnection`](crate::client::ClientBuilder::reconnection).
#[derive(Clone, Copy, Debug)]
pub struct ReconnectionConfig {
    /// The number of attempts before giving up, or `None` for no limit, like
    /// `reconnectionAttempts` (default: `None`).
    pub attempts: Option<u32>,
    /// The delay before the first attempt, like `reconnectionDelay` (default:
    /// 1 second).
    pub delay: Duration,
    /// The longest delay, like `reconnectionDelayMax` (default: 5 seconds).
    pub delay_max: Duration,
    /// The largest random share of each delay to add or subtract, like
    /// `randomizationFactor` (default: 0.5). A value outside 0 to 1 means no
    /// jitter, as in JS.
    pub randomization_factor: f64,
}

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            attempts: None,
            delay: Duration::from_secs(1),
            delay_max: Duration::from_secs(5),
            randomization_factor: 0.5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_config_default_is_32() {
        let c = ChannelConfig::default();
        assert_eq!(
            (c.engine, c.transport, c.manager, c.socket),
            (32, 32, 32, 32)
        );
    }

    #[test]
    fn channel_config_from_unit_matches_default() {
        let c = ChannelConfig::from(());
        assert_eq!(
            (c.engine, c.transport, c.manager, c.socket),
            (32, 32, 32, 32)
        );
    }

    #[test]
    fn channel_config_from_usize_uniform() {
        let c = ChannelConfig::from(8_usize);
        assert_eq!((c.engine, c.transport, c.manager, c.socket), (8, 8, 8, 8));
    }
}
