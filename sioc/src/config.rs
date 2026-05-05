/// Channel buffer capacities for each internal MPSC queue.
///
/// Construct via [`From<()>`] for defaults, [`From<usize>`] for uniform sizing,
/// or build manually for per-channel control.
#[derive(Clone, Copy, Debug)]
pub struct ChannelConfig {
    /// Engine task inbox: frames from the transport and messages from the Socket.IO layer.
    pub engine: usize,
    /// Transport task inbox: encoded frames from the engine task.
    pub transport: usize,
    /// Manager task inbox: directives from all namespace senders.
    pub manager: usize,
    /// Per-namespace inbox: signals delivered to each [`SocketReceiver`](crate::client::SocketReceiver).
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
    fn from(_: ()) -> Self {
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
