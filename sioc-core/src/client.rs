//! Socket.IO Client Implementation
//!
//! This module provides the high-level client API for Socket.IO connections.
//! The client uses the Active Router pattern for managing protocol state.
//!
//! # Architecture
//!
//! The client follows a strictly typed, active actor architecture:
//! - **Packet Types**: Hierarchical type system prevents invalid states
//! - **Router**: Centralized state machine for ID assignment and binary reassembly
//! - **Commands**: Explicitly typed operations via `RouterCommand`
//!
//! # Examples
//!
//! ## Basic Connection
//!
//! ```no_run
//! use sioc_core::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut client = connect("http://localhost:3000".to_string()).await?;
//!
//!     // Receive messages
//!     while let Some(packet) = client.recv().await {
//!         println!("Received: {:?}", packet);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Sending Events
//!
//! ```no_run
//! use sioc_core::prelude::*;
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = connect("http://localhost:3000".to_string()).await?;
//!     let sender = client.sender();
//!
//!     // Send a simple event
//!     let base = BasePacket::new("/".into(), Bytes::from(r#"["hello","world"]"#));
//!     sender.send(RouterCommand::SendEvent(base)).await.unwrap();
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Sending Events with Acknowledgements
//!
//! ```no_run
//! use sioc_core::prelude::*;
//! use bytes::Bytes;
//! use tokio::sync::oneshot;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = connect("http://localhost:3000".to_string()).await?;
//!     let sender = client.sender();
//!
//!     // Send event expecting acknowledgement
//!     let (tx, rx) = oneshot::channel();
//!     let base = BasePacket::new("/".into(), Bytes::from(r#"["ping"]"#));
//!     sender.send(RouterCommand::SendEventWithAck {
//!         data: base,
//!         ack: tx,
//!     }).await.unwrap();
//!
//!     // Wait for acknowledgement
//!     let ack_packet = rx.await.unwrap();
//!     println!("Received ack: {:?}", ack_packet);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Sending Binary Events
//!
//! ```no_run
//! use sioc_core::prelude::*;
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = connect("http://localhost:3000".to_string()).await?;
//!     let sender = client.sender();
//!
//!     // Prepare binary data
//!     let image_data = Bytes::from(vec![0xFF, 0xD8, 0xFF, 0xE0]); // JPEG header
//!     let mut attachments = Attachments::new();
//!     attachments.push(image_data);
//!
//!     // Send binary event
//!     let base = BasePacket::new(
//!         "/".into(),
//!         Bytes::from(r#"["image",{"_placeholder":true,"num":0}]"#)
//!     );
//!     let header = BinaryPacket::new(base, 1);
//!
//!     sender.send(RouterCommand::SendBinaryEvent {
//!         header,
//!         payload: attachments,
//!     }).await.unwrap();
//!
//!     Ok(())
//! }
//! ```

use crate::error::Result;
use crate::packet::Packet;
use crate::router::{RouterCommand, router_loop};
use sioc_engine::builder::ClientBuilder as EioBuilder;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// High-level client handle for a Socket.IO session.
///
/// The client owns:
/// - A channel to send commands to the router
/// - A channel to receive complete messages from the router
/// - A handle to the background router task
#[derive(Debug)]
pub struct SioClient {
    /// Sender for router commands (outgoing).
    pub router_tx: mpsc::Sender<RouterCommand>,
    /// Receiver for complete Socket.IO messages (incoming).
    pub sio_rx: mpsc::Receiver<Packet>,
    /// Handle to the background router task.
    #[allow(dead_code)]
    handle: JoinHandle<()>,
}

impl SioClient {
    /// Get a clonable sender for emitting router commands.
    ///
    /// This allows multiple tasks to share the ability to send packets.
    pub fn sender(&self) -> mpsc::Sender<RouterCommand> {
        self.router_tx.clone()
    }

    /// Receive the next incoming Socket.IO message.
    ///
    /// Returns `None` if the connection is closed.
    pub async fn recv(&mut self) -> Option<Packet> {
        self.sio_rx.recv().await
    }
}

/// Connect to a Socket.IO server and spawn the background router task.
///
/// This function:
/// 1. Establishes an Engine.IO connection
/// 2. Spawns the router task for protocol management
/// 3. Returns a client handle
///
/// # Arguments
/// * `url` - The server URL (e.g., "http://localhost:3000")
///
/// # Returns
/// A `SioClient` that owns the session.
///
/// # Example
/// ```no_run
/// use sioc_core::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut client = connect("http://localhost:3000".to_string()).await?;
///
///     // Send a message
///     let base = BasePacket::new("/".into(), bytes::Bytes::from(r#"["hello"]"#));
///     client.sender().send(RouterCommand::SendEvent(base)).await.unwrap();
///
///     // Receive messages
///     while let Some(packet) = client.recv().await {
///         println!("Received: {:?}", packet);
///     }
///
///     Ok(())
/// }
/// ```
pub async fn connect(url: String) -> Result<SioClient> {
    // 1. Establish Engine.IO connection
    let eio_client = EioBuilder::new(url.parse()?).build().await?;

    // 2. Setup Router channels
    let (router_tx, router_rx) = mpsc::channel(32);
    let (sio_tx, sio_rx) = mpsc::channel(32);

    // 3. Spawn Router task (active actor)
    let handle = tokio::spawn(router_loop(eio_client, router_rx, sio_tx));

    Ok(SioClient {
        router_tx,
        sio_rx,
        handle,
    })
}
