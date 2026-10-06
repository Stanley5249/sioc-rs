//! Runs both manager loops for one session.

use eioc::prelude::Message;
use tokio::sync::mpsc;

use crate::error::ManagerError;
use crate::manager::client_packet::ConnectRequest;
use crate::manager::routes::Routes;

/// Routes packets between the namespace handles and the engine until the
/// session ends.
///
/// Closes the session by dropping `client_message_tx` once the client handle
/// and every namespace are gone, then returns when the engine closes
/// `server_message_rx`.
///
/// # Errors
///
/// Returns an error if the engine channel closes early or the server breaks the
/// protocol.
pub async fn run(
    connect_request_rx: mpsc::Receiver<ConnectRequest>,
    server_message_rx: mpsc::Receiver<Message>,
    client_message_tx: mpsc::Sender<Message>,
) -> Result<(), ManagerError> {
    let routes = Routes::default();

    // The server-message loop tells the client-packet loop which namespace
    // generations the server confirmed, so their buffered events can go out.
    // The channel is unbounded so that delivering server packets never waits on
    // the client's sending direction. It stays short because each generation
    // travels at most once.
    let (connected_generation_tx, connected_generation_rx) = mpsc::unbounded_channel();

    tokio::try_join!(
        crate::manager::server_message::server_messages_to_packets(
            server_message_rx,
            &routes,
            connected_generation_tx
        ),
        crate::manager::client_packet::client_packets_to_messages(
            connect_request_rx,
            connected_generation_rx,
            &routes,
            client_message_tx
        ),
    )?;

    Ok(())
}
