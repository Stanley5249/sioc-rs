//! Runs the client-packet loop beside the supervisor of its sessions.

use std::future::Future;

use eioc::prelude::Message;
use tokio::sync::mpsc;

use crate::error::ManagerError;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::routes::Routes;
use crate::manager::supervisor::Supervisor;

/// Routes packets between the namespace handles and one engine session after
/// another until the client ends.
///
/// The client ends once the client handle and every namespace are gone, or
/// after an internal error.
///
/// # Errors
///
/// Returns an internal error, which is a library bug.
pub async fn run<F, Fut>(
    connect_request_rx: mpsc::Receiver<ConnectRequest>,
    supervisor: Supervisor<F>,
) -> Result<(), ManagerError>
where
    F: FnMut(mpsc::Sender<Message>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    let routes = Routes::default();

    // Each channel holds at most one item, because the client-packet loop asks
    // for one session at a time and waits for it.
    let (session_request_tx, session_request_rx) = mpsc::channel(1);
    let (session_tx, session_rx) = mpsc::channel(1);

    // Each loop waits for the other to hang up before it returns.
    let (client_result, supervisor_result) = tokio::join!(
        crate::manager::client_packet::client_packets_to_messages(
            connect_request_rx,
            session_request_tx,
            session_rx,
            &routes
        ),
        supervisor.supervise(session_request_rx, session_tx, &routes),
    );

    client_result?;
    supervisor_result
}
