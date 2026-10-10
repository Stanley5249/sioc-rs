//! Sends what the namespace handles ask for.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::ops::ControlFlow;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{Event, Message};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::client::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::engine::{EngineEvent, ReadyState};
use crate::manager::open_request::{OpenHandles, OpenRequest};
use crate::manager::routes::Routes;
use crate::packet::{ClientPacket, Packet};

/// The client-packet loop's view of one namespace.
///
/// It lives until its client packets end, which can be after the server
/// closed the namespace. A reopen of the same name waits in `reopen` until
/// then, so each name has at most one entry.
struct Namespace {
    ns: ByteString,
    /// The JSON auth payload, sent with the CONNECT packet to every engine.
    auth: ByteString,
    /// Set by the server's CONNECT response to the open engine; events wait
    /// in `send_buffer` until then.
    connected: bool,
    send_buffer: Vec<Message>,
    /// A request to open the same name again, held until this entry ends.
    reopen: Option<ConnectRequest>,
}

/// Waits for the next client packet of one namespace.
///
/// Once `closed` is cancelled, closes the channel, so the packets sent before
/// still arrive and then the channel ends, as it does when every handle drops.
/// Hands the receiver back, so the caller decides whether to keep listening.
async fn recv_client_packet(
    ns: ByteString,
    mut client_packet_rx: mpsc::Receiver<ClientPacket>,
    closed: CancellationToken,
) -> (
    ByteString,
    Option<ClientPacket>,
    mpsc::Receiver<ClientPacket>,
    CancellationToken,
) {
    // Prefer the channel, so buffered packets drain before the close takes effect.
    let client_packet = tokio::select! {
        biased;
        client_packet = client_packet_rx.recv() => client_packet,
        () = closed.cancelled() => {
            client_packet_rx.close();
            client_packet_rx.recv().await
        }
    };

    (ns, client_packet, client_packet_rx, closed)
}

/// Takes a reopen whose old namespace has ended, or else the client's next
/// connect request.
///
/// Cancel safe: taking a reopen has no suspend point, and `recv` is cancel
/// safe.
async fn next_connect_request(
    reopens: &mut VecDeque<ConnectRequest>,
    connect_request_rx: &mut mpsc::Receiver<ConnectRequest>,
) -> Option<ConnectRequest> {
    match reopens.pop_front() {
        Some(request) => Some(request),
        None => connect_request_rx.recv().await,
    }
}

/// Sends what the namespace handles ask for, across engines, until the client
/// ends.
///
/// Runs the client-packet loop beside
/// [`run_engines`](crate::manager::engine::run_engines), and both share the
/// routes. Each direction has its own loop, so neither waits on the other.
///
/// # Errors
///
/// Returns an internal error, which is a library bug.
pub async fn client_packets_to_messages<F, Fut>(
    connect_request_rx: mpsc::Receiver<ConnectRequest>,
    connect_engine: F,
    channels: ChannelConfig,
    backoff: Option<Backoff>,
) -> Result<(), ManagerError>
where
    F: FnMut(mpsc::Sender<Event>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    let routes = Routes::default();

    // The client-packet loop sends the next request only after the previous
    // engine's result arrived, so one slot is enough.
    let (open_request_tx, open_request_rx) = mpsc::channel(1);

    let (result, ()) = tokio::join!(
        send_client_packets(
            &routes,
            connect_request_rx,
            open_request_tx,
            channels,
            backoff
        ),
        crate::manager::engine::run_engines(&routes, open_request_rx, connect_engine, channels),
    );

    result
}

/// The client-packet loop.
///
/// Opens the first engine at once, like socket.io-client's `Manager`, and
/// another one after the backoff delay whenever an engine closes while
/// namespaces are open. Without an open engine, holds events in each
/// namespace's send buffer and discards acks.
///
/// Closes the engine once no namespace is open, like socket.io-client's
/// `Manager._destroy`, and opens the next one with the next namespace.
///
/// Ends once the client handle and every namespace are gone, so having no
/// namespace at startup or between namespaces keeps it open. Then closes every
/// namespace and waits for the engine to finish. Dropping `open_request_tx`
/// then ends `run_engines`.
///
/// # Panics
///
/// Panics if a client packet arrives for a namespace this loop does not hold,
/// which one entry per name rules out.
#[expect(
    clippy::too_many_lines,
    reason = "one select! arm per input; splitting the arms would pass most of the loop state around"
)]
async fn send_client_packets(
    routes: &Routes,
    mut connect_request_rx: mpsc::Receiver<ConnectRequest>,
    open_request_tx: mpsc::Sender<OpenRequest>,
    channels: ChannelConfig,
    mut backoff: Option<Backoff>,
) -> Result<(), ManagerError> {
    let mut namespaces = HashMap::<ByteString, Namespace>::new();
    let mut client_packets = FuturesUnordered::new();
    let mut reopens = VecDeque::new();
    let mut client_open = true;
    let mut result = Ok(());

    // One ack id counter per name, kept after the namespace closes, so a late
    // ack for a closed namespace never answers an event of the same name
    // reopened. Like socket.io-client's `Socket.ids` on the `Socket` that
    // `Manager.nsps` keeps for each name, which it never prunes.
    let mut next_ack_ids = HashMap::<ByteString, u64>::new();

    // Open the first engine at once, like socket.io-client's `Manager`.
    let mut state = start_engine(&open_request_tx, channels, routes, &namespaces, 0).await?;

    while client_open || !namespaces.is_empty() || !reopens.is_empty() {
        // Every handler sends only to the open engine, so waiting there holds
        // up only this direction, which no other arm could serve anyway.
        tokio::select! {
            connect_request = next_connect_request(&mut reopens, &mut connect_request_rx), if client_open || !reopens.is_empty() => {
                let Some(request) = connect_request else {
                    client_open = false;
                    continue;
                };

                let Some(request) = defer_reopen(request, &mut namespaces, routes) else {
                    continue;
                };

                let ConnectRequest { ns, payload, client_packet_rx, closed, server_packet_tx, terminal_packet_tx, reply_tx } = request;

                if !routes.insert(ns.clone(), server_packet_tx, terminal_packet_tx, closed.clone()) {
                    // A cancelled connect drops its reply receiver.
                    let _ = reply_tx.send(Err(crate::error::SocketError::NamespaceConflict { ns }));
                    continue;
                }

                let namespace = Namespace {
                    ns: ns.clone(),
                    auth: payload.clone(),
                    connected: false,
                    send_buffer: Vec::new(),
                    reopen: None,
                };

                namespaces.insert(ns.clone(), namespace);
                next_ack_ids.entry(ns.clone()).or_default();
                client_packets.push(recv_client_packet(ns.clone(), client_packet_rx, closed));

                match &state {
                    ReadyState::Open(engine) => {
                        send_wire_packet(&engine.client_message_tx, &ns, Packet::Connect(payload), None).await?;
                    }
                    ReadyState::Closed => {
                        reset_backoff(&mut backoff);

                        state = start_engine(&open_request_tx, channels, routes, &namespaces, 0).await?;
                    }
                    // The CONNECT goes out with the next engine.
                    ReadyState::Closing { .. } | ReadyState::Reconnecting(_) => {}
                }

                // The caller may have cancelled connect while the request was queued.
                let _ = reply_tx.send(Ok(()));
            }

            Some((ns, client_packet, client_packet_rx, closed)) = client_packets.next() => {
                let engine = state.engine();

                let Some(client_packet) = client_packet else {
                    let namespace = namespaces
                        .remove(&ns)
                        .expect("a namespace lives until its client packets end");

                    close_namespace(&ns, routes, engine).await?;
                    reopens.extend(namespace.reopen);

                    if routes.is_empty() && reopens.is_empty() {
                        state.destroy();
                    }

                    continue;
                };

                let namespace = namespaces
                    .get_mut(&ns)
                    .expect("a namespace lives until its client packets end");

                send_client_packet(namespace, routes, engine, &mut next_ack_ids, client_packet).await?;
                client_packets.push(recv_client_packet(ns, client_packet_rx, closed));
            }

            event = state.next_event() => match event {
                // The handshake succeeded, so the next drop starts counting
                // attempts again, like socket.io-client's `Manager.onreconnect`.
                EngineEvent::Opened => reset_backoff(&mut backoff),

                EngineEvent::Connected(ns) => {
                    // A late report can find the name closed and reopened, with
                    // the new route not confirmed yet, so check the route.
                    let namespace = namespaces.get_mut(&ns).filter(|_| routes.is_connected(&ns));

                    if let (Some(engine), Some(namespace)) = (state.engine(), namespace) {
                        flush_send_buffer(namespace, engine).await?;
                    }
                }

                EngineEvent::ServerEnded => state.close(true),

                EngineEvent::Closed(engine_result) => {
                    // The finished receiver must never be polled again.
                    let ReadyState::Closing { reconnect, .. } = std::mem::replace(&mut state, ReadyState::Closed) else {
                        unreachable!("only a closing engine reports its result");
                    };

                    match close_engine(engine_result, &mut namespaces, routes, backoff.as_mut(), reconnect) {
                        ControlFlow::Continue(next) => state = next,
                        ControlFlow::Break(error) => {
                            result = Err(error);
                            break;
                        }
                    }

                    // A namespace opened while the client closed the engine, so
                    // the next one opens at once, like socket.io-client's
                    // `Socket.connect`, which calls `Manager.open`.
                    if !reconnect && !routes.is_empty() {
                        reset_backoff(&mut backoff);

                        state = start_engine(&open_request_tx, channels, routes, &namespaces, 0).await?;
                    }
                }

                EngineEvent::ReconnectDue => {
                    let attempt = backoff.as_ref().map_or(0, Backoff::attempts);
                    state = start_engine(&open_request_tx, channels, routes, &namespaces, attempt).await?;
                }
            }
        }
    }

    tracing::debug!("client ended");

    // Closing the namespaces ends their receivers and fails their pending acks.
    routes.clear();
    drop(client_packets);

    state.close(false);

    if let ReadyState::Closing { .. } = state {
        let EngineEvent::Closed(engine_result) = state.next_event().await else {
            unreachable!("a closing engine reports only its result");
        };

        if is_fatal(&engine_result) {
            return engine_result;
        }
    }

    result
}

/// Returns the request if its name is free. Otherwise holds it on the old
/// namespace, whose handles are still draining after the server closed it,
/// or refuses it if the name is open or already has a reopen waiting.
fn defer_reopen(
    request: ConnectRequest,
    namespaces: &mut HashMap<ByteString, Namespace>,
    routes: &Routes,
) -> Option<ConnectRequest> {
    let Some(old) = namespaces.get_mut(&request.ns) else {
        return Some(request);
    };

    if routes.is_open(&request.ns) || old.reopen.is_some() {
        let ns = request.ns;

        // A cancelled connect drops its reply receiver.
        let _ = request
            .reply_tx
            .send(Err(crate::error::SocketError::NamespaceConflict { ns }));
    } else {
        old.reopen = Some(request);
    }

    None
}

/// Handles a finished engine: fails the acks of sent events and picks the
/// next state, like socket.io-client's `Manager.onclose` and `reconnect`.
///
/// Breaks with an internal error. Closes every namespace when reconnection is
/// off or gives up, but keeps the client, like socket.io-client, whose
/// `Manager` and `Socket`s outlive `reconnect_failed`.
fn close_engine(
    engine_result: Result<(), ManagerError>,
    namespaces: &mut HashMap<ByteString, Namespace>,
    routes: &Routes,
    backoff: Option<&mut Backoff>,
    reconnect: bool,
) -> ControlFlow<ManagerError, ReadyState> {
    tracing::debug!("engine closed");

    // Like socket.io-client's `Socket.onclose`, which calls `_clearAcks`.
    routes.clear_acks();

    for namespace in namespaces.values_mut() {
        namespace.connected = false;
    }

    if is_fatal(&engine_result) {
        return ControlFlow::Break(engine_result.unwrap_err());
    }

    // Like `Manager._destroy`, wait for the next namespace instead.
    if !reconnect || routes.is_empty() {
        return ControlFlow::Continue(ReadyState::Closed);
    }

    let Some(backoff) = backoff else {
        tracing::warn!("reconnection is off");
        routes.clear();
        return ControlFlow::Continue(ReadyState::Closed);
    };

    let Some(delay) = backoff.next_delay() else {
        tracing::warn!(attempts = backoff.attempts(), "gave up reconnecting");
        routes.clear();
        return ControlFlow::Continue(ReadyState::Closed);
    };

    tracing::info!(attempt = backoff.attempts(), ?delay, "reconnecting");

    ControlFlow::Continue(ReadyState::Reconnecting(Box::pin(tokio::time::sleep(
        delay,
    ))))
}

/// Logs an engine's network or server error, and returns whether the result is
/// an internal error, which ends the client.
fn is_fatal(engine_result: &Result<(), ManagerError>) -> bool {
    match engine_result {
        Err(error) if error.is_internal() => true,
        Err(error) => {
            tracing::warn!(%error, "engine failed");
            false
        }
        Ok(()) => false,
    }
}

/// Restarts the attempt count, if reconnection is on.
fn reset_backoff(backoff: &mut Option<Backoff>) {
    if let Some(backoff) = backoff {
        backoff.reset();
    }
}

/// Opens an engine and queues CONNECT with the stored auth payload for every
/// open namespace, so the CONNECTs go out first on the new connection.
///
/// Namespaces closed by the server stay closed.
///
/// # Panics
///
/// Panics if `run_engines` has not taken the previous request, which the
/// previous engine's result rules out.
async fn start_engine(
    open_request_tx: &mpsc::Sender<OpenRequest>,
    channels: ChannelConfig,
    routes: &Routes,
    namespaces: &HashMap<ByteString, Namespace>,
    attempt: u32,
) -> Result<ReadyState, ManagerError> {
    let (request, engine) = OpenRequest::new(attempt, channels.engine);

    // Hand the request over before the CONNECTs, because the engine reads
    // them only once it runs.
    open_request_tx
        .try_send(request)
        .expect("run_engines takes each request before it reports that engine's result");

    tracing::debug!(attempt, "engine opened");

    for namespace in namespaces.values() {
        if routes.is_open(&namespace.ns) {
            let packet = Packet::Connect(namespace.auth.clone());
            send_wire_packet(&engine.client_message_tx, &namespace.ns, packet, None).await?;
        }
    }

    Ok(ReadyState::Open(engine))
}

/// Closes one namespace after its client packets end.
///
/// Only a namespace the client closed still has its route. Sends DISCONNECT
/// only if the server confirmed the namespace to the open engine, like
/// socket.io-client's `Socket.disconnect`, because the server closes the whole
/// connection on any other packet for a namespace it has not joined.
async fn close_namespace(
    ns: &ByteString,
    routes: &Routes,
    engine: Option<&OpenHandles>,
) -> Result<(), ManagerError> {
    let Some(connected) = routes.close_client(ns) else {
        return Ok(());
    };

    tracing::debug!(%ns, "client closed");

    if let Some(engine) = engine.filter(|_| connected) {
        send_wire_packet(&engine.client_message_tx, ns, Packet::Disconnect, None).await?;
    }

    Ok(())
}

/// Sends the events a namespace buffered until the server confirmed it.
async fn flush_send_buffer(
    namespace: &mut Namespace,
    engine: &OpenHandles,
) -> Result<(), ManagerError> {
    namespace.connected = true;

    if !namespace.send_buffer.is_empty() {
        tracing::trace!(ns = %namespace.ns, count = namespace.send_buffer.len(), "flushed send buffer");
    }

    for message in namespace.send_buffer.drain(..) {
        engine.client_message_tx.send(message).await?;
    }

    Ok(())
}

/// Encodes one event or ack, holding events until the server confirms the
/// namespace to the open engine.
///
/// Discards packets the handles sent before the server closed the namespace,
/// because the server no longer accepts them, and acks until the server
/// confirms the namespace to the open engine.
async fn send_client_packet(
    namespace: &mut Namespace,
    routes: &Routes,
    engine: Option<&OpenHandles>,
    next_ack_ids: &mut HashMap<ByteString, u64>,
    client_packet: ClientPacket,
) -> Result<(), ManagerError> {
    let ns = &namespace.ns;

    if !routes.is_open(ns) {
        tracing::debug!(%ns, "discarded client packet for a closed namespace");
        return Ok(());
    }

    match client_packet {
        ClientPacket::Event {
            payload,
            ack_tx,
            attachments,
        } => {
            // Register before sending, so the server's answer always finds it.
            let id = ack_tx.map(|ack_tx| {
                let next_ack_id = next_ack_ids
                    .get_mut(ns)
                    .expect("a namespace's counter outlives it");
                let id = *next_ack_id;
                *next_ack_id += 1;
                routes.register_ack(ns, id, ack_tx);
                id
            });

            let packet = match &attachments {
                None => Packet::Event { payload, id },
                Some(attachments) => Packet::BinaryEvent {
                    payload,
                    id,
                    count: attachments.len(),
                },
            };

            let Some(engine) = engine.filter(|_| namespace.connected) else {
                tracing::trace!(%ns, %packet, "buffered packet");

                let messages = encode_packet(ns, &packet, attachments);
                namespace.send_buffer.extend(messages);

                return Ok(());
            };

            send_wire_packet(&engine.client_message_tx, ns, packet, attachments).await
        }
        ClientPacket::Ack {
            payload,
            id,
            attachments,
        } => {
            // The event came from an earlier engine, and its id means nothing
            // to the server now.
            let Some(engine) = engine.filter(|_| routes.is_connected(ns)) else {
                tracing::debug!(%ns, id, "discarded ack for an unconfirmed namespace");
                return Ok(());
            };

            let packet = match &attachments {
                None => Packet::Ack { payload, id },
                Some(attachments) => Packet::BinaryAck {
                    payload,
                    id,
                    count: attachments.len(),
                },
            };

            send_wire_packet(&engine.client_message_tx, ns, packet, attachments).await
        }
    }
}

/// Encodes a packet and sends its messages to the engine.
async fn send_wire_packet(
    client_message_tx: &mpsc::Sender<Message>,
    ns: &ByteString,
    packet: Packet,
    attachments: Option<Vec<Bytes>>,
) -> Result<(), ManagerError> {
    tracing::trace!(%ns, %packet, "sent packet");

    for message in encode_packet(ns, &packet, attachments) {
        client_message_tx.send(message).await?;
    }

    Ok(())
}

/// Encodes a packet as one text message followed by its binary attachments.
fn encode_packet(
    ns: &ByteString,
    packet: &Packet,
    attachments: Option<Vec<Bytes>>,
) -> impl Iterator<Item = Message> {
    let text = Message::Text(packet.encode(ns).into());
    let binaries = attachments.into_iter().flatten().map(Message::Binary);

    std::iter::once(text).chain(binaries)
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::error::TryRecvError;

    use super::*;
    use crate::error::SocketError;

    fn namespace(ns: &str) -> Namespace {
        Namespace {
            ns: ns.into(),
            auth: ByteString::new(),
            connected: false,
            send_buffer: Vec::new(),
            reopen: None,
        }
    }

    #[test]
    fn defer_reopen_passes_a_free_name() {
        let mut namespaces = HashMap::new();
        let (request, _handles) = ConnectRequest::new("/".into(), ByteString::new(), 1, 1);
        assert!(defer_reopen(request, &mut namespaces, &Routes::default()).is_some());
    }

    #[test]
    fn defer_reopen_holds_one_request_while_the_old_namespace_drains() {
        let routes = Routes::default();
        let mut namespaces = HashMap::from([(ByteString::from("/"), namespace("/"))]);

        let (request, mut handles) = ConnectRequest::new("/".into(), ByteString::new(), 1, 1);
        assert!(defer_reopen(request, &mut namespaces, &routes).is_none());
        assert!(namespaces["/"].reopen.is_some());
        assert!(matches!(
            handles.reply_rx.try_recv(),
            Err(TryRecvError::Empty)
        ));

        let (request, mut handles) = ConnectRequest::new("/".into(), ByteString::new(), 1, 1);
        assert!(defer_reopen(request, &mut namespaces, &routes).is_none());
        assert!(matches!(
            handles.reply_rx.try_recv(),
            Ok(Err(SocketError::NamespaceConflict { .. }))
        ));
    }

    #[test]
    fn defer_reopen_refuses_an_open_name() {
        let routes = Routes::default();
        let (server_packet_tx, _server_packet_rx) = mpsc::channel(1);
        let (terminal_packet_tx, _terminal_packet_rx) = oneshot::channel();
        assert!(routes.insert(
            "/".into(),
            server_packet_tx,
            terminal_packet_tx,
            CancellationToken::new()
        ));
        let mut namespaces = HashMap::from([(ByteString::from("/"), namespace("/"))]);

        let (request, mut handles) = ConnectRequest::new("/".into(), ByteString::new(), 1, 1);
        assert!(defer_reopen(request, &mut namespaces, &routes).is_none());
        assert!(namespaces["/"].reopen.is_none());
        assert!(matches!(
            handles.reply_rx.try_recv(),
            Ok(Err(SocketError::NamespaceConflict { .. }))
        ));
    }
}
