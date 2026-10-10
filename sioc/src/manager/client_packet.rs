//! Sends what the namespace handles ask for.

use std::future::Future;
use std::ops::ControlFlow;

use bytes::Bytes;
use bytestring::ByteString;
use eioc::prelude::{Message, ServerMessage};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::mpsc;

use crate::config::ChannelConfig;
use crate::error::ManagerError;
use crate::manager::backoff::Backoff;
use crate::manager::connect_request::ConnectRequest;
use crate::manager::engine::ReadyState;
use crate::manager::namespaces::{ClientEnd, Namespaces};
use crate::manager::open_request::{EngineEvent, OpenHandles, OpenRequest};
use crate::packet::{ClientPacket, Packet};

/// Receives the next client packet of a namespace, with one send-buffer slot
/// reserved first. Reserving before receiving bounds the work retained outside
/// the client inbox, so an unconfirmed namespace eventually applies
/// backpressure without holding up any other namespace.
///
/// Closing bypasses the buffer reservation and drains accepted packets.
/// Flushed packets still go out; unconfirmed packets are discarded.
async fn recv_client_packet(
    mut client_end: ClientEnd,
) -> (
    ClientEnd,
    Option<ClientPacket>,
    Option<mpsc::OwnedPermit<Vec<Message>>>,
) {
    let buffer_permit = tokio::select! {
        biased;
        () = client_end.closed.cancelled() => {
            client_end.client_packet_rx.close();
            None
        }
        permit = client_end.buffered_messages_tx.clone().reserve_owned() => {
            Some(permit.expect("an open namespace retains its send-buffer receiver"))
        }
    };

    // Prefer the channel, so buffered packets drain before the close takes effect.
    let client_packet = tokio::select! {
        biased;
        client_packet = client_end.client_packet_rx.recv() => client_packet,
        () = client_end.closed.cancelled() => {
            client_end.client_packet_rx.close();
            client_end.client_packet_rx.recv().await
        }
    };

    (client_end, client_packet, buffer_permit)
}

/// Sends what the namespace handles ask for, across engines, until the client
/// ends.
///
/// Runs the client-packet loop beside
/// [`run_engines`](crate::manager::engine::run_engines), and both share the
/// namespaces. Each direction has its own loop, so neither waits on the other.
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
    F: FnMut(mpsc::Sender<ServerMessage>, mpsc::Receiver<Message>) -> Fut,
    Fut: Future<Output = Result<(), eioc::error::Error>>,
{
    let namespaces = Namespaces::default();

    // The client-packet loop sends the next request only after the previous
    // engine's result arrived, so one slot is enough.
    let (open_request_tx, open_request_rx) = mpsc::channel(1);

    let (result, ()) = tokio::join!(
        send_client_packets(
            &namespaces,
            connect_request_rx,
            open_request_tx,
            channels,
            backoff
        ),
        crate::manager::engine::run_engines(&namespaces, open_request_rx, connect_engine, channels),
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
/// Ends once the client handle and every namespace's handles are gone, so
/// having no namespace at startup or between namespaces keeps it open. Then
/// closes every namespace and waits for the engine to finish. Dropping
/// `open_request_tx` then ends `run_engines`.
async fn send_client_packets(
    namespaces: &Namespaces,
    mut connect_request_rx: mpsc::Receiver<ConnectRequest>,
    open_request_tx: mpsc::Sender<OpenRequest>,
    channels: ChannelConfig,
    mut backoff: Option<Backoff>,
) -> Result<(), ManagerError> {
    // One pending receive per namespace with live handles.
    let mut client_packets = FuturesUnordered::new();
    let mut client_open = true;
    let mut state = ReadyState::Closed;

    // Keep all fallible driving inside this block, so every error follows the
    // same namespace cancellation and engine half-close path below.
    let result = async {
        // Open the first engine at once, like socket.io-client's `Manager`.
        state = start_engine(&open_request_tx, channels, namespaces, 0).await?;

        while client_open || !client_packets.is_empty() {
            // Every handler sends only to the open engine, so waiting there
            // holds up only this direction, which no other arm could serve.
            tokio::select! {
                request = connect_request_rx.recv(), if client_open => {
                    let Some(request) = request else {
                        client_open = false;
                        continue;
                    };

                    let opened = open_namespace(
                        request,
                        namespaces,
                        &mut state,
                        &mut backoff,
                        &open_request_tx,
                        channels,
                    )
                    .await?;
                    client_packets.extend(opened.map(recv_client_packet));
                }

                Some((client_end, client_packet, buffer_permit)) = client_packets.next() => {
                    let Some(client_packet) = client_packet else {
                        let reopen = close_namespace(namespaces, &client_end.ns, &state).await?;

                        if let Some(request) = reopen {
                            let opened = open_namespace(
                                request,
                                namespaces,
                                &mut state,
                                &mut backoff,
                                &open_request_tx,
                                channels,
                            )
                            .await?;
                            client_packets.extend(opened.map(recv_client_packet));
                        }

                        if !namespaces.any_open() {
                            state.destroy();
                        }

                        continue;
                    };

                    let engine = state.engine();
                    let ns = &client_end.ns;
                    send_client_packet(namespaces, ns, engine, client_packet, buffer_permit)
                        .await?;
                    client_packets.push(recv_client_packet(client_end));
                }

                event = state.next_event() => match event {
                    // The handshake succeeded, so the next drop starts counting
                    // attempts again, like socket.io-client's `Manager.onreconnect`.
                    EngineEvent::Open => reset_backoff(&mut backoff),

                    EngineEvent::Connect(ns) => {
                        if let Some(engine) = state.engine() {
                            flush_send_buffer(namespaces, &ns, engine).await?;
                        }
                    }

                    EngineEvent::Close(engine_result) => {
                        // The finished receiver must never be polled again.
                        let closing = std::mem::replace(&mut state, ReadyState::Closed);
                        let ReadyState::Closing { reconnect, .. } = closing else {
                            unreachable!("only a closing engine reports its result");
                        };

                        let backoff_mut = backoff.as_mut();
                        match close_engine(engine_result, namespaces, backoff_mut, reconnect) {
                            ControlFlow::Continue(next) => state = next,
                            ControlFlow::Break(error) => return Err(error),
                        }

                        // A namespace opened while the client closed the engine,
                        // so the next one opens at once, like socket.io-client's
                        // `Socket.connect`, which calls `Manager.open`.
                        if !reconnect && namespaces.any_open() {
                            reset_backoff(&mut backoff);

                            state = start_engine(&open_request_tx, channels, namespaces, 0)
                                .await?;
                        }
                    }

                    EngineEvent::Reconnect => {
                        let attempt = backoff.as_ref().map_or(0, Backoff::attempts);
                        state = start_engine(&open_request_tx, channels, namespaces, attempt)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }
    .await;

    tracing::debug!("client ended");

    // Closing the namespaces ends their receivers and fails their pending acks.
    namespaces.close_all();
    drop(client_packets);

    state.close(false);

    if let ReadyState::Closing { .. } = state {
        let EngineEvent::Close(engine_result) = state.next_event().await else {
            unreachable!("a closing engine reports only its result");
        };

        if is_fatal(&engine_result) {
            return engine_result;
        }
    }

    result
}

/// Opens a namespace as [`Namespaces::open`] decides, and returns the client
/// end to receive from.
///
/// Sends CONNECT on the open engine, or opens an engine if none is running.
/// Otherwise the CONNECT goes out with the next engine.
async fn open_namespace(
    request: ConnectRequest,
    namespaces: &Namespaces,
    state: &mut ReadyState,
    backoff: &mut Option<Backoff>,
    open_request_tx: &mpsc::Sender<OpenRequest>,
    channels: ChannelConfig,
) -> Result<Option<ClientEnd>, ManagerError> {
    let auth = request.payload.clone();

    let Some(client_end) = namespaces.open(request, channels.manager) else {
        return Ok(None);
    };

    match state {
        ReadyState::Open(engine) => {
            let packet = Packet::Connect(auth);
            send_wire_packet(&engine.client_message_tx, &client_end.ns, packet, None).await?;
        }
        ReadyState::Closed => {
            reset_backoff(backoff);

            *state = start_engine(open_request_tx, channels, namespaces, 0).await?;
        }
        ReadyState::Closing { .. } | ReadyState::Reconnecting(_) => {}
    }

    Ok(Some(client_end))
}

/// Closes one namespace after its client packets end, and returns the request
/// waiting to open the name again.
///
/// Sends DISCONNECT only if the server confirmed the namespace to the open
/// engine, like socket.io-client's `Socket.disconnect`, because the server
/// closes the whole connection on any other packet for a namespace it has not
/// joined.
async fn close_namespace(
    namespaces: &Namespaces,
    ns: &ByteString,
    state: &ReadyState,
) -> Result<Option<ConnectRequest>, ManagerError> {
    let (connected, reopen) = namespaces.close_client(ns);

    tracing::debug!(%ns, "client closed");

    if let Some(engine) = state.engine().filter(|_| connected) {
        send_wire_packet(&engine.client_message_tx, ns, Packet::Disconnect, None).await?;
    }

    Ok(reopen)
}

/// Handles a finished engine: fails the acks of sent events and picks the
/// next state, like socket.io-client's `Manager.onclose` and `reconnect`.
///
/// Breaks with an internal error. Closes every namespace when reconnection is
/// off or gives up, but keeps the client, like socket.io-client, whose
/// `Manager` and `Socket`s outlive `reconnect_failed`.
fn close_engine(
    engine_result: Result<(), ManagerError>,
    namespaces: &Namespaces,
    backoff: Option<&mut Backoff>,
    reconnect: bool,
) -> ControlFlow<ManagerError, ReadyState> {
    tracing::debug!("engine closed");

    // Like socket.io-client's `Socket.onclose`, which calls `_clearAcks`.
    namespaces.clear_acks();

    if is_fatal(&engine_result) {
        return ControlFlow::Break(engine_result.unwrap_err());
    }

    // Like `Manager._destroy`, wait for the next namespace instead.
    if !reconnect || !namespaces.any_open() {
        return ControlFlow::Continue(ReadyState::Closed);
    }

    let Some(backoff) = backoff else {
        tracing::warn!("reconnection is off");
        namespaces.close_all();
        return ControlFlow::Continue(ReadyState::Closed);
    };

    let Some(delay) = backoff.next_delay() else {
        tracing::warn!(attempts = backoff.attempts(), "gave up reconnecting");
        namespaces.close_all();
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
    namespaces: &Namespaces,
    attempt: u32,
) -> Result<ReadyState, ManagerError> {
    let (request, engine) = OpenRequest::new(attempt, channels.engine);

    // Hand the request over before the CONNECTs, because the engine reads
    // them only once it runs.
    open_request_tx
        .try_send(request)
        .expect("run_engines takes each request before it reports that engine's result");

    tracing::debug!(attempt, "engine opened");

    for (ns, auth) in namespaces.auths() {
        send_wire_packet(&engine.client_message_tx, &ns, Packet::Connect(auth), None).await?;
    }

    Ok(ReadyState::Open(engine))
}

/// Sends the events a namespace buffered until the server confirmed it.
async fn flush_send_buffer(
    namespaces: &Namespaces,
    ns: &ByteString,
    engine: &OpenHandles,
) -> Result<(), ManagerError> {
    // A late report can find the name closed and reopened, with the new
    // namespace not confirmed yet, which `flush` skips.
    let Some(messages) = namespaces.flush(ns) else {
        return Ok(());
    };

    if !messages.is_empty() {
        tracing::trace!(%ns, count = messages.len(), "flushed send buffer");
    }

    for message in messages {
        engine.client_message_tx.send(message).await?;
    }

    Ok(())
}

/// Encodes one event or ack, holding events until the client-packet loop
/// flushed the namespace's send buffer.
///
/// Discards packets the handles sent before the server closed the namespace,
/// because the server no longer accepts them, and acks until the server
/// confirms the namespace to the open engine.
async fn send_client_packet(
    namespaces: &Namespaces,
    ns: &ByteString,
    engine: Option<&OpenHandles>,
    client_packet: ClientPacket,
    buffer_permit: Option<mpsc::OwnedPermit<Vec<Message>>>,
) -> Result<(), ManagerError> {
    if !namespaces.is_open(ns) {
        tracing::debug!(%ns, "discarded client packet for a closed namespace");
        return Ok(());
    }

    match client_packet {
        ClientPacket::Event {
            payload,
            ack_tx,
            attachments,
        } => {
            let engine = engine.filter(|_| namespaces.is_flushed(ns));
            if engine.is_none() && buffer_permit.is_none() {
                tracing::debug!(%ns, "discarded buffered event for a closing namespace");
                return Ok(());
            }

            // Register before sending, so the server's answer always finds it.
            let id = ack_tx.map(|ack_tx| namespaces.register_ack(ns, ack_tx));

            let packet = match &attachments {
                None => Packet::Event { payload, id },
                Some(attachments) => Packet::BinaryEvent {
                    payload,
                    id,
                    count: attachments.len(),
                },
            };

            let Some(engine) = engine else {
                tracing::trace!(%ns, %packet, "buffered packet");

                if let Some(permit) = buffer_permit {
                    permit.send(encode_packet(ns, &packet, attachments).collect());
                }

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
            let Some(engine) = engine.filter(|_| namespaces.is_confirmed(ns)) else {
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
