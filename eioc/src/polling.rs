//! HTTP long-polling transport tasks for Engine.IO v4.

use std::pin::pin;

use base64::prelude::{BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use bytestring::ByteString;
use reqwest::Client;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use url::Url;

use crate::ENGINE_IO_VERSION;
use crate::error::{PollingError, TransportError, WebSocketError};
use crate::packet::{Frame, Handshake, Packet};
use crate::websocket::{WebSocketConnector, WebSocketStream};

const SEPARATOR: char = '\x1e';

impl Frame {
    fn decode_binary(data: &[u8]) -> Result<Self, PollingError> {
        Ok(Self::Binary(Bytes::from(BASE64_STANDARD.decode(data)?)))
    }

    fn decode_packet(bytes: &ByteString) -> Result<Self, PollingError> {
        Ok(Self::Packet(Packet::decode(bytes)?))
    }

    fn decode(bytes: &ByteString) -> Result<Self, PollingError> {
        let mut chars = bytes.chars();

        if chars.next().is_some_and(|b| b == 'b') {
            Self::decode_binary(chars.as_str().as_bytes())
        } else {
            Self::decode_packet(bytes)
        }
    }

    fn write(&self, buffer: &mut String) {
        match self {
            Frame::Packet(packet) => packet.write(buffer),
            Frame::Binary(bytes) => {
                buffer.push('b');
                BASE64_STANDARD.encode_string(bytes, buffer);
            }
        }
    }
}

fn decode_frames(bytes: &ByteString) -> Result<Vec<Frame>, PollingError> {
    bytes
        .split(SEPARATOR)
        .map(|s| Frame::decode(&bytes.slice_ref(s)))
        .collect()
}

fn encode_frames(frames: &[Frame]) -> String {
    let mut buffer = String::new();
    for (i, frame) in frames.iter().enumerate() {
        if i > 0 {
            buffer.push(SEPARATOR);
        }
        frame.write(&mut buffer);
    }
    buffer
}

/// Takes an ordered prefix whose wire encoding fits the handshake limit.
/// A single oversized frame travels alone, matching engine.io-client.
fn take_batch(
    first: Frame,
    frames: impl Iterator<Item = Frame>,
    max_payload: u64,
) -> (Vec<Frame>, Option<Frame>) {
    let mut size = encode_frames(std::slice::from_ref(&first)).len() as u64;
    let mut batch = vec![first];
    // python-engineio accepts at most sixteen packets per POST. Its handshake
    // advertises only a byte limit, so keep that interoperability ceiling too.
    for frame in frames.take(15) {
        let next_size = encode_frames(std::slice::from_ref(&frame)).len() as u64;
        if size.saturating_add(1).saturating_add(next_size) > max_payload {
            return (batch, Some(frame));
        }
        size += 1 + next_size;
        batch.push(frame);
    }
    (batch, None)
}

/// Builds the polling URL by appending the EIO version and transport
/// parameters.
fn polling_url(mut base_url: Url) -> Url {
    base_url
        .query_pairs_mut()
        .append_pair("EIO", ENGINE_IO_VERSION)
        .append_pair("transport", "polling");
    base_url
}

async fn get(client: &Client, url: &Url) -> Result<Vec<Frame>, PollingError> {
    let response = client
        .get(url.as_str())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    tracing::trace!(bytes = response.len(), "received polling payload");
    decode_frames(&ByteString::from(response))
}

async fn get_frame(client: &Client, url: &Url) -> Result<Frame, PollingError> {
    let response = client
        .get(url.as_str())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    tracing::trace!(bytes = response.len(), "received polling payload");

    Frame::decode(&ByteString::from(response))
}

async fn post(client: &Client, url: &Url, frames: &[Frame]) -> Result<(), PollingError> {
    let body = encode_frames(frames);
    tracing::trace!(bytes = body.len(), "sent polling payload");

    let response = client
        .post(url.as_str())
        .body(body)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    if !response.eq_ignore_ascii_case("ok") {
        return Err(PollingError::Response(response));
    }

    Ok(())
}

/// Batches client frames into POST requests until `pause` fires or the engine
/// closes `client_frame_rx`.
///
/// When the engine closes `client_frame_rx`, posts a `Close` packet to end the
/// session.
///
/// Batches contain at most sixteen frames and respect the handshake's wire-byte
/// limit. A single oversized frame travels alone, matching engine.io-client's
/// batching behavior.
#[tracing::instrument(level = "debug", skip_all)]
async fn post_client_frames(
    client: &Client,
    url: &Url,
    client_frame_rx: &mut mpsc::Receiver<Frame>,
    pause: &CancellationToken,
    max_payload: u64,
) -> Result<Stop, TransportError> {
    let mut pending = None;

    loop {
        // Pause only while idle, because a POST in flight may carry frames.
        let first = if let Some(frame) = pending.take() {
            Some(frame)
        } else {
            tokio::select! {
                () = pause.cancelled() => {
                    tracing::debug!("paused polling post");
                    return Ok(Stop::Paused);
                }
                frame = client_frame_rx.recv() => frame,
            }
        };

        let Some(first) = first else {
            tracing::trace!("sent close packet");

            post(client, url, &[Packet::Close.into()]).await?;

            return Ok(Stop::Ended);
        };

        // Empty or disconnected means the ready prefix has ended. Pending
        // frames are posted before honoring an upgrade pause.
        let ready = std::iter::from_fn(|| {
            if pause.is_cancelled() {
                None
            } else {
                client_frame_rx.try_recv().ok()
            }
        });
        let (buffer, remainder) = take_batch(first, ready, max_payload);
        pending = remainder;

        post(client, url, &buffer).await?;
    }
}

/// Forwards server frames to the engine until `pause` fires or the server sends
/// `Close`.
///
/// The `Close` packet itself stays here, because the transport ending is
/// what tells the engine the session is over.
#[tracing::instrument(level = "debug", skip_all)]
async fn get_server_frames(
    client: &Client,
    url: &Url,
    server_frame_tx: &mpsc::Sender<Frame>,
    pause: &CancellationToken,
) -> Result<Stop, TransportError> {
    // Pause only between requests, because the server answers the GET in
    // flight once the upgrade probe succeeds, and the answer may carry frames.
    while !pause.is_cancelled() {
        for frame in get(client, url).await? {
            if frame == Frame::Packet(Packet::Close) {
                tracing::debug!("server closed");

                return Ok(Stop::Ended);
            }

            server_frame_tx.send(frame).await?;
        }
    }

    tracing::debug!("paused polling get");

    Ok(Stop::Paused)
}

/// Runs the GET and POST loops until both pause or either one ends the session.
async fn poll(
    client: &Client,
    url: &Url,
    server_frame_tx: &mpsc::Sender<Frame>,
    client_frame_rx: &mut mpsc::Receiver<Frame>,
    pause: &CancellationToken,
    max_payload: u64,
) -> Result<Stop, TransportError> {
    let mut get = pin!(get_server_frames(client, url, server_frame_tx, pause));
    let mut post = pin!(post_client_frames(
        client,
        url,
        client_frame_rx,
        pause,
        max_payload
    ));

    // A pause lets the other loop finish its request. An ended session
    // abandons it, because its result no longer matters.
    tokio::select! {
        stop = &mut get => match stop? {
            Stop::Paused => post.await,
            Stop::Ended => Ok(Stop::Ended),
        },
        stop = &mut post => match stop? {
            Stop::Paused => get.await,
            Stop::Ended => Ok(Stop::Ended),
        },
    }
}

/// Polls until the session ends, or until `upgrade` connects and polling
/// pauses.
///
/// Returns the upgraded stream, or `None` if the session ended first. A failed
/// upgrade falls back to long polling for the rest of the session.
async fn poll_until_upgraded(
    client: &Client,
    url: &Url,
    server_frame_tx: &mpsc::Sender<Frame>,
    client_frame_rx: &mut mpsc::Receiver<Frame>,
    upgrade: impl Future<Output = Result<WebSocketStream, WebSocketError>>,
    max_payload: u64,
) -> Result<Option<WebSocketStream>, TransportError> {
    let pause = CancellationToken::new();
    let mut poll = pin!(poll(
        client,
        url,
        server_frame_tx,
        client_frame_rx,
        &pause,
        max_payload
    ));

    tokio::select! {
        stop = &mut poll => {
            stop?;
            Ok(None)
        }
        result = upgrade => match result {
            Ok(stream) => {
                pause.cancel();

                match poll.await? {
                    Stop::Paused => Ok(Some(stream)),
                    Stop::Ended => Ok(None),
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to upgrade, continuing long polling");

                poll.await?;

                Ok(None)
            }
        },
    }
}

/// Runs the full polling transport lifecycle: handshake, GET/POST loops, and
/// optional WebSocket upgrade.
///
/// Finishes once the engine closes `client_frame_rx`. If the server ends the
/// session first, drops `server_frame_tx` and discards client frames until the
/// engine closes `client_frame_rx`.
///
/// # Errors
///
/// Returns an error if a network, protocol, or channel failure occurs.
#[tracing::instrument(skip_all)]
pub async fn transport<C>(
    client: Client,
    base_url: Url,
    connector: C,
    handshake_tx: oneshot::Sender<Handshake>,
    server_frame_tx: mpsc::Sender<Frame>,
    mut client_frame_rx: mpsc::Receiver<Frame>,
) -> Result<(), TransportError>
where
    C: WebSocketConnector,
{
    let mut url = polling_url(base_url.clone());

    let span = tracing::debug_span!("connect", %url);

    let handshake = match get_frame(&client, &url).instrument(span).await? {
        Frame::Packet(Packet::Open(handshake)) => handshake,
        frame => return Err(TransportError::Open(frame)),
    };

    url.query_pairs_mut().append_pair("sid", &handshake.sid);
    let can_upgrade = handshake.can_upgrade_to_websocket();
    let sid = handshake.sid.clone();
    let max_payload = handshake.max_payload;

    handshake_tx
        .send(handshake)
        .map_err(TransportError::Handshake)?;

    let stream = if can_upgrade {
        let upgrade = crate::websocket::connect(base_url, Some(&sid), connector);

        poll_until_upgraded(
            &client,
            &url,
            &server_frame_tx,
            &mut client_frame_rx,
            upgrade,
            max_payload,
        )
        .await?
    } else {
        poll(
            &client,
            &url,
            &server_frame_tx,
            &mut client_frame_rx,
            &CancellationToken::new(),
            max_payload,
        )
        .await?;

        None
    };

    if let Some(stream) = stream {
        tracing::debug!("paused polling transport");

        return crate::websocket::transport(stream, None, server_frame_tx, client_frame_rx).await;
    }

    // The engine may queue frames before it learns the server ended the
    // session, and the closed session cannot accept them.
    drop(server_frame_tx);
    while client_frame_rx.recv().await.is_some() {}

    Ok(())
}
/// Why a polling loop stopped.
enum Stop {
    /// The upgrade paused polling.
    Paused,
    /// The engine or the server ended the session.
    Ended,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_count_encoded_bytes_and_keep_order() {
        let text = || Packet::Message("a".into()).into();
        for (limit, count) in [(4, 1), (5, 2), (6, 2), (8, 3)] {
            let (batch, pending) = take_batch(text(), [text(), text()].into_iter(), limit);
            assert_eq!(batch.len(), count, "limit {limit}");
            assert_eq!(pending.is_some(), count < 3);
        }
        let binary = Frame::Binary(Bytes::from_static(b"abc")); // bYWJj = 5 bytes
        let unicode = Frame::Packet(Packet::Message("台".into())); // 4 UTF-8 bytes
        let (batch, pending) = take_batch(binary.clone(), [unicode.clone()].into_iter(), 9);
        assert_eq!(batch, [binary]);
        assert_eq!(pending, Some(unicode));

        let oversized: Frame = Packet::Message("oversized".into()).into();
        let (batch, pending) = take_batch(oversized.clone(), [text()].into_iter(), 1);
        assert_eq!(batch, [oversized]);
        assert_eq!(pending, Some(text()));

        let mut frames = (0..32).map(|_| text());
        let (batch, pending) = take_batch(text(), frames.by_ref(), u64::MAX);
        assert_eq!(batch.len(), 16);
        assert!(pending.is_none());
        assert_eq!(frames.count(), 17);
    }

    #[tokio::test]
    async fn posts_respect_limit_and_close_after_all_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for _ in 0..3 {
                let (mut tcp, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let (header_end, length) = loop {
                    let mut chunk = [0; 1024];
                    let count = tcp.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&chunk[..count]);
                    if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if request.len() >= end + 4 + length {
                            break (end + 4, length);
                        }
                    }
                };
                bodies.push(
                    String::from_utf8(request[header_end..header_end + length].to_vec()).unwrap(),
                );
                tcp.write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                )
                .await
                .unwrap();
            }
            bodies
        });
        let (tx, mut rx) = mpsc::channel(4);
        for payload in ["aaaa", "bbbb", "cccc"] {
            tx.send(Packet::Message(payload.into()).into())
                .await
                .unwrap();
        }
        drop(tx);
        post_client_frames(&Client::new(), &url, &mut rx, &CancellationToken::new(), 11)
            .await
            .unwrap();
        assert_eq!(server.await.unwrap(), ["4aaaa\x1e4bbbb", "4cccc", "1"]);
    }
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn bss(s: &'static str) -> ByteString {
        ByteString::from_static(s)
    }

    /// Answers every HTTP request with `body` after `delay`, or never when
    /// `body` is `None`.
    async fn http_server(body: Option<&'static str>, delay: Duration) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move {
            loop {
                let (mut tcp, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buffer = [0; 4096];
                    while tcp.read(&mut buffer).await.is_ok_and(|n| n > 0) {
                        // POSTs always succeed; GETs answer `body` after `delay`, or never.
                        let body = if buffer.starts_with(b"POST") {
                            "ok"
                        } else {
                            let Some(body) = body else { continue };
                            tokio::time::sleep(delay).await;
                            body
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if tcp.write_all(response.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        url
    }

    #[tokio::test]
    async fn get_finishes_request_in_flight_when_paused() {
        let url = http_server(Some("4data"), Duration::from_millis(100)).await;
        let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
        let pause = CancellationToken::new();
        let pause_later = pause.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            pause_later.cancel();
        });
        let client = Client::new();
        let stop = get_server_frames(&client, &url, &server_frame_tx, &pause)
            .await
            .unwrap();
        assert!(matches!(stop, Stop::Paused));
        assert!(matches!(
            server_frame_rx.recv().await.unwrap(),
            Frame::Packet(Packet::Message(m)) if m == "data"
        ));
    }

    #[tokio::test]
    async fn get_ends_at_server_close() {
        let url = http_server(Some("1"), Duration::ZERO).await;
        let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
        let client = Client::new();
        let stop = get_server_frames(&client, &url, &server_frame_tx, &CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(stop, Stop::Ended));
        server_frame_rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    async fn poll_abandons_get_when_engine_closes() {
        let url = http_server(None, Duration::ZERO).await;
        let (server_frame_tx, _frame_rx) = mpsc::channel(4);
        let (client_frame_tx, mut client_frame_rx) = mpsc::channel(4);
        drop(client_frame_tx);
        let client = Client::new();
        let pause = CancellationToken::new();
        let poll = poll(
            &client,
            &url,
            &server_frame_tx,
            &mut client_frame_rx,
            &pause,
            1_000_000,
        );
        let stop = tokio::time::timeout(Duration::from_secs(5), poll)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(stop, Stop::Ended));
    }

    #[tokio::test]
    async fn failed_upgrade_falls_back_to_long_polling() {
        let url = http_server(Some("1"), Duration::from_millis(50)).await;
        let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
        let (_transport_tx, mut client_frame_rx) = mpsc::channel(4);
        let client = Client::new();
        let upgrade = async { Err(WebSocketError::Closed) };
        let stream = poll_until_upgraded(
            &client,
            &url,
            &server_frame_tx,
            &mut client_frame_rx,
            upgrade,
            1_000_000,
        )
        .await
        .unwrap();
        assert!(stream.is_none());
        server_frame_rx.try_recv().unwrap_err();
    }

    #[test]
    fn polling_url_appends_params() {
        let base = Url::parse("http://localhost:3000/socket.io/").unwrap();
        let url = polling_url(base);
        let query = url.query().unwrap();
        assert!(query.contains("EIO=4"));
        assert!(query.contains("transport=polling"));
    }

    #[test]
    fn frame_decode_text_packet() {
        let frame = Frame::decode(&bss("4hello")).unwrap();
        assert!(matches!(frame, Frame::Packet(Packet::Message(m)) if m == "hello"));
    }

    #[test]
    fn frame_decode_binary_base64() {
        use base64::prelude::{BASE64_STANDARD, Engine as _};
        let encoded = BASE64_STANDARD.encode(b"abc");
        let input = ByteString::from(format!("b{encoded}"));
        let frame = Frame::decode(&input).unwrap();
        assert!(matches!(frame, Frame::Binary(b) if b.as_ref() == b"abc"));
    }

    #[test]
    fn frame_decode_invalid_base64_is_error() {
        Frame::decode(&bss("b!!!")).unwrap_err();
    }

    #[test]
    fn frame_write_packet() {
        let frame = Frame::Packet(Packet::Message("hello".into()));
        let mut buf = String::new();
        frame.write(&mut buf);
        assert_eq!(buf, "4hello");
    }

    #[test]
    fn frame_write_binary() {
        use base64::prelude::{BASE64_STANDARD, Engine as _};
        let raw = Bytes::from_static(b"abc");
        let frame = Frame::Binary(raw);
        let mut buf = String::new();
        frame.write(&mut buf);
        assert_eq!(buf, format!("b{}", BASE64_STANDARD.encode(b"abc")));
    }

    #[test]
    fn decode_encode_frames_roundtrip() {
        let text = bss("4hello\x1e4world");
        let frames = decode_frames(&text).unwrap();
        assert_eq!(frames.len(), 2);
        let encoded = encode_frames(&frames);
        assert_eq!(encoded, "4hello\x1e4world");
    }

    #[test]
    fn encode_frames_single() {
        let frames = vec![Frame::Packet(Packet::Pong("probe".into()))];
        assert_eq!(encode_frames(&frames), "3probe");
    }

    #[test]
    fn encode_frames_empty() {
        assert_eq!(encode_frames(&[]), "");
    }

    #[test]
    fn decode_frames_error_propagates() {
        decode_frames(&bss("9invalid")).unwrap_err();
    }
}
