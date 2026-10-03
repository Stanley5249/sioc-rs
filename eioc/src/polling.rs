//! HTTP long-polling transport tasks for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::error::{PollingError, TransportError, WebSocketError};
use crate::packet::{Frame, Handshake, Packet};
use crate::prelude::WebSocketStream;
use crate::websocket::WebSocketConnector;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use bytestring::ByteString;
use reqwest::Client;
use std::pin::pin;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use url::Url;

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

/// Builds the polling URL by appending the EIO version and transport parameters.
fn polling_url(mut base_url: Url) -> Url {
    base_url
        .query_pairs_mut()
        .append_pair("EIO", ENGINE_IO_VERSION)
        .append_pair("transport", "polling");
    base_url
}

/// Wraps a [`reqwest::Client`] with Engine.IO HTTP polling helpers.
#[derive(Clone)]
pub struct PollingClient(pub Client);

impl PollingClient {
    async fn get(&self, url: &Url) -> Result<Vec<Frame>, PollingError> {
        let response = self
            .0
            .get(url.as_str())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        tracing::trace!(bytes = response.len(), "<- GET");
        decode_frames(&ByteString::from(response))
    }

    async fn get_frame(&self, url: &Url) -> Result<Frame, PollingError> {
        let response = self
            .0
            .get(url.as_str())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        tracing::trace!(bytes = response.len(), "<- GET");

        Frame::decode(&ByteString::from(response))
    }

    async fn post(&self, url: &Url, frames: &[Frame]) -> Result<(), PollingError> {
        let body = encode_frames(frames);
        tracing::trace!(bytes = body.len(), "-> POST");

        let response = self
            .0
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

    /// Batches client frames into POST requests until `pause` fires or the engine closes `client_frame_rx`.
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn post_client_frames(
        &self,
        url: &Url,
        client_frame_rx: &mut mpsc::Receiver<Frame>,
        pause: &CancellationToken,
    ) -> Result<Stop, TransportError> {
        let mut buffer = Vec::with_capacity(8);

        loop {
            // Pause only while idle, because a POST in flight may carry frames.
            let count = tokio::select! {
                () = pause.cancelled() => {
                    tracing::debug!("paused polling POST");
                    return Ok(Stop::Paused);
                }
                count = client_frame_rx.recv_many(&mut buffer, 8) => count,
            };

            if count == 0 {
                return Ok(Stop::Ended);
            }

            self.post(url, &buffer).await?;
            buffer.clear();
        }
    }

    /// Forwards server frames to the engine until `pause` fires or the server sends `Close`.
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn get_server_frames(
        &self,
        url: &Url,
        server_frame_tx: &mpsc::Sender<Frame>,
        pause: &CancellationToken,
    ) -> Result<Stop, TransportError> {
        // Pause only between requests, because the server answers the GET in
        // flight once the upgrade probe succeeds, and the answer may carry frames.
        while !pause.is_cancelled() {
            for frame in self.get(url).await? {
                let close = frame == Frame::Packet(Packet::Close);

                server_frame_tx.send(frame).await?;

                if close {
                    return Ok(Stop::Ended);
                }
            }
        }

        tracing::debug!("paused polling GET");

        Ok(Stop::Paused)
    }

    /// Runs the GET and POST loops until both pause or either one ends the session.
    async fn poll(
        &self,
        url: &Url,
        server_frame_tx: &mpsc::Sender<Frame>,
        client_frame_rx: &mut mpsc::Receiver<Frame>,
        pause: &CancellationToken,
    ) -> Result<Stop, TransportError> {
        let mut get = pin!(self.get_server_frames(url, server_frame_tx, pause));
        let mut post = pin!(self.post_client_frames(url, client_frame_rx, pause));

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

    /// Polls until the session ends, or until `upgrade` connects and polling pauses.
    ///
    /// Returns the upgraded stream, or `None` if the session ended first. A failed
    /// upgrade falls back to long polling for the rest of the session.
    async fn poll_until_upgraded(
        &self,
        url: &Url,
        server_frame_tx: &mpsc::Sender<Frame>,
        client_frame_rx: &mut mpsc::Receiver<Frame>,
        upgrade: impl Future<Output = Result<WebSocketStream, WebSocketError>>,
    ) -> Result<Option<WebSocketStream>, TransportError> {
        let pause = CancellationToken::new();
        let mut poll = pin!(self.poll(url, server_frame_tx, client_frame_rx, &pause));

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
                    tracing::warn!(%error, "upgrade failed, falling back to long polling");

                    poll.await?;

                    Ok(None)
                }
            },
        }
    }

    /// Runs the full polling transport lifecycle: handshake, GET/POST loops, and optional WebSocket upgrade.
    ///
    /// Finishes once the engine closes `client_frame_rx`. If the server ends the session first,
    /// drops `server_frame_tx` and discards client frames until the engine closes `client_frame_rx`.
    ///
    /// # Errors
    ///
    /// Returns an error if a network, protocol, or channel failure occurs.
    #[tracing::instrument(skip_all, err)]
    pub async fn transport<C>(
        self,
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

        let handshake = match self.get_frame(&url).instrument(span).await? {
            Frame::Packet(Packet::Open(handshake)) => handshake,
            frame => return Err(TransportError::Open(frame)),
        };

        url.query_pairs_mut().append_pair("sid", &handshake.sid);
        let can_upgrade = handshake.can_upgrade_to_websocket();
        let sid = handshake.sid.clone();

        handshake_tx
            .send(handshake)
            .map_err(TransportError::SendHandshake)?;

        let stream = if can_upgrade {
            let upgrade = WebSocketStream::connect(base_url, Some(&sid), connector);

            self.poll_until_upgraded(&url, &server_frame_tx, &mut client_frame_rx, upgrade)
                .await?
        } else {
            self.poll(
                &url,
                &server_frame_tx,
                &mut client_frame_rx,
                &CancellationToken::new(),
            )
            .await?;

            None
        };

        if let Some(stream) = stream {
            tracing::debug!("paused polling transport");

            return stream
                .transport(None, server_frame_tx, client_frame_rx)
                .await;
        }

        // The engine may queue frames before it learns the server ended the
        // session, and the closed session cannot accept them.
        drop(server_frame_tx);
        while client_frame_rx.recv().await.is_some() {}

        Ok(())
    }
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
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn bss(s: &'static str) -> ByteString {
        ByteString::from_static(s)
    }

    /// Answers every HTTP request with `body` after `delay`, or never when `body` is `None`.
    async fn http_server(body: Option<&'static str>, delay: Duration) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move {
            loop {
                let (mut tcp, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buffer = [0; 4096];
                    while tcp.read(&mut buffer).await.is_ok_and(|n| n > 0) {
                        let Some(body) = body else { continue };
                        tokio::time::sleep(delay).await;
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
        let client = PollingClient(Client::new());
        let stop = client
            .get_server_frames(&url, &server_frame_tx, &pause)
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
        let client = PollingClient(Client::new());
        let stop = client
            .get_server_frames(&url, &server_frame_tx, &CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(stop, Stop::Ended));
        assert!(matches!(
            server_frame_rx.recv().await.unwrap(),
            Frame::Packet(Packet::Close)
        ));
    }

    #[tokio::test]
    async fn poll_abandons_get_when_engine_closes() {
        let url = http_server(None, Duration::ZERO).await;
        let (server_frame_tx, _frame_rx) = mpsc::channel(4);
        let (client_frame_tx, mut client_frame_rx) = mpsc::channel(4);
        drop(client_frame_tx);
        let client = PollingClient(Client::new());
        let pause = CancellationToken::new();
        let poll = client.poll(&url, &server_frame_tx, &mut client_frame_rx, &pause);
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
        let client = PollingClient(Client::new());
        let upgrade = async { Err(WebSocketError::Closed) };
        let stream = client
            .poll_until_upgraded(&url, &server_frame_tx, &mut client_frame_rx, upgrade)
            .await
            .unwrap();
        assert!(stream.is_none());
        assert!(matches!(
            server_frame_rx.recv().await.unwrap(),
            Frame::Packet(Packet::Close)
        ));
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
