//! Single steps on one WebSocket stream: opening it, probing an upgrade, and
//! sending or receiving one frame.

use futures_util::SinkExt;
use tracing::Instrument;
use url::Url;

use crate::ENGINE_IO_VERSION;
use crate::connector::{WebSocketConnector, WebSocketStream};
use crate::error::WebSocketError;
use crate::packet::{Frame, PROBE, Packet};

/// Builds the WebSocket URL by converting the scheme and appending
/// EIO/transport/sid parameters.
pub fn websocket_url(mut url: Url, sid: Option<&str>) -> Url {
    let scheme = match url.scheme() {
        "http" => Some("ws"),
        "https" => Some("wss"),
        _ => None,
    };

    if let Some(scheme) = scheme {
        url.set_scheme(scheme)
            .expect("http, https, ws, and wss are special schemes, so they swap freely");
    }

    {
        let mut query = url.query_pairs_mut();

        query
            .append_pair("EIO", ENGINE_IO_VERSION)
            .append_pair("transport", "websocket");

        if let Some(sid) = sid {
            query.append_pair("sid", sid);
        }
    }

    url
}

/// Opens a [`WebSocketStream`], running the upgrade probe when `sid` is
/// present.
///
/// # Errors
///
/// Returns an error if the connection or probe fails.
pub async fn connect<C>(
    base_url: Url,
    sid: Option<&str>,
    connector: C,
) -> Result<WebSocketStream, WebSocketError>
where
    C: WebSocketConnector,
{
    let url = websocket_url(base_url, sid);

    let span = tracing::debug_span!("connect", %url);

    let mut stream = connector.connect(url).instrument(span).await?;

    if sid.is_some() {
        probe_upgrade(&mut stream).await?;
    }

    Ok(stream)
}

/// Waits for the next frame, returning an error if the stream is closed.
pub async fn recv_frame(stream: &mut WebSocketStream) -> Result<Frame, WebSocketError> {
    crate::websocket::message::next_frame(stream)
        .await?
        .ok_or(WebSocketError::Closed)
}

/// Sends one frame.
pub async fn send_frame(stream: &mut WebSocketStream, frame: Frame) -> Result<(), WebSocketError> {
    Ok(stream
        .send(crate::websocket::message::encode_frame(frame))
        .await?)
}

/// Sends a probe `Ping` and expects a matching `Pong`, confirming the WebSocket
/// path is live.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn probe_upgrade(stream: &mut WebSocketStream) -> Result<(), WebSocketError> {
    tracing::debug!("sent probe ping");

    send_frame(stream, Packet::Ping(PROBE).into()).await?;

    match recv_frame(stream).await? {
        Frame::Packet(Packet::Pong(payload)) if payload == PROBE => {
            tracing::debug!("received probe pong");
        }

        frame => return Err(WebSocketError::Probe(frame)),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_url_http_becomes_ws() {
        let url = Url::parse("http://localhost:3000/socket.io/").unwrap();
        let ws = websocket_url(url, None);
        assert_eq!(ws.scheme(), "ws");
        let q = ws.query().unwrap();
        assert!(q.contains("EIO=4"));
        assert!(q.contains("transport=websocket"));
    }

    #[test]
    fn websocket_url_https_becomes_wss() {
        let url = Url::parse("https://example.com/socket.io/").unwrap();
        let ws = websocket_url(url, None);
        assert_eq!(ws.scheme(), "wss");
    }

    #[test]
    fn websocket_url_appends_sid() {
        let url = Url::parse("http://localhost/socket.io/").unwrap();
        let ws = websocket_url(url, Some("abc123"));
        assert!(ws.query().unwrap().contains("sid=abc123"));
    }

    #[test]
    fn websocket_url_no_sid_when_none() {
        let url = Url::parse("http://localhost/socket.io/").unwrap();
        let ws = websocket_url(url, None);
        assert!(!ws.query().unwrap().contains("sid="));
    }

    #[test]
    fn websocket_url_unknown_scheme_unchanged() {
        let url = Url::parse("ws://localhost/socket.io/").unwrap();
        let ws = websocket_url(url, None);
        assert_eq!(ws.scheme(), "ws");
    }
}
