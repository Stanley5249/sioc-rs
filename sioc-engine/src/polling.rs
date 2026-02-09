//! HTTP long-polling transport for Engine.IO v4.

use crate::ENGINE_IO_VERSION;
use crate::error::{Error, PacketError, Result};
use crate::packet::{EioPacket, Frame, Handshake};
use base64::prelude::*;
use bytes::{BufMut, Bytes, BytesMut};
use reqwest::Client;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use url::Url;

const SEPARATOR: u8 = 0x1e;

fn decode_binary(bytes: Bytes) -> Result<Bytes, PacketError> {
    Ok(Bytes::from(BASE64_STANDARD.decode(&bytes[1..])?))
}

fn encode_binary(buffer: &mut BytesMut, bytes: &[u8]) {
    let b64 = BASE64_STANDARD.encode(bytes);
    buffer.put_u8(b'b');
    buffer.put_slice(b64.as_bytes());
}

fn encode_frames<'a, I>(frames: I) -> Bytes
where
    I: IntoIterator<Item = &'a Frame>,
{
    let mut buffer = BytesMut::new();
    for (i, frame) in frames.into_iter().enumerate() {
        if i > 0 {
            buffer.put_u8(SEPARATOR);
        }
        match frame {
            Frame::Packet(packet) => buffer.put_slice(&packet.encode()),
            Frame::Binary(bytes) => encode_binary(&mut buffer, bytes),
        }
    }
    buffer.freeze()
}

fn decode_frame(bytes: Bytes) -> Result<Frame> {
    if bytes.first().is_some_and(|&b| b == b'b') {
        Ok(Frame::Binary(decode_binary(bytes)?))
    } else {
        Ok(Frame::Packet(EioPacket::decode(bytes)?))
    }
}

/// Decode all frames from a polling response body.
///
/// `Bytes::split` on a separator always yields at least one element, so the
/// returned `Vec` is guaranteed to be non-empty for any non-error response.
fn decode_frames(bytes: Bytes) -> Result<Vec<Frame>> {
    let frames = bytes
        .split(|&b| b == SEPARATOR)
        .map(|s| decode_frame(bytes.slice_ref(s)))
        .collect::<Result<Vec<_>>>()?;

    Ok(frames)
}

async fn get_loop(url: Url, client: Client, tx: mpsc::Sender<Frame>) -> Result<()> {
    loop {
        let bytes = client
            .get(url.clone())
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        for frame in decode_frames(bytes)? {
            tx.send(frame).await?;
        }
    }
}

async fn post_loop(url: Url, client: Client, mut rx: mpsc::Receiver<Frame>) -> Result<()> {
    let mut buffer = Vec::with_capacity(32);

    while rx.recv_many(&mut buffer, 32).await != 0 {
        let body = encode_frames(&buffer);
        buffer.clear();

        let text = client
            .post(url.clone())
            .body(body)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        if text != "ok" {
            return Err(Error::UnexpectedBody {
                description: "expected 'ok' response to polling POST".to_string(),
                body: text,
            });
        }
    }
    Ok(())
}

/// Stateful HTTP long-polling transport for Engine.IO v4.
///
/// Each HTTP GET response may contain multiple frames separated by `0x1e`.
/// A background task continually polls the server and decodes these frames,
/// placing them into a channel consumed via [`recv`](PollingTransport::recv).
///
/// Outgoing frames are sent through a bounded channel to a background
/// `post_loop` task, which batches and POSTs them.
///
/// Dropping the transport closes both background loops via channel drop.
#[derive(Debug)]
pub struct PollingTransport {
    pub url: Url,
    pub client: Client,
    pub rx: mpsc::Receiver<Frame>,
    pub tx: mpsc::Sender<Frame>,
    pub get_handle: JoinHandle<Result<()>>,
    pub post_handle: JoinHandle<Result<()>>,
}

impl PollingTransport {
    /// Establish a polling transport, perform the handshake, and spawn the
    /// background poll and post loops.
    ///
    /// The first GET extracts the `Open` packet and `Handshake`, then appends
    /// `sid` to the URL for all subsequent requests.
    pub async fn connect(base_url: Url, client: Client) -> Result<(Self, Handshake)> {
        let mut url = base_url;

        url.query_pairs_mut()
            .append_pair("EIO", &ENGINE_IO_VERSION.to_string())
            .append_pair("transport", "polling");

        // First GET — extract the Open/Handshake packet.
        let bytes = client
            .get(url.clone())
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        let handshake = match decode_frame(bytes)? {
            Frame::Packet(EioPacket::Open(handshake)) => handshake,
            other => {
                return Err(other.unexpected("expected Open packet as first frame"));
            }
        };

        url.query_pairs_mut().append_pair("sid", &handshake.sid);

        let url = url;

        let (get_tx, get_rx) = mpsc::channel(32);
        let (post_tx, post_rx) = mpsc::channel(32);

        let get_handle = tokio::spawn(get_loop(url.clone(), client.clone(), get_tx));
        let post_handle = tokio::spawn(post_loop(url.clone(), client.clone(), post_rx));

        let transport = Self {
            url,
            client,
            rx: get_rx,
            tx: post_tx,
            get_handle,
            post_handle,
        };

        Ok((transport, handshake))
    }

    /// Receives the next frame from the poll loop.
    pub async fn recv(&mut self) -> Option<Frame> {
        self.rx.recv().await
    }

    /// Sends a frame through the post loop.
    pub async fn send(&mut self, frame: Frame) -> Result<()> {
        Ok(self.tx.send(frame).await?)
    }

    /// Closes the transport by aborting the background tasks.
    pub async fn close(self) -> Result<()> {
        self.get_handle.await??;
        self.post_handle.await??;
        Ok(())
    }
}
