use base64::prelude::*;
use bytes::{BufMut, BytesMut};
use http::HeaderMap;
use reqwest::{Client, ClientBuilder};
use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use url::Url;

use crate::error::{Error, Result};
use crate::packet::{Message, Packet};

/// Polling sender - the write half
#[derive(Debug, Clone)]
pub struct PollingSender {
    client: Client,
    base_url: Url,
}

impl PollingSender {
    /// Sends a message through polling (HTTP POST)
    pub async fn send(&mut self, msg: Message) -> Result<()> {
        let body = match msg {
            Message::Binary(data) => {
                let encoded = BASE64_STANDARD.encode(&data);
                let mut result = BytesMut::with_capacity(1 + encoded.len());
                result.put_u8(b'b');
                result.extend_from_slice(encoded.as_bytes());
                result.freeze()
            }
            Message::Text(data) => data,
        };

        let mut url = self.base_url.clone();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        url.query_pairs_mut().append_pair("t", &timestamp);

        let status = self
            .client
            .post(url)
            .body(body)
            .send()
            .await?
            .status()
            .as_u16();

        if status != 200 {
            return Err(Error::IncompleteHttp(status));
        }

        Ok(())
    }
}

/// Polling receiver - the read half
#[derive(Debug)]
pub struct PollingReceiver {
    rx: mpsc::Receiver<Result<Packet>>,
}

impl PollingReceiver {
    pub async fn recv(&mut self) -> Result<Packet> {
        self.rx.recv().await.ok_or(Error::Closed)?
    }
}

/// Creates a new polling connection and splits it into sender/receiver
pub async fn connect(
    base_url: Url,
    headers: Option<HeaderMap>,
) -> (PollingSender, PollingReceiver) {
    let client = match headers.clone() {
        Some(map) => ClientBuilder::new().default_headers(map).build().unwrap(),
        None => Client::new(),
    };

    let mut url = base_url;
    url.query_pairs_mut().append_pair("transport", "polling");

    let sender = PollingSender {
        client: client.clone(),
        base_url: url.clone(),
    };

    // Create channel for background task communication
    let (tx, rx) = mpsc::channel(32);

    // Spawn background polling task
    tokio::spawn(async move {
        // Move URL construction OUTSIDE the loop
        let base_url = url.clone(); // The base URL with transport=polling

        loop {
            // Only clone if strictly necessary for the client.get() builder
            // Optimized: Modify query pairs on a clone for this request only
            let mut request_url = base_url.clone();

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string();

            request_url.query_pairs_mut().append_pair("t", &timestamp);

            match client.get(request_url).send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    if status != 200 {
                        let _ = tx.send(Err(Error::IncompleteHttp(status))).await;
                        break;
                    }

                    match response.bytes().await {
                        Ok(bytes) => {
                            // Parse payload - packets separated by \x1e
                            const SEPARATOR: u8 = 0x1e;
                            let packets: Result<Vec<Packet>> = bytes
                                .split(|&c| c == SEPARATOR)
                                .filter(|slice| !slice.is_empty()) // Skip empty slices
                                .map(|slice| Packet::try_from(bytes.slice_ref(slice)))
                                .collect();

                            match packets {
                                Ok(packets) => {
                                    // Send each packet through the channel
                                    for packet in packets {
                                        if tx.send(Ok(packet)).await.is_err() {
                                            // Receiver dropped, exit
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e.into())).await;
                            break;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e.into())).await;
                    // Don't break on network errors, retry
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
            }

            // Removed sleep for instant long-polling
        }
    });

    let receiver = PollingReceiver { rx };

    (sender, receiver)
}
