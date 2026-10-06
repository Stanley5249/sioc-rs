//! Single polling requests.

use bytestring::ByteString;
use reqwest::Client;
use url::Url;

use crate::ENGINE_IO_VERSION;
use crate::error::PollingError;
use crate::packet::Frame;

/// Builds the polling URL by appending the EIO version and transport
/// parameters.
pub fn polling_url(mut base_url: Url) -> Url {
    base_url
        .query_pairs_mut()
        .append_pair("EIO", ENGINE_IO_VERSION)
        .append_pair("transport", "polling");
    base_url
}

pub async fn get_frames(client: &Client, url: &Url) -> Result<Vec<Frame>, PollingError> {
    let response = client
        .get(url.as_str())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    tracing::trace!(bytes = response.len(), "received polling payload");
    crate::polling::payload::decode_payload(&ByteString::from(response))
}

pub async fn post_frames(client: &Client, url: &Url, frames: &[Frame]) -> Result<(), PollingError> {
    let body = crate::polling::payload::encode_payload(frames);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polling_url_appends_params() {
        let base = Url::parse("http://localhost:3000/socket.io/").unwrap();
        let url = polling_url(base);
        let query = url.query().unwrap();
        assert!(query.contains("EIO=4"));
        assert!(query.contains("transport=polling"));
    }
}
