//! Outgoing packets and the shared lifetime of namespace sender clones.

use tokio::sync::mpsc;

use crate::client::SocketSender;
use crate::error::{PayloadError, SocketError};
use crate::event::Emit;
use crate::manager::connect_request::ConnectRequest;
use crate::marker::{AckMarker, HasAck, NoAck, NoBinary};
use crate::packet::ClientPacket;

struct TestEmit;

impl Emit<NoAck, NoBinary> for TestEmit {
    type Output = ();

    fn prepare(self) -> Result<(ClientPacket, ()), PayloadError> {
        Ok((
            ClientPacket::Event {
                payload: r#"["test"]"#.into(),
                ack_tx: None,
                attachments: None,
            },
            (),
        ))
    }
}

fn socket_sender() -> (SocketSender, mpsc::Receiver<ClientPacket>) {
    let (request, handles) = ConnectRequest::new("/".into(), "".into(), 8, 8);
    let (sender, _receiver, _reply) = handles.into_socket();
    (sender, request.client_packet_rx)
}

#[tokio::test]
async fn dropping_last_clone_closes_client_packet_channel() {
    let (sender, mut client_packet_rx) = socket_sender();
    let clone = sender.clone();
    drop(sender);
    client_packet_rx.try_recv().unwrap_err();
    drop(clone);
    assert!(client_packet_rx.recv().await.is_none());
}

#[tokio::test]
async fn disconnect_closes_every_clone_and_keeps_sent_packets() {
    let (sender, mut client_packet_rx) = socket_sender();
    let clone = sender.clone();
    sender.emit(TestEmit).await.unwrap();
    sender.disconnect();
    assert!(matches!(
        clone.emit(TestEmit).await,
        Err(SocketError::Closed)
    ));
    // The manager drains what was sent before;
    // `client_close_sends_earlier_packets_first` covers the rest.
    assert!(matches!(
        client_packet_rx.try_recv(),
        Ok(ClientPacket::Event { .. })
    ));
    client_packet_rx.try_recv().unwrap_err();
}

#[tokio::test]
async fn disconnect_after_close_returns() {
    let (sender, client_packet_rx) = socket_sender();
    drop(client_packet_rx);
    sender.disconnect();
    sender.disconnect();
}

#[tokio::test]
async fn closed_resolves_for_every_clone_after_disconnect() {
    let (sender, _client_packet_rx) = socket_sender();
    let clone = sender.clone();
    sender.disconnect();
    clone.closed().await;
}

#[tokio::test]
async fn rejects_invalid_binary_attachment_counts_before_enqueueing() {
    let (sender, mut rx) = socket_sender();
    for count in [0, 11] {
        for is_ack in [false, true] {
            let attachments = Some(vec![bytes::Bytes::new(); count]);
            let packet = if is_ack {
                ClientPacket::Ack {
                    payload: "[]".into(),
                    id: 0,
                    attachments,
                }
            } else {
                ClientPacket::Event {
                    payload: r#"["x"]"#.into(),
                    ack_tx: None,
                    attachments,
                }
            };
            assert!(
                matches!(sender.send(packet).await, Err(SocketError::AttachmentCount { count: actual }) if actual == count),
                "{count}, ack={is_ack}"
            );
            assert!(matches!(
                rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
    }
}

#[tokio::test]
async fn emit_sends_event_packet() {
    let (sender, mut rx) = socket_sender();
    sender.emit(TestEmit).await.unwrap();
    assert!(matches!(rx.try_recv().unwrap(), ClientPacket::Event { .. }));
}

#[tokio::test]
async fn emit_returns_error_on_closed_channel() {
    let (sender, rx) = socket_sender();
    drop(rx);
    assert!(matches!(
        sender.emit(TestEmit).await,
        Err(SocketError::Closed)
    ));
}

#[tokio::test]
async fn acknowledge_sends_ack_packet_with_correct_id() {
    let (mut request, handles) = ConnectRequest::new("/".into(), "".into(), 8, 8);
    let (ack_packet_tx, mut ack_packet_rx) = mpsc::channel(8);
    // Publish the separate route that manager confirmation installs.
    request.ack_route_tx.send_replace(Some(ack_packet_tx));
    let (sender, _receiver, _reply) = handles.into_socket();
    let id = HasAck::<()>::parse(Some(5)).unwrap();
    sender.acknowledge(id, ()).await.unwrap();
    let ClientPacket::Ack { id, .. } = ack_packet_rx.try_recv().unwrap() else {
        panic!("expected Ack packet");
    };
    assert_eq!(id, 5);
    assert!(matches!(
        request.client_packet_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
