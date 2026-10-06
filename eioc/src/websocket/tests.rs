//! Tests for the WebSocket transport.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMsg;
use tokio_tungstenite::{MaybeTlsStream, accept_async, client_async};
use tokio_util::sync::CancellationToken;

use crate::connector::WebSocketStream;
use crate::error::{TransportError, WebSocketError};
use crate::packet::{Frame, Handshake, Packet};

#[tokio::test]
async fn forward_client_frames_drains_after_peer_close() {
    let (client, mut server) = ws_pair().await;
    let (sink, mut stream) = client.split();
    server.close(None).await.unwrap();
    assert!(
        crate::websocket::message::next_frame(&mut stream)
            .await
            .unwrap()
            .is_none()
    );

    // The reader has changed tungstenite's state before its stop signal
    // reaches the writer. A queued frame must still finish gracefully.
    let (client_frame_tx, client_frame_rx) = mpsc::channel(1);
    client_frame_tx
        .send(Packet::Message("late".into()).into())
        .await
        .unwrap();
    drop(client_frame_tx);
    crate::websocket::forward::forward_client_frames(
        sink,
        client_frame_rx,
        CancellationToken::new(),
    )
    .await
    .unwrap();
}

async fn ws_pair() -> (
    WebSocketStream,
    tokio_tungstenite::WebSocketStream<TcpStream>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        accept_async(tcp).await.unwrap()
    });
    let tcp = TcpStream::connect(addr).await.unwrap();
    let (ws, _) = client_async("ws://127.0.0.1/", MaybeTlsStream::Plain(tcp))
        .await
        .unwrap();
    let client = ws;
    let server = server_task.await.unwrap();
    (client, server)
}

#[tokio::test]
async fn recv_frame_decodes_text_frame_as_packet() {
    let (mut client, mut server) = ws_pair().await;
    server.send(WsMsg::text("4hello")).await.unwrap();
    let frame = crate::websocket::stream::recv_frame(&mut client)
        .await
        .unwrap();
    assert!(matches!(frame, Frame::Packet(Packet::Message(m)) if m == "hello"));
}

#[tokio::test]
async fn next_frame_ends_at_peer_close_frame() {
    let (mut client, mut server) = ws_pair().await;
    server.send(WsMsg::Close(None)).await.unwrap();
    assert!(
        crate::websocket::message::next_frame(&mut client)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn next_frame_ends_at_close_packet() {
    let (mut client, mut server) = ws_pair().await;
    server.send(WsMsg::text("1")).await.unwrap();
    assert!(
        crate::websocket::message::next_frame(&mut client)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn recv_frame_decodes_binary_frame() {
    let (mut client, mut server) = ws_pair().await;
    server.send(WsMsg::binary(b"data".as_ref())).await.unwrap();
    let frame = crate::websocket::stream::recv_frame(&mut client)
        .await
        .unwrap();
    assert!(matches!(frame, Frame::Binary(b) if b.as_ref() == b"data"));
}

#[tokio::test]
async fn recv_frame_invalid_packet_id_is_error() {
    let (mut client, mut server) = ws_pair().await;
    server.send(WsMsg::text("9bad")).await.unwrap();
    crate::websocket::stream::recv_frame(&mut client)
        .await
        .unwrap_err();
}

#[tokio::test]
async fn send_frame_encodes_packet_as_text() {
    let (mut client, mut server) = ws_pair().await;
    crate::websocket::stream::send_frame(&mut client, Frame::Packet(Packet::Pong("probe".into())))
        .await
        .unwrap();
    let msg = server.next().await.unwrap().unwrap();
    assert_eq!(msg.to_text().unwrap(), "3probe");
}

#[tokio::test]
async fn send_frame_encodes_binary() {
    let (mut client, mut server) = ws_pair().await;
    crate::websocket::stream::send_frame(&mut client, Frame::Binary(Bytes::from_static(b"raw")))
        .await
        .unwrap();
    let msg = server.next().await.unwrap().unwrap();
    assert!(msg.is_binary());
    assert_eq!(msg.into_data().as_ref(), b"raw");
}

#[tokio::test]
async fn probe_upgrade_succeeds_on_matching_pong() {
    let (mut client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        let msg = server.next().await.unwrap().unwrap();
        assert_eq!(msg.to_text().unwrap(), "2probe");
        server.send(WsMsg::text("3probe")).await.unwrap();
    });
    crate::websocket::stream::probe_upgrade(&mut client)
        .await
        .unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn probe_upgrade_fails_on_wrong_frame() {
    let (mut client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        let _ = server.next().await.unwrap().unwrap();
        server.send(WsMsg::text("4unexpected")).await.unwrap();
    });
    assert!(matches!(
        crate::websocket::stream::probe_upgrade(&mut client).await,
        Err(WebSocketError::Probe(_))
    ));
    server_task.await.unwrap();
}

#[tokio::test]
async fn run_no_handshake_sends_upgrade() {
    let (client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        let msg = server.next().await.unwrap().unwrap();
        assert_eq!(msg.to_text().unwrap(), "5");
        while let Some(Ok(_)) = server.next().await {}
    });
    let (server_frame_tx, _) = mpsc::channel(4);
    let (_, client_frame_rx) = mpsc::channel::<Frame>(4);
    crate::websocket::session::run(client, None, server_frame_tx, client_frame_rx)
        .await
        .unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn run_with_handshake_reads_open_packet() {
    let open_text = r#"0{"sid":"abc","upgrades":[],"pingInterval":25000,"pingTimeout":5000,"maxPayload":1000000}"#;
    let (client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        server.send(WsMsg::text(open_text)).await.unwrap();
        while let Some(Ok(_)) = server.next().await {}
    });
    let (handshake_tx, handshake_rx) = oneshot::channel();
    let (server_frame_tx, _) = mpsc::channel(4);
    let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
    drop(client_frame_tx);
    crate::websocket::session::run(client, Some(handshake_tx), server_frame_tx, client_frame_rx)
        .await
        .unwrap();
    assert_eq!(&*handshake_rx.await.unwrap().sid, "abc");
    server_task.await.unwrap();
}

#[tokio::test]
async fn run_with_handshake_non_open_is_error() {
    let (client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        server.send(WsMsg::text("4hello")).await.unwrap();
        while let Some(Ok(_)) = server.next().await {}
    });
    let (handshake_tx, _) = oneshot::channel();
    let (server_frame_tx, _) = mpsc::channel(4);
    let (_, client_frame_rx) = mpsc::channel::<Frame>(4);
    let result = crate::websocket::session::run(
        client,
        Some(handshake_tx),
        server_frame_tx,
        client_frame_rx,
    )
    .await;
    assert!(matches!(result, Err(TransportError::Open(_))));
    let _ = server_task.await;
}

#[tokio::test]
async fn run_with_handshake_dropped_receiver_is_error() {
    let open_text = r#"0{"sid":"abc","upgrades":[],"pingInterval":25000,"pingTimeout":5000,"maxPayload":1000000}"#;
    let (client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        server.send(WsMsg::text(open_text)).await.unwrap();
        while let Some(Ok(_)) = server.next().await {}
    });
    let (handshake_tx, handshake_rx) = oneshot::channel::<Handshake>();
    drop(handshake_rx);
    let (server_frame_tx, _) = mpsc::channel(4);
    let (_, client_frame_rx) = mpsc::channel::<Frame>(4);
    let result = crate::websocket::session::run(
        client,
        Some(handshake_tx),
        server_frame_tx,
        client_frame_rx,
    )
    .await;
    assert!(matches!(result, Err(TransportError::Handshake(_))));
    let _ = server_task.await;
}

#[tokio::test]
async fn run_forwards_server_frame_to_engine() {
    let (client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        let _ = server.next().await; // consume Upgrade
        server.send(WsMsg::text("4data")).await.unwrap();
        let _ = server.close(None).await;
        while let Some(Ok(_)) = server.next().await {}
    });
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
    let transport = tokio::spawn(crate::websocket::session::run(
        client,
        None,
        server_frame_tx,
        client_frame_rx,
    ));
    let action = server_frame_rx.recv().await.unwrap();
    assert!(matches!(action, Frame::Packet(Packet::Message(m)) if m == "data"));
    drop(client_frame_tx);
    transport.await.unwrap().unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn run_discards_client_frames_after_server_close() {
    let (client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        let _ = server.next().await; // consume Upgrade
        server.close(None).await.unwrap();
        while let Some(Ok(_)) = server.next().await {}
    });
    let (server_frame_tx, mut server_frame_rx) = mpsc::channel(4);
    let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
    let transport = tokio::spawn(crate::websocket::session::run(
        client,
        None,
        server_frame_tx,
        client_frame_rx,
    ));
    assert!(server_frame_rx.recv().await.is_none());
    client_frame_tx
        .send(Frame::Packet(Packet::Close))
        .await
        .unwrap();
    drop(client_frame_tx);
    transport.await.unwrap().unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn run_sends_client_frames_while_engine_is_full() {
    let (client, mut server) = ws_pair().await;
    let (server_frame_tx, _frame_rx) = mpsc::channel(1);
    let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
    tokio::spawn(crate::websocket::session::run(
        client,
        None,
        server_frame_tx,
        client_frame_rx,
    ));
    let _ = server.next().await; // consume Upgrade
    for text in ["4fills", "4blocks"] {
        server.send(WsMsg::text(text)).await.unwrap();
    }
    client_frame_tx
        .send(Frame::Packet(Packet::Message("out".into())))
        .await
        .unwrap();
    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), server.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.to_text().unwrap(), "4out");
}

#[tokio::test]
async fn run_forwards_client_frame_to_server() {
    let (client, mut server) = ws_pair().await;
    let server_task = tokio::spawn(async move {
        let msg = server.next().await.unwrap().unwrap();
        assert_eq!(msg.to_text().unwrap(), "5");
        let msg = server.next().await.unwrap().unwrap();
        assert_eq!(msg.to_text().unwrap(), "4out");
        let msg = server.next().await.unwrap().unwrap();
        assert_eq!(msg.to_text().unwrap(), "1");
        while let Some(Ok(_)) = server.next().await {}
    });
    let (server_frame_tx, _) = mpsc::channel(4);
    let (client_frame_tx, client_frame_rx) = mpsc::channel::<Frame>(4);
    client_frame_tx
        .send(Frame::Packet(Packet::Message("out".into())))
        .await
        .unwrap();
    drop(client_frame_tx);
    crate::websocket::session::run(client, None, server_frame_tx, client_frame_rx)
        .await
        .unwrap();
    server_task.await.unwrap();
}
