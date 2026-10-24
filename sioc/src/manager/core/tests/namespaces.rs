use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError;

use super::rig::{Effect, Rig};
use crate::error::SocketError;
use crate::packet::ServerPacket;

type Case = (&'static str, fn(&ServerPacket) -> bool);

#[test]
fn server_end_fails_waiters_and_bypasses_a_full_receiver() {
    let cases: [Case; 2] = [
        ("1", |packet| matches!(packet, ServerPacket::Disconnect)),
        ("4{\"message\":\"no\"}", |packet| {
            matches!(packet, ServerPacket::ConnectError(_))
        }),
    ];

    for (end, is_terminal) in cases {
        let mut rig = Rig::new(8);
        let mut handles = rig.open_with("/", "{}", 1);
        rig.handshake();
        rig.pull();
        // The Connect packet fills the receiver.
        rig.confirm("/");
        rig.effects();

        let mut sent = rig.emit_ack("/", "[\"a\"]");
        rig.pull();
        let mut buffered = rig.emit_ack("/", "[\"b\"]");

        rig.server(end);

        assert_eq!(sent.try_recv().unwrap_err(), TryRecvError::Closed);
        assert_eq!(buffered.try_recv().unwrap_err(), TryRecvError::Closed);
        assert!(is_terminal(&handles.terminal_packet_rx.try_recv().unwrap()));
        assert!(matches!(
            handles.server_packet_rx.try_recv(),
            Ok(ServerPacket::Connect(_))
        ));
        assert!(matches!(
            handles.server_packet_rx.try_recv(),
            Err(MpscTryRecvError::Disconnected)
        ));
        assert!(handles.closed.is_cancelled());
        assert_eq!(rig.effects(), []);

        // The old handles drain to the end of their inbox, which lets the
        // engine go.
        rig.poll("/");
        assert_eq!(rig.effects(), [Effect::Close]);
    }
}

#[test]
fn disconnect_after_confirmation_sends_the_buffer_first() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.emit("/", "[\"e1\"]");
    rig.emit("/", "[\"e2\"]");

    rig.disconnect("/");
    assert_eq!(rig.effects(), []);

    assert_eq!(rig.pull_one().unwrap(), "2[\"e1\"]");
    assert_eq!(rig.pull_one().unwrap(), "2[\"e2\"]");
    assert_eq!(rig.effects(), []);
    assert_eq!(rig.pull_one().unwrap(), "1");
    assert_eq!(rig.effects(), [Effect::Close]);
    assert!(rig.pull_one().is_none());
}

#[test]
fn disconnect_before_confirmation_discards_the_buffer() {
    // CONNECT has not left yet, so the server never hears of the namespace.
    let mut rig = Rig::new(8);
    rig.open("/", "{}");
    rig.emit("/", "[\"e1\"]");
    rig.disconnect("/");
    assert_eq!(rig.pull(), Vec::<String>::new());
    assert_eq!(rig.effects(), [Effect::Open, Effect::Close]);

    // CONNECT left but the server has not confirmed it: no DISCONNECT.
    let mut rig = Rig::new(8);
    rig.open("/", "{}");
    rig.handshake();
    assert_eq!(rig.pull(), ["0{}"]);
    rig.emit("/", "[\"e1\"]");
    rig.disconnect("/");
    assert_eq!(rig.pull(), Vec::<String>::new());
    assert_eq!(rig.effects(), [Effect::Open, Effect::Close]);
}

#[test]
fn reopen_waits_for_the_old_handles_and_keeps_ack_ids() {
    let mut rig = Rig::new(8);
    let _first = rig.connected("/");
    let mut old_ack = rig.emit_ack("/", "[\"x\"]");
    rig.pull();

    rig.server("1");
    assert_eq!(old_ack.try_recv().unwrap_err(), TryRecvError::Closed);

    let mut second = rig.open("/", "{\"n\":2}");
    assert_eq!(second.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(rig.effects(), []);

    let mut third = rig.open("/", "{}");
    assert!(matches!(
        third.reply_rx.try_recv(),
        Ok(Err(SocketError::NamespaceConflict { .. }))
    ));

    // The old inbox ends, so the waiting request takes the name on the same
    // engine.
    rig.poll("/");
    assert!(matches!(second.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), []);

    assert_eq!(rig.pull(), ["0{\"n\":2}"]);
    rig.confirm("/");
    let _new_ack = rig.emit_ack("/", "[\"y\"]");
    assert_eq!(rig.pull(), ["21[\"y\"]"]);
}

#[test]
fn a_reopen_waits_for_the_disconnecting_buffer() {
    let mut rig = Rig::new(8);
    rig.connected("/");
    rig.emit("/", "[\"e1\"]");
    rig.disconnect("/");

    let mut again = rig.open("/", "{\"n\":2}");
    assert_eq!(again.reply_rx.try_recv().unwrap_err(), TryRecvError::Empty);

    assert_eq!(rig.pull(), ["2[\"e1\"]", "1", "0{\"n\":2}"]);
    assert!(matches!(again.reply_rx.try_recv(), Ok(Ok(()))));
    assert_eq!(rig.effects(), []);
}

#[test]
fn engine_loss_while_disconnecting_ends_the_namespace() {
    let mut rig = Rig::reconnecting(8);
    rig.connected("/");
    let mut ack = rig.emit_ack("/", "[\"e1\"]");
    rig.disconnect("/");

    rig.close();
    assert_eq!(ack.try_recv().unwrap_err(), TryRecvError::Closed);
    assert_eq!(rig.effects(), []);
    assert!(rig.core.is_idle());
}
