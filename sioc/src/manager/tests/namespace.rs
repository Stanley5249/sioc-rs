//! Namespace reuse keeps old deliveries and cleanup with the old handles.

use tokio::sync::{mpsc, oneshot};

use super::rig::{Rig, event_ack};
use crate::manager::state::ManagerInput;

#[test]
fn reopen_discards_the_old_held_delivery_before_installing_new_handles() {
    let mut rig = Rig::new();
    let old = rig.connect("/", "{}", 1);
    rig.handshake();
    rig.messages();
    rig.confirm("/");
    rig.server("2[\"old\"]");
    assert!(rig.core.held_sender().is_some());

    old.closed.cancel();
    let mut new = rig.connect("/", "{}", 1);
    assert_eq!(
        new.reply_rx.try_recv().unwrap_err(),
        oneshot::error::TryRecvError::Empty
    );
    rig.step(ManagerInput::Disconnect("/".into()));

    assert!(rig.core.held_sender().is_none());
    new.reply_rx.try_recv().unwrap().unwrap();
    assert_eq!(
        new.server_packet_rx.try_recv().unwrap_err(),
        mpsc::error::TryRecvError::Empty
    );
}

#[test]
fn repeated_termination_keeps_the_old_participation_draining() {
    let mut rig = Rig::new();
    let _old = rig.connected("/");
    let (packet, mut waiter) = event_ack("[\"old\"]");
    rig.send("/", packet);
    rig.messages();
    rig.server("1");
    let mut new = rig.connect("/", "{}", 1);
    rig.server("1");
    rig.server("4{\"message\":\"again\"}");
    assert_eq!(
        new.reply_rx.try_recv().unwrap_err(),
        oneshot::error::TryRecvError::Empty
    );
    assert_eq!(
        waiter.try_recv().unwrap_err(),
        oneshot::error::TryRecvError::Closed
    );
    rig.step(ManagerInput::Disconnect("/".into()));
    new.reply_rx.try_recv().unwrap().unwrap();
}

#[test]
fn partial_binary_input_stays_with_the_old_participation_during_reuse() {
    for header_after_end in [false, true] {
        let mut rig = Rig::new();
        let _keep_engine = rig.connected("/keep");
        let old = rig.connected("/");
        if !header_after_end {
            rig.server(r#"51-["old",{"_placeholder":true,"num":0}]"#);
        }
        old.closed.cancel();
        rig.step(ManagerInput::Disconnect("/".into()));
        rig.messages();
        if header_after_end {
            rig.server(r#"51-["old",{"_placeholder":true,"num":0}]"#);
        }
        let mut new = rig.connect("/", "{}", 1);
        rig.messages();

        rig.server_binary(b"old attachment");
        assert_eq!(
            new.server_packet_rx.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty
        );
        rig.confirm("/");
        new.server_packet_rx.try_recv().unwrap();
        rig.server(r#"51-["new",{"_placeholder":true,"num":0}]"#);
        rig.server_binary(b"new attachment");
        new.server_packet_rx.try_recv().unwrap();
    }
}
