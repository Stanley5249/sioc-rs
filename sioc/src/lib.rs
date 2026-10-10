#![doc = include_str!("../README.md")]

pub mod ack;
pub mod binary;
pub mod client;
pub mod config;
pub mod error;
pub mod event;
// The manager runs inside the task that `ClientBuilder::open` spawns, behind
// `Client::join` and the namespace handles, so it has no public surface.
mod manager;
pub mod marker;
pub mod packet;
pub mod payload;

/// Convenience re-exports for common usage.
pub mod prelude {
    pub use eioc::prelude::{TransportStrategy, WebSocketConnector, WebSocketStream};
    pub use sioc_macros::{AckType, DeserializePayload, EventRouter, EventType, SerializePayload};

    pub use crate::ack::{Ack, AckHandle, Acknowledge};
    pub use crate::binary::{AttachmentsBuilder, MAX_ATTACHMENTS, Placeholder};
    pub use crate::client::{Client, ClientBuilder, SocketReceiver, SocketSender};
    pub use crate::config::{ChannelConfig, ReconnectionConfig};
    pub use crate::event::{Emit, Event, EventHandler, EventRouter};
    pub use crate::marker::{
        AckId, AckMarker, AckType, BinaryMarker, EventType, HasAck, HasBinary, NoAck, NoBinary,
    };
    pub use crate::packet::{Connect, ConnectError, DynAck, DynEvent, Ns, ServerPacket};
    pub use crate::payload::{
        DeserializePayload, SerializePayload, ack_from_json, ack_to_json, event_from_json,
        event_to_json,
    };
}
