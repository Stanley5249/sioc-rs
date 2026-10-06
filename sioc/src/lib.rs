#![doc = include_str!("../README.md")]

pub mod ack;
pub mod binary;
pub mod client;
pub mod error;
pub mod event;
mod manager;
pub mod marker;
pub mod packet;
pub mod payload;

pub use client::SocketSender;

/// Convenience re-exports for common usage.
pub mod prelude {
    pub use crate::ack::{Ack, AckHandle, AckType};
    pub use crate::binary::{AttachmentsBuilder, Placeholder};
    pub use crate::client::{
        Acknowledge, ChannelConfig, Client, ClientBuilder, Emit, SocketReceiver, SocketSender,
    };
    pub use crate::event::{Event, EventHandler, EventRouter, EventType};
    pub use crate::marker::{AckId, AckMarker, BinaryMarker, HasAck, HasBinary, NoAck, NoBinary};
    pub use crate::packet::{Connect, ConnectError, DynAck, DynEvent, Ns, ServerPacket};
    pub use crate::payload::{
        DeserializePayload, SerializePayload, ack_from_json, ack_to_json, event_from_json,
        event_to_json,
    };

    pub use eioc::prelude::{TransportStrategy, WebSocketConnector, WebSocketStream};

    pub use sioc_macros::{AckType, DeserializePayload, EventRouter, EventType, SerializePayload};
}
