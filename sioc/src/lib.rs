#![doc = include_str!("../README.md")]

pub mod ack;
pub mod binary;
pub mod client;
pub mod config;
pub mod error;
pub mod event;
pub mod marker;
pub mod payload;

pub mod prelude {
    pub use crate::ack::{Ack, AckHandle, AckType};
    pub use crate::binary::{AttachmentsBuilder, Placeholder};
    pub use crate::client::{
        Acknowledge, Client, ClientBuilder, Emit, SocketReceiver, SocketSender,
    };
    pub use crate::config::ChannelConfig;
    pub use crate::event::{Event, EventHandler, EventType};
    pub use crate::marker::{AckId, AckMarker, BinaryMarker, HasAck, HasBinary, NoAck, NoBinary};
    pub use crate::payload::{
        DeserializePayload, SerializePayload, ack_from_json, ack_to_json, event_from_json,
        event_to_json,
    };

    pub use sioc_socket::packet::{Connect, ConnectError, DynAck, DynEvent, Ns, Signal};

    pub use sioc_engine::error::WebSocketError;
    pub use sioc_engine::prelude::{TransportStrategy, WebSocketConnector, WebSocketStream};

    pub use sioc_macros::{AckType, DeserializePayload, EventRouter, EventType, SerializePayload};
}
