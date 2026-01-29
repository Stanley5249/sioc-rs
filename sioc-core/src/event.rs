//! Type-safe event handling with acknowledgements.
//!
//! The `Event` trait defines how to convert typed event enums into
//! Socket.IO packets for transmission. It's typically implemented automatically
//! via the `#[derive(Event)]` macro.
//!
//! ## Design
//!
//! - **Event Name Extraction**: Get the Socket.IO event name for each variant
//! - **Serialization in User Thread**: JSON serialization happens before channel send
//! - **Binary Support**: Automatically handle `BinaryIndex` fields
//! - **Type Safety**: Compile-time verification of event structure and ack types
//!
//! ## Example
//!
use serde::{Serialize, de::DeserializeOwned};

/// ```rust,ignore
/// use sioc_macros::Event;
/// use serde::Serialize;
///
/// #[derive(Event, Serialize)]
/// enum ClientEvents {
///     #[event("chat")]
///     Chat { message: String }, // Ack type: ()
///
///     #[event("ping")]
///     Ping(i32), // Ack type: String
/// }
///
/// impl Event for ClientEvents {
///     type Ack = String; // For this example, assuming Ping expects String ack
///     // ... other methods
/// }
/// ```
use crate::{error::Result, packet::EventPayload};

/// Trait for types that can be emitted as Socket.IO events.
///
/// This trait defines how to convert a typed event into a `Packet` that
/// can be sent over the Socket.IO connection. It handles serialization
/// and packet construction, and specifies the expected acknowledgement type.
///
/// # Derivable
///
/// This trait is typically derived using `#[derive(Event)]`:
///
/// ```rust,ignore
/// use sioc_macros::Event;
/// use serde::Serialize;
///
/// #[derive(Event, Serialize)]
/// enum ClientEvents {
///     #[event("message")]
///     Message(String), // Ack type: ()
///
///     #[event("join")]
///     Join { room: String }, // Ack type: bool
/// }
/// ```
///
/// # Implementation Details
///
/// The generated implementation:
///
/// 1. Extracts the event name from the `#[event("...")]` attribute
/// 2. Serializes the payload using `serde_json`
/// 3. Wraps the result in a JSON array: `["event_name", payload]`
/// 4. Returns an `EventPayload` with the serialized data
///
/// # Binary Events
///
/// If a variant contains a field of type `BinaryIndex`, the macro
/// automatically generates code to:
///
/// 1. Serialize the struct with `BinaryIndex` placeholders
/// 2. Return a `Packet` with `PacketType::BinaryEvent`
/// 3. Indicate that binary attachments should be added separately
///
/// ```rust,ignore
/// use sioc_core::prelude::*;
/// use serde::Serialize;
///
/// #[derive(Event, Serialize)]
/// enum ClientEvents {
///     #[event("upload")]
///     Upload {
///         filename: String,
///         data: BinaryIndex, // Auto-detected by macro
///     }, // Ack type: String
/// }
/// ```
///
/// Manual Implementation
///
/// Manual implementation is possible but not recommended. Here's an example:
///
/// ```rust
/// use sioc_core::prelude::*;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// enum MyEvents {
///     Ping(i32), // Ack type: String
/// }
///
/// impl Event for MyEvents {
///     type Ack = String;
///
///     fn name(&self) -> &'static str {
///         match self {
///             MyEvents::Ping(_) => "ping",
///         }
///     }
///
///     fn into_event_payload(&self) -> Result<EventPayload> {
///         let event_name = self.name();
///         let payload = match self {
///             MyEvents::Ping(val) => val,
///         };
///
///         // Serialize as tuple: (event_name, payload)
///         let json = serde_json::to_vec(&(event_name, payload))
///             .map_err(|e| Error::Json(e))?;
///
///         Ok(EventPayload {
///             id: None,
///             data: bytes::Bytes::from(json),
///             attachments: Attachments::new(),
///         })
///     }
/// }
/// ```
pub trait Event: Sized + Serialize {
    /// The expected response type for this event.
    /// Use `()` if no specific response is expected.
    type Ack: DeserializeOwned + Send;

    /// The event name (e.g., "login", "chat").
    fn name(&self) -> &'static str;

    /// Convert this event into a `Packet` ready for transmission.
    ///
    /// This method handles serialization and packet construction.
    /// The serialization happens in the caller's thread, not in the Engine.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use sioc_core::prelude::*;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// enum MyEvents {
    ///     Ping(i32),
    /// }
    ///
    /// impl Event for MyEvents {
    ///     type Ack = ();
    ///
    ///     fn name(&self) -> &'static str {
    ///         "ping"
    ///     }
    ///
    ///     fn into_event_payload(&self) -> Result<EventPayload> {
    ///         let data = serde_json::to_vec(&(self.name(), self))?;
    ///         Ok(EventPayload {
    ///             id: None,
    ///             data: bytes::Bytes::from(data),
    ///             attachments: Attachments::new(),
    ///         })
    ///     }
    /// }
    ///
    /// let event = MyEvents::Ping(42);
    /// let payload = event.into_event_payload()?;
    /// // payload.data contains ["ping", 42]
    /// ```
    fn into_event_payload(&self) -> Result<EventPayload>
    where
        Self: Serialize;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde::Serialize;

    #[derive(Serialize)]
    enum TestEvents {
        Message(String),
        Count(i32),
    }

    impl Event for TestEvents {
        type Ack = (); // Simplified for testing

        fn name(&self) -> &'static str {
            match self {
                TestEvents::Message(_) => "message",
                TestEvents::Count(_) => "count",
            }
        }

        fn into_event_payload(&self) -> Result<EventPayload> {
            let event_name = self.name();
            let json = match self {
                TestEvents::Message(s) => serde_json::to_vec(&(event_name, s))?,
                TestEvents::Count(n) => serde_json::to_vec(&(event_name, n))?,
            };
            Ok(EventPayload {
                id: None,
                data: Bytes::from(json),
                attachments: Default::default(),
            })
        }
    }

    #[test]
    fn test_event_name() {
        let event = TestEvents::Message("hello".into());
        assert_eq!(event.name(), "message");

        let event = TestEvents::Count(42);
        assert_eq!(event.name(), "count");
    }

    #[test]
    fn test_into_event_payload() {
        let event = TestEvents::Message("hello".into());
        let payload = event.into_event_payload().unwrap();

        let (event_name, payload_data): (String, String) =
            serde_json::from_slice(&payload.data).unwrap();

        assert_eq!(event_name, "message");
        assert_eq!(payload_data, "hello");
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct ComplexPayload {
        user: String,
        message: String,
        timestamp: i64,
    }

    #[derive(Serialize)]
    enum ComplexEvents {
        Chat(ComplexPayload),
    }

    impl Event for ComplexEvents {
        type Ack = ();

        fn name(&self) -> &'static str {
            match self {
                ComplexEvents::Chat(_) => "chat",
            }
        }

        fn into_event_payload(&self) -> Result<EventPayload> {
            let event_name = self.name();
            let payload = match self {
                ComplexEvents::Chat(p) => p,
            };

            let json = serde_json::to_vec(&(event_name, payload))?;
            Ok(EventPayload {
                id: None,
                data: Bytes::from(json),
                attachments: Default::default(),
            })
        }
    }

    #[test]
    fn test_complex_payload() {
        let event = ComplexEvents::Chat(ComplexPayload {
            user: "alice".into(),
            message: "hello".into(),
            timestamp: 1234567890,
        });

        let payload = event.into_event_payload().unwrap();
        let (event_name, payload_data): (String, ComplexPayload) =
            serde_json::from_slice(&payload.data).unwrap();

        assert_eq!(event_name, "chat");
        assert_eq!(payload_data.user, "alice");
        assert_eq!(payload_data.message, "hello");
        assert_eq!(payload_data.timestamp, 1234567890);
    }
}
