# sioc-macros

Derive macros for [`sioc`](https://docs.rs/sioc).

| Derive               | Description                                                                                                                                                                                                         |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EventType`          | Implements [`EventType`] by providing the event name constant and declaring whether the event uses acknowledgement and binary attachments.                                                                          |
| `AckType`            | Implements [`AckType`] by declaring whether the acknowledgement carries binary attachments.                                                                                                                         |
| `SerializePayload`   | Implements [`SerializePayload`] by encoding struct fields as sequential JSON array elements. All fields must implement [`serde::Serialize`].                                                                        |
| `DeserializePayload` | Implements [`DeserializePayload`] by decoding sequential JSON array elements into struct fields. All fields must implement [`serde::Deserialize`].                                                                  |
| `EventRouter`        | Implements [`EventRouter`] and `TryFrom<DynEvent>` by routing an incoming event to the matching variant by name. Each variant must wrap [`Event<E>`] where `E` implements [`EventType`] and [`DeserializePayload`]. |

## Struct-level attributes

| Attribute             | Applies to             | Effect                                                                                                              |
| --------------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `event(name = "str")` | [`EventType`]          | Overrides the event name. By default the struct name is converted to snake_case, so `MyEvent` becomes `"my_event"`. |
| `event(ack = Type)`   | [`EventType`]          | Declares that this event expects an acknowledgement of type `Type`. Without this the event has no acknowledgement.  |
| `event(binary)`       | [`EventType`]          | Marks the event as carrying binary attachments. Without this the event has no binary attachments.                   |
| `ack(binary)`         | [`AckType`]            | Marks the acknowledgement as carrying binary attachments. Without this the ack has no binary attachments.           |
| `strict`              | [`DeserializePayload`] | Rejects payloads that contain more elements than the struct has fields. By default, extra elements are ignored.     |

## Field-level attributes

| Attribute | Applies to                                   | Effect                                                                                                                                                                         |
| --------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `flatten` | [`SerializePayload`], [`DeserializePayload`] | Each element of this collection field is serialized as a separate array element, and all remaining array elements are collected into it on the way in. Must be the last field. |

## License

MIT OR Apache-2.0.

[`EventType`]: https://docs.rs/sioc/latest/sioc/event/trait.EventType.html
[`AckType`]: https://docs.rs/sioc/latest/sioc/ack/trait.AckType.html
[`SerializePayload`]: https://docs.rs/sioc/latest/sioc/payload/trait.SerializePayload.html
[`DeserializePayload`]: https://docs.rs/sioc/latest/sioc/payload/trait.DeserializePayload.html
[`EventRouter`]: https://docs.rs/sioc/latest/sioc/event/trait.EventRouter.html
[`Event<E>`]: https://docs.rs/sioc/latest/sioc/event/struct.Event.html
[`serde::Serialize`]: https://docs.rs/serde/latest/serde/trait.Serialize.html
[`serde::Deserialize`]: https://docs.rs/serde/latest/serde/trait.Deserialize.html
