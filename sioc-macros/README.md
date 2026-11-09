# sioc-macros

Derive macros for [`sioc`](https://docs.rs/sioc).

| Derive               | Implements                                        |
| -------------------- | ------------------------------------------------- |
| `EventType`          | [`EventType`] [^event-type]                       |
| `AckType`            | [`AckType`] [^ack-type]                           |
| `SerializePayload`   | [`SerializePayload`] [^payload]                   |
| `DeserializePayload` | [`DeserializePayload`] [^payload]                 |
| `EventRouter`        | [`EventRouter`] and `TryFrom<DynEvent>` [^router] |

## Struct-level attributes

| Attribute                      | Applies to             | Effect                                     |
| ------------------------------ | ---------------------- | ------------------------------------------ |
| `#[sioc(event(name = "str"))]` | [`EventType`]          | Overrides the event name. [^name]          |
| `#[sioc(event(ack = "Type"))]` | [`EventType`]          | Expects an acknowledgement of type `Type`. |
| `#[sioc(event(binary))]`       | [`EventType`]          | Carries binary attachments.                |
| `#[sioc(ack(binary))]`         | [`AckType`]            | Carries binary attachments.                |
| `#[sioc(strict)]`              | [`DeserializePayload`] | Rejects extra elements. [^strict]          |

## Field-level attributes

| Attribute          | Applies to                                   | Effect                                 |
| ------------------ | -------------------------------------------- | -------------------------------------- |
| `#[sioc(flatten)]` | [`SerializePayload`], [`DeserializePayload`] | Spreads a collection field. [^flatten] |

Without attributes, an event has no acknowledgement and no binary attachments, and an ack has no binary attachments.

[^event-type]: Provides the event name constant and declares whether the event uses an acknowledgement and binary attachments.

[^ack-type]: Declares whether the acknowledgement carries binary attachments.

[^payload]: Encodes or decodes the struct fields as sequential JSON array elements. Every field must implement [`serde::Serialize`] or [`serde::Deserialize`].

[^router]: Routes an incoming event to the variant with the matching name. Each variant wraps [`Event<E>`], where `E` implements [`EventType`] and [`DeserializePayload`].

[^name]: By default, the struct name in `snake_case`, so `MyEvent` becomes `"my_event"`.

[^strict]: Elements left after the fields are read fail decoding, unless a trailing flatten field consumes them. By default, extra elements are ignored.

[^flatten]: Each element of the collection is its own array element on the way out, and all remaining array elements are collected into it on the way in. Place it on the last field.

## License

MIT OR Apache-2.0.

[`EventType`]: https://docs.rs/sioc/latest/sioc/marker/trait.EventType.html
[`AckType`]: https://docs.rs/sioc/latest/sioc/marker/trait.AckType.html
[`SerializePayload`]: https://docs.rs/sioc/latest/sioc/payload/trait.SerializePayload.html
[`DeserializePayload`]: https://docs.rs/sioc/latest/sioc/payload/trait.DeserializePayload.html
[`EventRouter`]: https://docs.rs/sioc/latest/sioc/event/trait.EventRouter.html
[`Event<E>`]: https://docs.rs/sioc/latest/sioc/event/struct.Event.html
[`serde::Serialize`]: https://docs.rs/serde/latest/serde/trait.Serialize.html
[`serde::Deserialize`]: https://docs.rs/serde/latest/serde/trait.Deserialize.html
