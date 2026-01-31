use sioc_core::error::Result;
use sioc_core::event::Event;

/// Create a new event packet from a typed event.
pub fn to_event<E: Event>(event: E) -> Result<sioc_core::packet::EventPacket> {
    let data = event.to_json()?;
    Ok(sioc_core::packet::EventPacket::new("/".into(), data))
}

/// Prelude module for convenient imports.
///
/// Import everything you need with:
///
/// ```rust
/// use sioc::prelude::*;
/// ```
pub mod prelude {
    pub use sioc_core::prelude::*;
    pub use sioc_macros::Event;
}

/// Current version of Sioc.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
