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
