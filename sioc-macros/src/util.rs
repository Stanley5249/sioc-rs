//! Utility functions for procedural macros.

use syn::{Attribute, Result};

/// Extract the event name from `#[event("name")]` or `#[event(name = "name")]` attribute.
pub fn parse_event_name(attrs: &[Attribute]) -> Result<Option<String>> {
    for attr in attrs {
        if attr.path().is_ident("event") {
            // Try positional: #[event("name")]
            if let Ok(lit) = attr.parse_args::<syn::LitStr>() {
                return Ok(Some(lit.value()));
            }

            // Try named: #[event(name = "name")]
            let mut name = None;
            let parser = syn::meta::parser(|meta| {
                if meta.path.is_ident("name") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    name = Some(value.value());
                    Ok(())
                } else {
                    // Ignore other attributes or error?
                    Ok(())
                }
            });

            if attr.parse_args_with(parser).is_ok() && let Some(n) = name {
                return Ok(Some(n));
            }
        }
    }
    Ok(None)
}
