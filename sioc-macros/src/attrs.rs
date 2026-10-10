use darling::{FromDeriveInput, FromField, FromMeta, FromVariant};

#[derive(FromDeriveInput)]
#[darling(attributes(sioc))]
pub struct SiocInput {
    pub ident: syn::Ident,
    pub generics: syn::Generics,
    pub strict: darling::util::Flag,
    #[darling(default)]
    pub event: EventMeta,
    #[darling(default)]
    pub ack: AckMeta,
    pub data: darling::ast::Data<SiocVariant, SiocField>,
}

#[derive(FromVariant)]
#[darling(attributes(sioc))]
pub struct SiocVariant {
    pub ident: syn::Ident,
    pub fields: darling::ast::Fields<SiocField>,
}

#[derive(FromField)]
#[darling(attributes(sioc))]
pub struct SiocField {
    pub ident: Option<syn::Ident>,
    pub ty: syn::Type,
    /// Collect remaining sequence elements into this field via
    /// `#[sioc(flatten)]`.
    ///
    /// Must be the last field because it consumes the remaining sequence.
    pub flatten: darling::util::Flag,
}

#[derive(Default, FromMeta)]
#[darling(default)]
pub struct EventMeta {
    pub name: Option<syn::LitStr>,
    pub ack: Option<syn::Type>,
    pub binary: darling::util::Flag,
}

#[derive(Default, FromMeta)]
#[darling(default)]
pub struct AckMeta {
    pub binary: darling::util::Flag,
}

/// Validates the shared serialization and deserialization flatten contract.
pub fn validate_flatten(fields: &darling::ast::Fields<SiocField>) -> darling::Result<()> {
    for (index, field) in fields.iter().enumerate() {
        if field.flatten.is_present() && index + 1 != fields.len() {
            return Err(
                darling::Error::custom("flatten must be the last field").with_span(&field.ty)
            );
        }
    }
    Ok(())
}

/// Chooses a generated type name outside the caller's generic parameters.
pub fn fresh_type_ident(generics: &syn::Generics, base: &str) -> syn::Ident {
    let mut name = base.to_owned();
    while generics.params.iter().any(|param| match param {
        syn::GenericParam::Type(param) => param.ident == name,
        syn::GenericParam::Const(param) => param.ident == name,
        syn::GenericParam::Lifetime(_) => false,
    }) {
        name.push('_');
    }
    syn::Ident::new(&name, proc_macro2::Span::call_site())
}

/// Chooses a generated lifetime outside the caller's lifetime parameters.
pub fn fresh_lifetime(generics: &syn::Generics) -> syn::Lifetime {
    let mut name = "__sioc_de".to_owned();
    while generics
        .lifetimes()
        .any(|param| param.lifetime.ident == name)
    {
        name.push('_');
    }
    syn::Lifetime::new(&format!("'{name}"), proc_macro2::Span::call_site())
}

#[cfg(test)]
mod tests {
    #[test]
    fn payload_derives_reject_flatten_before_the_last_field() {
        for source in [
            "struct Input { #[sioc(flatten)] rest: Vec<u32>, tail: u32 }",
            "struct Input(#[sioc(flatten)] Vec<u32>, u32);",
            "struct Input { #[sioc(flatten)] a: Vec<u32>, #[sioc(flatten)] b: Vec<u32> }",
        ] {
            let input: syn::DeriveInput = syn::parse_str(source).unwrap();
            assert!(
                crate::serialize_payload::expand(&input).is_err(),
                "{source}"
            );
            assert!(
                crate::deserialize_payload::expand(&input).is_err(),
                "{source}"
            );
        }
    }

    #[test]
    fn all_derives_reject_invalid_attributes() {
        for source in [
            "#[sioc(unknown)] struct Input;",
            "#[sioc(event(unknown))] struct Input;",
            "#[sioc(event(name = 7))] struct Input;",
            "#[sioc(event(ack = 7))] struct Input;",
            "#[sioc(ack(unknown))] struct Input;",
            "struct Input { #[sioc(unknown)] value: u32 }",
        ] {
            let input: syn::DeriveInput = syn::parse_str(source).unwrap();
            assert!(crate::event_type::expand(&input).is_err(), "{source}");
            assert!(crate::ack_type::expand(&input).is_err(), "{source}");
            assert!(
                crate::serialize_payload::expand(&input).is_err(),
                "{source}"
            );
            assert!(
                crate::deserialize_payload::expand(&input).is_err(),
                "{source}"
            );
            assert!(crate::event_router::expand(&input).is_err(), "{source}");
        }
    }
}
