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
    /// Recommended on the last field. Placing it earlier means fields after it
    /// cannot be deserialized since the flatten field consumes all remaining
    /// sequence elements.
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

#[cfg(test)]
mod tests {
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
