#![allow(clippy::needless_continue)]

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
    /// Collect remaining sequence elements into this field via `#[sioc(flatten)]`.
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
