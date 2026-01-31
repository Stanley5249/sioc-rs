use darling::{FromDeriveInput, FromVariant};

#[derive(FromDeriveInput)]
#[darling(supports(enum_any, struct_any))]
#[darling(attributes(sioc))]
pub struct EmitInput {
    pub ident: syn::Ident,
    pub generics: syn::Generics,
    pub data: darling::ast::Data<EmitVariant, syn::Field>,
    #[darling(default)]
    pub event: Option<String>,
}

#[derive(FromVariant)]
#[darling(attributes(sioc))]
pub struct EmitVariant {
    pub ident: syn::Ident,
    pub fields: darling::ast::Fields<syn::Field>,
    #[darling(default)]
    pub event: Option<String>,
}
