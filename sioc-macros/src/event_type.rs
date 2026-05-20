//! Derive macro implementation for the `EventType` trait.

use crate::attrs::SiocInput;
use darling::FromDeriveInput;
use heck::ToSnakeCase;
use proc_macro2::TokenStream;
use quote::quote;

/// Expands `#[derive(EventType)]` into an `EventType` trait implementation.
pub fn expand(input: &syn::DeriveInput) -> darling::Result<TokenStream> {
    let input = SiocInput::from_derive_input(input)?;

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let ident = &input.ident;

    let event_name = if let Some(name) = input.event.name {
        name
    } else {
        let name = input.ident.to_string().to_snake_case();
        let span = input.ident.span();
        syn::LitStr::new(&name, span)
    };

    let ack_marker = if let Some(ack_type) = input.event.ack {
        quote! { ::sioc::prelude::HasAck<#ack_type> }
    } else {
        quote! { ::sioc::prelude::NoAck }
    };

    let binary_marker = if input.event.binary.is_present() {
        quote! { ::sioc::prelude::HasBinary }
    } else {
        quote! { ::sioc::prelude::NoBinary }
    };

    Ok(quote! {
        impl #impl_generics ::sioc::prelude::EventType for #ident #type_generics #where_clause {
            const NAME: &'static str = #event_name;
            type Ack = #ack_marker;
            type Binary = #binary_marker;
        }
    })
}
