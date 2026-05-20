//! Derive macro implementation for the `AckType` trait.

use crate::attrs::SiocInput;
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::quote;

/// Expands `#[derive(AckType)]` into an `AckType` trait implementation.
///
/// Generates `type Binary` for the annotated struct.
pub fn expand(input: &syn::DeriveInput) -> darling::Result<TokenStream> {
    let input = SiocInput::from_derive_input(input)?;

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let ident = &input.ident;

    let binary_marker = if input.ack.binary.is_present() {
        quote! { ::sioc::prelude::HasBinary }
    } else {
        quote! { ::sioc::prelude::NoBinary }
    };

    Ok(quote! {
        impl #impl_generics ::sioc::prelude::AckType for #ident #type_generics #where_clause {
            type Binary = #binary_marker;
        }
    })
}
