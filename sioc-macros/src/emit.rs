//! Implementation of the `#[derive(Emit)]` macro.
//!
//! This module generates the `SiocEmit` trait implementation which
//! provides event name extraction and serialization for Socket.IO events.

use crate::util;
use darling::{FromDeriveInput, FromVariant};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Result};

#[derive(FromDeriveInput)]
#[darling(supports(enum_any))]
struct EmitInput {
    ident: syn::Ident,
    generics: syn::Generics,
    data: darling::ast::Data<EmitVariant, ()>,
}

#[derive(FromVariant)]
#[darling(forward_attrs(event))]
struct EmitVariant {
    ident: syn::Ident,
    fields: darling::ast::Fields<syn::Field>,
    attrs: Vec<syn::Attribute>,
}

/// Expand the Emit derive macro.
pub fn expand(input: DeriveInput) -> Result<TokenStream> {
    // Use darling to parse the top-level input
    let input = EmitInput::from_derive_input(&input)?;
    let enum_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Extract variants from darling's Data enum
    let variants = match input.data {
        darling::ast::Data::Enum(v) => v,
        _ => return Err(Error::new_spanned(enum_name, "Emit only supports enums")),
    };

    let mut event_name_arms = Vec::new();
    let mut to_packet_arms = Vec::new();

    for variant in variants {
        let variant_ident = &variant.ident;

        // 1. Parse event name from forwarded attributes
        let event_name = util::parse_event_name(&variant.attrs)?
            .ok_or_else(|| Error::new_spanned(variant_ident, "Missing #[event(...)] attribute"))?;

        // Generate event_name() match arm
        let pattern = match variant.fields.style {
            darling::ast::Style::Unit => quote! { #enum_name::#variant_ident },
            darling::ast::Style::Tuple => quote! { #enum_name::#variant_ident(..) },
            darling::ast::Style::Struct => quote! { #enum_name::#variant_ident { .. } },
        };

        event_name_arms.push(quote! {
            #pattern => #event_name,
        });

        // Generate to_packet() match arm with CORRECT flattening
        match variant.fields.style {
            // Unit: ["event"]
            darling::ast::Style::Unit => {
                to_packet_arms.push(quote! {
                    #enum_name::#variant_ident => {
                        let json = serde_json::to_vec(&[#event_name]).map_err(::sioc_core::error::Error::Json)?;
                        // CHANGED: Return EventPayload directly
                        EventPayload::new(bytes::Bytes::from(json))
                    }
                });
            }

            // Tuple: ["event", arg1, arg2]
            // We flatten by creating a tuple ("event", arg1, arg2)
            darling::ast::Style::Tuple => {
                let count = variant.fields.len();
                let field_names: Vec<_> =
                    (0..count).map(|i| quote::format_ident!("f{}", i)).collect();

                let pattern = quote! { #enum_name::#variant_ident( #(#field_names),* ) };

                // Create flat tuple for serialization
                let tuple_elems = quote! { #event_name, #(#field_names),* };

                to_packet_arms.push(quote! {
                    #pattern => {
                        let json = serde_json::to_vec(&(#tuple_elems))
                            .map_err(::sioc_core::error::Error::Json)?;
                        // CHANGED: Return EventPayload directly
                        EventPayload::new(bytes::Bytes::from(json))
                    }
                });
            }

            // Struct: ["event", { "key": "value" }]
            // We serialize as ("event", object_map)
            darling::ast::Style::Struct => {
                let field_names: Vec<_> = variant
                    .fields
                    .fields
                    .iter()
                    .map(|f| f.ident.as_ref().unwrap())
                    .collect();

                let pattern = quote! { #enum_name::#variant_ident { #(#field_names),* } };

                // Construct the payload object
                let payload = quote! {
                    serde_json::json!({
                        #(stringify!(#field_names): #field_names),*
                    })
                };

                to_packet_arms.push(quote! {
                    #pattern => {
                        let tuple = (#event_name, #payload);
                        let json = serde_json::to_vec(&tuple)
                            .map_err(::sioc_core::error::Error::Json)?;
                        // CHANGED: Return EventPayload directly
                        EventPayload::new(bytes::Bytes::from(json))
                    }
                });
            }
        }
    }

    // CHANGED: Implement 'Event' trait instead of 'Emit'
    let expanded = quote! {
        impl #impl_generics Event for #enum_name #ty_generics #where_clause {
            type Ack = (); // Default to unit for now

            fn name(&self) -> &'static str {
                match self { #(#event_name_arms)* }
            }

            // CHANGED: Method name and return type
            fn into_event_payload(&self) -> Result<EventPayload> {
                Ok(match self {
                    #(#to_packet_arms)*
                })
            }
        }
    };

    Ok(expanded)
}
