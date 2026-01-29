//! Implementation of the `#[derive(Receive)]` macro.
//!
//! This module generates `TryFrom<&Packet>` implementation which
//! deserializes Socket.IO packets into typed event enums.

use crate::util;
use darling::{FromDeriveInput, FromVariant};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Result};

#[derive(FromDeriveInput)]
#[darling(supports(enum_any))]
struct ReceiveInput {
    ident: syn::Ident,
    generics: syn::Generics,
    data: darling::ast::Data<ReceiveVariant, ()>,
}

#[derive(FromVariant)]
#[darling(forward_attrs(event))]
struct ReceiveVariant {
    ident: syn::Ident,
    fields: darling::ast::Fields<syn::Field>,
    attrs: Vec<syn::Attribute>,
}

/// Expand the Receive derive macro.
pub fn expand(input: DeriveInput) -> Result<TokenStream> {
    let input = ReceiveInput::from_derive_input(&input)?;
    let enum_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let variants = match input.data {
        darling::ast::Data::Enum(v) => v,
        _ => return Err(Error::new_spanned(enum_name, "Receive only supports enums")),
    };

    let mut match_arms = Vec::new();

    for variant in variants {
        let variant_ident = &variant.ident;
        let event_name = util::parse_event_name(&variant.attrs)?
            .ok_or_else(|| Error::new_spanned(variant_ident, "Missing #[event(...)] attribute"))?;

        let body = match variant.fields.style {
            // Unit variant: Event::Ping
            // Expects: ["ping"]
            darling::ast::Style::Unit => {
                quote! {
                    if items.len() != 1 {
                        return Err(Error::Protocol(
                            format!("Event '{}' expects no arguments, got {}", #event_name, items.len() - 1)
                        ));
                    }
                    Ok(#enum_name::#variant_ident)
                }
            }

            // Tuple variant: Event::Message(A, B)
            // Expects: ["message", a, b]
            darling::ast::Style::Tuple => {
                let field_count = variant.fields.len();
                let field_types: Vec<_> = variant.fields.fields.iter().map(|f| &f.ty).collect();

                let field_deserializers: Vec<_> = (0..field_count)
                    .map(|i| {
                        let idx = i + 1; // Skip items[0] which is the event name
                        let ty = field_types[i];
                        quote! {
                            serde_json::from_str::<#ty>(items[#idx].get())
                                .map_err(Error::Json)?
                        }
                    })
                    .collect();

                quote! {
                    let expected_len = 1 + #field_count;
                    if items.len() < expected_len {
                        return Err(Error::Protocol(
                            format!(
                                "Event '{}' expects {} arguments, got {}",
                                #event_name,
                                #field_count,
                                items.len() - 1
                            )
                        ));
                    }

                    Ok(#enum_name::#variant_ident(
                        #(#field_deserializers),*
                    ))
                }
            }

            // Struct variant: Event::Login { user: String, pass: String }
            // Expects: ["login", {"user": "alice", "pass": "secret"}]
            darling::ast::Style::Struct => {
                let field_names: Vec<_> = variant.fields.fields.iter()
                    .map(|f| f.ident.as_ref().unwrap())
                    .collect();
                let field_types: Vec<_> = variant.fields.fields.iter().map(|f| &f.ty).collect();

                quote! {
                    if items.len() < 2 {
                        return Err(Error::Protocol(
                            format!(
                                "Event '{}' expects 1 object argument, got {} items",
                                #event_name,
                                items.len() - 1
                            )
                        ));
                    }

                    #[derive(serde::Deserialize)]
                    struct Payload {
                        #(#field_names: #field_types,)*
                    }

                    let payload: Payload = serde_json::from_str(items[1].get())
                        .map_err(Error::Json)?;

                    Ok(#enum_name::#variant_ident {
                        #(#field_names: payload.#field_names,)*
                    })
                }
            }
        };

        match_arms.push(quote! {
            #event_name => {
                #body
            }
        });
    }

    let expanded = quote! {
        impl #impl_generics ::core::convert::TryFrom<&Packet> for #enum_name #ty_generics #where_clause {
            type Error = Error;

            fn try_from(packet: &Packet) -> ::core::result::Result<Self, Self::Error> {
                // Parse the JSON array as a vector of raw values
                // items[0] = event name (string)
                // items[1..] = arguments (can be any JSON values)
                let items: Vec<&serde_json::value::RawValue> =
                    serde_json::from_slice(&packet.data)
                        .map_err(Error::Json)?;

                if items.is_empty() {
                    return Err(Error::Protocol(
                        "Packet data is empty, expected at least event name".to_string()
                    ));
                }

                // Extract event name from items[0]
                let event_name: String = serde_json::from_str(items[0].get())
                    .map_err(Error::Json)?;

                match event_name.as_str() {
                    #(#match_arms)*
                    unknown => Err(Error::UnknownEvent(
                        unknown.to_string()
                    )),
                }
            }
        }
    };

    Ok(expanded)
}
