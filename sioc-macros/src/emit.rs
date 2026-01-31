use darling::{FromDeriveInput, FromVariant};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Result};

#[derive(FromDeriveInput)]
#[darling(supports(enum_any, struct_any))]
#[darling(attributes(sioc))]
struct EmitInput {
    ident: syn::Ident,
    generics: syn::Generics,
    data: darling::ast::Data<EmitVariant, syn::Field>,

    #[darling(default)]
    event: Option<String>,
}

#[derive(FromVariant)]
#[darling(attributes(sioc))]
struct EmitVariant {
    ident: syn::Ident,
    fields: darling::ast::Fields<syn::Field>,

    #[darling(default)]
    event: Option<String>,
}

pub fn expand(input: DeriveInput) -> Result<TokenStream> {
    let input = EmitInput::from_derive_input(&input)?;
    let struct_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let (name_body, payload_body) = match input.data {
        darling::ast::Data::Enum(variants) => {
            let mut event_name_arms = Vec::new();
            let mut to_packet_arms = Vec::new();

            for variant in variants {
                let variant_ident = &variant.ident;
                let event_name_str = variant.event.ok_or_else(|| {
                    Error::new_spanned(variant_ident, "Missing #[sioc(event = \"...\")] attribute")
                })?;

                let pattern = match variant.fields.style {
                    darling::ast::Style::Unit => quote! { #struct_name::#variant_ident },
                    darling::ast::Style::Tuple => quote! { #struct_name::#variant_ident(..) },
                    darling::ast::Style::Struct => quote! { #struct_name::#variant_ident { .. } },
                };

                event_name_arms.push(quote! {
                    #pattern => #event_name_str,
                });

                match variant.fields.style {
                    darling::ast::Style::Unit => {
                        to_packet_arms.push(quote! {
                            #struct_name::#variant_ident => {
                                let json = serde_json::to_vec(&[#event_name_str])
                                    .map_err(sioc_core::error::Error::Json)?;
                                bytes::Bytes::from(json)
                            }
                        });
                    }
                    darling::ast::Style::Tuple => {
                        let count = variant.fields.len();
                        let field_names: Vec<_> =
                            (0..count).map(|i| quote::format_ident!("f{}", i)).collect();
                        let pattern = quote! { #struct_name::#variant_ident( #(#field_names),* ) };

                        // Serialize Tuple Refs: (name, &f0, &f1...)
                        let tuple_elems = quote! { #event_name_str, #(#field_names),* };

                        to_packet_arms.push(quote! {
                            #pattern => {
                                let json = serde_json::to_vec(&(#tuple_elems))
                                    .map_err(sioc_core::error::Error::Json)?;
                                bytes::Bytes::from(json)
                            }
                        });
                    }
                    darling::ast::Style::Struct => {
                        let field_names: Vec<_> = variant
                            .fields
                            .fields
                            .iter()
                            .map(|f| f.ident.as_ref().unwrap())
                            .collect();
                        let field_types: Vec<_> =
                            variant.fields.fields.iter().map(|f| &f.ty).collect();

                        let pattern = quote! { #struct_name::#variant_ident { #(#field_names),* } };

                        // Shadow Struct for serialization
                        let shadow_struct = quote! {
                            #[derive(serde::Serialize)]
                            struct ShadowPayload<'a> {
                                #(#field_names: &'a #field_types),*
                            }
                        };

                        let shadow_init = quote! {
                            ShadowPayload {
                                #(#field_names: #field_names),*
                            }
                        };

                        to_packet_arms.push(quote! {
                            #pattern => {
                                #shadow_struct
                                let payload = #shadow_init;
                                let tuple = (#event_name_str, payload);
                                let json = serde_json::to_vec(&tuple)
                                    .map_err(sioc_core::error::Error::Json)?;
                                bytes::Bytes::from(json)
                            }
                        });
                    }
                }
            }

            (
                quote! { match self { #(#event_name_arms)* } },
                quote! { match self { #(#to_packet_arms)* } },
            )
        }
        darling::ast::Data::Struct(fields) => {
            let event_name_str = input.event.ok_or_else(|| {
                Error::new_spanned(
                    struct_name,
                    "Structs must have #[sioc(event = \"...\")] attribute",
                )
            })?;

            let json_logic = match fields.style {
                darling::ast::Style::Unit => {
                    quote! {
                        let json = serde_json::to_vec(&[#event_name_str])
                            .map_err(sioc_core::error::Error::Json)?;
                        bytes::Bytes::from(json)
                    }
                }
                darling::ast::Style::Tuple => {
                    let indices: Vec<_> = (0..fields.len()).map(syn::Index::from).collect();
                    let tuple_elems = quote! { #event_name_str, #(&self.#indices),* };
                    quote! {
                        let json = serde_json::to_vec(&(#tuple_elems))
                            .map_err(sioc_core::error::Error::Json)?;
                        bytes::Bytes::from(json)
                    }
                }
                darling::ast::Style::Struct => {
                    let field_names: Vec<_> = fields
                        .fields
                        .iter()
                        .map(|f| f.ident.as_ref().unwrap())
                        .collect();
                    let field_types: Vec<_> = fields.fields.iter().map(|f| &f.ty).collect();

                    let shadow_struct = quote! {
                        #[derive(serde::Serialize)]
                        struct ShadowPayload<'a> {
                            #(#field_names: &'a #field_types),*
                        }
                    };

                    let shadow_init = quote! {
                        ShadowPayload {
                            #(#field_names: &self.#field_names),*
                        }
                    };

                    quote! {
                        #shadow_struct
                        let payload = #shadow_init;
                        let tuple = (#event_name_str, payload);
                        let json = serde_json::to_vec(&tuple)
                            .map_err(sioc_core::error::Error::Json)?;
                        bytes::Bytes::from(json)
                    }
                }
            };

            (quote! { #event_name_str }, json_logic)
        }
    };

    let expanded = quote! {
        impl #impl_generics sioc_core::event::Event for #struct_name #ty_generics #where_clause {
            fn name(&self) -> &'static str {
                #name_body
            }

            fn to_payload(&self) -> sioc_core::error::Result<bytes::Bytes> {
                Ok({ #payload_body })
            }
        }
    };
    Ok(expanded)
}
