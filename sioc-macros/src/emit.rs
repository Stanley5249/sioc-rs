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
                        // Unit variant: ["event_name"]
                        to_packet_arms.push(quote! {
                            #struct_name::#variant_ident => {
                                struct SerAdapter;

                                impl serde::Serialize for SerAdapter {
                                    fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                                    where
                                        S: serde::Serializer,
                                    {
                                        use serde::ser::SerializeTuple;
                                        let mut tuple = serializer.serialize_tuple(1)?;
                                        tuple.serialize_element(#event_name_str)?;
                                        tuple.end()
                                    }
                                }

                                let json = serde_json::to_vec(&SerAdapter)
                                    .map_err(sioc_core::error::Error::Json)?;
                                bytes::Bytes::from(json)
                            }
                        });
                    }
                    darling::ast::Style::Tuple => {
                        // Tuple variant: ["event_name", field0, field1, ...]
                        let count = variant.fields.len();
                        let field_names: Vec<_> =
                            (0..count).map(|i| quote::format_ident!("f{}", i)).collect();
                        let field_types: Vec<_> =
                            variant.fields.fields.iter().map(|f| &f.ty).collect();
                        let field_indices: Vec<syn::Index> =
                            (0..count).map(syn::Index::from).collect();
                        let pattern = quote! { #struct_name::#variant_ident( #(#field_names),* ) };

                        to_packet_arms.push(quote! {
                            #pattern => {
                                struct SerAdapter<'a>(#(&'a #field_types),*);

                                impl<'a> serde::Serialize for SerAdapter<'a> {
                                    fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                                    where
                                        S: serde::Serializer,
                                    {
                                        use serde::ser::SerializeTuple;
                                        let mut tuple = serializer.serialize_tuple(1 + #count)?;
                                        tuple.serialize_element(#event_name_str)?;
                                        #(tuple.serialize_element(&self.#field_indices)?;)*
                                        tuple.end()
                                    }
                                }

                                let adapter = SerAdapter(#(#field_names),*);
                                let json = serde_json::to_vec(&adapter)
                                    .map_err(sioc_core::error::Error::Json)?;
                                bytes::Bytes::from(json)
                            }
                        });
                    }
                    darling::ast::Style::Struct => {
                        // Named struct variant: FLATTENED ["event_name", val1, val2, ...]
                        let field_names: Vec<_> = variant
                            .fields
                            .fields
                            .iter()
                            .map(|f| f.ident.as_ref().unwrap())
                            .collect();
                        let field_types: Vec<_> =
                            variant.fields.fields.iter().map(|f| &f.ty).collect();
                        let field_count = field_names.len();
                        let field_indices: Vec<syn::Index> =
                            (0..field_count).map(syn::Index::from).collect();

                        let pattern = quote! { #struct_name::#variant_ident { #(#field_names),* } };

                        to_packet_arms.push(quote! {
                            #pattern => {
                                struct SerAdapter<'a>(#(&'a #field_types),*);

                                impl<'a> serde::Serialize for SerAdapter<'a> {
                                    fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                                    where
                                        S: serde::Serializer,
                                    {
                                        use serde::ser::SerializeTuple;
                                        let mut tuple = serializer.serialize_tuple(1 + #field_count)?;
                                        tuple.serialize_element(#event_name_str)?;
                                        #(tuple.serialize_element(&self.#field_indices)?;)*
                                        tuple.end()
                                    }
                                }

                                let adapter = SerAdapter(#(#field_names),*);
                                let json = serde_json::to_vec(&adapter)
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
                    // Unit struct: ["event_name"]
                    quote! {
                        struct SerAdapter;

                        impl serde::Serialize for SerAdapter {
                            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                            where
                                S: serde::Serializer,
                            {
                                use serde::ser::SerializeTuple;
                                let mut tuple = serializer.serialize_tuple(1)?;
                                tuple.serialize_element(#event_name_str)?;
                                tuple.end()
                            }
                        }

                        let json = serde_json::to_vec(&SerAdapter)
                            .map_err(sioc_core::error::Error::Json)?;
                        bytes::Bytes::from(json)
                    }
                }
                darling::ast::Style::Tuple => {
                    // Tuple struct: ["event_name", field0, field1, ...]
                    let count = fields.len();
                    let field_types: Vec<_> = fields.fields.iter().map(|f| &f.ty).collect();
                    let indices: Vec<syn::Index> = (0..count).map(syn::Index::from).collect();
                    let self_indices = indices.clone();

                    quote! {
                        struct SerAdapter<'a>(#(&'a #field_types),*);

                        impl<'a> serde::Serialize for SerAdapter<'a> {
                            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                            where
                                S: serde::Serializer,
                            {
                                use serde::ser::SerializeTuple;
                                let mut tuple = serializer.serialize_tuple(1 + #count)?;
                                tuple.serialize_element(#event_name_str)?;
                                #(tuple.serialize_element(&self.#indices)?;)*
                                tuple.end()
                            }
                        }

                        let adapter = SerAdapter(#(&self.#self_indices),*);
                        let json = serde_json::to_vec(&adapter)
                            .map_err(sioc_core::error::Error::Json)?;
                        bytes::Bytes::from(json)
                    }
                }
                darling::ast::Style::Struct => {
                    // Named struct: FLATTENED ["event_name", val1, val2, ...]
                    let field_names: Vec<_> = fields
                        .fields
                        .iter()
                        .map(|f| f.ident.as_ref().unwrap())
                        .collect();
                    let field_types: Vec<_> = fields.fields.iter().map(|f| &f.ty).collect();
                    let field_count = field_names.len();
                    let field_indices: Vec<syn::Index> =
                        (0..field_count).map(syn::Index::from).collect();
                    let self_field_names = field_names.clone();

                    quote! {
                        struct SerAdapter<'a>(#(&'a #field_types),*);

                        impl<'a> serde::Serialize for SerAdapter<'a> {
                            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                            where
                                S: serde::Serializer,
                            {
                                use serde::ser::SerializeTuple;
                                let mut tuple = serializer.serialize_tuple(1 + #field_count)?;
                                tuple.serialize_element(#event_name_str)?;
                                #(tuple.serialize_element(&self.#field_indices)?;)*
                                tuple.end()
                            }
                        }

                        let adapter = SerAdapter(#(&self.#self_field_names),*);
                        let json = serde_json::to_vec(&adapter)
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
