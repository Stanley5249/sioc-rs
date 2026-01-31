use darling::{FromDeriveInput, FromVariant};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
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

    let name_body = generate_name_match(&input, struct_name)?;
    let serialize_body = generate_serialization(&input, struct_name)?;

    Ok(quote! {
        impl #impl_generics sioc_core::event::Event for #struct_name #ty_generics #where_clause {
            fn name(&self) -> &'static str {
                #name_body
            }

            fn to_payload(&self) -> sioc_core::error::Result<bytes::Bytes> {
                use serde::ser::{Serialize, Serializer, SerializeTuple};

                struct SerAdapter<'a>(&'a #struct_name);

                impl<'a> Serialize for SerAdapter<'a> {
                    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
                    where S: Serializer {
                        #serialize_body
                    }
                }

                // Error conversion is handled automatically by #[from] in sioc_core::Error
                let json = serde_json::to_vec(&SerAdapter(self))?;
                Ok(bytes::Bytes::from(json))
            }
        }
    })
}

fn generate_name_match(input: &EmitInput, struct_name: &syn::Ident) -> Result<TokenStream> {
    match &input.data {
        darling::ast::Data::Enum(variants) => {
            let mut arms = Vec::new();
            for variant in variants {
                let ident = &variant.ident;
                let event = variant
                    .event
                    .as_deref()
                    .ok_or_else(|| Error::new_spanned(ident, "Missing #[sioc(event = \"...\")]"))?;
                let pattern = match variant.fields.style {
                    darling::ast::Style::Unit => quote!(#struct_name::#ident),
                    darling::ast::Style::Tuple => quote!(#struct_name::#ident(..)),
                    darling::ast::Style::Struct => quote!(#struct_name::#ident{..}),
                };
                arms.push(quote!(#pattern => #event,));
            }
            Ok(quote!(match self { #(#arms)* }))
        }
        darling::ast::Data::Struct(_) => {
            let event = input.event.as_deref().ok_or_else(|| {
                Error::new_spanned(struct_name, "Missing #[sioc(event = \"...\")]")
            })?;
            Ok(quote!(#event))
        }
    }
}

fn generate_serialization(input: &EmitInput, struct_name: &syn::Ident) -> Result<TokenStream> {
    let body = match &input.data {
        darling::ast::Data::Enum(variants) => {
            let mut arms = Vec::new();
            for variant in variants {
                let ident = &variant.ident;
                let event = variant.event.as_deref().unwrap();
                // 1 for event name + N fields
                let tuple_len = 1 + variant.fields.len();

                match variant.fields.style {
                    darling::ast::Style::Unit => {
                        arms.push(quote! {
                            #struct_name::#ident => {
                                let mut seq = serializer.serialize_tuple(1)?;
                                seq.serialize_element(#event)?;
                                seq.end()
                            }
                        });
                    }
                    darling::ast::Style::Tuple => {
                        let ids: Vec<_> = (0..variant.fields.len())
                            .map(|i| format_ident!("f{}", i))
                            .collect();
                        arms.push(quote! {
                            #struct_name::#ident( #(#ids),* ) => {
                                let mut seq = serializer.serialize_tuple(#tuple_len)?;
                                seq.serialize_element(#event)?;
                                #( seq.serialize_element(#ids)?; )*
                                seq.end()
                            }
                        });
                    }
                    darling::ast::Style::Struct => {
                        let ids: Vec<_> = variant
                            .fields
                            .fields
                            .iter()
                            .map(|f| f.ident.as_ref().unwrap())
                            .collect();
                        // Flatten named fields -> serialize as tuple elements
                        arms.push(quote! {
                            #struct_name::#ident { #(#ids),* } => {
                                let mut seq = serializer.serialize_tuple(#tuple_len)?;
                                seq.serialize_element(#event)?;
                                #( seq.serialize_element(#ids)?; )*
                                seq.end()
                            }
                        });
                    }
                }
            }
            quote!(match &self.0 { #(#arms)* })
        }
        darling::ast::Data::Struct(fields) => {
            let event = input.event.as_deref().unwrap();
            let tuple_len = 1 + fields.len();

            match fields.style {
                darling::ast::Style::Unit => quote! {
                    let mut seq = serializer.serialize_tuple(1)?;
                    seq.serialize_element(#event)?;
                    seq.end()
                },
                darling::ast::Style::Tuple => {
                    let indices: Vec<_> = (0..fields.len()).map(syn::Index::from).collect();
                    quote! {
                        let mut seq = serializer.serialize_tuple(#tuple_len)?;
                        seq.serialize_element(#event)?;
                        #( seq.serialize_element(&self.0.#indices)?; )*
                        seq.end()
                    }
                }
                darling::ast::Style::Struct => {
                    let ids: Vec<_> = fields
                        .fields
                        .iter()
                        .map(|f| f.ident.as_ref().unwrap())
                        .collect();
                    // Flatten named fields -> serialize as tuple elements
                    quote! {
                        let mut seq = serializer.serialize_tuple(#tuple_len)?;
                        seq.serialize_element(#event)?;
                        #( seq.serialize_element(&self.0.#ids)?; )*
                        seq.end()
                    }
                }
            }
        }
    };
    Ok(body)
}
