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
#[darling(attributes(event))]
struct EmitVariant {
    ident: syn::Ident,
    fields: darling::ast::Fields<syn::Field>,

    // Capture #[event(name = "foo")]
    #[darling(default, rename = "name")]
    name: Option<String>,
}

pub fn expand(input: DeriveInput) -> Result<TokenStream> {
    let input = EmitInput::from_derive_input(&input)?;
    let enum_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let variants = match input.data {
        darling::ast::Data::Enum(v) => v,
        _ => return Err(Error::new_spanned(enum_name, "Event only supports enums")),
    };

    let mut event_name_arms = Vec::new();
    let mut to_packet_arms = Vec::new();

    for variant in variants {
        let variant_ident = &variant.ident;

        // Resolve name from darling fields
        let event_name_str = variant.name.ok_or_else(|| {
            Error::new_spanned(variant_ident, "Missing #[event(name = \"...\")] attribute")
        })?;

        let pattern = match variant.fields.style {
            darling::ast::Style::Unit => quote! { #enum_name::#variant_ident },
            darling::ast::Style::Tuple => quote! { #enum_name::#variant_ident(..) },
            darling::ast::Style::Struct => quote! { #enum_name::#variant_ident { .. } },
        };

        event_name_arms.push(quote! {
            #pattern => #event_name_str,
        });

        match variant.fields.style {
            darling::ast::Style::Unit => {
                to_packet_arms.push(quote! {
                    #enum_name::#variant_ident => {
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
                let pattern = quote! { #enum_name::#variant_ident( #(#field_names),* ) };
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
                let pattern = quote! { #enum_name::#variant_ident { #(#field_names),* } };
                let payload = quote! {
                    serde_json::json!({
                        #(stringify!(#field_names): #field_names),*
                    })
                };

                to_packet_arms.push(quote! {
                    #pattern => {
                        let tuple = (#event_name_str, #payload);
                        let json = serde_json::to_vec(&tuple)
                            .map_err(sioc_core::error::Error::Json)?;
                        bytes::Bytes::from(json)
                    }
                });
            }
        }
    }

    let expanded = quote! {
        impl #impl_generics sioc_core::event::Event for #enum_name #ty_generics #where_clause {
            fn name(&self) -> &'static str {
                match self { #(#event_name_arms)* }
            }

            fn to_json(&self) -> sioc_core::error::Result<bytes::Bytes> {
                Ok(match self {
                    #(#to_packet_arms)*
                })
            }
        }
    };
    Ok(expanded)
}
