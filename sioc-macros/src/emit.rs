use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Result};

use crate::codegen;
use crate::input;

pub fn expand(input: DeriveInput) -> Result<TokenStream> {
    let input = input::EmitInput::from_derive_input(&input)?;
    let struct_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let name_body = codegen::generate_name_match(&input, struct_name)?;
    let serialize_body = codegen::generate_serialization(&input, struct_name)?;
    let deserialize_body = codegen::generate_deserialization(&input, struct_name)?;

    Ok(quote! {
        impl #impl_generics sioc_core::event::Event for #struct_name #ty_generics #where_clause {
            fn name(&self) -> &'static str {
                #name_body
            }

            fn to_payload(&self) -> sioc_core::error::Result<Vec<u8>> {
                use serde::ser::{Serialize, Serializer, SerializeTuple};

                struct SerAdapter<'a>(&'a #struct_name);

                impl<'a> Serialize for SerAdapter<'a> {
                    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
                    where S: Serializer {
                        #serialize_body
                    }
                }

                // Error conversion is handled automatically by #[from] in sioc_core::Error
                serde_json::to_vec(&SerAdapter(self)).map_err(Into::into)
            }

            fn from_payload(payload: &[u8]) -> sioc_core::error::Result<Self> {
                #deserialize_body
            }
        }
    })
}
