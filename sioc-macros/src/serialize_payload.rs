use crate::attrs::{SiocField, SiocInput};
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::quote;

pub fn expand(input: &syn::DeriveInput) -> darling::Result<TokenStream> {
    let input = SiocInput::from_derive_input(input)?;

    let fields = match input.data {
        darling::ast::Data::Struct(f) => f,
        darling::ast::Data::Enum(..) => {
            return Err(
                darling::Error::unsupported_shape_with_expected("enum", &"struct")
                    .with_span(&input.ident),
            );
        }
    };

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let ident = &input.ident;
    let body = generate_body(&fields);

    Ok(quote! {
        impl #impl_generics ::sioc::prelude::SerializePayload for #ident #type_generics #where_clause {
            fn serialize_payload<S>(&self, __seq: &mut S) -> ::std::result::Result<(), S::Error>
            where
                S: ::serde::ser::SerializeSeq,
            {
                #body
            }
        }
    })
}

fn generate_body(fields: &darling::ast::Fields<SiocField>) -> TokenStream {
    let it = fields.iter().enumerate().map(|(i, field)| {
        let accessor = if let Some(name) = &field.ident {
            quote! { #name }
        } else {
            let index = syn::Index::from(i);
            quote! { #index }
        };

        if field.flatten.is_present() {
            quote! {
                for el in &self.#accessor {
                    __seq.serialize_element(el)?;
                }
            }
        } else {
            quote! { __seq.serialize_element(&self.#accessor)?; }
        }
    });

    quote! {
        #(#it)*
        ::std::result::Result::Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enum_input_returns_error() {
        let input: syn::DeriveInput = syn::parse_str("enum Foo { A(i32) }").unwrap();
        expand(&input).unwrap_err();
    }

    #[test]
    fn named_struct_succeeds() {
        let input: syn::DeriveInput = syn::parse_str("struct Foo { x: i32, y: String }").unwrap();
        expand(&input).unwrap();
    }

    #[test]
    fn tuple_struct_succeeds() {
        let input: syn::DeriveInput = syn::parse_str("struct Foo(i32, String);").unwrap();
        expand(&input).unwrap();
    }

    #[test]
    fn flatten_field_succeeds() {
        let input: syn::DeriveInput =
            syn::parse_str("struct Foo { x: i32, #[sioc(flatten)] rest: Vec<i32> }").unwrap();
        expand(&input).unwrap();
    }
}
