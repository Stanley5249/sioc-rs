use crate::attrs::{SiocField, SiocInput};
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::quote;

pub fn expand(input: syn::DeriveInput) -> darling::Result<TokenStream> {
    let input = SiocInput::from_derive_input(&input)?;

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
    let body = generate_body(fields);

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

fn generate_body(fields: darling::ast::Fields<SiocField>) -> TokenStream {
    let it = fields.iter().enumerate().map(|(i, field)| {
        let accessor = match &field.ident {
            Some(name) => quote! { #name },
            None => {
                let index = syn::Index::from(i);
                quote! { #index }
            }
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
