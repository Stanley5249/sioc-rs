use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::attrs::{SiocField, SiocInput};

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

    crate::attrs::validate_flatten(&fields)?;
    let lifetime = crate::attrs::fresh_lifetime(&input.generics);
    let sequence = crate::attrs::fresh_type_ident(&input.generics, "__SiocSequence");
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let ident = &input.ident;
    let body = generate_body(&fields, input.strict.is_present());

    Ok(quote! {
        impl #impl_generics ::sioc::prelude::DeserializePayload for #ident #type_generics #where_clause {
            fn deserialize_payload<#lifetime, #sequence>(__seq: &mut #sequence) -> ::std::result::Result<Self, #sequence::Error>
            where
                #sequence: ::serde::de::SeqAccess<#lifetime>,
            {
                #body
            }
        }
    })
}

fn var_for_field(pos: usize) -> TokenStream {
    let var = format_ident!("__sioc_field_{pos}");
    quote! { #var }
}

fn generate_body(fields: &darling::ast::Fields<SiocField>, strict: bool) -> TokenStream {
    let field_count = fields.iter().filter(|f| !f.flatten.is_present()).count();

    let vars: Vec<_> = fields
        .iter()
        .enumerate()
        .map(|(pos, _)| var_for_field(pos))
        .collect();

    let decls = fields.iter().zip(vars.iter()).enumerate().map(|(i, (field, var))| {
        if field.flatten.is_present() {
            let field_type = &field.ty;
            quote! {
                let mut #var: #field_type = ::std::default::Default::default();
                while let ::std::option::Option::Some(el) = __seq.next_element()? {
                    #var.push(el);
                }
            }
        } else {
            quote! {
                let #var = __seq.next_element()?
                    .ok_or_else(|| ::serde::de::Error::invalid_length(#i, &"expected element"))?;
            }
        }
    });

    let drain = if strict {
        quote! {
            let mut __extra = 0usize;
            while __seq.next_element::<::serde::de::IgnoredAny>()?.is_some() {
                __extra += 1;
            }
            if __extra > 0 {
                return ::std::result::Result::Err(
                    ::serde::de::Error::invalid_length(
                        #field_count + __extra,
                        &::std::format!("exactly {} elements", #field_count).as_str(),
                    )
                );
            }
        }
    } else {
        quote! {
            while __seq.next_element::<::serde::de::IgnoredAny>()?.is_some() {}
        }
    };

    let named_fields = fields.iter().zip(&vars).map(|(field, var)| {
        let name = &field.ident;
        quote! { #name: #var }
    });
    let construct = match fields.style {
        darling::ast::Style::Struct => quote! { Self { #(#named_fields),* } },
        darling::ast::Style::Tuple => quote! { Self(#(#vars),*) },
        darling::ast::Style::Unit => quote! { Self },
    };

    quote! {
        #(#decls)*
        #drain
        ::std::result::Result::Ok(#construct)
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
}
