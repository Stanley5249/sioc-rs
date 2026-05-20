use crate::attrs::{SiocField, SiocInput};
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

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
    let body = generate_body(&fields, input.strict.is_present());

    Ok(quote! {
        impl #impl_generics ::sioc::prelude::DeserializePayload for #ident #type_generics #where_clause {
            fn deserialize_payload<'de, S>(__seq: &mut S) -> ::std::result::Result<Self, S::Error>
            where
                S: ::serde::de::SeqAccess<'de>,
            {
                #body
            }
        }
    })
}

fn var_for_field(f: &SiocField, pos: usize) -> TokenStream {
    if let Some(name) = &f.ident {
        quote! { #name }
    } else {
        let v = format_ident!("__field_{pos}");
        quote! { #v }
    }
}

fn generate_body(fields: &darling::ast::Fields<SiocField>, strict: bool) -> TokenStream {
    let field_count = fields.iter().filter(|f| !f.flatten.is_present()).count();

    let vars: Vec<_> = fields
        .iter()
        .enumerate()
        .map(|(pos, f)| var_for_field(f, pos))
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

    let construct = match fields.style {
        darling::ast::Style::Struct => quote! { Self { #(#vars),* } },
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
        assert!(expand(&input).is_err());
    }
}
