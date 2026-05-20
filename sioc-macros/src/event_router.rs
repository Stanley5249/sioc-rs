use crate::attrs::SiocInput;
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

struct ExpandData {
    enum_ident: syn::Ident,
    helper_ident: syn::Ident,
    visitor_ident: syn::Ident,
    variants: Vec<(syn::Ident, syn::Type)>,
}

fn parse_expand_data(input: SiocInput) -> darling::Result<ExpandData> {
    let enum_ident = input.ident.clone();

    let raw_variants = match input.data {
        darling::ast::Data::Enum(v) => v,
        darling::ast::Data::Struct(..) => {
            return Err(
                darling::Error::unsupported_shape_with_expected("struct", &"enum")
                    .with_span(&enum_ident),
            );
        }
    };

    let shape = darling::util::ShapeSet::new([darling::util::Shape::Newtype]);
    for variant in &raw_variants {
        shape
            .check(&variant.fields)
            .map_err(|e| e.with_span(&variant.ident))?;
    }

    let variants = raw_variants
        .into_iter()
        .map(|v| {
            let ty = v.fields.into_iter().next().unwrap().ty;
            (v.ident, ty)
        })
        .collect();

    let helper_ident = format_ident!("__{enum_ident}Helper");
    let visitor_ident = format_ident!("__{enum_ident}Visitor");

    Ok(ExpandData {
        enum_ident,
        helper_ident,
        visitor_ident,
        variants,
    })
}

pub fn expand(input: &syn::DeriveInput) -> darling::Result<TokenStream> {
    let input = SiocInput::from_derive_input(input)?;
    let ExpandData {
        enum_ident,
        helper_ident,
        visitor_ident,
        variants,
    } = parse_expand_data(input)?;

    let helper_variants = variants.iter().map(|(vi, ty)| {
        quote! { #vi(<#ty as ::sioc::prelude::EventHandler>::Payload) }
    });

    let visit_arms = variants.iter().map(|(vi, ty)| {
        quote! {
            <#ty as ::sioc::prelude::EventHandler>::Payload::NAME => Self::Value::#vi(
                <#ty as ::sioc::prelude::EventHandler>::Payload::deserialize_payload(&mut seq)?,
            ),
        }
    });

    let all_names = variants
        .iter()
        .map(|(_, ty)| quote! { <#ty as ::sioc::prelude::EventHandler>::Payload::NAME });

    let name_arms = variants.iter().map(|(vi, ty)| {
        quote! {
            #enum_ident::#vi(_) => <#ty as ::sioc::prelude::EventHandler>::Payload::NAME,
        }
    });

    let from_event_arms = variants.iter().map(|(vi, ty)| {
        quote! {
            #helper_ident::#vi(args) => {
                #enum_ident::#vi(<#ty as ::sioc::prelude::EventHandler>::handle(
                    args,
                    event.id,
                    event.attachments,
                )?)
            }
        }
    });

    Ok(quote! {
        enum #helper_ident {
            #(#helper_variants),*
        }

        impl<'de> ::serde::Deserialize<'de> for #helper_ident {
            fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                deserializer.deserialize_seq(#visitor_ident)
            }
        }

        struct #visitor_ident;

        impl<'de> ::serde::de::Visitor<'de> for #visitor_ident {
            type Value = #helper_ident;

            fn expecting(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                f.write_str("a Socket.IO event payload")
            }

            fn visit_seq<V>(self, mut seq: V) -> ::std::result::Result<Self::Value, V::Error>
            where
                V: ::serde::de::SeqAccess<'de>,
            {
                let name: &str = seq
                    .next_element()?
                    .ok_or_else(|| ::serde::de::Error::invalid_length(0, &"event name"))?;

                Ok(match name {
                    #(#visit_arms)*
                    _ => {
                        return Err(::serde::de::Error::unknown_variant(name, &[#(#all_names),*]));
                    }
                })
            }
        }

        impl ::sioc::prelude::EventRouter for #enum_ident {
            fn name(&self) -> &'static str {
                match self {
                    #(#name_arms)*
                }
            }
        }

        impl ::std::convert::TryFrom<::sioc::prelude::DynEvent> for #enum_ident {
            type Error = ::sioc::error::EventError;

            fn try_from(event: ::sioc::prelude::DynEvent) -> ::std::result::Result<Self, Self::Error> {
                let helper = ::sioc::payload::from_json::<#helper_ident>(&event.payload)?;

                Ok(match helper {
                    #(#from_event_arms)*
                })
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_input_returns_error() {
        let input: syn::DeriveInput = syn::parse_str("struct Foo { x: i32 }").unwrap();
        assert!(expand(&input).is_err());
    }
}
