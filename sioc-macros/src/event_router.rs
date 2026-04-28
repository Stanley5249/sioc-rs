use crate::attrs::SiocInput;
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub fn expand(input: syn::DeriveInput) -> darling::Result<TokenStream> {
    let input = SiocInput::from_derive_input(&input)?;
    let enum_ident = &input.ident;

    let variants = match input.data {
        darling::ast::Data::Enum(v) => v,
        darling::ast::Data::Struct(..) => {
            return Err(
                darling::Error::unsupported_shape_with_expected("struct", &"enum")
                    .with_span(&input.ident),
            );
        }
    };

    let shape = darling::util::ShapeSet::new([darling::util::Shape::Newtype]);

    for variant in &variants {
        shape
            .check(&variant.fields)
            .map_err(|e| e.with_span(&variant.ident))?;
    }

    let helper_ident = format_ident!("__{enum_ident}Helper");
    let visitor_ident = format_ident!("__{enum_ident}Visitor");

    let variants = variants
        .into_iter()
        .map(|v| {
            let ident = v.ident;
            let ty = v.fields.into_iter().next().unwrap().ty;
            (ident, ty)
        })
        .collect::<Vec<_>>();

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

        impl ::std::convert::TryFrom<::sioc::prelude::DynEvent> for #enum_ident {
            type Error = ::sioc::error::EventError;

            fn try_from(event: ::sioc::prelude::DynEvent) -> ::std::result::Result<Self, Self::Error> {
                let helper = ::sioc::payload::deserialize(&event.payload)?;

                Ok(match helper {
                    #(#from_event_arms)*
                })
            }
        }
    })
}
