/// Code generation utilities for the Event derive macro.
///
/// This module provides functions to generate the implementation code for the
/// `Event` trait methods (`name`, `to_payload`, and `from_payload`) based on
/// the parsed input from the derive macro. It handles both enum and struct
/// variants, supporting unit, tuple, and named field styles.
///
/// The generated code uses strict flattening for serialization (["event_name", field1, field2, ...])
/// and manual JSON array parsing for deserialization.
use crate::input::EmitInput;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, Result};

/// Generates the implementation for the `name` method of the Event trait.
///
/// This function creates a match expression that returns the event name
/// associated with each variant (for enums) or the struct itself (for structs).
/// The event names are extracted from the `#[sioc(event = "...")]` attributes.
///
/// # Arguments
///
/// * `input` - The parsed input containing the enum/struct definition and attributes.
/// * `struct_name` - The identifier of the type being derived.
///
/// # Returns
///
/// A `TokenStream` representing the generated match expression for the `name` method.
///
/// # Errors
///
/// Returns an error if a required `#[sioc(event = "...")]` attribute is missing.
pub fn generate_name_match(input: &EmitInput, struct_name: &syn::Ident) -> Result<TokenStream> {
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

/// Generates the implementation for the `to_payload` method of the Event trait.
///
/// This function creates serialization code that converts the event into a JSON byte vector
/// using the strict flattening format: `["event_name", field1, field2, ...]`.
/// It uses a `SerAdapter` struct to implement the `Serialize` trait for serde.
///
/// # Arguments
///
/// * `input` - The parsed input containing the enum/struct definition and attributes.
/// * `struct_name` - The identifier of the type being derived.
///
/// # Returns
///
/// A `TokenStream` representing the generated serialization logic for the `to_payload` method.
///
/// # Notes
///
/// - For enums, generates a match on `&self.0` (assuming SerAdapter wraps the type).
/// - For structs, generates direct serialization based on field style.
/// - Fields are serialized as tuple elements after the event name.
pub fn generate_serialization(input: &EmitInput, struct_name: &syn::Ident) -> Result<TokenStream> {
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

/// Generates the implementation for the `from_payload` method of the Event trait.
///
/// This function creates deserialization code that parses a JSON byte slice
/// in the strict flattening format: `["event_name", field1, field2, ...]` and
/// reconstructs the event instance. It performs manual sequence reading and
/// validation of the array length and event name.
///
/// # Arguments
///
/// * `input` - The parsed input containing the enum/struct definition and attributes.
/// * `struct_name` - The identifier of the type being derived.
///
/// # Returns
///
/// A `TokenStream` representing the generated deserialization logic for the `from_payload` method.
///
/// # Errors
///
/// The generated code will return errors for:
/// - Invalid JSON format.
/// - Non-array JSON values.
/// - Empty arrays.
/// - Non-string first elements.
/// - Mismatched event names.
/// - Incorrect array lengths.
/// - Unknown event names (for enums).
///
/// # Notes
///
/// - Uses `serde_json::from_slice` to parse the input.
/// - For enums, matches the event name to determine the variant.
/// - For structs, verifies the event name matches the expected one.
/// - Fields are deserialized from subsequent array elements using `serde_json::from_value`.
pub fn generate_deserialization(
    input: &EmitInput,
    struct_name: &syn::Ident,
) -> Result<TokenStream> {
    let body = quote! {
        let value: serde_json::Value = serde_json::from_slice(payload).map_err(sioc_core::error::Error::from)?;
        let arr = value.as_array().ok_or_else(|| sioc_core::error::Error::Protocol("expected array".into()))?;
        if arr.is_empty() {
            return Err(sioc_core::error::Error::Protocol("empty array".into()));
        }
        let name = arr[0].as_str().ok_or_else(|| sioc_core::error::Error::Protocol("first element not string".into()))?;
    };

    let deser_match = match &input.data {
        darling::ast::Data::Enum(variants) => {
            let mut arms = Vec::new();
            for variant in variants {
                let ident = &variant.ident;
                let event = variant.event.as_deref().unwrap();
                let field_count = variant.fields.len();
                let expected_len = 1 + field_count;

                let deser_arm = match variant.fields.style {
                    darling::ast::Style::Unit => {
                        quote! {
                            if arr.len() != 1 {
                                return Err(sioc_core::error::Error::Protocol(format!("expected 1 element for {}", #event)));
                            }
                            Ok(#struct_name::#ident)
                        }
                    }
                    darling::ast::Style::Tuple => {
                        let deser_fields: Vec<_> = (0..field_count).map(|i| {
                            let idx = i + 1;
                            quote! { serde_json::from_value(arr[#idx].clone()).map_err(sioc_core::error::Error::from)? }
                        }).collect();
                        quote! {
                            if arr.len() != #expected_len {
                                return Err(sioc_core::error::Error::Protocol(format!("expected {} elements for {}", #expected_len, #event)));
                            }
                            Ok(#struct_name::#ident( #(#deser_fields),* ))
                        }
                    }
                    darling::ast::Style::Struct => {
                        let field_names: Vec<_> = variant
                            .fields
                            .fields
                            .iter()
                            .map(|f| f.ident.as_ref().unwrap())
                            .collect();
                        let deser_fields: Vec<_> = field_names.iter().enumerate().map(|(i, name)| {
                            let idx = i + 1;
                            quote! { #name: serde_json::from_value(arr[#idx].clone()).map_err(sioc_core::error::Error::from)? }
                        }).collect();
                        quote! {
                            if arr.len() != #expected_len {
                                return Err(sioc_core::error::Error::Protocol(format!("expected {} elements for {}", #expected_len, #event)));
                            }
                            Ok(#struct_name::#ident { #(#deser_fields),* })
                        }
                    }
                };
                arms.push(quote! {
                    #event => {
                        #deser_arm
                    }
                });
            }
            quote! {
                match name {
                    #(#arms)*
                    _ => Err(sioc_core::error::Error::UnknownEvent(name.to_string())),
                }
            }
        }
        darling::ast::Data::Struct(fields) => {
            let event = input.event.as_deref().unwrap();
            let field_count = fields.len();
            let expected_len = 1 + field_count;

            let check_name = quote! {
                if name != #event {
                    return Err(sioc_core::error::Error::Protocol(format!("expected event {}, got {}", #event, name)));
                }
            };

            let deser_arm = match fields.style {
                darling::ast::Style::Unit => {
                    quote! {
                        if arr.len() != 1 {
                            return Err(sioc_core::error::Error::Protocol(format!("expected 1 element for {}", #event)));
                        }
                        Ok(#struct_name)
                    }
                }
                darling::ast::Style::Tuple => {
                    let deser_fields: Vec<_> = (0..field_count).map(|i| {
                        let idx = i + 1;
                        quote! { serde_json::from_value(arr[#idx].clone()).map_err(sioc_core::error::Error::from)? }
                    }).collect();
                    quote! {
                        if arr.len() != #expected_len {
                            return Err(sioc_core::error::Error::Protocol(format!("expected {} elements for {}", #expected_len, #event)));
                        }
                        Ok(#struct_name( #(#deser_fields),* ))
                    }
                }
                darling::ast::Style::Struct => {
                    let field_names: Vec<_> = fields
                        .fields
                        .iter()
                        .map(|f| f.ident.as_ref().unwrap())
                        .collect();
                    let deser_fields: Vec<_> = field_names.iter().enumerate().map(|(i, name)| {
                        let idx = i + 1;
                        quote! { #name: serde_json::from_value(arr[#idx].clone()).map_err(sioc_core::error::Error::from)? }
                    }).collect();
                    quote! {
                        if arr.len() != #expected_len {
                            return Err(sioc_core::error::Error::Protocol(format!("expected {} elements for {}", #expected_len, #event)));
                        }
                        Ok(#struct_name { #(#deser_fields),* })
                    }
                }
            };
            quote! {
                #check_name
                #deser_arm
            }
        }
    };

    Ok(quote! {
        #body
        #deser_match
    })
}
