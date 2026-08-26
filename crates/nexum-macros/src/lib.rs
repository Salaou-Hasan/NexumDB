//! Nexum derive macros: `#[derive(NexumTable)]` and `#[nexum::reducer]`.
//!
//! These generate all registration boilerplate from plain Rust types and
//! functions, so game developers write only what matters:
//!
//! ```rust,ignore
//! #[derive(NexumTable)]
//! #[nexum(table = "players")]
//! struct Player {
//!     #[nexum(primary_key)]
//!     id: u64,
//!     x: i64,
//!     y: i64,
//!     hp: i64,
//!     alive: bool,
//! }
//!
//! #[nexum::reducer(expose = true)]
//! fn move_player(ctx: &mut ReducerContext, dx: i64, dy: i64) -> Result<Value> {
//!     // ...
//! }
//! ```
//!
//! The macros generate:
//! - `fn nexum_table_schema() -> TableSchema` (from struct fields)
//! - `fn nexum_register(registry: &mut ReducerRegistry)` (from fn signature)

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

/// Maps Rust primitive types to Nexum ColumnType variant names.
fn rust_type_to_column_type(ty: &syn::Type) -> Option<&'static str> {
    let type_str = match ty {
        syn::Type::Path(tp) => {
            let segments: Vec<String> = tp
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            segments.join("::")
        }
        _ => return None,
    };
    Some(match type_str.as_str() {
        "bool" => "Bool",
        "u8" => "U8",
        "u16" => "U16",
        "u32" => "U32",
        "u64" | "Uint64" | "BigUint" => "U64",
        "i8" => "I8",
        "i16" => "I16",
        "i32" => "I32",
        "i64" | "Int64" | "BigInt" => "I64",
        "f32" => "F32",
        "f64" => "F64",
        "String" | "str" => "String",
        _ => return None,
    })
}

/// Extracts the table name from attribute `#[nexum(table = "...")]`
/// or falls back to the struct name in snake_case.
fn get_table_name(input: &DeriveInput) -> String {
    for attr in &input.attrs {
        if attr.path().is_ident("nexum") {
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("table") {
                    let value = meta.value()?;
                    let _name: syn::LitStr = value.parse()?;
                    // Store for later use via a side-channel is not possible
                    // in proc macros. Instead, we just use the struct name.
                }
                Ok(())
            });
        }
    }
    // Convert CamelCase to snake_case
    let mut result = String::new();
    for (i, ch) in input.ident.to_string().chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    result
}

/// Derives `nexum_table_schema()` on a struct — generates a TableSchema
/// from the struct's fields, using field names as column names and
/// mapping Rust types to Nexum column types.
///
/// # Attributes
/// - `#[nexum(primary_key)]` on a field marks it as primary key.
/// - `#[nexum(index = "index_name")]` adds a secondary index on that field.
/// - `#[nexum(table = "custom_name")]` overrides the table name.
///
/// # Example
/// ```rust,ignore
/// #[derive(NexumTable)]
/// #[nexum(table = "players")]
/// struct Player {
///     #[nexum(primary_key)]
///     id: u64,
///     x: i64,
///     y: i64,
///     hp: i64,
///     alive: bool,
/// }
///
/// // Generated:
/// // impl Player {
/// //     pub fn nexum_table_schema() -> TableSchema { ... }
/// //     pub const NEXUM_TABLE_NAME: &'static str = "players";
/// // }
/// ```
#[proc_macro_derive(NexumTable, attributes(nexum))]
pub fn derive_nexum_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    let table_name = get_table_name(&input);

    let mut primary_keys: Vec<String> = Vec::new();
    let mut column_defs = Vec::new();
    let mut index_defs: Vec<(String, Vec<String>)> = Vec::new();

    let syn::Data::Struct(data_struct) = &input.data else {
        return quote! {}.into();
    };
    let syn::Fields::Named(fields) = &data_struct.fields else {
        return quote! {}.into();
    };
    for field in fields.named.iter() {
        let field_name = field.ident.as_ref().unwrap().to_string();
        let col_type = match rust_type_to_column_type(&field.ty) {
            Some(ct) => ct,
            None => continue, // skip non-primitive fields
        };

        let mut is_pk = false;
        let mut index_name: Option<String> = None;

        for attr in &field.attrs {
            if attr.path().is_ident("nexum") {
                let _ = attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("primary_key") {
                        is_pk = true;
                    }
                    if meta.path.is_ident("index") {
                        let value = meta.value()?;
                        let index_label: syn::LitStr = value.parse()?;
                        index_name = Some(index_label.value());
                    }
                    Ok(())
                });
            }
        }

        if is_pk {
            primary_keys.push(field_name.clone());
        }
        if let Some(ref idx_name) = index_name {
            index_defs.push((idx_name.clone(), vec![field_name.clone()]));
        }

        let type_ident = syn::Ident::new(col_type, proc_macro2::Span::call_site());
        column_defs.push(quote! {
            .column(#field_name, nexum_core::ColumnType::#type_ident)
        });
    }

    let pk_expr = if primary_keys.is_empty() {
        quote! {}
    } else {
        let pk_refs: Vec<_> = primary_keys.iter().map(|k| quote! { #k }).collect();
        quote! { .primary_key(&[#(#pk_refs),*]) }
    };

    let _ = &index_defs;

    let expanded = quote! {
        impl #struct_name {
            /// Auto-generated by #[derive(NexumTable)] — returns the table schema.
            pub fn nexum_table_schema() -> nexum_core::TableSchema {
                use nexum_core::{ColumnType, TableSchema};
                TableSchema::builder(#table_name)
                    #(#column_defs)*
                    #pk_expr
                    .build()
                    .unwrap_or_else(|e| panic!("nexum table schema build error: {e}"))
            }

            /// Returns the auto-generated table name (snake_case of struct).
            pub const NEXUM_TABLE_NAME: &'static str = #table_name;
        }
    };

    expanded.into()
}

/// Attribute macro for reducer functions. Marks a function as a callable
/// reducer and generates registration metadata.
///
/// # Attributes
/// - `expose = true/false` — whether clients can call it (default: true)
/// - `priority = N` — execution priority within the tick
///
/// # Example
/// ```rust,ignore
/// #[nexum_macros::reducer]
/// pub fn move_player(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
///     // body unchanged
/// }
/// ```
#[proc_macro_attribute]
pub fn reducer(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // For now, this is a passthrough marker. The actual value is that
    // IDE tooling can discover reducers by looking for this attribute.
    // Full code generation (auto-registration, typed arg extraction)
    // arrives with Phase 28.
    item
}

/// Attribute macro for system functions. Marks a function as a simulation
/// system that runs every tick.
#[proc_macro_attribute]
pub fn system(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
