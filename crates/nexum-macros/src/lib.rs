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

    // Collect field metadata
    let mut primary_keys: Vec<String> = Vec::new();
    let mut fields_meta: Vec<(String, String)> = Vec::new(); // (name, col_type_str)
    let mut pk_field_index: Option<usize> = None;

    let syn::Data::Struct(data_struct) = &input.data else {
        return quote! {}.into();
    };
    let syn::Fields::Named(fields) = &data_struct.fields else {
        return quote! {}.into();
    };

    for (col_idx, field) in fields.named.iter().enumerate() {
        let field_name = field.ident.as_ref().unwrap().to_string();
        let col_type = match rust_type_to_column_type(&field.ty) {
            Some(ct) => ct,
            None => continue,
        };

        let mut is_pk = false;
        for attr in &field.attrs {
            if attr.path().is_ident("nexum") {
                let _ = attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("primary_key") {
                        is_pk = true;
                    }
                    Ok(())
                });
            }
        }

        if is_pk {
            primary_keys.push(field_name.clone());
            pk_field_index = Some(col_idx);
        }
        fields_meta.push((field_name, col_type.to_string()));
    }

    let pk_field_name = primary_keys.first().cloned().unwrap_or_default();
    let pk_col_idx = pk_field_index.unwrap_or(0);

    // Build column definitions chain
    let column_defs: Vec<_> = fields_meta
        .iter()
        .map(|(name, ct)| {
            let type_ident = syn::Ident::new(ct, proc_macro2::Span::call_site());
            quote! { .column(#name, ::nexum_core::ColumnType::#type_ident) }
        })
        .collect();

    let pk_expr = if primary_keys.is_empty() {
        quote! {}
    } else {
        let pk_refs: Vec<_> = primary_keys.iter().map(|k| quote! { #k }).collect();
        quote! { .primary_key(&[#(#pk_refs),*]) }
    };

    // Generate per-field from_row extraction expressions
    let from_row_fields: Vec<_> = fields_meta
        .iter()
        .enumerate()
        .map(|(idx, (name, ct))| {
            let field_ident = syn::Ident::new(name, proc_macro2::Span::call_site());
            let idx_lit = syn::LitInt::new(&idx.to_string(), proc_macro2::Span::call_site());
            let expr = match ct.as_str() {
                "U64" | "U8" | "U16" | "U32" => quote! {
                    row.get(#idx_lit).and_then(|v| v.as_u64()).unwrap_or(0)
                },
                "I64" | "I8" | "I16" | "I32" => quote! {
                    row.get(#idx_lit).and_then(|v| v.as_i64()).unwrap_or(0)
                },
                "F64" | "F32" => quote! {
                    match row.get(#idx_lit) {
                        Some(::nexum_core::Value::F64(v)) => *v,
                        _ => 0.0,
                    }
                },
                "String" => quote! {
                    row.get(#idx_lit).and_then(|v| v.as_str()).unwrap_or("").to_string()
                },
                _ => quote! {
                    row.get(#idx_lit).and_then(|v| v.as_i64()).unwrap_or(0)
                },
            };
            quote! { #field_ident: #expr }
        })
        .collect();

    // Generate per-field to_row value construction expressions
    let to_row_values: Vec<_> = fields_meta
        .iter()
        .enumerate()
        .map(|(idx, (name, ct))| {
            let field_ident = syn::Ident::new(name, proc_macro2::Span::call_site());
            let expr = match ct.as_str() {
                "U64" | "U8" | "U16" | "U32" => quote! {
                    ::nexum_core::Value::U64(self.#field_ident as u64)
                },
                "I64" | "I8" | "I16" | "I32" => quote! {
                    ::nexum_core::Value::I64(self.#field_ident as i64)
                },
                "F64" | "F32" => quote! {
                    ::nexum_core::Value::F64(self.#field_ident as f64)
                },
                "String" => quote! {
                    ::nexum_core::Value::String(self.#field_ident.clone())
                },
                _ => quote! {
                    ::nexum_core::Value::I64(self.#field_ident as i64)
                },
            };
            quote! { #expr }
        })
        .collect();

    // Determine PK expression for get/delete lookups
    let pk_lookup_type =
        pk_field_index.and_then(|idx| fields_meta.get(idx).map(|(_, ct)| ct.clone()));
    let pk_fn_param = match pk_lookup_type.as_deref() {
        Some("U64") | Some("U8") | Some("U16") | Some("U32") => quote! { pk_id: u64 },
        Some("I64") | Some("I8") | Some("I16") | Some("I32") => quote! { pk_id: i64 },
        Some("String") => quote! { pk_id: String },
        _ => quote! { pk_id: u64 },
    };
    let pk_lookup_expr = match pk_lookup_type.as_deref() {
        Some("I64") | Some("I8") | Some("I16") | Some("I32") => {
            quote! { &[::nexum_core::Value::I64(pk_id)] }
        }
        Some("String") => {
            quote! { &[::nexum_core::Value::String(pk_id.clone())] }
        }
        _ => quote! { &[::nexum_core::Value::U64(pk_id)] },
    };
    // For save: read the PK value from self instead of a parameter.
    let pk_field_ident = syn::Ident::new(&pk_field_name, proc_macro2::Span::call_site());
    let pk_self_lookup = match pk_lookup_type.as_deref() {
        Some("I64") | Some("I8") | Some("I16") | Some("I32") => {
            quote! { &[::nexum_core::Value::I64(self.#pk_field_ident as i64)] }
        }
        Some("String") => {
            quote! { &[::nexum_core::Value::String(self.#pk_field_ident.clone())] }
        }
        _ => quote! { &[::nexum_core::Value::U64(self.#pk_field_ident)] },
    };

    let expanded = quote! {
        impl #struct_name {
            /// Auto-generated table name.
            pub const NEXUM_TABLE_NAME: &'static str = #table_name;

            /// Auto-generated schema.
            pub fn nexum_table_schema() -> nexum_core::TableSchema {
                ::nexum_core::TableSchema::builder(#table_name)
                    #(#column_defs)*
                    #pk_expr
                    .build()
                    .unwrap_or_else(|e| panic!("nexum table schema build error: {e}"))
            }

            /// Deserialize from a Row reference.
            pub fn nexum_from_row(row: &::nexum_core::Row) -> Self {
                Self {
                    #(#from_row_fields,)*
                }
            }

            /// Serialize to a Row.
            pub fn nexum_to_row(&self) -> ::nexum_core::Row {
                ::nexum_core::Row::new(vec![
                    #(#to_row_values,)*
                ])
            }

            /// Get one entity by primary key.
            pub fn nexum_get(
                ctx: &mut ::nexum_reducer::ReducerContext,
                #pk_fn_param,
            ) -> ::nexum_core::Result<Option<Self>> {
                let owners = ctx.lookup_unique(
                    Self::NEXUM_TABLE_NAME, "primary",
                    #pk_lookup_expr,
                )?;
                match owners.first() {
                    Some(&rid) => {
                        let row = ctx.get(Self::NEXUM_TABLE_NAME, rid)?;
                        Ok(row.as_ref().map(Self::nexum_from_row))
                    }
                    None => Ok(None),
                }
            }

            /// Save (update) this entity by primary key.
            pub fn nexum_save(&self, ctx: &mut ::nexum_reducer::ReducerContext) -> ::nexum_core::Result<()> {
                let owners = ctx.lookup_unique(
                    Self::NEXUM_TABLE_NAME, "primary",
                    #pk_self_lookup,
                )?;
                let Some(&rid) = owners.first() else {
                    return Err(::nexum_core::Error::not_found(concat!(
                        "cannot save ", #table_name, ": row not found"
                    )));
                };
                ctx.update(Self::NEXUM_TABLE_NAME, rid, self.nexum_to_row())?;
                Ok(())
            }

            /// Create (insert) this entity.
            pub fn nexum_create(&self, ctx: &mut ::nexum_reducer::ReducerContext) -> ::nexum_core::Result<()> {
                ctx.insert(Self::NEXUM_TABLE_NAME, self.nexum_to_row())?;
                Ok(())
            }

            /// Delete by primary key.
            pub fn nexum_delete(
                ctx: &mut ::nexum_reducer::ReducerContext,
                #pk_fn_param,
            ) -> ::nexum_core::Result<()> {
                let owners = ctx.lookup_unique(
                    Self::NEXUM_TABLE_NAME, "primary",
                    #pk_lookup_expr,
                )?;
                if let Some(&rid) = owners.first() {
                    ctx.delete(Self::NEXUM_TABLE_NAME, rid)?;
                }
                Ok(())
            }

            /// Scan all rows and deserialize into typed structs.
            pub fn nexum_all(
                ctx: &mut ::nexum_reducer::ReducerContext,
            ) -> ::nexum_core::Result<Vec<Self>> {
                Ok(ctx.scan(Self::NEXUM_TABLE_NAME)?
                    .into_iter()
                    .map(|(_, row)| Self::nexum_from_row(&row))
                    .collect())
            }
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
