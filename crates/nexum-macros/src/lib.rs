//! Nexum macros: derive `NexumTable` on structs to generate schema + CRUD.
//!
//! ```rust,ignore
//! #[derive(nexum_macros::NexumTable)]
//! struct Player {
//!     id: u64,     // first field = primary key
//!     x: i64,
//!     hp: i64,
//! }
//!
//! // Auto-generated:
//! // Player::get(ctx, id) -> Result<Option<Player>>
//! // player.save(ctx) -> Result<()>
//! // player.create(ctx) -> Result<()>
//! // Player::delete(ctx, id) -> Result<()>
//! // Player::all(ctx) -> Vec<Player>
//! // Player::schema() -> TableSchema
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

/// Maps Rust primitive types to Nexum ColumnType variant names.
fn col_type(ty: &syn::Type) -> Option<&'static str> {
    let syn::Type::Path(tp) = ty else { return None };
    let name: String = tp
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect::<Vec<_>>()
        .join("::");
    Some(match name.as_str() {
        "bool" => "Bool",
        "u8" => "U8",
        "u16" => "U16",
        "u32" => "U32",
        "u64" => "U64",
        "i8" => "I8",
        "i16" => "I16",
        "i32" => "I32",
        "i64" => "I64",
        "f32" => "F32",
        "f64" => "F64",
        "String" | "str" => "String",
        _ => return None,
    })
}

fn snake_case(s: &str) -> String {
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    out
}

/// Derive `NexumTable` on a struct to generate schema + full typed CRUD.
///
/// The first field is treated as the primary key. Field names become column
/// names; Rust types map to ColumnType variants automatically.
#[proc_macro_derive(NexumTable, attributes(primary_key))]
pub fn derive_nexum_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    let table_name = snake_case(&struct_name.to_string());

    let syn::Data::Struct(data_struct) = &input.data else {
        return quote! {
            compile_error!("NexumTable can only be applied to structs");
        }
        .into();
    };
    let syn::Fields::Named(fields_named) = &data_struct.fields else {
        return quote! {
            compile_error!("NexumTable requires named fields");
        }
        .into();
    };

    let named = &fields_named.named;

    // ── collect field metadata ──
    let mut pk_field: Option<syn::Ident> = None;
    let mut pk_type: Option<&syn::Type> = None;
    let mut column_defs: Vec<_> = Vec::new();

    for (col_idx, field) in named.iter().enumerate() {
        let fname = field.ident.as_ref().unwrap();
        let Some(ct) = col_type(&field.ty) else {
            continue;
        };

        if col_idx == 0 {
            pk_field = Some(fname.clone());
            pk_type = Some(&field.ty);
        }

        let ct_ident = syn::Ident::new(ct, proc_macro2::Span::call_site());
        column_defs.push(quote! {
            .column(stringify!(#fname), ::nexum_core::ColumnType::#ct_ident)
        });
    }

    let pk_expr = match &pk_field {
        Some(pk) => {
            let name_str = snake_case(&pk.to_string());
            quote! { .primary_key(&[#name_str]) }
        }
        None => quote! {},
    };

    let Some(pk_ident) = &pk_field else {
        return quote! { compile_error!("NexumTable requires at least one field"); }.into();
    };
    let Some(pk_ty) = pk_type else {
        return quote! { compile_error!("cannot resolve primary key type"); }.into();
    };

    // PK lookup expressions
    let pk_fn_param = quote! { #pk_ident: #pk_ty };
    let pk_lookup = match pk_ty {
        syn::Type::Path(tp)
            if tp
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .as_deref()
                == Some("i64") =>
        {
            quote! { &[::nexum_core::Value::I64(#pk_ident as i64)] }
        }
        syn::Type::Path(tp)
            if tp
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .as_deref()
                == Some("String") =>
        {
            quote! { &[::nexum_core::Value::String(#pk_ident.clone())] }
        }
        _ => quote! { &[::nexum_core::Value::U64(#pk_ident)] },
    };
    let pk_self_lookup = match pk_ty {
        syn::Type::Path(tp)
            if tp
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .as_deref()
                == Some("i64") =>
        {
            quote! { &[::nexum_core::Value::I64(self.#pk_ident)] }
        }
        syn::Type::Path(tp)
            if tp
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .as_deref()
                == Some("String") =>
        {
            quote! { &[::nexum_core::Value::String(self.#pk_ident.clone())] }
        }
        _ => quote! { &[::nexum_core::Value::U64(self.#pk_ident)] },
    };

    // ── per-field from_row extraction ──
    let from_row_fields: Vec<_> = named.iter().filter_map(|f| {
        let ident = f.ident.as_ref()?;
        let ct = col_type(&f.ty)?;
        let idx = syn::LitInt::new(
            &named.iter().position(|x| x == f).unwrap().to_string(),
            proc_macro2::Span::call_site(),
        );
        Some(match ct {
            "U64" => quote! { #ident: row.get(#idx).and_then(|v| v.as_u64()).unwrap_or(0) },
            "I64" => quote! { #ident: row.get(#idx).and_then(|v| v.as_i64()).unwrap_or(0) },
            "Bool" => quote! { #ident: matches!(row.get(#idx), Some(::nexum_core::Value::Bool(true))) },
            "F64" => quote! { #ident: match row.get(#idx) { Some(::nexum_core::Value::F64(v)) => *v, _ => 0.0 } },
            "String" => quote! { #ident: row.get(#idx).and_then(|v| v.as_str()).unwrap_or("").to_string() },
            _ => return None,
        })
    }).collect::<Vec<_>>();

    // ── per-field to_row construction ──
    let to_row_values: Vec<_> = named
        .iter()
        .filter_map(|f| {
            let ident = f.ident.as_ref()?;
            let ct = col_type(&f.ty)?;
            let i = ident.clone();
            match ct {
                "U64" => Some(quote! { ::nexum_core::Value::U64(self.#i) }),
                "I64" => Some(quote! { ::nexum_core::Value::I64(self.#i) }),
                "Bool" => Some(quote! { ::nexum_core::Value::Bool(self.#i) }),
                "F64" => Some(quote! { ::nexum_core::Value::F64(self.#i) }),
                "String" => Some(quote! { ::nexum_core::Value::String(self.#i.clone()) }),
                _ => None,
            }
        })
        .collect();

    // ── generate impl ──
    let expanded = quote! {
        impl #struct_name {
            /// Auto-generated table name.
            pub const TABLE_NAME: &'static str = #table_name;

            /// Auto-generated schema from struct fields.
            pub fn schema() -> ::nexum_core::TableSchema {
                ::nexum_core::TableSchema::builder(#table_name)
                    #(#column_defs)*
                    #pk_expr
                    .build()
                    .unwrap_or_else(|e| panic!("table build error: {e}"))
            }

            /// Deserialize from a Row reference.
            pub fn from_row(row: &::nexum_core::Row) -> Self {
                Self {
                    #(#from_row_fields,)*
                }
            }

            /// Serialize to a Row.
            pub fn to_row(&self) -> ::nexum_core::Row {
                ::nexum_core::Row::new(vec![
                    #(#to_row_values,)*
                ])
            }

            /// Get one entity by primary key.
            pub fn get(
                ctx: &mut ::nexum_reducer::ReducerContext,
                #pk_fn_param,
            ) -> ::nexum_core::Result<Option<Self>> {
                let owners = ctx.lookup_unique(
                    Self::TABLE_NAME, "primary", #pk_lookup,
                )?;
                match owners.first() {
                    Some(&rid) => Ok(ctx.get(Self::TABLE_NAME, rid)?.as_ref().map(Self::from_row)),
                    None => Ok(None),
                }
            }

            /// Save this entity (update by primary key).
            pub fn save(
                &self,
                ctx: &mut ::nexum_reducer::ReducerContext,
            ) -> ::nexum_core::Result<()> {
                let owners = ctx.lookup_unique(
                    Self::TABLE_NAME, "primary", #pk_self_lookup,
                )?;
                let Some(&rid) = owners.first() else {
                    return Err(::nexum_core::Error::not_found("save: row not found"));
                };
                ctx.update(Self::TABLE_NAME, rid, self.to_row())?;
                Ok(())
            }

            /// Create (insert) this entity.
            pub fn create(
                &self,
                ctx: &mut ::nexum_reducer::ReducerContext,
            ) -> ::nexum_core::Result<()> {
                ctx.insert(Self::TABLE_NAME, self.to_row())?;
                Ok(())
            }

            /// Delete by primary key.
            pub fn delete(
                ctx: &mut ::nexum_reducer::ReducerContext,
                #pk_fn_param,
            ) -> ::nexum_core::Result<()> {
                let owners = ctx.lookup_unique(
                    Self::TABLE_NAME, "primary", #pk_lookup,
                )?;
                if let Some(&rid) = owners.first() {
                    ctx.delete(Self::TABLE_NAME, rid)?;
                }
                Ok(())
            }

            /// Scan all rows and deserialize into typed structs.
            pub fn all(
                ctx: &mut ::nexum_reducer::ReducerContext,
            ) -> ::nexum_core::Result<Vec<Self>> {
                Ok(ctx.scan(Self::TABLE_NAME)?
                    .into_iter()
                    .map(|(_, row)| Self::from_row(&row))
                    .collect())
            }
        }
    };

    expanded.into()
}

// ─── #[reducer] marker ───────────────────────────────────────────────────

/// Marks a function as a reducer callable by clients.
#[proc_macro_attribute]
pub fn reducer(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

// ─── #[system] marker ────────────────────────────────────────────────────

/// Marks a function as a simulation system running every tick.
#[proc_macro_attribute]
pub fn system(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
