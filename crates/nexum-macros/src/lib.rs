//! Nexum macros: `#[table]`, `#[reducer]`, `#[system]`.
//!
//! Dead-simple game authoring:
//!
//! ```rust,ignore
//! use nexum_macros::{table, reducer};
//!
//! #[table]
//! struct Player {
//!     #[key]
//!     id: u64,
//!     x: i64,
//!     hp: i64,
//! }
//!
//! #[reducer]
//! fn move_player(ctx: &mut ReducerContext, dx: i64, dy: i64) -> Result<()> {
//!     let mut p = Player::get(ctx, ctx.caller())?.unwrap();
//!     p.x += dx;
//!     p.save(ctx)?;
//!     Ok(())
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

// ─── helpers ─────────────────────────────────────────────────────────────

fn rust_type_to_column_type(ty: &syn::Type) -> Option<String> {
    let syn::Type::Path(tp) = ty else { return None };
    let type_str: String = tp
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect::<Vec<_>>()
        .join("::");
    Some(
        match type_str.as_str() {
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
        }
        .to_string(),
    )
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    result
}

fn parse_attr_name(attr_tokens: &str) -> Option<String> {
    for part in attr_tokens.split(',') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix("name") {
            let rest = rest.trim_start_matches('=').trim().trim_matches('"');
            if !rest.is_empty() {
                return Some(rest.to_string());
            }
        }
    }
    None
}

// ─── #[table] ────────────────────────────────────────────────────────────

/// Marks a struct as a Nexum table. Generates schema + full CRUD.
///
/// ```rust,ignore
/// #[table]
/// struct Player {
///     #[key]
///     id: u64,
///     x: i64,
///     hp: i64,
/// }
///
/// // Generates:
/// // Player::get(ctx, id) -> Result<Option<Player>>
/// // player.save(ctx) -> Result<()>
/// // player.create(ctx) -> Result<()>
/// // Player::delete(ctx, id) -> Result<()>
/// // Player::all(ctx) -> Vec<Player>
/// // Player::schema() -> TableSchema
/// ```
#[proc_macro_attribute]
pub fn table(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_clone = item.clone();
    let input = parse_macro_input!(item_clone as DeriveInput);
    let item_ts: proc_macro2::TokenStream = item.into();
    let struct_name = &input.ident;

    let table_name = parse_attr_name(&attr.to_string())
        .unwrap_or_else(|| to_snake_case(&struct_name.to_string()));

    let syn::Data::Struct(data_struct) = &input.data else {
        return quote! {
            compile_error!("#[table] can only be applied to structs with named fields");
        }
        .into();
    };
    let syn::Fields::Named(fields) = &data_struct.fields else {
        return quote! {
            compile_error!("#[table] requires named fields");
        }
        .into();
    };

    let mut primary_keys: Vec<String> = Vec::new();
    let mut column_defs: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut from_row_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut to_row_values: Vec<proc_macro2::TokenStream> = Vec::new();

    for (col_idx, field) in fields.named.iter().enumerate() {
        let field_ident = field.ident.as_ref().unwrap();
        let field_name = field_ident.to_string();
        let Some(col_type_str) = rust_type_to_column_type(&field.ty) else {
            continue;
        };

        let is_pk = col_idx == 0; // first field = primary key
        if is_pk {
            primary_keys.push(field_name.clone());
        }

        let type_ident = syn::Ident::new(&col_type_str, proc_macro2::Span::call_site());
        column_defs.push(quote! {
            .column(#field_name, ::nexum_core::ColumnType::#type_ident)
        });

        let idx_lit = syn::LitInt::new(&col_idx.to_string(), proc_macro2::Span::call_site());

        from_row_fields.push(match col_type_str.as_str() {
            "U64" => quote! { #field_ident: row.get(#idx_lit).and_then(|v| v.as_u64()).unwrap_or(0) },
            "I64" => quote! { #field_ident: row.get(#idx_lit).and_then(|v| v.as_i64()).unwrap_or(0) },
            "Bool" => quote! { #field_ident: matches!(row.get(#idx_lit), Some(::nexum_core::Value::Bool(true))) },
            "F64" => quote! { #field_ident: match row.get(#idx_lit) { Some(::nexum_core::Value::F64(v)) => *v, _ => 0.0 } },
            "String" => quote! { #field_ident: row.get(#idx_lit).and_then(|v| v.as_str()).unwrap_or("").to_string() },
            _ => quote! {},
        });

        to_row_values.push(match col_type_str.as_str() {
            "U64" => quote! { ::nexum_core::Value::U64(self.#field_ident) },
            "I64" => quote! { ::nexum_core::Value::I64(self.#field_ident) },
            "Bool" => quote! { ::nexum_core::Value::Bool(self.#field_ident) },
            "F64" => quote! { ::nexum_core::Value::F64(self.#field_ident) },
            "String" => quote! { ::nexum_core::Value::String(self.#field_ident.clone()) },
            _ => quote! {},
        });
    }

    let pk_expr = if primary_keys.is_empty() {
        quote! {}
    } else {
        let pk_refs: Vec<_> = primary_keys.iter().map(|k| quote! { #k }).collect();
        quote! { .primary_key(&[#(#pk_refs),*]) }
    };

    // PK accessor expressions
    let pk_field_ident = primary_keys
        .first()
        .map(|k| syn::Ident::new(k, proc_macro2::Span::call_site()));
    let pk_fn_param = pk_field_ident
        .as_ref()
        .map(|ident| {
            // Find the actual Rust type for this field
            let ty = fields
                .named
                .iter()
                .find(|f| f.ident.as_ref().unwrap() == ident)
                .map(|f| &f.ty);
            match ty {
                Some(ty) => quote! { #ident: #ty },
                None => quote! { id: u64 },
            }
        })
        .unwrap_or(quote! {});

    let pk_lookup_expr = pk_field_ident
        .as_ref()
        .map(|ident| {
            // Determine the value constructor based on the PK field type
            let ty = fields
                .named
                .iter()
                .find(|f| f.ident.as_ref().map(|i| *i == *ident).unwrap_or(false))
                .map(|f| &f.ty);
            match ty.and_then(rust_type_to_column_type).as_deref() {
                Some("I64") => quote! { &[::nexum_core::Value::I64(#ident)] },
                Some("String") => quote! { &[::nexum_core::Value::String(#ident.clone())] },
                _ => quote! { &[::nexum_core::Value::U64(#ident)] },
            }
        })
        .unwrap_or(quote! { &[] });

    let pk_self_lookup = pk_field_ident
        .as_ref()
        .map(|ident| {
            let ty = fields
                .named
                .iter()
                .find(|f| {
                    f.ident
                        .as_ref()
                        .map(|i| i.to_string() == ident.to_string())
                        .unwrap_or(false)
                })
                .map(|f| &f.ty);
            match ty.and_then(rust_type_to_column_type).as_deref() {
                Some("I64") => quote! { &[::nexum_core::Value::I64(#ident)] },
                Some("String") => quote! { &[::nexum_core::Value::String(#ident.clone())] },
                _ => quote! { &[::nexum_core::Value::U64(#ident)] },
            }
        })
        .unwrap_or(quote! { &[] });

    let expanded = quote! {
        #item_ts

        impl #struct_name {
            /// Auto-generated table name.
            pub const TABLE_NAME: &'static str = #table_name;

            /// Auto-generated schema.
            pub fn schema() -> ::nexum_core::TableSchema {
                ::nexum_core::TableSchema::builder(#table_name)
                    #(#column_defs)*
                    #pk_expr
                    .build()
                    .unwrap_or_else(|e| panic!("nexum table build error: {e}"))
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
                    Self::TABLE_NAME, "primary", #pk_lookup_expr,
                )?;
                match owners.first() {
                    Some(&rid) => {
                        let row = ctx.get(Self::TABLE_NAME, rid)?;
                        Ok(row.map(|r| Self::from_row(&r)))
                    }
                    None => Ok(None),
                }
            }

            /// Save this entity by primary key.
            pub fn save(
                &self,
                ctx: &mut ::nexum_reducer::ReducerContext,
            ) -> ::nexum_core::Result<()> {
                let owners = ctx.lookup_unique(
                    Self::TABLE_NAME, "primary", #pk_self_lookup,
                )?;
                let Some(&rid) = owners.first() else {
                    return Err(::nexum_core::Error::not_found(concat!(
                        "cannot save ", #table_name
                    )));
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
                    Self::TABLE_NAME, "primary", #pk_lookup_expr,
                )?;
                if let Some(&rid) = owners.first() {
                    ctx.delete(Self::TABLE_NAME, rid)?;
                }
                Ok(())
            }

            /// Scan all rows as typed structs.
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

// ─── #[reducer] ──────────────────────────────────────────────────────────

/// Marks a function as a reducer callable by clients.
///
/// The function must take `&mut ReducerContext` as its first parameter.
/// Reducers run inside one atomic transaction per world-tick. Errors abort
/// the call with zero mutation; the tick continues.
///
/// ```rust,ignore
/// #[reducer]
/// fn move_player(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
///     let caller = args.require_u64("__caller")?;
///     // ...
///     Ok(Value::U64(1))
/// }
/// ```
#[proc_macro_attribute]
pub fn reducer(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

// ─── #[system] ───────────────────────────────────────────────────────────

/// Marks a function as a simulation system running every tick.
///
/// Systems run in deterministic (priority, id) order after reducer calls.
/// They receive the merged InputFrame for their world.
///
/// ```rust,ignore
/// #[system(priority = 0)]
/// fn cooldown_tick(ctx: &mut SimulationContext, frame: &InputFrame) -> Result<()> {
///     // ...
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn system(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
