//! Nexum macros: derive `NexumTable` and attribute `#[table]`, `#[reducer]`,
//! `#[subscription]` for declarative game authoring.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Fields, FieldsNamed, Ident, ItemFn, LitInt, LitStr, Meta,
    MetaList, MetaNameValue, Type,
    parse::{Parser, Result as SynResult},
    parse_macro_input,
    spanned::Spanned,
};

/// Maps Rust primitive types to Nexum ColumnType variant names.
fn col_type(ty: &Type) -> Option<&'static str> {
    let Type::Path(tp) = ty else { return None };
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
        "Bytes" | "Vec<u8>" => "Bytes",
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

fn has_attr(attrs: &[Attribute], name: &str) -> bool {
    attrs.iter().any(|a| a.path().is_ident(name))
}

fn get_attr_lit_str(attrs: &[Attribute], name: &str) -> Option<String> {
    attrs.iter().find_map(|a| {
        if a.path().is_ident(name) {
            let mut result = None;
            let _ = a.parse_nested_meta(|meta| {
                if meta.path.is_ident("name")
                    && let Ok(v) = meta.value()
                    && let Ok(s) = v.parse::<LitStr>()
                {
                    result = Some(s.value());
                }
                Ok(())
            });
            result
        } else {
            None
        }
    })
}

/// Field metadata for a supported column.
struct FieldMeta {
    ident: Ident,
    ty: Type,
    col_type: &'static str,
    is_pk: bool,
    index_name: Option<String>,
    index_unique: bool,
}

/// Extract field metadata, validating all fields have supported types.
fn extract_fields(named: &FieldsNamed) -> SynResult<Vec<FieldMeta>> {
    let mut fields = Vec::new();
    for f in &named.named {
        let ident = f
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new(f.span(), "field must have a name"))?
            .clone();
        let ct = col_type(&f.ty).ok_or_else(|| {
            syn::Error::new(
                f.ty.span(),
                format!("unsupported column type: {}", quote::quote!(#f.ty)),
            )
        })?;
        let is_pk = has_attr(&f.attrs, "primary_key");
        let index_name = get_attr_lit_str(&f.attrs, "index")
            .or_else(|| get_attr_lit_str(&f.attrs, "unique_index"));
        let index_unique = has_attr(&f.attrs, "unique_index");
        fields.push(FieldMeta {
            ident,
            ty: f.ty.clone(),
            col_type: ct,
            is_pk,
            index_name,
            index_unique,
        });
    }
    Ok(fields)
}

/// Derive `NexumTable` on a struct to generate schema + full typed CRUD.
///
/// ```rust,ignore
/// #[derive(nexum_macros::NexumTable)]
/// struct Player {
///     #[primary_key]
///     id: u64,
///     x: i64,
///     #[index]
///     y: i64,
/// }
/// ```
///
/// The first field with `#[primary_key]` (or first field if none) becomes the PK.
/// Multiple `#[primary_key]` fields create a composite PK.
/// Fields with `#[index]` or `#[unique_index]` get secondary indexes.
#[proc_macro_derive(NexumTable, attributes(primary_key, index, unique_index, table_name))]
pub fn derive_nexum_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    // Allow #[table_name = "override"] on the struct to override the default
    // snake_case name.
    let table_name = input
        .attrs
        .iter()
        .find(|a| a.path().is_ident("table_name"))
        .and_then(|a| {
            let syn::Meta::NameValue(nv) = &a.meta else {
                return None;
            };
            let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) = &nv.value
            else {
                return None;
            };
            Some(s.value())
        })
        .unwrap_or_else(|| snake_case(&struct_name.to_string()));

    let Data::Struct(data_struct) = &input.data else {
        return quote! {
            compile_error!("NexumTable can only be applied to structs");
        }
        .into();
    };
    let Fields::Named(fields_named) = &data_struct.fields else {
        return quote! {
            compile_error!("NexumTable requires named fields");
        }
        .into();
    };

    let fields = match extract_fields(fields_named) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    if fields.is_empty() {
        return quote! {
            compile_error!("NexumTable requires at least one field");
        }
        .into();
    }

    // Primary key fields (composite if multiple)
    let pk_fields: Vec<_> = fields.iter().filter(|f| f.is_pk).collect();
    let pk_fields = if pk_fields.is_empty() {
        vec![&fields[0]]
    } else {
        pk_fields
    };

    // ── build column definitions ──
    let column_defs: Vec<_> = fields
        .iter()
        .map(|fm| {
            let fname = &fm.ident;
            let ct_ident = Ident::new(fm.col_type, proc_macro2::Span::call_site());
            quote! { .column(stringify!(#fname), ::nexum_core::ColumnType::#ct_ident) }
        })
        .collect();

    // ── primary key ──
    let pk_names: Vec<_> = pk_fields
        .iter()
        .map(|f| snake_case(&f.ident.to_string()))
        .collect();
    let pk_expr = quote! { .primary_key(&[#(#pk_names),*]) };

    // ── indexes ──
    let index_exprs: Vec<_> = fields
        .iter()
        .filter_map(|fm| {
            let ident = &fm.ident;
            fm.index_name.as_ref().map(|name| {
                if fm.index_unique {
                    quote! { .unique_index(#name, &[stringify!(#ident)]) }
                } else {
                    quote! { .index(#name, &[stringify!(#ident)]) }
                }
            })
        })
        .collect();

    // ── per-field from_row extraction (using supported-field index) ──
    let from_row_fields: Vec<_> = fields.iter().enumerate().map(|(idx, fm)| {
        let ident = &fm.ident;
        let idx_lit = LitInt::new(&idx.to_string(), proc_macro2::Span::call_site());
        match fm.col_type {
            "U64" => quote! { #ident: row.get(#idx_lit).and_then(|v| v.as_u64()).unwrap_or(0) },
            "I64" => quote! { #ident: row.get(#idx_lit).and_then(|v| v.as_i64()).unwrap_or(0) },
            "Bool" => quote! { #ident: matches!(row.get(#idx_lit), Some(::nexum_core::Value::Bool(true))) },
            "F64" => quote! { #ident: match row.get(#idx_lit) { Some(::nexum_core::Value::F64(v)) => *v, _ => 0.0 } },
            "String" => quote! { #ident: row.get(#idx_lit).and_then(|v| v.as_str()).unwrap_or("").to_string() },
            "Bytes" => quote! { #ident: row.get(#idx_lit).and_then(|v| v.as_bytes().map(|b| b.to_vec())).unwrap_or_default() },
            _ => quote! { #ident: Default::default() },
        }
    }).collect();

    // ── per-field to_row construction ──
    let to_row_values: Vec<_> = fields
        .iter()
        .map(|fm| {
            let ident = &fm.ident;
            match fm.col_type {
                "U64" => quote! { ::nexum_core::Value::U64(self.#ident) },
                "I64" => quote! { ::nexum_core::Value::I64(self.#ident) },
                "Bool" => quote! { ::nexum_core::Value::Bool(self.#ident) },
                "F64" => quote! { ::nexum_core::Value::F64(self.#ident) },
                "String" => quote! { ::nexum_core::Value::String(self.#ident.clone()) },
                "Bytes" => quote! { ::nexum_core::Value::Bytes(self.#ident.clone()) },
                _ => quote! { ::nexum_core::Value::U64(0) },
            }
        })
        .collect();

    // ── PK lookup expressions ──
    let pk_fn_params: Vec<_> = pk_fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let ty = &f.ty;
            quote! { #ident: #ty }
        })
        .collect();

    let pk_lookups: Vec<_> = pk_fields.iter().map(|f| {
        let ident = &f.ident;
        let ty = &f.ty;
        let is_i64 = matches!(ty, Type::Path(tp) if tp.path.segments.last().map(|s| s.ident == "i64").unwrap_or(false));
        let is_string = matches!(ty, Type::Path(tp) if tp.path.segments.last().map(|s| s.ident == "String").unwrap_or(false));
        if is_i64 {
            quote! { ::nexum_core::Value::I64(#ident as i64) }
        } else if is_string {
            quote! { ::nexum_core::Value::String(#ident.clone()) }
        } else {
            quote! { ::nexum_core::Value::U64(#ident) }
        }
    }).collect();

    let pk_self_lookups: Vec<_> = pk_fields.iter().map(|f| {
        let ident = &f.ident;
        let ty = &f.ty;
        let is_i64 = matches!(ty, Type::Path(tp) if tp.path.segments.last().map(|s| s.ident == "i64").unwrap_or(false));
        let is_string = matches!(ty, Type::Path(tp) if tp.path.segments.last().map(|s| s.ident == "String").unwrap_or(false));
        if is_i64 {
            quote! { ::nexum_core::Value::I64(self.#ident) }
        } else if is_string {
            quote! { ::nexum_core::Value::String(self.#ident.clone()) }
        } else {
            quote! { ::nexum_core::Value::U64(self.#ident) }
        }
    }).collect();

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
                    #(#index_exprs)*
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

            /// Get one entity by primary key (composite if multiple).
            pub fn get(
                ctx: &mut ::nexum_reducer::ReducerContext,
                #(#pk_fn_params),*
            ) -> ::nexum_core::Result<Option<Self>> {
                let pk_lookup = &[#(#pk_lookups),*];
                let owners = ctx.lookup_unique(
                    Self::TABLE_NAME, "primary", pk_lookup,
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
                let pk_self_lookup = &[#(#pk_self_lookups),*];
                let owners = ctx.lookup_unique(
                    Self::TABLE_NAME, "primary", pk_self_lookup,
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
                #(#pk_fn_params),*
            ) -> ::nexum_core::Result<()> {
                let pk_lookup = &[#(#pk_lookups),*];
                let owners = ctx.lookup_unique(
                    Self::TABLE_NAME, "primary", pk_lookup,
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

// ─── #[table] attribute macro (ADR-027 style) ────────────────────────────

/// Attribute macro for table definition (preferred API per ADR-027).
/// ```rust,ignore
/// #[nexum_macros::table(name = "players", primary_key = "id")]
/// struct Player {
///     #[nexum_macros::primary_key]
///     id: u64,
///     x: i64,
///     #[nexum_macros::index(name = "pos")]
///     y: i64,
/// }
/// ```
#[proc_macro_attribute]
pub fn table(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Add the derive macro to the item
    let mut item_str = item.to_string();
    // Find the struct/enum and prepend the derive attribute
    if let Some(pos) = item_str.find("struct ") {
        item_str.insert_str(pos, "#[derive(nexum_macros::NexumTable)]\n");
    } else if let Some(pos) = item_str.find("enum ") {
        item_str.insert_str(pos, "#[derive(nexum_macros::NexumTable)]\n");
    }
    item_str.parse().unwrap()
}

// ─── #[reducer] attribute macro ────────────────────────────────────────

/// Marks a function as a native reducer and generates a registration helper.
///
/// ```rust,ignore
/// #[nexum_macros::reducer(id = 1, name = "move_player")]
/// pub fn move_player(ctx: &mut nexum_reducer::ReducerContext, args: &nexum_reducer::ReducerArgs) -> nexum_core::Result<nexum_core::Value> {
///     // ...
/// }
/// ```
///
/// Generates a `pub const MOVE_PLAYER_REDUCER: nexum_reducer::ReducerDefinition`
/// that can be registered via `world.native_mut().register(MOVE_PLAYER_REDUCER)`.
#[proc_macro_attribute]
pub fn reducer(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;
    let fn_vis = &input.vis;
    let fn_sig = &input.sig;
    let fn_block = &input.block;

    // Parse attributes: id = <u64>, name = "string"
    let args = parse_macro_input!(attr as MetaList);
    let mut reducer_id: Option<u64> = None;
    let mut reducer_name: Option<String> = None;

    let tokens = args.tokens;
    let parser = syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated;
    let metas = parser.parse2(tokens).unwrap_or_default();

    for meta in metas {
        if let Meta::NameValue(MetaNameValue { path, value, .. }) = meta {
            if path.is_ident("id") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(lit),
                    ..
                }) = value
                {
                    reducer_id = lit.base10_parse().ok();
                }
            } else if path.is_ident("name")
                && let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit),
                    ..
                }) = value
            {
                reducer_name = Some(lit.value());
            }
        }
    }

    let reducer_id = reducer_id.unwrap_or_else(|| {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        fn_name.to_string().hash(&mut hasher);
        hasher.finish()
    });

    let reducer_name = reducer_name.unwrap_or_else(|| fn_name.to_string());

    let const_name = Ident::new(
        &format!("{}_REDUCER", fn_name.to_string().to_uppercase()),
        fn_name.span(),
    );

    let expanded = quote! {
        #fn_vis #fn_sig #fn_block

        /// Auto-generated reducer definition for registration.
        #fn_vis const #const_name: ::nexum_reducer::ReducerDefinition = {
            ::nexum_reducer::ReducerDefinition::new(
                ::nexum_core::ReducerId::from_u64(#reducer_id),
                #reducer_name,
                #fn_name,
            ).expect("valid reducer definition")
        };
    };

    expanded.into()
}

// ─── #[subscription] attribute macro ──────────────────────────────────

/// Defines a subscription query for a table.
/// ```rust,ignore
/// #[nexum_macros::subscription(table = "players", predicate = "x > 0 AND y > 0")]
/// struct PlayerNearOrigin;
/// ```
#[proc_macro_attribute]
pub fn subscription(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item // Placeholder; Query builder API is runtime, not macro-generated
}

// ─── #[system] marker (kept for compatibility) ────────────────────────

/// Marks a function as a simulation system running every tick.
#[proc_macro_attribute]
pub fn system(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
