//! Proc-macros for `pg-walstream`: the `WalTable` derive and the `wal_table`
//! attribute (two ways to bind a type to a table name).
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, LitStr};

/// Derives `pg_walstream::WalTable`, binding the type to a PostgreSQL table
/// name given by `#[wal(table = "...")]`.
#[proc_macro_derive(WalTable, attributes(wal))]
pub fn derive_wal_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut table: Option<LitStr> = None;
    for attr in &input.attrs {
        if attr.path().is_ident("wal") {
            let res = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("table") {
                    if table.is_some() {
                        return Err(meta.error("duplicate `table` key in `wal` attribute"));
                    }
                    table = Some(meta.value()?.parse()?);
                    Ok(())
                } else {
                    Err(meta.error("unknown `wal` key; expected `table`"))
                }
            });
            if let Err(e) = res {
                return e.to_compile_error().into();
            }
        }
    }

    let table = match table {
        Some(t) => t,
        None => {
            return syn::Error::new_spanned(
                ident,
                "#[derive(WalTable)] requires #[wal(table = \"...\")]",
            )
            .to_compile_error()
            .into();
        }
    };

    quote! {
        impl #impl_generics ::pg_walstream::WalTable for #ident #ty_generics #where_clause {
            const TABLE: &'static str = #table;
        }
    }
    .into()
}

/// Attribute form of [`WalTable`](macro@WalTable): binds a type to a table name in a single annotation, with the name inline.
///
/// ```ignore
/// #[pg_walstream::wal_table("users")]
/// #[derive(serde::Deserialize)]
/// struct User { id: i64 }
/// assert_eq!(<User as pg_walstream::WalTable>::TABLE, "users");
/// ```
///
/// Equivalent to `#[derive(WalTable)] #[wal(table = "users")]`. The struct is re-emitted unchanged (other attributes like `#[derive(...)]` are preserved), plus a generated `impl WalTable`.
#[proc_macro_attribute]
pub fn wal_table(attr: TokenStream, item: TokenStream) -> TokenStream {
    let table = parse_macro_input!(attr as LitStr);
    let input = parse_macro_input!(item as DeriveInput);
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        #input
        impl #impl_generics ::pg_walstream::WalTable for #ident #ty_generics #where_clause {
            const TABLE: &'static str = #table;
        }
    }
    .into()
}
