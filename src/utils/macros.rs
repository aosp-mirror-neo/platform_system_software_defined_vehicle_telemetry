// Copyright 2024 Google LLC

//! Utility proc macros.

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote_spanned;
use syn::{
    parse2, spanned::Spanned, ItemTrait, Path, TraitBound, TraitBoundModifier, TypeParamBound,
};

/// Applies the provided super trait to the trait. Its main use is to conditionally add a super
/// trait for tests.
///
/// Example use:
/// ```rust
/// #[cfg_attr(test, sdv_telemetry_utils_macros::with_super_trait(downcast_rs::Downcast))]
/// trait Foo {
///   // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn with_super_trait(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    with_super_trait_impl(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

fn with_super_trait_impl(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let path = parse2::<Path>(attr)?;
    let mut modified_trait = parse2::<ItemTrait>(item)?;

    modified_trait.supertraits.push(TypeParamBound::Trait(TraitBound {
        paren_token: None,
        modifier: TraitBoundModifier::None,
        lifetimes: None,
        path,
    }));

    let span = modified_trait.span();
    Ok(quote_spanned!(span =>
        #modified_trait
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use quote::quote;

    fn assert_token_stream_eq(a: TokenStream, b: TokenStream) {
        assert_eq!(format!("{a}"), format!("{b}"))
    }

    #[test]
    fn adds_super_trait_when_other_trait_present() {
        let result = with_super_trait_impl(
            quote!(Send),
            quote!(
                trait Foo: Sync {
                    fn a();
                }
            ),
        );

        assert_token_stream_eq(
            result.unwrap(),
            quote!(
                trait Foo: Sync + Send {
                    fn a();
                }
            ),
        );
    }

    #[test]
    fn adds_super_trait_when_no_trait_present() {
        let result = with_super_trait_impl(
            quote!(Send),
            quote!(
                trait Foo {
                    fn a();
                }
            ),
        );

        assert_token_stream_eq(
            result.unwrap(),
            quote!(
                trait Foo: Send {
                    fn a();
                }
            ),
        );
    }
}
