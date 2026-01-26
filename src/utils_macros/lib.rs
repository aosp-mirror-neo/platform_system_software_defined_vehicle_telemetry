// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
/// #[cfg_attr(test, sdv_telemetry_utils::with_super_trait(downcast_rs::Downcast))]
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
