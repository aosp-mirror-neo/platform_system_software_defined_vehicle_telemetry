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

use proc_macro2::TokenStream;
use quote::{format_ident, quote_spanned};
use syn::{
    parenthesized, parse::Parse, parse::ParseStream, punctuated::Pair, punctuated::Punctuated,
    spanned::Spanned, Attribute, Expr, ExprClosure, Ident, Token,
};

pub(crate) struct MacroInput {
    attrs: Vec<Attribute>,
    name_prefix: Ident,
    test_fn: ExprClosure,
    cases: Punctuated<CaseInput, Token![,]>,
}

impl Parse for MacroInput {
    /// Input format:
    /// ```
    /// #[test]
    /// #[abc]
    /// some_name,
    /// |a, b| {},
    /// case_1: (arg_1, arg_2,...),
    /// case_2: (arg_1, arg_2,...),
    /// ```
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let name_prefix = input.parse()?;
        input.parse::<Token![,]>()?;
        let test_fn = input.parse()?;
        input.parse::<Token![,]>()?;
        let cases = input.call(Punctuated::parse_terminated)?;

        Ok(Self { attrs, name_prefix, test_fn, cases })
    }
}

struct CaseInput {
    name: Ident,
    args: Punctuated<Expr, Token![,]>,
}

impl Parse for CaseInput {
    /// Input format:
    /// ```
    /// case_name: (arg_1, arg_2,...)
    /// ```
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name = input.parse()?;
        input.parse::<Token![:]>()?;
        let args;
        parenthesized!(args in input);
        let args = args.call(Punctuated::parse_terminated)?;

        Ok(Self { name, args })
    }
}

pub(crate) fn parameterized_test_impl(input: MacroInput) -> TokenStream {
    let is_async = matches!(*input.test_fn.body, Expr::Async(_));

    input
        .cases
        .into_iter()
        .map(|case| {
            let test_fn = &input.test_fn;
            let args = case.args;

            let span = case.name.span();

            let (fn_async, fn_body) = if is_async {
                // For async tests, we cannot simply use the same trick we use for sync tests and
                // call the closure directly, because that doesn't work when the closure takes
                // references. Once Rust implements async closures (apparently in 1.85), we could
                // simplify this code and use the same approach we use for sync tests. See this
                // Rust playground [1] for a comparison between the approaches.
                //
                // [1]: https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=f182e571828136355cd472fde218cfde

                let arg_types = &test_fn.inputs;
                let args = arg_types.pairs().map(Pair::into_value).zip(args).map(|(t, v)| {
                    let span = t.span();
                    quote_spanned! {span=> let #t = #v; }
                });
                let body = &test_fn.body;
                (
                    Some(Token![async](span)),
                    quote_spanned! {span=>
                        // This is needed for the edge case of having zero args (which doesn't
                        // really make sense in the first place, but is theoretically possible).
                        // Without this, an `unused_braces` lint triggers.
                        let _ = ();

                        #(#args)*

                        #body.await
                    },
                )
            } else {
                (
                    None,
                    quote_spanned! {span=>
                       #[expect(clippy::redundant_closure_call)]
                       (#test_fn)(#args)
                    },
                )
            };

            let attrs = &input.attrs;
            let test_name = format_ident!("{}_{}", input.name_prefix, case.name, span = span);
            quote_spanned! {span=>
                #(#attrs)*
                #fn_async fn #test_name() {
                    #fn_body
                }
            }
        })
        .collect::<TokenStream>()
}
