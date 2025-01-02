// Copyright 2025 Google LLC

use proc_macro2::TokenStream;
use quote::{format_ident, quote_spanned};
use syn::{
    parenthesized, parse::Parse, parse::ParseStream, punctuated::Punctuated, Attribute, Expr,
    Ident, Token,
};

pub(crate) struct MacroInput {
    attrs: Vec<Attribute>,
    name_prefix: Ident,
    test_fn: Expr,
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
    input
        .cases
        .into_iter()
        .map(|case| {
            let attrs = &input.attrs;
            let test_name =
                format_ident!("{}_{}", input.name_prefix, case.name, span = case.name.span());
            let test_fn = &input.test_fn;
            let args = case.args;

            let span = case.name.span();
            quote_spanned! {span=>
                #(#attrs)*
                fn #test_name() {
                    #[expect(clippy::redundant_closure_call)]
                    (#test_fn)(#args)
                }
            }
        })
        .collect::<TokenStream>()
}
