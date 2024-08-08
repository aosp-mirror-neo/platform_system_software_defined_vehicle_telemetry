// Copyright 2024 Google LLC

//! Proc macros for unit tests.

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::{quote_spanned, ToTokens};
use std::time::Duration;
use syn::spanned::Spanned;
use syn::visit_mut::{self, VisitMut};
use syn::{parse2, Expr, ItemFn, LitInt};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

/// Fail the test after the specified timeout in `ms`, or the default timeout of 1 second if no
/// timeout is specified. Only works for tests using `#[tokio::test]`, and only checks the timeout
/// while `await`ing something. While non-async code runs, the timeout is _not_ checked.
///
/// The main purpose of this is to make sure that async tests fail instead of timeout, because an
/// async test that hangs blocks all other tests of the test suite and doesn't provide a useful
/// stacktrace. This implementation ensures that on failure, the correct line number is provided in
/// the stacktrace.
///
/// This is a temporary solution until b/306763068 is fixed.
#[proc_macro_attribute]
pub fn timeout(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    timeout_impl(attr.into(), item.into()).unwrap_or_else(|err| err.to_compile_error()).into()
}

fn timeout_impl(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let duration = parse2::<LitInt>(if attr.is_empty() {
        DEFAULT_TIMEOUT.as_millis().into_token_stream()
    } else {
        attr
    })?;
    let duration = duration.base10_parse::<u64>()?.into_token_stream();

    let test_case = parse2::<ItemFn>(item)?;
    let attrs = &test_case.attrs;
    let vis = &test_case.vis;
    let sig = &test_case.sig;

    let mut block = parse2::<Expr>(test_case.block.to_token_stream()).expect("Should never fail.");
    WrapAwaitWithTimeoutVisitor::new(duration).visit_expr_mut(&mut block);

    let span = test_case.span();
    Ok(quote_spanned! {span=>
        #(#attrs)*
        #vis #sig {
            let __test_utils_macro_timeout_start = std::time::Instant::now();
            #block
        }
    })
}

struct WrapAwaitWithTimeoutVisitor {
    duration: TokenStream,
}

impl WrapAwaitWithTimeoutVisitor {
    fn new(duration: TokenStream) -> Self {
        Self { duration }
    }

    /// Wrap the provided `await`able statement using `tokio::time::timeout`. The use of
    /// `quote_spanned` ensures that a panic caused by this code is attributed to the original line
    /// in the source file.
    fn await_with_timeout(&self, statement: TokenStream) -> TokenStream {
        let duration = &self.duration;
        let span = statement.span();
        quote_spanned! {span=>
            {
                let __test_utils_macro_timeout_time_left =
                    std::time::Duration::from_millis(#duration)
                        .checked_sub(__test_utils_macro_timeout_start.elapsed())
                        .unwrap_or_else(|| panic!("Test timeout of {}ms reached!", #duration));
                tokio::time::timeout(
                    __test_utils_macro_timeout_time_left,
                    #statement,
                )
                    .await
                    .unwrap_or_else(|_| panic!("Test timeout of {}ms reached (call stalled for {}ms)!", #duration, __test_utils_macro_timeout_time_left.as_millis()))
            }
        }
    }
}

impl VisitMut for WrapAwaitWithTimeoutVisitor {
    fn visit_expr_mut(&mut self, node: &mut Expr) {
        if let Expr::Await(ref mut expr) = node {
            assert!(expr.attrs.is_empty(), "Attributes on an await expression are currently not supported by the `timeout` macro.");

            // First, visit whatever is being `await`ed.
            visit_mut::visit_expr_mut(self, &mut expr.base);
            // Then, apply the timeout.
            *node = Expr::Verbatim(self.await_with_timeout(expr.base.clone().into_token_stream()));
        } else {
            visit_mut::visit_expr_mut(self, node);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use quote::quote;

    fn assert_token_stream_eq(a: TokenStream, b: TokenStream) {
        assert_eq!(format!("{a}"), format!("{b}"))
    }

    #[test]
    fn fails_if_argument_is_not_a_number() {
        let result = timeout_impl(
            quote!("asdasda"),
            quote!(
                #[tokio::test]
                async fn test_case() {
                    assert_eq!(1, 2);
                }
            ),
        );

        assert!(result.is_err());
    }

    #[test]
    // Disable formatting of the quoted code, because an extra `,` or `{ ... }` added by the
    // formatter would fail the assertion (ideally, this attribute would only be applied to the
    // `quote!` call in the `assert_token_stream_eq` below, but that is not currently possible:
    // https://github.com/rust-lang/rust/issues/15701).
    #[rustfmt::skip]
    fn successfully_transforms_test_case_fn_without_timeout_specified() {
        let result = timeout_impl(
            quote!(),
            quote!(
                #[tokio::test]
                async fn test_case() {
                    bar().await;
                }
            ),
        )
        .unwrap();

        assert_token_stream_eq(
            result,
            quote!(
                #[tokio::test]
                async fn test_case() {
                    let __test_utils_macro_timeout_start = std::time::Instant::now();
                    {
                        {
                            let __test_utils_macro_timeout_time_left =
                            std::time::Duration::from_millis(1000u64)
                                .checked_sub(__test_utils_macro_timeout_start.elapsed())
                                .unwrap_or_else(|| panic!("Test timeout of {}ms reached!", 1000u64));
                            tokio::time::timeout(
                                __test_utils_macro_timeout_time_left,
                                bar(),
                            )
                            .await
                            .unwrap_or_else(|_| panic!("Test timeout of {}ms reached (call stalled for {}ms)!", 1000u64, __test_utils_macro_timeout_time_left.as_millis()))
                        };
                    }
                }
            ),
        )
    }

    #[test]
    // Disable formatting of the quoted code, because an extra `,` or `{ ... }` added by the
    // formatter would fail the assertion (ideally, this attribute would only be applied to the
    // `quote!` call in the `assert_token_stream_eq` below, but that is not currently possible:
    // https://github.com/rust-lang/rust/issues/15701).
    #[rustfmt::skip]
    fn successfully_transforms_test_case_fn_with_timeout_specified() {
        let result = timeout_impl(
            quote!(10),
            quote!(
                #[tokio::test]
                async fn test_case() {
                    bar().await;
                }
            ),
        )
        .unwrap();

        assert_token_stream_eq(
            result,
            quote!(
                #[tokio::test]
                async fn test_case() {
                    let __test_utils_macro_timeout_start = std::time::Instant::now();
                    {
                        {
                            let __test_utils_macro_timeout_time_left =
                                std::time::Duration::from_millis(10u64)
                                    .checked_sub(__test_utils_macro_timeout_start.elapsed())
                                    .unwrap_or_else(|| panic!("Test timeout of {}ms reached!", 10u64));
                            tokio::time::timeout(
                                __test_utils_macro_timeout_time_left,
                                bar(),
                            )
                            .await
                            .unwrap_or_else(|_| panic!("Test timeout of {}ms reached (call stalled for {}ms)!", 10u64, __test_utils_macro_timeout_time_left.as_millis()))
                        };
                    }
                }
            ),
        )
    }

    #[test]
    // Disable formatting of the quoted code, because an extra `,` or `{ ... }` added by the
    // formatter would fail the assertion (ideally, this attribute would only be applied to the
    // `quote!` call in the `assert_token_stream_eq` below, but that is not currently possible:
    // https://github.com/rust-lang/rust/issues/15701).
    #[rustfmt::skip]
    fn successfully_transforms_test_case_fn_with_multiple_nested_awaits() {
        let result = timeout_impl(
            quote!(10),
            quote!(
                #[tokio::test]
                async fn test_case() {
                    foo().await.unwrap();
                    {
                        bar().await;
                        async {}.await.unwrap().unwrap();
                    }
                }
            ),
        )
        .unwrap();

        assert_token_stream_eq(
            result,
            quote!(
                #[tokio::test]
                async fn test_case() {
                    let __test_utils_macro_timeout_start = std::time::Instant::now();
                    {
                        {
                            let __test_utils_macro_timeout_time_left =
                                std::time::Duration::from_millis(10u64)
                                    .checked_sub(__test_utils_macro_timeout_start.elapsed())
                                    .unwrap_or_else(|| panic!("Test timeout of {}ms reached!", 10u64));
                            tokio::time::timeout(
                                __test_utils_macro_timeout_time_left,
                                foo(),
                            )
                            .await
                            .unwrap_or_else(|_| panic!("Test timeout of {}ms reached (call stalled for {}ms)!", 10u64, __test_utils_macro_timeout_time_left.as_millis()))
                        }
                        .unwrap();

                        {
                            {
                                let __test_utils_macro_timeout_time_left =
                                    std::time::Duration::from_millis(10u64)
                                        .checked_sub(__test_utils_macro_timeout_start.elapsed())
                                        .unwrap_or_else(|| panic!("Test timeout of {}ms reached!", 10u64));
                                tokio::time::timeout(
                                    __test_utils_macro_timeout_time_left,
                                    bar(),
                                )
                                .await
                                .unwrap_or_else(|_| panic!("Test timeout of {}ms reached (call stalled for {}ms)!", 10u64, __test_utils_macro_timeout_time_left.as_millis()))
                            };

                            {
                                let __test_utils_macro_timeout_time_left =
                                    std::time::Duration::from_millis(10u64)
                                        .checked_sub(__test_utils_macro_timeout_start.elapsed())
                                        .unwrap_or_else(|| panic!("Test timeout of {}ms reached!", 10u64));
                                tokio::time::timeout(
                                    __test_utils_macro_timeout_time_left,
                                    async {},
                                )
                                .await
                                .unwrap_or_else(|_| panic!("Test timeout of {}ms reached (call stalled for {}ms)!", 10u64, __test_utils_macro_timeout_time_left.as_millis()))
                            }
                            .unwrap()
                            .unwrap();
                        }
                    }
                }
            ),
        )
    }
}
