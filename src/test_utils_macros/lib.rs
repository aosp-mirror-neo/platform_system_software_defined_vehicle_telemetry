// Copyright 2024 Google LLC

//! Proc macros for unit tests.

extern crate proc_macro;

mod timeout;

use timeout::timeout_impl;

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
