// Copyright 2024 Google LLC

//! Proc macros for unit tests.

extern crate proc_macro;

mod parameterized_test;
mod timeout;

use syn::parse_macro_input;

use parameterized_test::{parameterized_test_impl, MacroInput as ParameterizedTestMacroInput};
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

/// Macro for generating parameterized tests. Arguments:
/// - Test attributes and a common test name prefix for all generated tests.
/// - A closure that takes a number of arguments, and provides the actual test body. If the
///   closure's body is an `async` block, then the test function will be marked as `async`.
/// - Multiple lines of `test_name_suffix: (arg1, arg2, ..., argN,),`, one per test case.
///
/// Example:
///
/// ```
/// parameterized_test!(
///     #[test]
///     is_equal,
///
///     |expected, a, b| {
///         assert_eq!(a == b, expected);
///     },
///
///     two_and_four: (false, 2, 4,),
///     two_and_two: (true, 2, 2,),
/// )
/// ```
///
/// This will generate the following code for you:
///
/// ```no_run
/// #[test]
/// fn is_equal_two_and_four() {
///     (|expected, a, b| {
///         assert_eq!(a == b, expected);
///     })(
///         false,
///         2,
///         4,
///     )
/// }
///
/// #[test]
/// fn is_equal_two_and_two() {
///     (|expected, a, b| {
///         assert_eq!(a == b, expected);
///     })(
///         true,
///         2,
///         2,
///     )
/// }
/// ```
///
/// Async Example:
///
/// ```
/// parameterized_test!(
///     #[tokio::test]
///     #[sdv_telemetry_test_utils::timeout]
///     is_equal,
///
///     |expected, a, b| async move {
///         something_async().await;
///         assert_eq!(a == b, expected);
///     },
///
///     two_and_four: (false, 2, 4,),
///     two_and_two: (true, 2, 2,),
/// )
/// ```
#[proc_macro]
pub fn parameterized_test(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as ParameterizedTestMacroInput);
    parameterized_test_impl(input).into()
}
