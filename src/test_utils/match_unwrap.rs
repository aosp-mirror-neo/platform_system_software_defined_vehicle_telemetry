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

/// Matches an expression against a pattern, returning a specified result on success, or panicking
/// with a detailed error message on failure.
///
/// # Syntax
///
/// ```text
/// match_unwrap!(expression, pattern [if guard] => result)
/// ```
///
/// # Panics
///
/// Panics if the expression does not match the provided pattern, or if the optional `if` guard
/// evaluates to `false`.
///
/// # Examples
///
/// Basic usage:
///
/// ```rust
/// let value = Foo { a: 10, b: 20 };
/// let b = match_unwrap!(value, Foo { a: 10, b } => b);
/// assert_eq!(b, 20);
/// ```
///
/// Using an `if` guard:
///
/// ```rust
/// let val = Foo(15);
/// let greater_than_ten = match_unwrap!(val, Foo(x) if x > 10 => x);
/// assert_eq!(greater_than_ten, 15);
/// ```
#[macro_export]
macro_rules! match_unwrap {
    ($expr:expr, $pat:pat $(if $guard:expr)? => $res:expr $(,)?) => {{
        // This wrapper function is necessary so that we can apply the `#[must_use]` attribute to
        // the macro.
        #[must_use = "The return value of `match_unwrap!` must be used. Use `sdv_telemetry_test_utils::assert_matches!` if you don't need to use the value."]
        #[inline(always)]
        fn __match_unwrap_must_use<T>(val: T) -> T {
            val
        }

        __match_unwrap_must_use(match $expr {
            $pat $(if $guard)? => $res,
            _ => panic!(
                // The message style below matches Rust's message style for its assertions.
                r#"assertion `left matches right` failed
  left: {:?}
 right: {}"#,
                &$expr,
                stringify!($pat $(if $guard)?)
            ),
        })
    }
}}

#[cfg(test)]
mod tests {
    #[derive(Debug, PartialEq)]
    enum Foo {
        A(i32),
        B { b: &'static str },
        C,
    }

    #[test]
    fn match_unwrap_basic() {
        let val = Foo::A(42);
        let inner = match_unwrap!(val, Foo::A(x) => x);
        assert_eq!(inner, 42);
    }

    #[test]
    fn match_unwrap_multiple() {
        let val = Foo::C;
        let matches = match_unwrap!(val, Foo::A(_) | Foo::C => true);
        assert!(matches);
    }

    #[test]
    fn match_unwrap_guard() {
        let val = Foo::A(15);
        let matched = match_unwrap!(val, Foo::A(x) if x > 10 => x);
        assert_eq!(matched, 15);
    }

    #[test]
    #[expect(unused_must_use)]
    fn match_unwrap_must_use() {
        let val = Foo::B { b: "test" };
        match_unwrap!(val, Foo::B { b } => b);
    }

    #[test]
    #[should_panic(
        expected = "assertion `left matches right` failed\n  left: A(42)\n right: Foo::B { b }"
    )]
    fn match_unwrap_panic_mismatch() {
        let val = Foo::A(42);
        let _b = match_unwrap!(val, Foo::B { b } => b);
    }

    #[test]
    #[should_panic(
        expected = "assertion `left matches right` failed\n  left: A(5)\n right: Foo::A(x) if x > 10"
    )]
    fn match_unwrap_panic_guard() {
        let val = Foo::A(5);
        let _ = match_unwrap!(val, Foo::A(x) if x > 10 => x);
    }
}
