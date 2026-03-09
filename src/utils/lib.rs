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

//! This module includes all the utility functions used in telemetry service

pub mod async_lazy_map;
pub mod atomic;
pub mod collection_extension;
pub mod conversions;
pub mod countdown_latch;
pub mod descriptor_utils;
pub mod duration_utils;
pub mod file_io_utils;
pub mod logging;
pub mod observable_value;
pub mod operation_result;
pub mod option_extension;
pub mod proto_reader;
pub mod value_holder;

pub use sdv_telemetry_utils_macros::with_super_trait;

/// A telemetry-specific debugging macro, inspired by `std::dbg!` ("TelemetryDeBuG").
///
/// This macro provides similar functionality to the standard `std::dbg!` macro but is tailored to
/// the specifics of Android development.
///
/// # Advantages over `std::dbg!`
///
/// 1.  Dual output: While `dbg!` only prints to `stderr`, `tdbg!` logs to both
///     `stderr` (via `std::eprintln!`) and the standard logging system (via `log::info!`).
///     This ensures that debug output is captured in standard Android logs (`logcat`), which makes
///     it easier to debug on device.
/// 2.  Function name context: In addition to file, line, and column numbers,
///     `tdbg!` automatically captures and includes the current function name in its
///     output, providing richer context for debugging.
/// 3.  Traceable prefix: All output is prefixed with `TelemetryDebug:` for
///     easy filtering and grepping in large log files.
/// 4.  Uses `{:?}` formatting for the value, which usually formats the value on a single line,
///     which is more concise than `std::dbg!`'s use of `{:#?}`, which formats the value on
///     multiple lines.
///
/// # Examples
///
/// ```
/// let a = 2;
/// let b = tdbg!(a * 2) + 1;
/// //      ^-- prints: TelemetryDebug: [src/main.rs:2:9:main] a * 2 = 4
/// assert_eq!(b, 5);
/// ```
///
/// Without arguments:
///
/// ```
/// tdbg!();
/// // ^-- prints: TelemetryDebug: [src/main.rs:1:1:main]
/// ```
#[macro_export]
macro_rules! tdbg {
    () => {
        let msg = ::std::format_args!(
            "TelemetryDebug: [{}:{}:{}:{}]",
            ::std::file!(),
            ::std::line!(),
            ::std::column!(),
            $crate::function_name!()
        );
        ::std::eprintln!("{msg}");
        ::log::info!("{msg}");
    };
    ($val:expr $(,)?) => {
        // Use of `match` here is intentional because it affects the lifetimes
        // of temporaries - https://stackoverflow.com/a/48732525/1063961
        match $val {
            tmp => {
                let msg = ::std::format_args!("TelemetryDebug: [{}:{}:{}:{}] {} = {:?}",
                    ::std::file!(),
                    ::std::line!(),
                    ::std::column!(),
                    $crate::function_name!(),
                    ::std::stringify!($val),
                    &&tmp as &dyn ::std::fmt::Debug,
                );
                ::std::eprintln!("{msg}");
                ::log::info!("{msg}");
                tmp
            }
        }
    };
    ($($val:expr),+ $(,)?) => {
        ($($crate::tdbg!($val)),+,)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! function_name {
    () => {{
        // This is a hacky but common way to get the name of the current function (e.g.,
        // `sdv_telemetry::foo::bar::some_function`). It works by taking the name of the type of
        // the function `f` and stripping the suffix `::f`, which then is the name of the enclosing
        // function.
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static ::std::primitive::str {
            ::std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        name.strip_suffix("::f").unwrap()
    }};
}
