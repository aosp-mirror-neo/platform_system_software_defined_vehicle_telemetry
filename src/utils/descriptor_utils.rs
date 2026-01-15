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

//! This module contains descriptors for well known protobuf types.

use lazy_static::lazy_static;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types;
use std::borrow::Cow;

lazy_static! {
    /// An array of file descriptors for well known protobuf types.
    pub static ref WELL_KNOWN_FILE_DESCRIPTORS: Vec<FileDescriptor> = vec![
        protobuf::descriptor::file_descriptor().clone(),
        well_known_types::any::file_descriptor().clone(),
        well_known_types::api::file_descriptor().clone(),
        well_known_types::duration::file_descriptor().clone(),
        well_known_types::empty::file_descriptor().clone(),
        well_known_types::field_mask::file_descriptor().clone(),
        well_known_types::source_context::file_descriptor().clone(),
        well_known_types::struct_::file_descriptor().clone(),
        well_known_types::timestamp::file_descriptor().clone(),
        well_known_types::type_::file_descriptor().clone(),
        well_known_types::wrappers::file_descriptor().clone()
    ];
}

/// Finds a message descriptor by its full name. If the name doesn't start with a dot, we prepend a
/// dot to the name.
pub fn find_message_by_full_name<'a>(
    file_descriptors: impl IntoIterator<Item = &'a FileDescriptor>,
    full_name: &str,
) -> Option<MessageDescriptor> {
    let full_name = if full_name.starts_with('.') {
        Cow::from(full_name)
    } else {
        Cow::from(format!(".{full_name}"))
    };

    // Iterate through all `file_descriptors` and find a `MessageDescriptor` for the given
    // `full_name`. `FileDescriptor::message_by_full_name` requires the name to start with dot.
    file_descriptors.into_iter().find_map(|fd| fd.message_by_full_name(&full_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::{eq, none, pat, some};
    use protobuf::{well_known_types::empty::Empty, MessageFull};
    use sdv_telemetry_test_utils::parameterized_test;

    parameterized_test!(
        #[googletest::test]
        find_message_by_full_name,

        |file_descriptors, full_name, expected_full_name: Option<String>| {
            let message_descriptor = find_message_by_full_name(file_descriptors, full_name);

            match expected_full_name {
                Some(expected_full_name) => {
                    expect_that!(
                        message_descriptor,
                        // We cannot compare `MessageDescriptor`s directly, because
                        // `MessageDescriptor::eq` is defined as pointer-equality, including the
                        // message's file descriptor, not value-equality. Hence, we fall back to
                        // just comparing the message names.
                        some(pat!(&MessageDescriptor { full_name(): eq(&expected_full_name) }))
                    );
                },
                None => {
                    expect_that!(message_descriptor, none());
                }
            }
        },

        missing_message: (&[], ".google.protobuf.Empty", None),
        leading_dot: (
            &[Empty::descriptor().file_descriptor().to_owned()],
            ".google.protobuf.Empty",
            Some(Empty::descriptor().full_name().to_owned())
        ),
        no_leading_dot: (
            &[Empty::descriptor().file_descriptor().to_owned()],
            "google.protobuf.Empty",
            Some(Empty::descriptor().full_name().to_owned())
        ),
    );
}
