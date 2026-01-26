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

use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types;
use std::borrow::Cow;
use std::sync::LazyLock;

/// An array of file descriptors for well known protobuf types.
static WELL_KNOWN_FILE_DESCRIPTORS: LazyLock<[FileDescriptor; 11]> = LazyLock::new(|| {
    [
        protobuf::descriptor::file_descriptor().to_owned(),
        well_known_types::any::file_descriptor().to_owned(),
        well_known_types::api::file_descriptor().to_owned(),
        well_known_types::duration::file_descriptor().to_owned(),
        well_known_types::empty::file_descriptor().to_owned(),
        well_known_types::field_mask::file_descriptor().to_owned(),
        well_known_types::source_context::file_descriptor().to_owned(),
        well_known_types::struct_::file_descriptor().to_owned(),
        well_known_types::timestamp::file_descriptor().to_owned(),
        well_known_types::type_::file_descriptor().to_owned(),
        well_known_types::wrappers::file_descriptor().to_owned(),
    ]
});

/// `FileDescriptorsAndWkts` (File Descriptors And Well Known Types) is a collection of
/// `FileDescriptor`s, which can be queried by message name. It always implicitly contains all
/// `WELL_KNOWN_FILE_DESCRIPTORS`.
#[derive(Debug)]
pub struct FileDescriptorsAndWkts {
    file_descriptors: Vec<FileDescriptor>,
}

impl TryFrom<Vec<FileDescriptorProto>> for FileDescriptorsAndWkts {
    type Error = protobuf::Error;

    /// Tries to convert the provided `FileDescriptorProto`s into `FileDescriptor`s. The provided
    /// file descriptor protos may depend on any of the `WELL_KNOWN_FILE_DESCRIPTORS`, which are
    /// automatically added as dependencies.
    fn try_from(file_descriptor_protos: Vec<FileDescriptorProto>) -> Result<Self, Self::Error> {
        let file_descriptors = FileDescriptor::new_dynamic_fds(
            file_descriptor_protos,
            // By specifying the well known file descriptors as dependencies, we allow any of the
            // `file_descriptor_protos` to refer to the well known file descriptors, without them
            // having to be explicitly specified.
            WELL_KNOWN_FILE_DESCRIPTORS.as_slice(),
        )?;

        Ok(Self { file_descriptors })
    }
}

impl FileDescriptorsAndWkts {
    /// Finds a message descriptor by its full name. If the name doesn't start with a dot, we
    /// prepend a dot to the name. If the message cannot be found inside the file descriptors, it is
    /// searched for in the well known types.
    pub fn find_message_by_full_name(&self, full_name: &str) -> Option<MessageDescriptor> {
        let full_name = if full_name.starts_with('.') {
            Cow::from(full_name)
        } else {
            Cow::from(format!(".{full_name}"))
        };

        // Iterate through all `file_descriptors` and find a `MessageDescriptor` for the given
        // `full_name`. `FileDescriptor::message_by_full_name` requires the name to start with dot.
        self.file_descriptors
            .iter()
            .chain(WELL_KNOWN_FILE_DESCRIPTORS.iter())
            .find_map(|fd| fd.message_by_full_name(&full_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::{
        contains_substring, eq, err, len, none, ok, pat, pointwise, result_of_ref, some,
    };
    use protobuf::well_known_types::{api::Api, empty::Empty};
    use protobuf::MessageFull;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::parameterized_test;

    #[googletest::test]
    fn well_known_types_has_expected_length() {
        expect_that!(WELL_KNOWN_FILE_DESCRIPTORS.as_slice(), len(eq(11)));
    }

    parameterized_test!(
        #[googletest::test]
        try_from,

        |file_descriptor_protos, want_file_descriptor_protos: Vec<FileDescriptorProto>| {
            let result = FileDescriptorsAndWkts::try_from(file_descriptor_protos);

            expect_that!(
                result,
                ok(pat!(&FileDescriptorsAndWkts {
                    // We cannot compare `FileDescriptor`s directly, because `FileDescriptor::eq` is
                    // defined as pointer-equality, not value-equality. Hence, we fall back to just
                    // comparing `FileDescriptorProto`s.
                    file_descriptors: ref pointwise!(
                        |want| pat!(&FileDescriptor { proto(): eq(want) }),
                        want_file_descriptor_protos.as_slice()
                    )
                }))
            );
        },

        empty: (vec![],vec![]),
        with_custom_descriptor: (
            vec![TestMessage::descriptor().file_descriptor().proto().to_owned()],
            vec![TestMessage::descriptor().file_descriptor().proto().to_owned()],
        ),
        with_well_known_descriptor: (
            vec![Api::descriptor().file_descriptor().proto().to_owned()],
            vec![Api::descriptor().file_descriptor().proto().to_owned()],
        ),
    );

    #[googletest::test]
    fn try_from_with_duplicate_well_known_type_fails() {
        let result = FileDescriptorsAndWkts::try_from(vec![
            TestMessage::descriptor().file_descriptor().proto().to_owned(),
            Empty::descriptor().file_descriptor().proto().to_owned(),
        ]);

        expect_that!(
            result,
            // The error enums are private to the protobuf crate, hence, we use the error's string
            // representation here.
            err(result_of_ref!(
                |err| format!("{err:?}"),
                contains_substring("NonUniqueDependencies")
            ))
        );
    }

    parameterized_test!(
        #[googletest::test]
        find_message_by_full_name,

        |file_descriptors: &[FileDescriptor], full_name, expected_full_name: Option<String>| {
            let file_descriptors = FileDescriptorsAndWkts { file_descriptors: file_descriptors.to_owned() };
            let message_descriptor = file_descriptors.find_message_by_full_name(full_name);

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

        missing_message: (&[], ".foo.Bar", None),


        well_known_type: (
            &[],
            ".google.protobuf.Empty",
            Some(Empty::descriptor().full_name().to_owned())
        ),

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

        duplicate_file_descriptor: (
            &[
                Api::descriptor().file_descriptor().to_owned(),
                Api::descriptor().file_descriptor().to_owned(),
            ],
            ".google.protobuf.Api",
            Some(Api::descriptor().full_name().to_owned()),
        ),
    );
}
