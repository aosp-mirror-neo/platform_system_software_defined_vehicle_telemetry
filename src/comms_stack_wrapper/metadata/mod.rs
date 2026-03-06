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

use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::any::Any;
use protobuf::MessageFull;
use sdv_telemetry_utils::option_extension::OptionExtension;

mod cache;
mod fetch;
mod generation;
mod generations;

pub(crate) use cache::MetadataCache;
pub(crate) use fetch::fetch_metadata;
pub(crate) use generation::MetadataGeneration;
pub(crate) use generation::MetadataGenerationId;
pub(crate) use generation::MetadataServiceType;
pub(crate) use generations::CurrentMetadataGenerations;
pub(crate) use generations::MetadataGenerationEvent;

/// The metadata retrieved for a service, containing the necessary descriptors.
#[derive(Clone, Debug)]
pub(crate) struct Metadata {
    pub(crate) config_descriptor: Option<MessageDescriptor>,
    pub(crate) message_descriptor: MessageDescriptor,
}

impl Metadata {
    /// Returns the config descriptor if it exists, otherwise returns the descriptor for `Any`.
    pub(crate) fn get_config_descriptor_or_any(&self) -> MessageDescriptor {
        self.config_descriptor.clone().unwrap_or(Any::descriptor())
    }

    fn message_descriptor_eq(a: &MessageDescriptor, b: &MessageDescriptor) -> bool {
        // We want to ensure that both `MessageDescriptor`s describe the same message. We
        // MUST NOT compare the `MessageDescriptor`s directly, because they are only equal
        // if the message is defined in the exact same file and position within that file.
        //
        // Instead, we need to compare the `DescriptorProto`s (returned by
        // `MessageDescriptor::proto`), which describe the actual message formats,
        // irrespective of the file they are in, as well as the "full name" (= package name
        // + message name).
        a.proto() == b.proto() && a.full_name() == b.full_name()
    }
}

impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.config_descriptor.eq_by(other.config_descriptor.as_ref(), Self::message_descriptor_eq)
            && Self::message_descriptor_eq(&self.message_descriptor, &other.message_descriptor)
    }
}

#[cfg(all(test, unit_tests))]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::eq;
    use googletest::matchers::not;
    use protobuf::well_known_types::any::Any;
    use protobuf::well_known_types::empty::Empty;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::MessageFull;
    use sdv_telemetry_test_proto_bool_value::test_bool_value_1::BoolValue as TestBoolValue1;
    use sdv_telemetry_test_proto_bool_value::test_bool_value_2::BoolValue as TestBoolValue2;
    use sdv_telemetry_test_utils::parameterized_test;

    #[googletest::test]
    fn test_get_config_descriptor_or_any_with_config_descriptor() {
        let metadata = Metadata {
            config_descriptor: Some(Timestamp::descriptor()),
            message_descriptor: Empty::descriptor(),
        };
        expect_that!(metadata.get_config_descriptor_or_any(), eq(&Timestamp::descriptor()));
    }

    #[googletest::test]
    fn test_get_config_descriptor_or_any_without_config_descriptor() {
        let metadata =
            Metadata { config_descriptor: None, message_descriptor: Empty::descriptor() };
        expect_that!(metadata.get_config_descriptor_or_any(), eq(&Any::descriptor()));
    }

    parameterized_test!(
        #[googletest::test]
        test_metadata_eq,

        |a: Metadata, b: Metadata| {
            expect_that!(a, eq(&b));
        },

        same_descriptors: (
            Metadata {
                config_descriptor: Some(Timestamp::descriptor()),
                message_descriptor: Empty::descriptor(),
            },
            Metadata {
                config_descriptor: Some(Timestamp::descriptor()),
                message_descriptor: Empty::descriptor(),
            },
        ),
        same_descriptors_no_config: (
            Metadata {
                config_descriptor: None,
                message_descriptor: Empty::descriptor(),
            },
            Metadata {
                config_descriptor: None,
                message_descriptor: Empty::descriptor(),
            },
        ),
        same_descriptors_different_files: (
            Metadata {
                config_descriptor: Some(TestBoolValue1::descriptor()),
                message_descriptor: TestBoolValue1::descriptor(),
            },
            Metadata {
                config_descriptor: Some(TestBoolValue2::descriptor()),
                message_descriptor: TestBoolValue2::descriptor(),
            },
        ),
    );

    parameterized_test!(
        #[googletest::test]
        test_metadata_not_eq,

        |a: Metadata, b: Metadata| {
            expect_that!(a, not(eq(&b)));
        },

        different_message_descriptor: (
            Metadata {
                config_descriptor: Some(Timestamp::descriptor()),
                message_descriptor: Empty::descriptor(),
            },
            Metadata {
                config_descriptor: Some(Timestamp::descriptor()),
                message_descriptor: Any::descriptor(),
            },
        ),
        different_config_descriptor: (
            Metadata {
                config_descriptor: Some(Timestamp::descriptor()),
                message_descriptor: Empty::descriptor(),
            },
            Metadata {
                config_descriptor: Some(Any::descriptor()),
                message_descriptor: Empty::descriptor(),
            },
        ),
        missing_config_descriptor: (
            Metadata {
                config_descriptor: Some(Timestamp::descriptor()),
                message_descriptor: Empty::descriptor(),
            },
            Metadata { config_descriptor: None, message_descriptor: Empty::descriptor() },
        ),
    );
}
