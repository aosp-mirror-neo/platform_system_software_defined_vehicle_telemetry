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

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::PublisherInfo::PublisherInfo;

use crate::PublisherName;

/// Describes a configurable publisher
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
#[non_exhaustive]
pub struct PublisherMetadata {
    /// The name of the publisher
    pub name: PublisherName,
    /// Serialized protobuf `FileDescriptorSet` containing the schema of the published message and
    /// its dependencies.
    pub file_descriptor_set: Vec<u8>,
    /// The fully qualified name of the protobuf message type being published.
    pub message_full_name: String,
    /// Indicates whether the publisher supports fetching data on demand.
    pub supports_on_demand_fetching: bool,
}

impl PublisherMetadata {
    /// Creates a new instance of `PublisherMetadata`.
    pub fn new(name: PublisherName) -> Self {
        Self {
            name,
            file_descriptor_set: vec![],
            message_full_name: String::new(),
            supports_on_demand_fetching: false,
        }
    }

    pub(crate) fn into_aidl(self) -> PublisherInfo {
        PublisherInfo {
            serviceName: self.name.0,
            fileDescriptorSet: Some(self.file_descriptor_set),
            messageTypeName: self.message_full_name,
            supportsGet: self.supports_on_demand_fetching,
            fileDescriptorProto: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::container_eq;
    use googletest::matchers::eq;
    use googletest::matchers::none;
    use googletest::matchers::pat;
    use googletest::matchers::some;

    #[googletest::test]
    fn into_aidl_success() {
        let metadata = PublisherMetadata {
            name: "test_publisher".parse::<PublisherName>().unwrap(),
            file_descriptor_set: vec![1, 2, 3],
            message_full_name: "test_message".to_string(),
            supports_on_demand_fetching: true,
        };

        let publisher_info = metadata.into_aidl();

        expect_that!(
            publisher_info,
            pat!(PublisherInfo {
                serviceName: eq("test_publisher"),
                fileDescriptorSet: some(container_eq([1, 2, 3])),
                messageTypeName: eq("test_message"),
                supportsGet: eq(&true),
                fileDescriptorProto: none(),
                ..
            })
        );
    }
}
