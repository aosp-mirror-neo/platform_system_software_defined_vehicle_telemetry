// Copyright 2025 Google LLC

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::PublisherInfo::PublisherInfo as PublisherInfoAidl;

/// Describes a configurable publisher
#[derive(Debug)]
pub struct PublisherInfo {
    /// Name of the publisher service. Must be unique across all publishers
    /// across SDV. Corresponds to AIDL `serviceName`.
    pub service_name: String,

    /// Protobuf FileDescriptorSet bytes containing the schema for published messages. Corresponds
    /// to AIDL `fileDescriptorSet`.
    ///
    /// Note: AIDL constraint mentions this should be < 10Kb. This is a runtime check, not enforced
    /// by the type system here.
    pub file_descriptor_set: Option<Vec<u8>>,

    /// The fully qualified name of the root Protobuf message type being published.
    /// This type must be defined within the `file_descriptor_proto`.
    /// Corresponds to AIDL `messageTypeName`.
    pub message_type_name: String,

    /// Indicates whether the publisher supports fetching the latest message
    /// without an active subscription (`get` operation).
    /// Corresponds to AIDL `supportsGet`.
    pub supports_get: bool,
}

impl PublisherInfo {
    /// Converts PublisherInfo into the representation
    /// expected by the AIDL layer
    pub(super) fn to_aidl_representation(&self) -> PublisherInfoAidl {
        PublisherInfoAidl {
            serviceName: self.service_name.clone(),
            fileDescriptorSet: self.file_descriptor_set.clone(),
            messageTypeName: self.message_type_name.clone(),
            supportsGet: self.supports_get,
            // Never set the deprecated `fileDescriptorProto` field.
            fileDescriptorProto: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::{elements_are, eq, none, some};

    #[googletest::test]
    fn to_aidl_representation_success() {
        let publisher_info = PublisherInfo {
            service_name: "test_service".to_string(),
            file_descriptor_set: Some(vec![1, 2, 3]),
            message_type_name: "test_message".to_string(),
            supports_get: true,
        };

        let aidl_publisher_info = publisher_info.to_aidl_representation();

        expect_that!(
            aidl_publisher_info,
            pat!(PublisherInfoAidl {
                serviceName: eq("test_service"),
                fileDescriptorSet: eq(some(elements_are![1, 2, 3])),
                messageTypeName: eq("test_message"),
                supportsGet: eq(true),
                fileDescriptorProto: none(),
            })
        );
    }
}
