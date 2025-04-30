// Copyright 2025 Google LLC

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::PublisherInfo::PublisherInfo as PublisherInfoAidl;

/// Describes a configurable publisher
#[derive(Debug)]
pub struct PublisherInfo {
    /// Name of the publisher service. Must be unique across all publishers
    /// across SDV. Corresponds to AIDL `serviceName`.
    pub service_name: String,

    /// Protobuf FileDescriptorProto bytes containing the schema for published messages.
    /// Corresponds to AIDL `fileDescriptorProto`.
    /// Note: AIDL constraint mentions this should be < 10Kb. This is a runtime
    /// check, not enforced by the type system here.
    pub file_descriptor_proto: Vec<u8>,

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
            // We clone the data to transfer ownership or provide a copy
            // to the AIDL representation.
            serviceName: self.service_name.clone(),
            fileDescriptorProto: self.file_descriptor_proto.clone(),
            messageTypeName: self.message_type_name.clone(),
            supportsGet: self.supports_get,
        }
    }
}
