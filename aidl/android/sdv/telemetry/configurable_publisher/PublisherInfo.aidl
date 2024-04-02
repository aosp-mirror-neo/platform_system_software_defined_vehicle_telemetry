// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry.configurable_publisher;

/**
 * Describes a configurable publisher
 */
@VintfStability
parcelable PublisherInfo {
    // Name of the publisher service. Must be unique across all publishers
    // across SDV. Must be non-null
    @utf8InCpp String serviceName;

    // Name of the publisher topic. Must be unique across one service name.
    // Must be non-null
    @utf8InCpp String topicName;

    // Protobuf descriptor that contains root published message and all its
    // nested sub-messages. Must be non-null. This schema is used by the telemetry
    // service to deserialize the data sent by the publisher.
    byte[] fileDescriptorProto;

    // Name of the type of the root published message. Must be non-null
    // The message must be declared in fileDescriptorProto.
    @utf8InCpp String messageTypeName;
}
