// Copyright 2023 Google LLC

package google.sdv.telemetry.configurable_publisher;

/**
 * Describes a configurable publisher
 */

@VintfStability
parcelable PublisherInfo {
    // Name of the publisher service. Must be unique across all publishers
    // across SDV. Must be non-null
    @utf8InCpp String serviceName;

    // Protobuf descriptor that contains root published message and all its
    // nested sub-messages. Must be non-null. This schema is used by the telemetry
    // service to deserialize the data sent by the publisher.
    //
    // fileDescriptorProto must be less than 10Kb to prevent binder buffer to be full.
    byte[] fileDescriptorProto;

    // Name of the type of the root published message. Must be non-null
    // The message must be declared in fileDescriptorProto.
    @utf8InCpp String messageTypeName;

    // Indicates whether the publisher provides latest message without subscribing.
    boolean supportsGet;
}
