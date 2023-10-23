// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry.configurable_publisher;

/**
 * A aidl parcelable containing all information about configurable publishers
 *
 */
parcelable PublisherInfo {
    // Name of the publisher service. Must be unique across all publishers
    // across SDV. Must be non-null
    String serviceName;

    // Name of the publisher topic. Must be unique across one service name.
    // Must be non-null
    String topicName;

    // Protobuf descriptor that contains root published message and all its
    // nested sub-messages. Must be non-null
    byte[] fileDescriptorProto;

    // Name of the type of the root published message. Must be non-null
    // The message must be declared in fileDescriptorProto.
    String messageTypeName;
}
