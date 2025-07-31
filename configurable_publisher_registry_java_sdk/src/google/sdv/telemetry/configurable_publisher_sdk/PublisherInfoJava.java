// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

import google.sdv.telemetry.configurable_publisher.PublisherInfo;

public class PublisherInfoJava {
    /**
     * Name of the publisher service. Must be unique across all publishers. Corresponds to AIDL
     * `serviceName`.
     */
    private final String serviceName;

    /**
     * Protobuf FileDescriptorProto bytes containing the schema for published messages. Note: There
     * might be a size constraint (e.g., < 10Kb) enforced at runtime.
     */
    private final byte[] fileDescriptorProto;

    /**
     * The fully qualified name of the root Protobuf message type being published. This type must be
     * defined within the `fileDescriptorProto`.
     */
    private final String messageTypeName;

    /**
     * Indicates whether the publisher supports fetching the latest message without an active
     * subscription (`get` operation).
     */
    private final boolean supportsGet;

    /**
     * Constructs a new PublisherInfo object.
     *
     * @param serviceName The unique name of the service.
     * @param fileDescriptorProto The protobuf schema as a byte array.
     * @param messageTypeName The fully qualified name of the message type.
     * @param supportsGet True if the publisher supports the 'get' operation.
     */
    public PublisherInfoJava(
            String serviceName,
            byte[] fileDescriptorProto,
            String messageTypeName,
            boolean supportsGet) {
        this.serviceName = serviceName;
        this.fileDescriptorProto = fileDescriptorProto;
        this.messageTypeName = messageTypeName;
        this.supportsGet = supportsGet;
    }

    /**
     * Converts this PublisherInfo object into the representation expected by the AIDL layer.
     *
     * @return A new PublisherInfo object with copied data.
     */
    PublisherInfo toAidlRepresentation() {
        PublisherInfo aidlInfo = new PublisherInfo();
        // The data is copied to transfer ownership to the AIDL representation.
        aidlInfo.serviceName = this.serviceName;
        aidlInfo.fileDescriptorProto = this.fileDescriptorProto.clone();
        aidlInfo.messageTypeName = this.messageTypeName;
        aidlInfo.supportsGet = this.supportsGet;
        return aidlInfo;
    }

    public String getServiceName() {
        return serviceName;
    }
}
