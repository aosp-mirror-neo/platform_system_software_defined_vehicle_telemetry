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
package google.sdv.telemetry.configurable_publisher_sdk;

import google.sdv.telemetry.configurable_publisher.PublisherInfo;

public class PublisherInfoJava {
    /**
     * Name of the publisher service. Must be unique across all publishers. Corresponds to AIDL
     * `serviceName`.
     */
    private final String serviceName;

    /**
     * Protobuf FileDescriptorSet bytes containing the schema for published messages. Note: There
     * might be a size constraint (e.g., < 10Kb) enforced at runtime.
     */
    private final byte[] fileDescriptorSet;

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
    public PublisherInfoJava(String serviceName, byte[] fileDescriptorSet, String messageTypeName,
                             boolean supportsGet) {
        this.serviceName = serviceName;
        this.fileDescriptorSet = fileDescriptorSet;
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
        aidlInfo.fileDescriptorSet = this.fileDescriptorSet.clone();
        aidlInfo.messageTypeName = this.messageTypeName;
        aidlInfo.supportsGet = this.supportsGet;
        // `fileDescriptorProto` is deprecated.
        aidlInfo.fileDescriptorProto = null;
        return aidlInfo;
    }

    public String getServiceName() {
        return serviceName;
    }
}
