//
// Copyright (C) 2023 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

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
