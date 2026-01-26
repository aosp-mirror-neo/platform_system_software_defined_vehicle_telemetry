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

import java.util.Optional;

public interface ConfigurablePublisher {

    /**
     * Subscribes to the publisher with a specific configuration by passing a Subscriber interface
     * object
     *
     * @param subscriber The subscriber that will receive messages.
     * @param configuration An optional byte array for configuration. Corresponds to
     *     `Option<&[u8]>`.
     * @return A subscription object that can be used to manage the subscription.
     * @throws ConfigurablePublisherException if the subscription cannot be created.
     */
    ConfigurablePublisherSubscription createSubscription(
            Subscriber subscriber, Optional<byte[]> configuration)
            throws ConfigurablePublisherException;

    /**
     * Provides the latest message for a given configuration.
     *
     * @param configuration An optional byte array for configuration. Corresponds to
     *     `Option<&[u8]>`.
     * @return An Optional containing the latest message as a byte array, or an empty Optional if no
     *     message is available.
     * @throws ConfigurablePublisherException if there is an error retrieving the message.
     */
    Optional<byte[]> getLatestMessage(Optional<byte[]> configuration)
            throws ConfigurablePublisherException;
}
