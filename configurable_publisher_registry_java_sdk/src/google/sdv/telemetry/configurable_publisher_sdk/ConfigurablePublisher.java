// Copyright 2025 Google LLC
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
