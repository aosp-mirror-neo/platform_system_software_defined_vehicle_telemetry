// Copyright 2023 Google LLC

package google.sdv.telemetry.configurable_publisher;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;
import google.sdv.telemetry.configurable_publisher.ISubscriber;

/**
 * Interface for configurable publishers (data publishers) to implement.
 * A configurable publisher can be manipulated using a configuration.
 * The configuration format is opaque to the telemetry service.
 */

@VintfStability
interface IConfigurablePublisher {
    /**
     * Subscribes to a registered publisher.
     * TODO: b/309128308: Design error mechanism for configurable publisher service
     *
     * @param subscriber used by publisher to send data
     * @param configuration optional configuration associated with the subscriber. Configuration is
     *         opaque to the telemetry service and is sent as part of the Metrics Configuration
     * @return IConfigurablePublisherSubscription configurable publishers subscription.
     *
     */
    IConfigurablePublisherSubscription createSubscription(
            in ISubscriber subscriber, in @nullable byte[] configuration);

    /**
     * Returns latest message for the provided configuration.
     *
     * @return Latest message, null if no messages were published,
     *         or INVALID_OPERATION if operation is not supported.
     */
    @nullable byte[] getLatestMessage(in @nullable byte[] configuration);
}
