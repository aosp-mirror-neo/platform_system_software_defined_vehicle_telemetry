// Copyright 2023 Google LLC. All Rights Reserved.

package google.sdv.telemetry.configurable_publisher;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;
import google.sdv.telemetry.configurable_publisher.ISubscriber;

/**
 * Interface for configurable publishers (data publishers) to implement.
 * A configurable publisher can be manipulated using a configuration.
 * The configuration format is opaque to the telemetry service.
 */

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
    IConfigurablePublisherSubscription create_subscription(
            in ISubscriber subscriber, in @nullable byte[] configuration);
}
