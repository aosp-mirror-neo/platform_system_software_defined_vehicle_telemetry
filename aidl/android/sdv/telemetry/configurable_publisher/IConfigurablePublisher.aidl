// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry.configurable_publisher;

import android.sdv.telemetry.configurable_publisher.ISubscriber;

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
     *
     */
    void subscribe(in ISubscriber subscriber, in @nullable byte[] configuration);

    /**
     * Unsubscribes from a registered publisher. It should return an error if the publisher is
     * already unsubscribed or the API call fails.
     * TODO: b/309128308: Design error mechanism for configurable publisher service
     * TODO: b/317902294: Design how to store/manage subscriptions to configurable publishers
     *
     * @param configuration optional configuration associated with the subscriber. Configuration is
     *         opaque to the telemetry service and is sent as part of the Metrics Configuration
     */
    void unsubscribe(in @nullable byte[] configuration);
}
