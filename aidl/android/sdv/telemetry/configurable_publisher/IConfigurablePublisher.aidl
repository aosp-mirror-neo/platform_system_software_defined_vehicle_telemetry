// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry.configurable_publisher;

import android.sdv.telemetry.configurable_publisher.ISubscriber;

/**
 * A binder interface facilitating configurable publishers to allow
 * subscribers to subscribe to them.
 *
 */
interface IConfigurablePublisher {
    /**
     * Subscribes to a registered publisher.
     * TODO: b/309128308: Design error mechanism for configurable publisher service
     *
     * @param subscriber: used by publisher to send data
     * @param configuration: optional configuration associated with the subscriber
     *
     */
    void subscribe(in ISubscriber subscriber, in @nullable byte[] configuration);
}
