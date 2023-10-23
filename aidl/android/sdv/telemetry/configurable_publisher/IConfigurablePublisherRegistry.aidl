// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry.configurable_publisher;

import android.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import android.sdv.telemetry.configurable_publisher.PublisherInfo;

/**
 * Service for configurable publishers to register themselves with telemetry service
 */
interface IConfigurablePublisherRegistry {
    /**
     * Registers configuratble publishers with
     * telemetry service.
     *
     * @param pInfo: information about the publisher. Refer to PublisherInfo for more
     *         information.
     * @param publisher: publisher interface used by subscribers
     *
     */
    void registerPublisher(in PublisherInfo pInfo, in IConfigurablePublisher publisher);
}
