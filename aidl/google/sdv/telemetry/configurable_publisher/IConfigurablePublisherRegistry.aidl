// Copyright 2023 Google LLC. All Rights Reserved.

package google.sdv.telemetry.configurable_publisher;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.PublisherInfo;

/**
 * Interface for telemetry service to implement.
 * Configurable publishers register to telemetry service
 * when they are available for publishing data.
 */

interface IConfigurablePublisherRegistry {
    /**
     * Registers configurable publishers with
     * telemetry service.
     *
     * @param pInfo information about the publisher. Refer to PublisherInfo for more
     *         information.
     * @param publisher publisher interface used by subscribers
     *
     */
    void registerPublisher(in PublisherInfo pInfo, in IConfigurablePublisher publisher);

    /**
     * Unregisters configurable publishers from telemetry service. Unregistering an already
     * unregistered or otherwise non-existent publisher has no effect and will be ignored.
     *
     * @param serviceName name of the publisher service. Must be unique across all publishers across
     * SDV. Must be non-null
     */
    void unregisterPublisher(in @utf8InCpp String serviceName);
}
