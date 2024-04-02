// Copyright 2023 Google LLC. All Rights Reserved.

package google.sdv.telemetry.configurable_publisher;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.PublisherInfo;

/**
 * Interface for telemetry service to implement.
 * Configurable publishers register to telemetry service
 * when they are available for publishing data.
 */
@VintfStability
interface IConfigurablePublisherRegistry {
    /**
     * Registers configurable publishers with
     * telemetry service.
     * TODO: b/331675833: add unregisterPublisher()
     *
     * @param pInfo information about the publisher. Refer to PublisherInfo for more
     *         information.
     * @param publisher publisher interface used by subscribers
     *
     */
    void registerPublisher(in PublisherInfo pInfo, in IConfigurablePublisher publisher);
}
