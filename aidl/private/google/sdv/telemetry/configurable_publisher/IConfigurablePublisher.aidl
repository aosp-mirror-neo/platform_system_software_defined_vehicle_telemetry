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
     * The configuration is expected to be small (<10Kb). This API is not suitable for
     * passing very large configurations as it may cause binder buffer to be full.
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
     * The configuration is expected to be small (<10Kb). This API is not suitable for
     * passing very large configurations as it may cause binder buffer to be full.
     *
     * The returned message must be small (<10Kb) to prevent binder buffer to be full.
     *
     * @return Latest message, null if no messages were published,
     *         or INVALID_OPERATION if operation is not supported.
     */
    @nullable byte[] getLatestMessage(in @nullable byte[] configuration);
}
