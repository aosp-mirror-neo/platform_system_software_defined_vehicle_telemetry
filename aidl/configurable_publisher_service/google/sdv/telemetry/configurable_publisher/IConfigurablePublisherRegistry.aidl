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
