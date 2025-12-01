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

/**
 * Interface for telemetry service to implement. Telemetry service subscribes to configurable
 * publisher as a data subscriber. The methods in this interface are named from the perspective of
 * the data publisher. Interface is not oneway to ensure sendData returns only after received by
 * telemetry service.
 */

@VintfStability
interface ISubscriber {
    /**
     * Sends data to the subscriber. The data format is already known by the subscriber.
     *
     * There is no check for maximum data size, however the caller must not send more than 10KB of
     * data at a time as all binders for a process shares the same 1MB buffer limit
     *
     * @param data binary serialized protobuf message sent to subscriber
     *
     */
    void sendData(in byte[] data);
}
