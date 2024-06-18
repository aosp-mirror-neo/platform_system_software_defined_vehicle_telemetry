// Copyright 2023 Google LLC. All Rights Reserved.

package google.sdv.telemetry.configurable_publisher;

/**
 * Interface for telemetry service to implement. Telemetry service subscribes to configurable
 * publisher as a data subscriber. The methods in this interface are named from the perspective of
 * the data publisher. Interface is not oneway to ensure sendData returns only after received by
 * telemetry service.
 */

interface ISubscriber {
    /**
     * Sends data to the subscriber. The data format is already known by the subscriber.
     * There is no check for maximum data size, however the caller must not send more than 10KB of
     * data at a time as all binders for a process shares the same 1MB buffer limit
     *
     * @param data binary serialized protobuf message sent to subscriber
     *
     */
    void sendData(in byte[] data);
}
