// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry.configurable_publisher;

/**
 * Implemented by a subscriber and sent as a
 * function argument to IConfigurablePublisher's subscribe function.
 */
interface ISubscriber {
    /**
     * Sends data to the subscriber
     *
     * @param data: binary serialized protobuf Any message sent to subscriber
     *
     */
    void sendData(in byte[] data);
}
