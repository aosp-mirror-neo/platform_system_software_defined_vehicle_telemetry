// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

public interface Subscriber {
    /**
     * Sends data to the subscriber.
     *
     * @param data The byte array of data to send.
     * @throws SubscriberException if an error occurs during the data transfer.
     */
    void sendData(byte[] data) throws SubscriberException;
}
