// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

class SubscriptionException extends Exception {
    public SubscriptionException(String message) {
        super(message);
    }
}

/**
 * Client facing interface that represents an active subscription to a ConfigurablePublisher. This
 * allows for managing the lifecycle of a subscription, such as unsubscribing.
 */
public interface ConfigurablePublisherSubscription {
    /** Cancels or closes the subscription. */
    void unsubscribe() throws SubscriptionException;
}
