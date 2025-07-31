// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

public class ConfigurablePublisherException extends Exception {

    /**
     * Private constructor to enforce the use of static factory methods.
     *
     * @param message The detail message for the exception.
     */
    private ConfigurablePublisherException(String message) {
        super(message);
    }

    /**
     * Creates an exception for when getting the latest message fails.
     *
     * @return A new ConfigurablePublisherException.
     */
    public static ConfigurablePublisherException getLatestMessageFailed() {
        return new ConfigurablePublisherException("No message found for the given configuration");
    }

    /**
     * Creates an exception for when creating a subscription fails.
     *
     * @return A new ConfigurablePublisherException.
     */
    public static ConfigurablePublisherException createSubscriptionFailed() {
        return new ConfigurablePublisherException("Create Subscription Failed");
    }

    /**
     * Creates an exception for an unknown or unspecified error.
     *
     * @return A new ConfigurablePublisherException.
     */
    public static ConfigurablePublisherException unknownError() {
        return new ConfigurablePublisherException("An unknown error occurred");
    }
}
