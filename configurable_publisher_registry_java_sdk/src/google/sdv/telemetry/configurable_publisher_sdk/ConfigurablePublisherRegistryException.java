// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

import android.os.RemoteException;

public class ConfigurablePublisherRegistryException extends Exception {

    /**
     * Private constructor to encourage using the static factory methods.
     *
     * @param message The detail message for the exception.
     * @param cause The underlying exception that caused this one.
     */
    private ConfigurablePublisherRegistryException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an exception for a failure to initialize the service. This corresponds to the
     * `CreateServiceFailed` variant.
     *
     * @param cause The specific binder status code that caused the failure.
     * @return A new ConfigurablePublisherRegistryException.
     */
    public static ConfigurablePublisherRegistryException createServiceFailed(String cause) {
        return new ConfigurablePublisherRegistryException(
                "Unable to initialize service " + cause, null);
    }

    /**
     * Creates an exception for a general binder error during a call. This corresponds to the
     * `BinderError` variant.
     *
     * @param cause The detailed binder status from the call.
     * @return A new ConfigurablePublisherRegistryException.
     */
    public static ConfigurablePublisherRegistryException binderError(RemoteException cause) {
        return new ConfigurablePublisherRegistryException(
                "Error when making a call to Binder", cause);
    }

    /**
     * Creates an exception for when a publisher is already registered.
     *
     * @param serviceName The name of the service that was already registered.
     * @return A new ConfigurablePublisherRegistryException.
     */
    public static ConfigurablePublisherRegistryException publisherAlreadyRegistered(
            String serviceName) {
        return new ConfigurablePublisherRegistryException(
                "Publisher with service name '" + serviceName + "' is already registered.", null);
    }

    /**
     * Creates an exception for when a publisher is not found during unregistration.
     *
     * @param serviceName The name of the service that was not found.
     * @return A new ConfigurablePublisherRegistryException.
     */
    public static ConfigurablePublisherRegistryException publisherNotFound(String serviceName) {
        return new ConfigurablePublisherRegistryException(
                "Publisher with service name '" + serviceName + "' was not found.", null);
    }
}
