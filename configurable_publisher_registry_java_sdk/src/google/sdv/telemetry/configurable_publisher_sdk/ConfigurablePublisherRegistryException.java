// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
