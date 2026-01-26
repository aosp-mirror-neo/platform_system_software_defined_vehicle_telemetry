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
