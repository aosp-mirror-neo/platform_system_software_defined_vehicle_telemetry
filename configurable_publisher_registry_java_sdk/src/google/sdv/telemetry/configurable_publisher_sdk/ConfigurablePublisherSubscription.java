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
