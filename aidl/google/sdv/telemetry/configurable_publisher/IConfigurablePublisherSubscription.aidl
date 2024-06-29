// Copyright 2024 Google LLC

package google.sdv.telemetry.configurable_publisher;

/**
 * A binder interface facilitating configurable publishers subscriptions to allow
 * subscribers to unsubscribe from them.
 */

interface IConfigurablePublisherSubscription {
    /**
     * Ends the underlying subscription.
     */
    void unsubscribe();
}
