// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

import android.os.RemoteException;
import android.util.Log;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;
import google.sdv.telemetry.configurable_publisher.ISubscriber;

import java.util.Optional;

/**
 * Implements the IConfigurablePublisher AIDL interface by extending the auto-generated Stub class.
 * This holds the core ConfigurablePublisher logic object provided by the client.
 */
class ConfigurablePublisherAidlImpl extends IConfigurablePublisher.Stub {

    private final String TAG = ConfigurablePublisherAidlImpl.class.getSimpleName();
    private final ConfigurablePublisher configurablePublisher;

    /**
     * Constructs the AIDL implementation wrapper.
     *
     * @param configurablePublisher The concrete implementation of the publisher logic.
     */
    public ConfigurablePublisherAidlImpl(ConfigurablePublisher configurablePublisher) {
        this.configurablePublisher = configurablePublisher;
    }

    @Override
    public IConfigurablePublisherSubscription createSubscription(
            ISubscriber subscriber, byte[] config) throws RemoteException {

        Subscriber subscriberImpl = new SubscriberImpl(subscriber);
        ConfigurablePublisherSubscription subscription = null;
        try {
            subscription =
                    this.configurablePublisher.createSubscription(
                            subscriberImpl, Optional.ofNullable(config));
        } catch (ConfigurablePublisherException e) {
            Log.e(TAG, "Error in creating subscription with client publisher " + e.getMessage());
            throw new RemoteException(
                    "An error occurred when creating a subscription" + e.getMessage());
        }

        IConfigurablePublisherSubscription subscriptionAidlImpl =
                new ConfigurablePublisherSubscriptionAidlImpl(subscription);
        return subscriptionAidlImpl;
    }

    @Override
    public byte[] getLatestMessage(byte[] configuration) throws RemoteException {
        try {
            return this.configurablePublisher
                    .getLatestMessage(Optional.ofNullable(configuration))
                    .orElse(null);
        } catch (ConfigurablePublisherException e) {
            Log.e(TAG, "Error in getting latest message " + e.getMessage());
            throw new RemoteException(
                    "An error occurred while getting the latest message: " + e.getMessage());
        }
    }

    @Override
    public String getInterfaceHash() {
        return IConfigurablePublisher.HASH;
    }

    @Override
    public int getInterfaceVersion() {
        return IConfigurablePublisher.VERSION;
    }
}
