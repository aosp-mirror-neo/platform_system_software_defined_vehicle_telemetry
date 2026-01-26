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
