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

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;

/** Internal class that implements the IConfigurablePublisherSubscription aidl interface */
class ConfigurablePublisherSubscriptionAidlImpl extends IConfigurablePublisherSubscription.Stub {

    private final ConfigurablePublisherSubscription configurablePublisherSubscription;

    /**
     * Constructs the AIDL implementation wrapper.
     *
     * @param configurablePublisherSubscription The client facing interface representing an active
     *     subscription.
     */
    ConfigurablePublisherSubscriptionAidlImpl(
            ConfigurablePublisherSubscription configurablePublisherSubscription) {
        this.configurablePublisherSubscription = configurablePublisherSubscription;
    }

    @Override
    public void unsubscribe() throws RemoteException {
        try {
            // The call is delegated to the wrapped business logic object.
            this.configurablePublisherSubscription.unsubscribe();
        } catch (SubscriptionException e) {
            // If the underlying business logic throws an error, we catch it
            // and wrap it in a RemoteException to send it back to the client.
            throw new RemoteException(
                    "Failed to unsubscribe due to an internal error: " + e.getMessage());
        }
    }

    @Override
    public String getInterfaceHash() {
        return IConfigurablePublisherSubscription.HASH;
    }

    @Override
    public int getInterfaceVersion() {
        return IConfigurablePublisherSubscription.VERSION;
    }
}
