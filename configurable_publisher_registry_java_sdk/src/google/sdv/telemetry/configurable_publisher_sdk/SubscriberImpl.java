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
import android.os.ServiceSpecificException;

import google.sdv.telemetry.configurable_publisher.ConfigurablePublisherStatusCodes;
import google.sdv.telemetry.configurable_publisher.ISubscriber;

/** A wrapper for the AIDL ISubscriber interface that is passed to the configurable publisher. */
class SubscriberImpl implements Subscriber {
    private final ISubscriber subscriberAidl;

    /**
     * Constructs a new SubscriberImpl.
     *
     * @param subscriberAidl The remote AIDL subscriber object.
     */
    SubscriberImpl(ISubscriber subscriberAidl) {
        this.subscriberAidl = subscriberAidl;
    }

    @Override
    public void sendData(byte[] data) throws SubscriberException {
        try {
            this.subscriberAidl.sendData(data);
        } catch (ServiceSpecificException e) {
            if (e.errorCode == ConfigurablePublisherStatusCodes.SEND_DATA_DESERIALIZATION_FAILED) {
                throw SubscriberException.deserializationFailed(e);
            } else {
                throw SubscriberException.unknownError(e);
            }
        } catch (RemoteException e) {
            throw SubscriberException.unknownError(e);
        }
    }
}
