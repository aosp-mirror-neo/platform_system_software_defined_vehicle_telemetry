// Copyright 2025 Google LLC
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
