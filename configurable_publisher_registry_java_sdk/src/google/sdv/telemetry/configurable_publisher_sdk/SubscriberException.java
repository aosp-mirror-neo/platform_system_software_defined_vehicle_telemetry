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

public class SubscriberException extends Exception {
    private SubscriberException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Factory method for creating a DeserializationFailed error.
     *
     * @param cause The underlying binder status error.
     * @return A new SubscriberException.
     */
    static SubscriberException deserializationFailed(ServiceSpecificException cause) {
        return new SubscriberException(
                "Unable to deserialize the data sent to the Subscriber.", cause);
    }

    /**
     * Factory method for creating an UnknownError.
     *
     * @param cause The underlying binder status error.
     * @return A new SubscriberException.
     */
    static SubscriberException unknownError(ServiceSpecificException cause) {
        return new SubscriberException("An unknown error occurred while sending data.", cause);
    }

    /**
     * Factory method for creating an UnknownError.
     *
     * @param cause The underlying binder status error.
     * @return A new RemoteException.
     */
    static SubscriberException unknownError(RemoteException cause) {
        return new SubscriberException("An unknown error occurred while sending data", cause);
    }
}
