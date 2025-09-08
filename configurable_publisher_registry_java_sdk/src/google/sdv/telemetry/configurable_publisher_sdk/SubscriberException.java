// Copyright 2025 Google LLC
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
    return new SubscriberException("Unable to deserialize the data sent to the Subscriber.", cause);
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
