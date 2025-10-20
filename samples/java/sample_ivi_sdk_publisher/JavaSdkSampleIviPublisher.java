// Copyright 2025 Google LLC

package google.sdv.telemetry.java.JavaSdkSampleIviPublisher;

import android.app.Service;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Bundle;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;
import com.google.android.sdv.telemetry.schemas.ErrorReportPublisher;
import google.sdv.telemetry.configurable_publisher_sdk.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * JavaSdkSampleIviPublisher
 */
public class JavaSdkSampleIviPublisher extends Service {
  private final String TAG = JavaSdkSampleIviPublisher.class.getSimpleName();
  private static final String SERVICE_NAME = "JavaSdkSampleIviPublisher";
  private static final String PUBLISHER_NAME = "java_sdk_sample_ivi_publisher";

  private PublisherImpl publisherInterfaceImpl;

  private ConfigurablePublisherRegistry mRegistry;
  private Thread publisherThread;

  public void onCreate() {
    this.publisherInterfaceImpl = new PublisherImpl();
    this.publisherThread = this.publisherInterfaceImpl.publishData();
    super.onCreate();
    Log.i(SERVICE_NAME, "on Create of service");

    try {
      mRegistry = ConfigurablePublisherRegistry.connect();
    } catch (ConfigurablePublisherRegistryException e) {
      Log.e(SERVICE_NAME, "Error in connecting to Publisher Registry");
    }

    PublisherInfoJava pubInfo = getPublisherInfo();
    if (pubInfo != null) {
      try {
        mRegistry.registerPublisher(pubInfo, publisherInterfaceImpl);
        Log.i(SERVICE_NAME, "Successfully Registered Publisher with Publisher Registry");
      } catch (ConfigurablePublisherRegistryException e) {
        Log.e(SERVICE_NAME, "Error in registering publisher: " + e.getMessage(), e);
      }
    } else {
      Log.e(SERVICE_NAME, "Could not create PublisherInfo, stopping service.");
      stopSelf();
    }
  }

  public void onDestroy() {
    super.onDestroy();
    try {
      this.mRegistry.unregisterPublisher(PUBLISHER_NAME);
      Log.i(SERVICE_NAME, "Unregistered publisher: " + PUBLISHER_NAME);
    } catch (ConfigurablePublisherRegistryException e) {
      Log.e(SERVICE_NAME, "Failed to unregister publisher: " + PUBLISHER_NAME, e);
    }
    this.publisherThread.interrupt();
  }

  /**
   * Returns the communication channel to the service.
   *
   * <p>With the new Configurable Publisher SDK, the publisher registers itself with the
   * registry service, so there is no need for the registry to bind to this service. Therefore,
   * this method can return null if no other clients are expected to bind to it.
   *
   * @param intent The Intent that was used to bind to this service.
   * @return Returns null as binding is not required for this service.
   */
  public IBinder onBind(final Intent intent) {
    Log.d(SERVICE_NAME, "onBind: " + intent);
    return null;
  }

  private PublisherInfoJava getPublisherInfo() {
    // 1. Get the descriptor byte array from our generated class.
    byte[] fileDescriptorProto = ErrorReportPublisher.DESCRIPTOR;

    // 2. Get the root message name from our generated class.
    String messageTypeName = ErrorReportPublisher.ROOT_MESSAGE_NAME;

    // 3. Create the PublisherInfoJava object.
    return new PublisherInfoJava(PUBLISHER_NAME, fileDescriptorProto, messageTypeName,
        true // Whether this publisher supports getLatestMessage().
    );
  }
}