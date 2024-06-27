// Copyright 2024 Google LLC

package google.sdv.telemetry.java.JavaSampleIviPublisher;

import google.sdv.telemetry.configurable_publisher.*;
import android.os.Bundle;
import android.util.Log;
import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.os.Bundle;
import android.content.ServiceConnection;
import android.content.Context;
import android.content.ComponentName;
import android.os.RemoteException;

import com.google.sdv.telemetry.samples.ErrorReportPublisher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HexFormat;
import java.util.Arrays;

/**
 * JavaSampleIviPublisher
 */
public class JavaSampleIviPublisher extends Service {

    private final String TAG = JavaSampleIviPublisher.class.getSimpleName();
    private static final String SERVICE_NAME = "JavaSampleIviPublisher";
    private static final String PUBLISHER_NAME = "java_sample_ivi_publisher";
    private static final String CONF_PUB_REGISTRY_URI =
        "google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry/default";
    private IPublisherImpl publisherInterfaceImpl;

    private IConfigurablePublisherRegistry mRegistry;
    private Thread publisherThread;

    public void onCreate() {
        this.publisherInterfaceImpl = new IPublisherImpl();
        this.publisherThread = this.publisherInterfaceImpl.publishData();
        super.onCreate();
        Log.i(SERVICE_NAME, "on Create of service");

        System.loadLibrary("binder_accessor");

        IBinder regService = BinderServiceAccessor
                .getService(CONF_PUB_REGISTRY_URI);
        mRegistry = IConfigurablePublisherRegistry.Stub.asInterface(regService);

        if (mRegistry != null) {
            Log.i(SERVICE_NAME, "Connecting to Publisher Registry in onCreate");
            PublisherInfo pubInfo = createSamplePublisherInfo();
            try {
                mRegistry.registerPublisher(pubInfo, publisherInterfaceImpl);
            } catch (RemoteException e) {
                Log.i(SERVICE_NAME, "Error in registering publisher");
            }
        }

        Log.i(SERVICE_NAME, "Successfully Connected to Publisher Registry");

    }

    public IBinder onBind(final Intent intent) {
        Log.d(SERVICE_NAME, "onBind: " + intent);
        return publisherInterfaceImpl.asBinder();
    }

    public void onDestroy() {
        super.onDestroy();
        try {
            this.mRegistry.unregisterPublisher(PUBLISHER_NAME);
            Log.i(SERVICE_NAME, "Unregistered publisher: " + PUBLISHER_NAME);
        } catch (RemoteException e) {
            Log.e(SERVICE_NAME, "Failed to unregister publisher: " + PUBLISHER_NAME);
        }
        this.publisherThread.interrupt();
    }

    private PublisherInfo createSamplePublisherInfo() {
        PublisherInfo pubInfo = new PublisherInfo();
        pubInfo.serviceName = PUBLISHER_NAME;

        try {
            InputStream input = getApplicationContext().getAssets()
                    .open("error_pub_proto_descriptor.bin");
            byte[] fileContent = input.readAllBytes();
            pubInfo.fileDescriptorProto = fileContent;
            Log.i(SERVICE_NAME, "Read descriptors.pb: "
                    + Arrays.toString(pubInfo.fileDescriptorProto));
        } catch (IOException e) {
            Log.i(SERVICE_NAME, "Failed to read error_pub_proto_descriptor.pb");
        }
        pubInfo.messageTypeName = ".com.google.sdv.telemetry.samples.ErrorReportData";
        pubInfo.supportsGet = true;
        return pubInfo;
    }
}