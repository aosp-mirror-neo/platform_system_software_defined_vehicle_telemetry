// Copyright 2025 Google LLC

package google.sdv.telemetry.java.statspublisher;

import android.app.Service;
import android.content.Intent;
import android.os.Build;
import android.os.Bundle;
import android.os.IBinder;
import android.util.Log;
import android.content.Context;
import android.os.RemoteException;
import android.os.ServiceManager;
import android.app.StatsManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry;
import google.sdv.telemetry.configurable_publisher.PublisherInfo;

/**
 * Service that publishes data from Statsd to the Configurable Publisher Registry.
 * StatsPublisherManager connects to the Configurable Publisher Registry and registers itself as a
 * publisher.
 *
 * <p>Subscriptions are handled by {@link StatsConfigurationManager}
 */
public class StatsPublisherManager extends Service {

    private final String TAG = StatsPublisherManager.class.getSimpleName();

    private static final long PULL_INTERVAL_MS = 10000; // 10 seconds
    private static final long CONFIG_KEY = 1939293; // Arbitrary Config Key

    private static final String PUBLISHER_NAME = "stats_publisher";
    private static final String CONF_PUB_REGISTRY_URI =
            "google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry/default";
    private static final String DESCRIPTOR_FILENAME = "descriptor.pb";
    private static final String DESCRIPTOR_MESSAGE_NAME =
            ".google.sdv.telemetry.java.statspublisher.AtomReportWrapper";

    private IConfigurablePublisherRegistry mRegistry;
    private StatsConfigurationManager mConfigurationManager;
    private StatsManager mStatsManager;

    @Override
    public void onCreate() {
        super.onCreate();

        mStatsManager = getSystemService(StatsManager.class);
        mConfigurationManager = new StatsConfigurationManager(mStatsManager);

        Log.i(TAG, "onCreate of service");

        // Get binder reference to the registry
        IBinder regService = ServiceManager.waitForService(CONF_PUB_REGISTRY_URI);
        mRegistry = IConfigurablePublisherRegistry.Stub.asInterface(regService);

        if (mRegistry != null) {
            Log.i(TAG, "Connecting to Publisher Registry in onCreate");
            PublisherInfo pubInfo = createPublisherInfo();

            if (pubInfo == null) {
                Log.i(TAG, "No descriptor found, exiting service");

                // Exit the service if there is no descriptor
                super.stopSelf();
                return;
            }

            try {
                // Make the publisher visible to the registry
                mRegistry.registerPublisher(pubInfo, mConfigurationManager);
            } catch (RemoteException e) {
                Log.i(TAG, "Error in registering publisher");
            }
        } else {
            Log.i(TAG, "Failed to connect to Publisher Registry in onCreate");
            super.stopSelf();
            return;
        }

        Log.i(TAG, "Successfully Connected to Publisher Registry");
    }

    @Override
    public IBinder onBind(final Intent intent) {
        Log.d(TAG, "onBind: " + intent);
        return mConfigurationManager.asBinder();
    }

    @Override
    public void onDestroy() {
        super.onDestroy();

        Log.i(TAG, "onDestroy called.");
    }

    private PublisherInfo createPublisherInfo() {
        PublisherInfo pubInfo = new PublisherInfo();
        pubInfo.serviceName = PUBLISHER_NAME;
        pubInfo.fileDescriptorProto = null;

        Context context = getApplicationContext();

        try (InputStream inputStream = context.getAssets().open(DESCRIPTOR_FILENAME)) {
            byte[] fileContent = inputStream.readAllBytes();
            pubInfo.fileDescriptorProto = fileContent;
            Log.i(TAG, "Successfully read " + DESCRIPTOR_FILENAME);
        } catch (IOException e) {
            Log.e(TAG, "Failed to read " + DESCRIPTOR_FILENAME + ": " + e.getMessage(), e);
            // Return null if there is no descriptor, gracefully exit the service
            return null;
        }

        pubInfo.messageTypeName = DESCRIPTOR_MESSAGE_NAME;
        pubInfo.supportsGet = false;
        return pubInfo;
    }
}
