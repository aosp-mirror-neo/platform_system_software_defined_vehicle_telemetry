// Copyright 2025 Google LLC

package google.sdv.telemetry.java.statspublisher;

import android.app.Service;
import android.content.Intent;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.util.Log;
import android.content.Context;
import android.app.StatsManager;
import android.app.StatsManager.StatsUnavailableException;

import google.sdv.telemetry.java.statspublisher.AtomsProto.Atom;
import google.sdv.telemetry.java.statspublisher.AtomsProto.AppCrashOccurred;
import google.sdv.telemetry.java.statspublisher.StatsdConfigProto.StatsdConfig;
import google.sdv.telemetry.java.statspublisher.StatsdConfigProto;
import google.sdv.telemetry.java.statspublisher.StatsLogProto;


import static google.sdv.telemetry.java.statspublisher.AtomsProto.Atom.APP_START_OCCURRED_FIELD_NUMBER;


public class StatsPublisher extends Service {

    private final String TAG = StatsPublisher.class.getSimpleName();
  // TODO: Change the interval b/424824652
    private static final long PULL_INTERVAL_MS = 10000; // 10 seconds
    private static final long CONFIG_KEY = 1939293; // Arbitrary Config Key


    // For matching configurations
    // TODO: use metric and matcher IDs for matching atoms b/425956575
    static final long APP_START_OCCURRED_ATOM_MATCHER_ID = 9;
    static final long APP_START_OCCURRED_EVENT_METRIC_ID = 10;


    private Handler handler;
    private Runnable pullReportsRunnable;

    private StatsManager mStatsManager;

    @Override
    public void onCreate() {
        super.onCreate();
        Log.i(TAG, "onCreate of service");

        handler = new Handler(Looper.getMainLooper());
        pullReportsRunnable = new Runnable() {
            @Override
            public void run() {
                pullReports();
                // Run it once every PULL INTERVAL seconds
                handler.postDelayed(this, PULL_INTERVAL_MS);
            }
        };

        // Start pulling reports periodically
        handler.postDelayed(pullReportsRunnable, PULL_INTERVAL_MS);

        Log.i(TAG, "Successfully scheduled report pulling.");


        // Get a reference to StatsManager.
        mStatsManager = getSystemService(StatsManager.class);

        // Initialize StatsManager with the Metrics Configurations
        // we want to use
        addMetricsConfigs();
    }

    @Override
    public IBinder onBind(final Intent intent) {
        Log.d(TAG, "onBind: " + intent);

        // TODO: bind service to telemetry service discovery b/424825148
        return null;
    }


    private void addMetricsConfigs() {
        Log.i(TAG, "Subscribing to StatsManager metrics configuration.");

        // Set up a configuration to allow adding metrics configs to statsd
        StatsdConfig.Builder builder = StatsdConfig.newBuilder()
                .setId(CONFIG_KEY).addAllowedLogSource("AID_SYSTEM");

        // Create a metrics configuration for the app started atom.
        StatsdConfig config = builder
                .addAtomMatcher(StatsdConfigProto.AtomMatcher.newBuilder()
                        // The id must be unique within StatsdConfig/matchers
                        .setId(APP_START_OCCURRED_ATOM_MATCHER_ID)
                        .setSimpleAtomMatcher(StatsdConfigProto.SimpleAtomMatcher.newBuilder()
                                .setAtomId(APP_START_OCCURRED_FIELD_NUMBER)))
                .addEventMetric(StatsdConfigProto.EventMetric.newBuilder()
                        // The id must be unique within StatsdConfig/metrics
                        .setId(APP_START_OCCURRED_EVENT_METRIC_ID)
                        .setWhat(APP_START_OCCURRED_ATOM_MATCHER_ID))
                .build();

        Log.i(TAG, "Created metrics configuration, adding to StatsManager.");

        try {
            mStatsManager.addConfig(CONFIG_KEY, config.toByteArray());
            Log.i(TAG, "Successfully added metrics config to statsManager");
        } catch (StatsUnavailableException e) {
            Log.i(TAG, "Error occurred" + e);
        }
    }



    private void pullReports() {
      Log.i(TAG, "Pulling reports...");

      // TODO: complete implementation of accessing statsmanager
      // and pulling reports. b/424625099
      try {
        StatsLogProto.ConfigMetricsReportList reports = StatsLogProto.
            ConfigMetricsReportList.parseFrom(mStatsManager.getReports(CONFIG_KEY));
      } catch (Exception e) {
        Log.i(TAG, "Error occurred while pulling reports: " + e);
      }

    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        // Stop pulling reports  whenever service is destroyed
        if (handler != null && pullReportsRunnable != null) {
            handler.removeCallbacks(pullReportsRunnable);
        }
        Log.i(TAG, "onDestroy called and report pulling stopped.");
    }
}
