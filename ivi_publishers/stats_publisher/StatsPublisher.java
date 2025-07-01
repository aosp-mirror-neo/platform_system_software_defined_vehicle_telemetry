// Copyright 2025 Google LLC

package google.sdv.telemetry.java.StatsPublisher;

import android.app.Service;
import android.content.Intent;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.util.Log;
import android.content.Context;

public class StatsPublisher extends Service {

    private final String TAG = StatsPublisher.class.getSimpleName();
    private static final String SERVICE_NAME = "StatsPublisher";
    // TODO: Change the interval b\424824652
    private static final long PULL_INTERVAL_MS = 10000; // 10 seconds

    private Handler handler;
    private Runnable pullReportsRunnable;

    @Override
    public void onCreate() {
        super.onCreate();
        Log.i(SERVICE_NAME, "onCreate of service");

        handler = new Handler(Looper.getMainLooper());
        pullReportsRunnable = new Runnable() {
            @Override
            public void run() {
                pullReports();
                // Run it once every PULL INTERVAL seconds
                handler.postDelayed(this, PULL_INTERVAL_MS);
            }
        };
    }

    @Override
    public IBinder onBind(final Intent intent) {
        Log.d(SERVICE_NAME, "onBind: " + intent);

        // TODO: bind service to telemetry service discovery b\424825148
        return null;
    }

    private void pullReports() {
      Log.i(SERVICE_NAME, "Pulling reports...");

      // TODO: access statsmanager and pull reports. b\424625099
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        // Stop pulling reports  whenever service is destroyed
        if (handler != null && pullReportsRunnable != null) {
            handler.removeCallbacks(pullReportsRunnable);
        }
        Log.i(SERVICE_NAME, "onDestroy called and report pulling stopped.");
    }
}
