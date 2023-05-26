// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry;

/**
 * Binder interface that enables sending telemetry reports
 * from telemetry service to client.
 *
 * @hide
 */
oneway interface ITelemetryReportListener {
    void onResult(in String metricsConfigName, in ParcelFileDescriptor reportFileDescriptor,
            in byte[] telemetryError, in int status);
}
