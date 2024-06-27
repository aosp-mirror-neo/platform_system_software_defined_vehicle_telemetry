// Copyright 2024 Google LLC

package google.sdv.telemetry.java.JavaSampleIviPublisher;

import android.os.IBinder;

/**
 * JavaBinderServiceAccessor
 */
public class BinderServiceAccessor {
    public static native IBinder getService(String name);
}