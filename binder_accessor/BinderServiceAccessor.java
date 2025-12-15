// Copyright 2024 Google LLC

package google.sdv.telemetry.java.Binder;

import android.os.IBinder;

/**
 * JavaBinderServiceAccessor
 */
public class BinderServiceAccessor {
    public static native IBinder getService(String name);
}
