<!-- Copyright 2025 Google LLC -->

# Instructions to run this example

```bash
# Startup IVI SDV target
lunch sdv_ivi_cf-trunk_staging-userdebug
cvd start

# Change to root on device
adb root

# Start Telemtry Service on IVI
adb shell sdv_ivi_telemetry_service_agent
# Start Service that serves as Sample Java Publisher on IVI
adb shell am startservice -n google.sdv.telemetry.java.JavaSdkSampleIviPublisher/.JavaSdkSampleIviPublisher

# Push Configuration and run sample client
adb push samples/sample_protos/error_publisher_java_sdk.textproto /tmp/
adb shell telemetry_rust_c_client --write-reports-to=/tmp/ /tmp/error_publisher_java_sdk.textproto
```

## Helpful Logging

To analyze logs to evaluate adding, activation and report generation from metrics configuration, you can run in a separate terminal:

```bash
adb logcat | grep Telemetry
```

To see logs from the sample publisher, use:

```bash
adb logcat | grep JavaSdkSampleIviPublisher
```
