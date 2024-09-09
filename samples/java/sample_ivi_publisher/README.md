<!-- Copyright 2024 Google LLC -->

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
adb shell am startservice -n google.sdv.telemetry.java.JavaSampleIviPublisher/.JavaSampleIviPublisher

# Push Configuration and run sample client
adb push samples/sample_protos/error_publisher.textproto /tmp/
adb shell telemetry_rust_c_client --write-reports-to=/tmp/ /tmp/error_publisher.textproto
```

## Running Sample on product partition
If you want to run the sample that runs on the product partition, run instead:

```bash
adb shell am startservice -n google.sdv.telemetry.java.JavaSampleIviPublisherProd/.JavaSampleIviPublisherProd
```

Note: You cannot run both services at the same time. To stop the services run the following commands:

```bash
# Stop Sample publisher on vendor partition
adb shell am stopservice -n google.sdv.telemetry.java.JavaSampleIviPublisher/.JavaSampleIviPublisher

# Stop Sample publisher on product partition
adb shell am stopservice -n google.sdv.telemetry.java.JavaSampleIviPublisherProd/.JavaSampleIviPublisherProd
```

## Helpful Logging

To analyze logs to evaluate adding, activation and report generation from metrics configuration, you can run in a separate terminal:

```bash
adb logcat | grep Telemetry
```

To see logs from the sample publisher, use:

```bash
adb logcat | grep JavaSampleIviPublisher
```
