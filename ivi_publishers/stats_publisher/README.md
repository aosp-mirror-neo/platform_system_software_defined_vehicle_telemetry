<!-- Copyright 2025 Google LLC -->


# Objective and Description

This application is a proof of concept that details pulling data from
`Statsd` in the Android System and aggregating data for the telemetry service.
It reuses the same atoms from `statsd`, but enables features like on-device
filtering to enable communication with the Configurable Publisher Registry.

## How to Run

This module is not built in any lunch target by default. In order to build,
add the following line to the `SDV_IVI_TELEMETRY_SAMPLES_PACKAGES` variable in
`/device/google/sdv/sdv_ivi_base/sdv_samples_ivi_telemetry.mk`:
```mk
telemetry_ivi_stats_publisher # line 16, also add a backslash (\) to the end of line 15.
```

Do not push this update to the file. It is meant to be for Proof-of-concept
testing.

Then, lunch and make:

```bash
# Startup IVI SDV target
lunch sdv_ivi_cf-trunk_staging-userdebug
m
```

## Instructions to run this proof of concept

Run the following commands to see the output of the service (note that for now,
you will need to inspect the logs to see the output):

```bash
cvd start

# Change to root on device
adb root

# Start publisher
adb shell am startservice -n google.sdv.telemetry.java.StatsPublisher/.StatsPublisher
```