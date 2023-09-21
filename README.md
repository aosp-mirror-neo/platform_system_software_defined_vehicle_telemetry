# Telemetry Services

This repository contains the telemetry specific services for the software defined vehicle.

See [go/sdv-telemetry](https://go/sdv-telemetry) for general information about SDV.

## Build Instructions

Run the following commands

```
$ UNSAFE_DISABLE_HIDDENAPI_FLAGS=true m -j

$ launch_cvd # start emulator

$ adb root

$ adb shell sdv_telemetry_service # start telemetry service
```

Run `telemetry_cpp_client` with the appropriate arguments.

Check logs with `adb logcat`

## Test Instructions

`UNSAFE_DISABLE_HIDDENAPI_FLAGS=true atest libsdv_telemetry_unit_tests`
