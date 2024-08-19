<!-- Copyright 2024 Google LLC -->

# Sample Vendor Client Application

This directory contains vendor client application which communicates with the
telemetry service using ABI stable C APIs.

## Telemetry Service

The telemetry service is written using rust and uses tokio runtime to handle
asynchronous API calls. It implements the aidl-generated trait defined in
`ITelemetryService.aidl`. The service is registered as an async binder service
using the `sdv_telemetry_service_agent` executable.

Run the client application with the paths to the serialized or textproto configs
passed as a command line argument.

## How to Use Sample Telemetry Client Application

### In terminal window 1

#### Lunch Target

For Core:

```bash
lunch sdv_core_cf-trunk_staging-userdebug
```

For IVI:

```bash
lunch sdv_ivi_cf-trunk_staging-userdebug
```

### Start Guest

```bash
cvd start
adb root
```

If on IVI, explicitely start telemetry service agent:

```bash
adb root
adb shell sdv_ivi_telemetry_service_agent
```

Push **each** metrics configuration to the vm either in binary format or as textproto following
legacy proto2 format.

```bash
adb push [config-path-on-host] data/local/tmp/[config-file-name]
```

Start logcat

```bash
adb logcat | grep -i 'telemetry'
```

### In terminal window 2

Start sample telemetry client application with the paths to each config **on the vm**
(e.g. `data/local/tmp/metrics_config.textproto`)

```bash
adb shell TelemetryRustCClient <path_to_config> ... <path_to_config>
```

For each passed metrics config you should see logs about the success/failure of adding and
activating the config similar to the following logs.

```bash
SdvTelemetryService: sdv_telemetry: addMetricsConfig: MetricsConfig { ... }

SdvTelemetryService: sdv_telemetry: activateMetricsConfig: ...
```

## How to use sample publishers

In a seperate terminal window, run the desired publisher after starting the vm and before start the
client application. Terminate the publisher using `CTRL + C` once the application is done.

```bash
adb root
adb shell telemetry_sample_publisher
```

The following sample publishers binaries exist.

Data tunnel publishers:

`dt_telemetry_sample_publisher` [proto](rust/sample_publisher/sample_publisher.proto)

`telemetry_sample_data_tunnel_publisher_speed` [proto](rust/data_tunnel_publisher_speed/speed_publisher.proto)

`telemetry_sample_data_tunnel_publisher_tire_pressure` [proto](rust/data_tunnel_publisher_tire_pressure/tire_pressure_publisher.proto)

`telemetry_sample_data_tunnel_publisher_error_report` [proto](rust/data_tunnel_publisher_error_report/error_report_publisher.proto)

Configurable publisher:

`telemetry_sample_publisher` [proto](rust/sample_publisher/sample_publisher.proto)

Publisher supporting getter-type subscription:

`telemetry_sample_getter_publisher` [proto](rust/sample_publisher/sample_publisher.proto)
