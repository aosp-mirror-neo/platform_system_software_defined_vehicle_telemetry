<!-- Copyright 2024 Google LLC -->

# Data Tunnel Publisher using SDV Middleware

## Code Generation

```shell
mm vsidlc && vsidlc --catalog-path ./catalog/ --output-path .
```

## Running

Publisher:

```shell
adb shell telemetry_sample_data_tunnel_publisher_middleware
```

Logs:

```shell
adb logcat '*:S' TelemetryMddlWrDataPub:V
```
