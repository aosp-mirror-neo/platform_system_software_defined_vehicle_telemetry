<!-- Copyright 2024 Google LLC -->

# Publisher using SDV Middleware

## Code Generation

```shell
mm vsidlc && vsidlc --catalog-path ./catalog/ --output-path . --genrule
```

## Running

Publisher:

```shell
adb shell telemetry_sample_middleware_publisher
```

Logs:

```shell
adb logcat '*:S' TelemetryMddlWrPub:V
```
