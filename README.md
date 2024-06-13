# Telemetry Services

This repository contains the telemetry specific services for the software
defined vehicle.

See [go/sdv-telemetry](http://go/sdv-telemetry) for general information about
SDV.

## Build Instructions

Configure the sdv core build variant:

```bash
source build/envsetup.sh
lunch sdv_core_cf-trunk_staging-userdebug
```

Run the following command:

```bash
m -j
```

## Test instructions

Check the readme inside samples folder for instructions on how to experiment
with the sample client applications.

Use the `tests/run_all_tests.sh` script to run all unit and integration tests:

```bash
bash tests/run_all_tests.sh
```

If you want to run specific tests only, use the instructions below.

### Unit Test Instructions

Starting with [ag/25844146](http://ag/25844146) unit tests are converted to
device side tests and instructions have changed. Run `cvd start` in a different
terminal:

```bash
cvd start
```

Run the tests:

```bash
atest libsdv_telemetry_unit_tests
```

### Integration Test Instructions

Start a Cuttlefish Virtual Device:

```bash
cvd start
```

Run the tests:

```bash
atest sdv_telemetry_integration_tests
```
