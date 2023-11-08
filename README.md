# Telemetry Services

This repository contains the telemetry specific services for the software defined vehicle.

See [go/sdv-telemetry](https://go/sdv-telemetry) for general information about SDV.

## Build Instructions

Run the following commands

`$ UNSAFE_DISABLE_HIDDENAPI_FLAGS=true m -j`

## Test instructions

Check the readme inside samples folder for instructions on how to experiment with the sample client applications.

## Unit Test Instructions

`UNSAFE_DISABLE_HIDDENAPI_FLAGS=true atest libsdv_telemetry_unit_tests`
