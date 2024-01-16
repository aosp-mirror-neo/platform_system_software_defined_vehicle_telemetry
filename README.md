# Telemetry Services

This repository contains the telemetry specific services for the software defined vehicle.

See [go/sdv-telemetry](https://go/sdv-telemetry) for general information about SDV.

## Build Instructions

Run the following commands

`$ m -j`

## Test instructions

Check the readme inside samples folder for instructions on how to experiment with the sample client applications.

## Unit Test Instructions

Starting with ag/25844146 unit tests are converted to device side tests and instructions have changed.
Run launch_cvd in a different terminal.
`$ launch_cvd`

run the tests
`$ atest libsdv_telemetry_unit_tests`
