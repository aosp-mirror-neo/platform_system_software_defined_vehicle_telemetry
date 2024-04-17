#!/bin/bash

# Set a descriptive title
echo "----------------------"
echo "SDV Telemetry Test Suite"
echo "----------------------"
echo

# Build latest code
echo "Building code"
m

# Stop CVD
echo "Stopping all VM instances"
cvd stop

# Clearing all VM instances
echo "Resetting VM instances..."
cvd reset -y

# Restart CVD for changes to take effect
echo "Starting VM instance..."
cvd start

# Run tests, capture output and exit codes
echo "----------------------"
echo "Running libsdv_telemetry_unit_tests"
echo "----------------------"
atest libsdv_telemetry_unit_tests

echo "----------------------"
echo "Running libsdv_telemetry_configurable_publisher_unit_tests"
echo "----------------------"
atest libsdv_telemetry_configurable_publisher_unit_tests

echo "----------------------"
echo "Running sdv_telemetry_integration_tests"
echo "----------------------"
atest sdv_telemetry_integration_tests
