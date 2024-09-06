#!/bin/bash
# Copyright 2024 Google LLC

set -e -o pipefail

log() {
  echo "----------------------"
  echo "$1"
  echo "----------------------"
  echo
}

log "SDV Telemetry Test Suite"

if [[ -z "$ANDROID_BUILD_TOP" ]]; then
  log "[ERROR] ANDROID_BUILD_TOP is missing. Please, lunch the Android target."
  exit -1
fi

source "$ANDROID_BUILD_TOP/build/envsetup.sh"

TEST_TARGETS='libsdv_telemetry_* sdv_telemetry_*'

log "Building code..."

# Running `m` is needed to build essential tools like `cvd`.
m
# Build all tests, but do not run them yet. We do this before restarting the VM
# so that we error earlier and do not need to wait for the VM to restart.
atest --build $TEST_TARGETS
# In addition to building the tests, also build all other Telemetry targets.
# This is because some of our targets do not have tests (e.g., the samples), but
# we want to make sure that they at least build successfully.
(cd $ANDROID_BUILD_TOP && mmm "system/software_defined_vehicle/telemetry")

log "Stopping all VM instances..."
cvd stop

log "Resetting VM instances..."
cvd reset -y

# Restart CVD for changes to take effect
log "Starting VM instance..."
cvd create

echo "Waiting for VM to come online."
until adb shell echo 'VM running' &>/dev/null; do
  echo "."
  sleep 1
done

log "Running telemetry tests..."
atest $TEST_TARGETS

exit 0
