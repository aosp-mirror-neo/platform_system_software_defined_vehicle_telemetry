#!/bin/bash
# Copyright 2024 Google LLC

set -e -o pipefail

if ! command -v jq &> /dev/null; then
  echo "[ERROR] The jq command is not installed. Please install is as follows:"
  echo ""
  echo "sudo apt install -y jq"
  exit 1
fi

log() {
  echo "----------------------"
  echo "$1"
  echo "----------------------"
  echo
}

log "SDV Telemetry Test Suite"

if [[ -z "$ANDROID_BUILD_TOP" ]]; then
  log "[ERROR] ANDROID_BUILD_TOP is missing. Please, lunch the Android target."
  exit 1
fi

# shellcheck source=/dev/null
source "$ANDROID_BUILD_TOP/build/envsetup.sh"

declare -a TEST_TARGETS
TEST_TARGETS=('sdv_telemetry_*' 'sdv_mt_telemetry_*')

log "Building code..."
# Running `m` is needed to build essential tools like `cvd`.
m
# Build all tests, but do not run them yet. We do this before restarting the VM
# so that we error earlier and do not need to wait for the VM to restart.
atest --build "${TEST_TARGETS[@]}"
# In addition to building the tests, also build all other Telemetry targets.
# This is because some of our targets do not have tests (e.g., the samples), but
# we want to make sure that they at least build successfully.
(cd "${ANDROID_BUILD_TOP}" && mmm "system/software_defined_vehicle/telemetry")

# Restart CVD for changes to take effect
log "Resetting VM instances..."
cvd reset -y
cvd clear

log "Starting VM instance..."
cvd create

log "Waiting for VM to come online..."
adb wait-for-device

log "Running tests..."
ADB_SERIAL=$(cvd fleet | jq --raw-output ".groups[0].instances[0].adb_serial")
NOTIFY_AS_NATIVE="${ADB_SERIAL}" atest "${TEST_TARGETS[@]}"

exit 0
