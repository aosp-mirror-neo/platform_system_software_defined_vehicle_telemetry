#!/bin/bash
set -e -o pipefail

log() {
    echo "----------------------"
    echo "$1"
    echo "----------------------"
    echo
}

if [[ -z "$ANDROID_BUILD_TOP" ]]
then
  log "[ERROR] ANDROID_BUILD_TOP is missing. Please, lunch the Android target."
  exit -1
fi

source "$ANDROID_BUILD_TOP/build/envsetup.sh"

log "SDV Telemetry Test Suite"

log "Building code..."
m

log "Stopping all VM instances..."
cvd stop

log "Resetting VM instances..."
cvd reset -y

# Restart CVD for changes to take effect
log "Starting VM instance..."
cvd start

log "Running telemetry tests..."
atest 'libsdv_telemetry_*' 'sdv_telemetry_*'

exit 0
