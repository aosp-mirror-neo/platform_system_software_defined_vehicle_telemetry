#!/bin/bash
# Copyright 2024 Google LLC

set -eu -o pipefail

if [[ -z "${ANDROID_BUILD_TOP+x}" ]]; then
    echo "[ERROR] ANDROID_BUILD_TOP is missing. Please, lunch the Android target."
    exit 1
fi

printf 'Metrics configs dir [or ENTER to use simulator/samples/metrics_configs]\n>'
read dir
if [[ $dir = "" ]]; then
    if [ -e simulator/samples/ ]; then
        METRICS_DIR=simulator/samples/metrics_configs
    else
        echo "Please run the script from telemetry folder to use simulator/samples/metrics_configs"
        exit 1
    fi
else
    if [ -e "$dir" ]; then
        METRICS_DIR=$dir
    else
        echo "$dir does not seem to exist, aborting"
        exit 1
    fi
fi

if [ -z "$(ls -A $METRICS_DIR)" ]; then
    echo "$METRICS_DIR does not contain any files, aborting"
    exit 1
fi

printf '\nPublisher configs and data dir [or ENTER to use simulator/samples/publisher_configs]\n>'
read dir
if [[ $dir = "" ]]; then
    if [ -e simulator/samples/ ]; then
        PUBLISHERS_DIR=simulator/samples/publisher_configs
    else
        echo "Please run the script from telemetry folder to use simulator/samples/publisher_configs"
        exit 1
    fi
else
    if [ -e "$dir" ]; then
        PUBLISHERS_DIR=$dir
    else
        echo "$dir does not seem to exist, aborting"
        exit 1
    fi
fi

if [ -z "$(ls -A $PUBLISHERS_DIR)" ]; then
    echo "$PUBLISHERS_DIR does not contain any files, aborting"
    exit 1
fi

cvd clear &>/dev/null
cvd start &>/dev/null &

printf 'Print simulator logs? (Y/n)?'
read answer

run_logcat=false
if [ "$answer" == "" ] || [ "$answer" != "${answer#[Yy]}" ]; then
    run_logcat=true
fi

printf "\nWaiting for VM to come online.\n"

sp='/-\|'
printf ' '

until adb shell echo 'VM running' &>/dev/null; do
    printf '\b%.1s' "$sp"
    sp=${sp#?}${sp%???}
    sleep .5
done
adb root &>/dev/null

if [ "$TARGET_PRODUCT" = "sdv_ivi_cf" ]; then
    adb shell sdv_ivi_telemetry_service_agent &>/dev/null &
    trap "pkill -f sdv_ivi_telemetry_service_agent" 0 2 3 15
fi

# Push metrics configs
METRICS_CONFIGS=""
for METRICS_CONFIG in "$METRICS_DIR"/*.textproto; do
    FILE_NAME=${METRICS_CONFIG##*/}
    adb push "$METRICS_CONFIG" "/data/local/tmp/$FILE_NAME" &>/dev/null
    METRICS_CONFIGS+="/data/local/tmp/$FILE_NAME "
done

# Push publisher configs and data
PUBLISHER_CONFIGS=""
for PUBLISHER_CONFIG in "$PUBLISHERS_DIR"/*; do
    FILE_NAME=${PUBLISHER_CONFIG##*/}
    adb push "$PUBLISHER_CONFIG" "/data/local/tmp/$FILE_NAME" &>/dev/null
    if [[ $FILE_NAME == *.textproto ]]; then
        PUBLISHER_CONFIGS+="/data/local/tmp/$FILE_NAME "
    fi
done

# Run simulator
printf "\nStarting simulator\n"
adb shell sdv_telemetry_simulator \
    --metrics-configs "$METRICS_CONFIGS" \
    --publisher-configs "$PUBLISHER_CONFIGS" \
    --no-binary-reports \
    --max-report-count 10 \
    --max-simulation-time minutes:2 &
SIMULATOR_PID=$!
trap "kill -9 $SIMULATOR_PID" 2 # Kills the simulator process when one of these signals is received: 2=interrupt

if $run_logcat; then
    adb logcat -v raw -v color -s SdvTelemetrySimulator &
    LOGCAT_PID=$!
else
    echo "Waiting for simulator to finish"
fi

wait $SIMULATOR_PID

if $run_logcat; then
    kill -9 "$LOGCAT_PID"
    wait "$LOGCAT_PID" 2>/dev/null || true
fi

echo "Simulator finished"

# Pull simulator output from vm
adb pull /data/local/tmp/telemetry_simulator_out/ &>/dev/null
echo "Simulator script finished, any output files pulled from the vm are in telemetry_simulator_out"
